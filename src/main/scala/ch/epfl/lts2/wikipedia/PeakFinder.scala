package ch.epfl.lts2.wikipedia

import java.io.File
import java.nio.file.Paths
import java.sql.Timestamp
import java.time._
import java.time.format.DateTimeFormatter
import breeze.linalg._
import breeze.stats._
import com.typesafe.config.ConfigFactory
import org.apache.spark.SparkConf
import org.apache.spark.graphx._
import org.apache.spark.sql.{Dataset, SparkSession}
import org.neo4j.spark._

case class PageRowThreshold(page_id:Long, threshold: Double)
case class PageVisitThrGroup(page_id:Long, threshold:Double, visits:List[(Timestamp, Int)])
case class PageVisitGroup(page_id:Long, visits:List[(Timestamp, Int)])
case class PageVisitElapsedGroup(page_id:Long, visits:List[(Int,Double)])

class PeakFinder(dbHost:String, dbPort:Int, dbUsername:String, dbPassword:String,
                 keySpace: String, tableVisits:String, tableStats:String, tableMeta:String,
                 boltUrl:String, neo4jUser:String, neo4jPass:String) extends PageCountStatsLoader with Serializable {
  lazy val sparkConfig: SparkConf = new SparkConf().setAppName("Wikipedia activity detector")
        .set("spark.cassandra.connection.host", dbHost)
        .set("spark.cassandra.connection.port", dbPort.toString)
        .set("spark.cassandra.auth.username", dbUsername)
        .set("spark.cassandra.auth.password", dbPassword)
        .set("spark.neo4j.bolt.url", boltUrl)
        .set("spark.neo4j.bolt.user", neo4jUser)
        .set("spark.neo4j.bolt.password", neo4jPass)

  lazy val session: SparkSession = SparkSession.builder.config(sparkConfig).getOrCreate()
  
  /**
    * Computes similarity of two time-series
    * @param v1 Time-series of edge start
    * @param v2 Time-series of edge end
    * @param upperBoundActivationsNumber Lower limit of time-series values
    * @param isFiltered Specifies if filtering is required (divides values by the number of spikes)
    * @param lambda Similarity threshold (discard pairs having a lower similarity)
    * @return Similarity measure
    */
  def compareTimeSeries(v1:(String, Option[List[(Timestamp, Int)]]), v2:(String, Option[List[(Timestamp, Int)]]), 
                        upperBoundActivationsNumber: Int = 0, isFiltered: Boolean, lambda: Double = 0.5): Double = {
    if (v1._2.isEmpty || v2._2.isEmpty) 0.0 // throw ?
    val v1Visits = v1._2.get.filter(p => p._2 > upperBoundActivationsNumber)
    val v2Visits = v2._2.get.filter(p => p._2 > upperBoundActivationsNumber)
    GraphUtils.compareTimeSeries(v1Visits, v2Visits, isFiltered, lambda)
  }
  
 
  def getStatsThreshold(pageStats:Dataset[PageStatRow], burstRate:Double):Dataset[PageRowThreshold] = {
    import session.implicits._
    pageStats.map(p => PageRowThreshold(p.page_id, p.mean + burstRate*scala.math.sqrt(p.variance))) 
  }
  
  
  def getStats(input: Dataset[PageVisitGroup], startDate:LocalDate, endDate:LocalDate):Dataset[PageStatRow] = {
    import session.implicits._
    val startTime = startDate.atStartOfDay
    val endTime = endDate.plusDays(1).atStartOfDay
    val visitsPeriod = Duration.between(startTime, endTime)
    val totalHours = visitsPeriod.toHours.toInt
    input.map(p => PageVisitElapsedGroup(p.page_id, p.visits.map(v => (Duration.between(startTime, v._1.toLocalDateTime).toHours.toInt, v._2.toDouble))))
         .map(p => (p.page_id, meanAndVariance(new VectorBuilder(p.visits.map(f => f._1).toArray, p.visits.map(f => f._2).toArray, p.visits.size, totalHours).toDenseVector)))
         .map(p => PageStatRow(p._1, p._2.mean, p._2.variance))    
  }
  
  def extractPeakActivity(input: Dataset[PageVisitGroup], startDate:LocalDate, endDate:LocalDate, inputExtended: Dataset[PageVisitGroup], startDateExtend:LocalDate,
                          burstRate:Double, burstCount:Int):Dataset[Long] = {
    import session.implicits._

    val pageStats = getStats(inputExtended, startDateExtend, endDate)
    val pageThr = getStatsThreshold(pageStats, burstRate)
    
    val inputGrp = input.join(pageThr, "page_id")
                        .as[PageVisitThrGroup]
    
    // get active page id
    inputGrp.map(p => (p, p.visits.count(v => v._2 > p.threshold)))
            .filter(k => k._2 > burstCount)
            .map(p => p._1.page_id)
            .distinct
  }

  def extractPeakActivityZscore(input: Dataset[PageVisitGroup], startDate:LocalDate, endDate:LocalDate, inputExtended: Dataset[PageVisitGroup], startDateExtend:LocalDate,
                          lag: Int, threshold: Double, influence: Double, activityThreshold:Int): Dataset[Long] = {
    import session.implicits._
    val startTime = startDateExtend.atStartOfDay
    val visitsPeriod = Duration.between(startTime, endDate.plusDays(1).atStartOfDay)
    val totalHours = visitsPeriod.toHours.toInt
    val extensionPeriod = Duration.between(startTime, startDate.atStartOfDay)
    val extensionHours = extensionPeriod.toHours.toInt

    val pageActivities = inputExtended.map(p => PageVisitElapsedGroup(p.page_id, p.visits.map(v => (Duration.between(startTime, v._1.toLocalDateTime).toHours.toInt, v._2.toDouble))))
                                      .map(p => (p.page_id, new VectorBuilder(p.visits.map(f => f._1).toArray, p.visits.map(f => f._2).toArray, p.visits.size, totalHours).toDenseVector))
                                      .map(p => (p._1, TimeSeriesUtils.smoothedZScore(p._2, lag, threshold, influence)))
                                      .map(p => (p._1, p._2.slice(extensionHours, p._2.length - 1)))// remove extension from time series

     pageActivities.map(p => (p._1, p._2.count(_ > 0)))
       .filter(p => p._2 > activityThreshold)
       .map(_._1).distinct
  }
  
  def extractActiveSubGraph(activeNodes:Dataset[Long]):Graph[String, Double] = {
    // setup neo4j connection
    val neo = Neo4j(session.sparkContext)
    val nodesQuery = "MATCH (p:Page) WHERE p.id in {nodelist} RETURN p.id AS id, p.title AS value"
    val relsQuery = "MATCH (p1)-[r]->(p2) WHERE p1.id IN {nodelist} AND p2.id IN {nodelist} RETURN p1.id AS source, p2.id AS target, type(r) AS value"
    val nodeList = activeNodes.collectAsList() // neo4j connector cannot take RDDs
    
    // perform query
    val graph:Graph[String,String] = neo.nodes(nodesQuery, Map("nodelist" -> nodeList))
                                        .rels(relsQuery, Map("nodelist" -> nodeList))
                                        .loadGraph     
    graph.mapEdges(_ => 1.0) // TODO use a tuple to keep track of relationship type
         .mapVertices((_, title) => xml.Utility.escape(title)) // escape special chars for xml/gexf output
  }
  
  
  def getVisitsTimeSeriesGroup(startDate:LocalDate, endDate:LocalDate):Dataset[PageVisitGroup] = getVisitsTimeSeriesGroup(session, keySpace, tableVisits, tableMeta, startDate, endDate)
  def getActiveTimeSeries(timeSeries:Dataset[PageVisitGroup], activeNodes:Dataset[Long]):Dataset[(Long, List[(Timestamp, Int)])] = {
    import session.implicits._
    timeSeries.join(activeNodes.toDF("page_id"), "page_id").as[PageVisitGroup].map(p => (p.page_id, p.visits))
  }
  
  
}
  
  
   object PeakFinder {
    def main(args:Array[String]) = {
      val dateFormatter = DateTimeFormatter.ofPattern("yyyyMMdd")
      val cfgBase = new ConfigFileOutputPathOpt(args)
      val cfgDefault = ConfigFactory.parseString("cassandra.db.port=9042,peakfinder.useTableStats=false,peakfinder.activityzscore=false")
      val cfg = ConfigFactory.parseFile(new File(cfgBase.cfgFile())).withFallback(cfgDefault)
      val pf = new PeakFinder(cfg.getString("cassandra.db.host"), cfg.getInt("cassandra.db.port"),
                              cfg.getString("cassandra.db.username"), cfg.getString("cassandra.db.password"),
                              cfg.getString("cassandra.db.keyspace"), cfg.getString("cassandra.db.tableVisits"),
                              cfg.getString("cassandra.db.tableStats"), cfg.getString("cassandra.db.tableMeta"),
                              cfg.getString("neo4j.bolt.url"), cfg.getString("neo4j.user"), cfg.getString("neo4j.password"))
      val startDate = LocalDate.parse(cfg.getString("peakfinder.startDate"))
      val endDate = LocalDate.parse(cfg.getString("peakfinder.endDate"))
      val activityZscore = cfg.getBoolean("peakfinder.activityZScore")
      if (startDate.isAfter(endDate))
         throw new IllegalArgumentException("Start date is after end date")

      // retrieve visits time series plus history of equal length
      val visitsExtend = Period.between(startDate, endDate).getDays
      val startDateExtend = startDate.minusDays(visitsExtend)
      val extendedTimeSeries = pf.getVisitsTimeSeriesGroup(startDateExtend, endDate)
      val filteredTimeSeries = pf.getVisitsTimeSeriesGroup(startDate, endDate)

      val activePages = if (!activityZscore)  pf.extractPeakActivity(filteredTimeSeries, startDate, endDate,
                                               extendedTimeSeries, startDateExtend,
                                               cfg.getDouble("peakfinder.burstRate"), cfg.getInt("peakfinder.burstCount"))
                        else pf.extractPeakActivityZscore(filteredTimeSeries, startDate, endDate, extendedTimeSeries, startDateExtend,
                                                          cfg.getInt("peakfinder.zscore.lag"), cfg.getDouble("peakfinder.zscore.influence"),
                                                          cfg.getDouble("peakfinder.zscore.threshold"), cfg.getInt("peakfinder.zscore.activityThreshold"))

      val activeTimeSeries = pf.getActiveTimeSeries(filteredTimeSeries, activePages)//.cache()
      
      

      val activePagesGraph = pf.extractActiveSubGraph(activePages).outerJoinVertices(activeTimeSeries.rdd)((_, title, visits) => (title, visits))

      
      val trainedGraph = activePagesGraph.mapTriplets(trplt => pf.compareTimeSeries(trplt.dstAttr, trplt.srcAttr, isFiltered = true, lambda = 0.5))
                                         .mapVertices((_, v) => v._1)
      val prunedGraph = GraphUtils.removeLowWeightEdges(trainedGraph, minWeight = 1.0)

      val cleanGraph = GraphUtils.removeSingletons(prunedGraph)
      val ccGraph = GraphUtils.getLargestConnectedComponent(cleanGraph)
      val finalGraph = GraphUtils.toUndirected(ccGraph)

      val outputPath = cfgBase.outputPath()
      if (outputPath.startsWith("hdfs://")) {
        val tmpPath = outputPath.replaceFirst("hdfs://", "")
        GraphUtils.saveGraphHdfs(finalGraph, weighted=true,
                                 fileName = Paths.get(tmpPath, "peaks_graph_" + dateFormatter.format(startDate) + "_" + dateFormatter.format(endDate) + ".gexf").toString)
      }
      else
        GraphUtils.saveGraph(finalGraph, weighted = true,
                           fileName = Paths.get(outputPath, "peaks_graph_" + dateFormatter.format(startDate) + "_" + dateFormatter.format(endDate) + ".gexf").toString)

    }
}