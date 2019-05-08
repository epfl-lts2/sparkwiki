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
import org.apache.spark.sql.{DataFrame, Dataset, Row, Column, SparkSession}
import org.apache.spark.sql.functions.{greatest, least, lit, udf, desc, col, array, explode}
import org.neo4j.spark._
import org.graphframes._
import scala.collection.mutable.WrappedArray

case class PageRowThreshold(page_id:Long, threshold: Double)
case class PageVisitThrGroup(page_id:Long, threshold:Double, visits:List[(Timestamp, Int)])
case class PageVisitGroup(page_id:Long, visits:List[(Timestamp, Int)])
case class PageVisitElapsedGroup(page_id:Long, visits:List[(Int,Double)])
case class PageNode(id:Long, title:String)

class PeakFinder(dbHost:String, dbPort:Int, dbUsername:String, dbPassword:String,
                 keySpace: String, tableVisits:String, tableStats:String, tableMeta:String,
                 boltUrl:String, neo4jUser:String, neo4jPass:String, outputPath:String) extends PageCountStatsLoader with Serializable {
  lazy val sparkConfig: SparkConf = new SparkConf().setAppName("Wikipedia activity detector")
        .set("spark.cassandra.connection.host", dbHost)
        .set("spark.cassandra.connection.port", dbPort.toString)
        .set("spark.cassandra.auth.username", dbUsername)
        .set("spark.cassandra.auth.password", dbPassword)
        .set("spark.neo4j.bolt.url", boltUrl)
        .set("spark.neo4j.bolt.user", neo4jUser)
        .set("spark.neo4j.bolt.password", neo4jPass)

  lazy val session: SparkSession = SparkSession.builder.config(sparkConfig).getOrCreate()

  val udfTimeSeriesSimilarity = udf { (v1: WrappedArray[Row], v2: WrappedArray[Row],
                                    startTime:Timestamp, totalHours:Int, isFiltered: Boolean, lambda:Double) =>
    // converts the array items into tuples, sorts by first item and returns first two tuples:
    val a1 = v1.map(r => (r.getAs[Timestamp](0), r.getAs[Int](1))).toList
    val a2 = v2.map(r => (r.getAs[Timestamp](0), r.getAs[Int](1))).toList
    compareTimeSeries(a1, a2, startTime, totalHours, isFiltered, lambda)
  }

  val udfTimeSeriesPearson = udf { (v1: WrappedArray[Row], v2: WrappedArray[Row], startTime:Timestamp, totalHours:Int) =>
    // converts the array items into tuples, sorts by first item and returns first two tuples:
    val a1 = v1.map(r => (r.getAs[Timestamp](0), r.getAs[Int](1))).toList
    val a2 = v2.map(r => (r.getAs[Timestamp](0), r.getAs[Int](1))).toList
    compareTimeSeriesPearson(a1, a2, startTime, totalHours)
                                 }
  /**
    * Computes similarity of two time-series
    * @param v1 Time-series of edge start
    * @param v2 Time-series of edge end
    * @param isFiltered Specifies if filtering is required (divides values by the number of spikes)
    * @param lambda Similarity threshold (discard pairs having a lower similarity)
    * @return Similarity measure
    */
  def compareTimeSeries(v1:List[(Timestamp, Int)], v2:List[(Timestamp, Int)],
                        startTime:Timestamp, totalHours:Int,
                        isFiltered: Boolean, lambda: Double = 0.5): Double = {


    if (v1.isEmpty || v2.isEmpty) 0.0
    else TimeSeriesUtils.compareTimeSeries(v1, v2, startTime.toLocalDateTime, totalHours, isFiltered, lambda)
  }

  private def compareTimeSeriesPearsonUnsafe(v1: Array[Double], v2:Array[Double]): Double = {
    // remove daily variations in visits data
    val vds1 = TimeSeriesUtils.removeDailyVariations(v1)
    val vds2 = TimeSeriesUtils.removeDailyVariations(v2)
    val c = TimeSeriesUtils.pearsonCorrelation(vds1, vds2)
    Math.max(0.0, c) // clip to 0, negative correlation means no interest for us
  }

  def compareTimeSeriesPearson(v1:List[(Timestamp, Int)], v2:List[(Timestamp, Int)], startTime:Timestamp, totalHours:Int): Double =
  {
    val vd1 = TimeSeriesUtils.densifyVisitList(v1, startTime.toLocalDateTime, totalHours)
    val vd2 = TimeSeriesUtils.densifyVisitList(v2, startTime.toLocalDateTime, totalHours)
    if (v1.isEmpty || v2.isEmpty) 0.0
    else compareTimeSeriesPearsonUnsafe(vd1, vd2) * totalHours // use scaling to mimick behavior of compareTimeSeries
  }

  def mergeEdges(e:Iterable[Edge[Double]]):Double = e.map(_.attr).max
 
  def getStatsThreshold(pageStats:Dataset[PageStatRow], burstRate:Double):Dataset[PageRowThreshold] = {
    import session.implicits._
    pageStats.map(p => PageRowThreshold(p.page_id, p.mean + burstRate*scala.math.sqrt(p.variance))) 
  }
  
  
  def getStats(input: Dataset[PageVisitGroup], startDate:LocalDate, endDate:LocalDate):Dataset[PageStatRow] = {
    import session.implicits._

    val totalHours = getPeriodHours(startDate, endDate)
    input.map(p => PageVisitElapsedGroup(p.page_id, p.visits.map(v => (Duration.between(startDate.atStartOfDay, v._1.toLocalDateTime).toHours.toInt, v._2.toDouble))))
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

  def writeParquet(df:DataFrame, outputPath: String) =  {
    df.write.mode("overwrite").option("compression", "gzip").parquet(outputPath)
  }

  def extractPeakActivityZscore(startDate:LocalDate, endDate:LocalDate, inputExtended: Dataset[PageVisitGroup], startDateExtend:LocalDate,
                          lag: Int, threshold: Double, influence: Double, activityThreshold:Int, saveOutput:Boolean=false): Dataset[Long] = {
    import session.implicits._

    val startTime = startDateExtend.atStartOfDay
    val totalHours = getPeriodHours(startDateExtend, endDate)
    val extensionHours = getPeriodHours(startDateExtend, startDate.minusDays(1)) // do not remove first day of studied period

    val activePages = inputExtended.map(p => PageVisitElapsedGroup(p.page_id, p.visits.map(v => (Duration.between(startTime, v._1.toLocalDateTime).toHours.toInt, v._2.toDouble))))
                                  .map(p => (p.page_id, new VectorBuilder(p.visits.map(f => f._1).toArray, p.visits.map(f => f._2).toArray, p.visits.size, totalHours).toDenseVector.toArray))
                                  .map(p => (p._1, TimeSeriesUtils.removeDailyVariations(p._2)))
                                  .map(p => (p._1, TimeSeriesUtils.smoothedZScore(p._2, lag, threshold, influence)))
                                  .map(p => (p._1, p._2.drop(extensionHours).count(_ > 0)))// remove extension from time series and count active hours
                                  .filter(_._2 >= activityThreshold) // discard insufficiently active pages

    if (saveOutput)
      writeParquet(activePages.toDF, Paths.get(outputPath, "activePages.pqt").toString)
    activePages.map(_._1)
  }
  
  def extractActiveSubGraph(activeNodes:Dataset[Long]):GraphFrame = {
    // setup neo4j connection
    import session.implicits._
    val neo = Neo4j(session.sparkContext)
    val nodesQuery = "MATCH (p:Page) WHERE p.id in {nodelist} RETURN p.id AS id, p.title AS title"
    val relsQuery = "MATCH (p1)-[r]->(p2) WHERE p1.id IN {nodelist} AND p2.id IN {nodelist} RETURN p1.id AS src, p2.id AS dst, type(r) AS edgeType"
    val nodeList = activeNodes.collectAsList() // neo4j connector cannot take RDDs
    
    // perform query
    val graph = neo.nodes(nodesQuery, Map("nodelist" -> nodeList))
                   .rels(relsQuery, Map("nodelist" -> nodeList))
                   .loadGraphFrame


    val v  = graph.vertices.as[PageNode].map(p => PageNode(p.id, xml.Utility.escape(p.title))) // escape special chars for xml/gexf output
    GraphFrame(v.toDF, graph.edges)
  }
  
  
  def getVisitsTimeSeriesGroup(startDate:LocalDate, endDate:LocalDate):Dataset[PageVisitGroup] = getVisitsTimeSeriesGroup(session, keySpace, tableVisits, tableMeta, startDate, endDate)
  def getActiveTimeSeries(timeSeries:Dataset[PageVisitGroup], activeNodes:Dataset[Long]):Dataset[PageVisitGroup]= {
    import session.implicits._
    timeSeries.join(activeNodes.toDF("page_id"), "page_id").as[PageVisitGroup]//.map(p => (p.page_id, p.visits))
  }

  def toWeightedUndirected(g: GraphFrame):GraphFrame = {
    import session.implicits._
    val edgesSingle = g.edges.as("set")
               .groupBy(least($"set.src", $"set.dst"), greatest($"set.src", $"set.dst"), $"set.edgeType")
               .count().toDF("src", "dst", "edgeType", "count").drop("count")
    GraphFrame(g.vertices, edgesSingle.withColumn("weight", lit(1.0)))
  }


  def cleanGraph(g:GraphFrame, minWeight:Double):GraphFrame = {
    import session.implicits._
    g.filterEdges($"weight" > minWeight).dropIsolatedVertices()
  }

  def computeEdgeWeights(g:GraphFrame, usePearson:Boolean, startTime:LocalDateTime, totalHours:Int, isFiltered:Boolean=true, lambda:Double=0.5):GraphFrame =  {
    import session.implicits._
    //triplets columns = (src, edge, dst)
    // src/dst = source/destination vertex with attribute columns
    val tt = g.triplets.withColumn("startTime", lit(Timestamp.valueOf(startTime))).withColumn("totalHours", lit(totalHours))
    val t = if (usePearson) tt.withColumn("weight", udfTimeSeriesPearson($"src.visits", $"dst.visits", $"startTime", $"totalHours"))
            else tt.withColumn("isFiltered", lit(isFiltered))
                   .withColumn("lambda", lit(lambda))
                   .withColumn("weight", udfTimeSeriesSimilarity($"src.visits", $"dst.visits", $"startTime", $"totalHours", $"isFiltered", $"lambda"))
    val edgesWeight = t.select($"edge.src", $"edge.dst", $"edge.edgeType", $"weight")
    GraphFrame(g.vertices, edgesWeight)
  }

  def getLargestConnectedComponent(g: GraphFrame):GraphFrame = {
    import session.implicits._
    val cc = g.connectedComponents.run()
    // get largest component id
    val lc = cc.groupBy($"component").count.orderBy(desc("count")).first.getLong(0)
    val vert = cc.filter($"component" === lc).drop("component")
    GraphFrame(vert.drop("component"), g.edges).dropIsolatedVertices()
  }
}
  
  
   object PeakFinder {
    def main(args:Array[String]) = {
      val dateFormatter = DateTimeFormatter.ofPattern("yyyyMMdd")
      val cfgBase = new ConfigFileOutputPathOpt(args)
      val cfgDefault = ConfigFactory.parseString("cassandra.db.port=9042,peakfinder.useTableStats=false" +
                                                 ",peakfinder.activityZScore=false,peakfinder.pearsonCorrelation=false," +
                                                 "peakfinder.zscore.saveOutput=false,peakfinder.minEdgeWeight=1.0")
      val cfg = ConfigFactory.parseFile(new File(cfgBase.cfgFile())).withFallback(cfgDefault)
      val outputPath = cfgBase.outputPath()
      val pf = new PeakFinder(cfg.getString("cassandra.db.host"), cfg.getInt("cassandra.db.port"),
                              cfg.getString("cassandra.db.username"), cfg.getString("cassandra.db.password"),
                              cfg.getString("cassandra.db.keyspace"), cfg.getString("cassandra.db.tableVisits"),
                              cfg.getString("cassandra.db.tableStats"), cfg.getString("cassandra.db.tableMeta"),
                              cfg.getString("neo4j.bolt.url"), cfg.getString("neo4j.user"), cfg.getString("neo4j.password"),
                              outputPath)
      val startDate = LocalDate.parse(cfg.getString("peakfinder.startDate"))
      val endDate = LocalDate.parse(cfg.getString("peakfinder.endDate"))
      val activityZscore = cfg.getBoolean("peakfinder.activityZScore")
      val pearsonCorr = cfg.getBoolean("peakfinder.pearsonCorrelation")
      if (startDate.isAfter(endDate))
         throw new IllegalArgumentException("Start date is after end date")

      // retrieve visits time series plus history of equal length
      val visitsExtend = Period.between(startDate, endDate).getDays
      val startDateExtend = startDate.minusDays(visitsExtend)
      val extendedTimeSeries = pf.getVisitsTimeSeriesGroup(startDateExtend, endDate)
      val filteredTimeSeries = pf.getVisitsTimeSeriesGroup(startDate, endDate)
      val totalHours = pf.getPeriodHours(startDate, endDate)
      val startTime = startDate.atStartOfDay

      val activePages = if (!activityZscore)
                            pf.extractPeakActivity(filteredTimeSeries, startDate, endDate,
                                                   extendedTimeSeries, startDateExtend,
                                                   burstRate = cfg.getDouble("peakfinder.burstRate"),
                                                   burstCount = cfg.getInt("peakfinder.burstCount"))
                        else
                            pf.extractPeakActivityZscore(startDate, endDate, extendedTimeSeries, startDateExtend,
                                                         lag = cfg.getInt("peakfinder.zscore.lag"),
                                                         threshold = cfg.getDouble("peakfinder.zscore.threshold"),
                                                         influence = cfg.getDouble("peakfinder.zscore.influence"),
                                                         activityThreshold = cfg.getInt("peakfinder.zscore.activityThreshold"),
                                                         saveOutput = cfg.getBoolean("peakfinder.zscore.saveOutput"))

      val activeTimeSeries = pf.getActiveTimeSeries(filteredTimeSeries, activePages).withColumnRenamed("page_id", "id")//.cache()
      
      
      val gf = pf.toWeightedUndirected(pf.extractActiveSubGraph(activePages))
      val vts = gf.vertices.join(activeTimeSeries, Seq("id"), "outer")
      val activePagesGraph = GraphFrame(vts, gf.edges)
      
      val trainedGraph = pf.computeEdgeWeights(activePagesGraph, pearsonCorr, startTime, totalHours, isFiltered=true, lambda=0.5)

      val cleanGraph = pf.cleanGraph(trainedGraph, cfg.getDouble("peakfinder.minEdgeWeight"))
      val finalGraph = pf.getLargestConnectedComponent(cleanGraph)



      if (outputPath.startsWith("hdfs://")) {
        val tmpPath = outputPath.replaceFirst("hdfs://", "")
        GraphUtils.saveGraphFrameHdfs(pf.session, finalGraph, weighted=true,
                                 fileName = Paths.get(tmpPath, "peaks_graph_" + dateFormatter.format(startDate) + "_" + dateFormatter.format(endDate) + ".gexf").toString)
      }
      else
        GraphUtils.saveGraphFrame(pf.session, finalGraph, weighted = true,
                           fileName = Paths.get(outputPath, "peaks_graph_" + dateFormatter.format(startDate) + "_" + dateFormatter.format(endDate) + ".gexf").toString)

    }
}