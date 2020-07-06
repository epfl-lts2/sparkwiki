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
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.neo4j.spark._
import org.rogach.scallop._


case class PageRowThreshold(page_id:Long, threshold: Double)
case class PageVisitThrGroup(page_id:Long, threshold:Double, visits:List[(Timestamp, Int)])
case class PageVisitGroup(page_id:Long, visits:List[(Timestamp, Int)])
case class PageVisitElapsedGroup(page_id:Long, visits:List[(Int,Double)])


class PeakFinderConfig(args: Seq[String]) extends ScallopConf(args) with Serialization {
  val cfgFile = opt[String](name="config", required=true)
  val outputPath = opt[String](name="outputPath", required=true)
  val language = opt[String](required=true, name="language")
  val parquetPagecounts = opt[Boolean](default=Some(false), name="parquetPagecounts")
  val parquetPagecountsPath = opt[String](default=Some(""), name="parquetPagecountPath")
  verify()
}

class PeakFinder(parquetPageCount:Boolean, parquetPagecountPath:String, dbHost:String, dbPort:Int, dbUsername:String, dbPassword:String,
                 keySpace: String, tableVisits:String, tableMeta:String,
                 neo4jUrl:String, neo4jUser:String, neo4jPass:String, neo4jDb:String, outputPath:String) extends Serializable {
  lazy val sparkConfig: SparkConf = new SparkConf().setAppName("Wikipedia activity detector")
        .set("spark.cassandra.connection.host", dbHost)
        .set("spark.cassandra.connection.port", dbPort.toString)
        .set("spark.cassandra.auth.username", dbUsername)
        .set("spark.cassandra.auth.password", dbPassword)
        .set("spark.neo4j.url", neo4jUrl)
        .set("spark.neo4j.user", neo4jUser)
        .set("spark.neo4j.password", neo4jPass)
        .set("spark.neo4j.database", neo4jDb)

  lazy val session: SparkSession = SparkSession.builder.config(sparkConfig).getOrCreate()

  val pageCountLoader = if (parquetPageCount) new PageCountStatsLoaderParquet(session, parquetPagecountPath)
                        else new PageCountStatsLoaderCassandra(session, keySpace, tableVisits, tableMeta)

  private def unextendTimeSeries(inputExtended:Dataset[PageVisitGroup], startDate:LocalDate):Dataset[PageVisitGroup] = {
    import session.implicits._
    val startTs = Timestamp.valueOf(startDate.atStartOfDay.minusNanos(1))
    inputExtended.map(p => PageVisitGroup(p.page_id, p.visits.filter(v => v._1.after(startTs))) ).cache()
  }

  /**
    * Computes similarity of two time-series
    * @param v1 Time-series of edge start
    * @param v2 Time-series of edge end
    * @param isFiltered Specifies if filtering is required (divides values by the number of spikes)
    * @param lambda Similarity threshold (discard pairs having a lower similarity)
    * @return Similarity measure
    */
  def compareTimeSeries(v1:(String, Option[List[(Timestamp, Int)]]), v2:(String, Option[List[(Timestamp, Int)]]),
                        startTime:LocalDateTime, totalHours:Int,
                        isFiltered: Boolean, lambda: Double = 0.5): Double = {


    val v1Visits = v1._2.getOrElse(List[(Timestamp, Int)]())
    val v2Visits = v2._2.getOrElse(List[(Timestamp, Int)]())
    if (v1Visits.isEmpty || v2Visits.isEmpty) 0.0
    else TimeSeriesUtils.compareTimeSeries(v1Visits, v2Visits, startTime, totalHours, isFiltered, lambda)
  }

  private def compareTimeSeriesPearsonUnsafe(v1: Array[Double], v2:Array[Double]): Double = {
    // remove daily variations in visits data
    val vds1 = TimeSeriesUtils.removeDailyVariations(v1)
    val vds2 = TimeSeriesUtils.removeDailyVariations(v2)
    val c = TimeSeriesUtils.pearsonCorrelation(vds1, vds2)
    Math.max(0.0, c) // clip to 0, negative correlation means no interest for us
  }

  def compareTimeSeriesPearson(v1:(String, Option[List[(Timestamp, Int)]]), v2:(String, Option[List[(Timestamp, Int)]]), startTime:LocalDateTime, totalHours:Int): Double =
  {
    val v1Safe = v1._2.getOrElse(List[(Timestamp, Int)]())
    val v2Safe = v2._2.getOrElse(List[(Timestamp, Int)]())
    val vd1 = TimeSeriesUtils.densifyVisitList(v1Safe, startTime, totalHours)
    val vd2 = TimeSeriesUtils.densifyVisitList(v2Safe, startTime, totalHours)
    if (v1Safe.isEmpty || v2Safe.isEmpty) 0.0
    else compareTimeSeriesPearsonUnsafe(vd1, vd2) * totalHours // use scaling to mimick behavior of compareTimeSeries
  }

  def mergeEdges(e:Iterable[Edge[Double]]):Double = e.map(_.attr).max
 
  def getStatsThreshold(pageStats:Dataset[PageStatRow], burstRate:Double):Dataset[PageRowThreshold] = {
    import session.implicits._
    pageStats.map(p => PageRowThreshold(p.page_id, p.mean + burstRate*scala.math.sqrt(p.variance))) 
  }
  
  
  def getStats(input: Dataset[PageVisitGroup], startDate:LocalDate, endDate:LocalDate):Dataset[PageStatRow] = {
    import session.implicits._

    val totalHours = TimeSeriesUtils.getPeriodHours(startDate, endDate)
    input.map(p => PageVisitElapsedGroup(p.page_id, p.visits.map(v => (Duration.between(startDate.atStartOfDay, v._1.toLocalDateTime).toHours.toInt, v._2.toDouble))))
         .map(p => (p.page_id, meanAndVariance(new VectorBuilder(p.visits.map(f => f._1).toArray, p.visits.map(f => f._2).toArray, p.visits.size, totalHours).toDenseVector)))
         .map(p => PageStatRow(p._1, p._2.mean, p._2.variance))    
  }
  
  def extractPeakActivity(startDate:LocalDate, endDate:LocalDate, inputExtended: Dataset[PageVisitGroup], startDateExtend:LocalDate,
                          burstRate:Double, burstCount:Int):Dataset[Long] = {
    import session.implicits._

    val pageStats = getStats(inputExtended, startDateExtend, endDate)
    val pageThr = getStatsThreshold(pageStats, burstRate)
    val input = unextendTimeSeries(inputExtended, startDate)
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
    val totalHours = TimeSeriesUtils.getPeriodHours(startDateExtend, endDate)
    val extensionHours = TimeSeriesUtils.getPeriodHours(startDateExtend, startDate.minusDays(1)) // do not remove first day of studied period

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
  
  def extractActiveSubGraph(activeNodes:Dataset[Long], includeCategories:Boolean):Graph[String, Double] = {
    // setup neo4j connection
    val neo = Neo4j(session.sparkContext)
    var nodesQuery = ""
    var relsQuery = ""
    if (includeCategories) {
        nodesQuery = "MATCH (p:Page) WHERE p.id in $nodelist RETURN p.id AS id, p.title AS value"
        relsQuery = "MATCH (p1)-[r]->(p2) WHERE p1.id IN $nodelist AND p2.id IN $nodelist RETURN p1.id AS source, p2.id AS target, type(r) AS value"
        } else {
            nodesQuery = "MATCH (p:Page) WHERE NOT 'Category' IN labels(p) AND p.id in $nodelist RETURN p.id AS id, p.title AS value"
            relsQuery = "MATCH (p1)-[r]->(p2) WHERE NOT 'Category' IN labels(p1) AND NOT 'Category' IN labels(p2) AND p1.id IN $nodelist AND p2.id IN $nodelist RETURN p1.id AS source, p2.id AS target, type(r) AS value"
        }

    val nodeList = activeNodes.collectAsList() // neo4j connector cannot take RDDs
    
    // perform query
    val graph:Graph[String,String] = neo.nodes(nodesQuery, Map("nodelist" -> nodeList))
                                        .rels(relsQuery, Map("nodelist" -> nodeList))
                                        .loadGraph     
    graph.mapEdges(_ => 1.0) // TODO use a tuple to keep track of relationship type
         .mapVertices((_, title) => xml.Utility.escape(title)) // escape special chars for xml/gexf output
  }
  
  
  def getVisitsTimeSeriesGroup(startDate:LocalDate, endDate:LocalDate, language:String):Dataset[PageVisitGroup] = pageCountLoader.getVisitsTimeSeriesGroup(startDate, endDate, language)

  def getActiveTimeSeries(timeSeries:Dataset[PageVisitGroup], activeNodes:Dataset[Long], startDate:LocalDate, totalHours:Int, dailyMinThreshold:Int):Dataset[(Long, List[(Timestamp, Int)])] = {
    import session.implicits._
    val input = unextendTimeSeries(timeSeries, startDate)

    val activeTimeSeries = input.join(activeNodes.toDF("page_id"), "page_id").as[PageVisitGroup]//.map(p => (p.page_id, p.visits))
    activeTimeSeries.map(p => (p, TimeSeriesUtils.densifyVisitList(p.visits, startDate.atStartOfDay, totalHours).grouped(24).map(_.sum).max))
                                           .filter(_._2 >= dailyMinThreshold)
                                           .map(p => (p._1.page_id, p._1.visits))
  }
  
  
}
  
  
   object PeakFinder {
    def main(args:Array[String]) = {
      val dateFormatter = DateTimeFormatter.ofPattern("yyyyMMdd")
      val cfgBase = new PeakFinderConfig(args)
      val cfgDefault = ConfigFactory.parseString("cassandra.db.port=9042,peakfinder.useTableStats=false" +
                                                 ",peakfinder.activityZScore=false,peakfinder.pearsonCorrelation=false," +
                                                 "peakfinder.zscore.saveOutput=false,peakfinder.minEdgeWeight=1.0,peakfinder.includeCategories=false")
      val cfg = ConfigFactory.parseFile(new File(cfgBase.cfgFile())).withFallback(cfgDefault)
      val outputPath = cfgBase.outputPath()
      val pf = new PeakFinder(cfgBase.parquetPagecounts(), cfgBase.parquetPagecountsPath(),
                              cfg.getString("cassandra.db.host"), cfg.getInt("cassandra.db.port"),
                              cfg.getString("cassandra.db.username"), cfg.getString("cassandra.db.password"),
                              cfg.getString("cassandra.db.keyspace"), cfg.getString("cassandra.db.tableVisits"),
                              cfg.getString("cassandra.db.tableMeta"),
                              cfg.getString("neo4j.url"), cfg.getString("neo4j.user"), cfg.getString("neo4j.password"),
                              cfg.getString("neo4j.database"),
                              outputPath)
      val startDate = LocalDate.parse(cfg.getString("peakfinder.startDate"))
      val endDate = LocalDate.parse(cfg.getString("peakfinder.endDate"))
      val activityZscore = cfg.getBoolean("peakfinder.activityZScore")
      val pearsonCorr = cfg.getBoolean("peakfinder.pearsonCorrelation")
      val includeCategories = cfg.getBoolean("peakfinder.includeCategories")

      if (startDate.isAfter(endDate))
         throw new IllegalArgumentException("Start date is after end date")

      // retrieve visits time series plus history of equal length
      val visitsExtend = Period.between(startDate, endDate).getDays
      val startDateExtend = startDate.minusDays(visitsExtend)
      val extendedTimeSeries = pf.getVisitsTimeSeriesGroup(startDateExtend, endDate, cfgBase.language()).cache()

      val totalHours = TimeSeriesUtils.getPeriodHours(startDate, endDate)
      val startTime = startDate.atStartOfDay

      val activePages = if (!activityZscore)
                            pf.extractPeakActivity(startDate, endDate,
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

      val activeTimeSeries = pf.getActiveTimeSeries(extendedTimeSeries, activePages, startDate,
                                                    totalHours = totalHours,
                                                    dailyMinThreshold = cfg.getInt("peakfinder.dailyMinThreshold"))//.cache()

      val activePagesGraph = GraphUtils.toUndirected(pf.extractActiveSubGraph(activePages, includeCategories).outerJoinVertices(activeTimeSeries.rdd)((_, title, visits) => (title, visits)), pf.mergeEdges)


      
      val trainedGraph = if (pearsonCorr)
                                activePagesGraph.mapTriplets(t => pf.compareTimeSeriesPearson(t.dstAttr, t.srcAttr, startTime, totalHours))
                                                .mapVertices((_, v) => v._1)
                            else
                                activePagesGraph.mapTriplets(t => pf.compareTimeSeries(t.dstAttr, t.srcAttr, startTime, totalHours,
                                                                                       isFiltered = false, lambda = 0.5))
                                                .mapVertices((_, v) => v._1)

      val prunedGraph = GraphUtils.removeLowWeightEdges(trainedGraph, minWeight = cfg.getDouble("peakfinder.minEdgeWeight"))

      val cleanGraph = GraphUtils.removeSingletons(prunedGraph)
      val finalGraph = GraphUtils.getLargestConnectedComponent(cleanGraph)



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
