package ch.epfl.lts2.wikipedia

import java.io.File
import java.nio.file.Paths
import java.sql.Timestamp
import java.time._
import java.time.format.DateTimeFormatter
import com.typesafe.config.{Config, ConfigFactory, ConfigValueFactory}
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.DataTypes
import org.apache.spark.sql.{Dataset, SparkSession}
import org.rogach.scallop._

    

class PagecountConf(args: Seq[String]) extends ScallopConf(args) with Serialization {
  val legacyPagecount = opt[Boolean](name="legacyPageCount")
  val cfgFile = opt[String](name="config", required=true)
  val basePath = opt[String](required = true, name="basePath")
  val startDate = opt[LocalDate](required = true, name="startDate")(singleArgConverter[LocalDate](LocalDate.parse(_)))
  val endDate = opt[LocalDate](required = true, name="endDate")(singleArgConverter[LocalDate](LocalDate.parse(_)))
  val pageDump = opt[String](required=true, name="pageDump")
  val langList = opt[List[String]](required=true, name="languages")
  val outputPath = opt[String](name="outputPath")
  verify()
}


case class Visit(time:Timestamp, count:Int, timeResolution: String)
case class PageVisits(languageCode:String, title:String, namespace:Int, visits:List[Visit])
case class PageVisitsIdFull(languageCode:String, title: String, namespace:Int, id:Int, visits:List[Visit])
case class PageVisitsId(languageCode:String, id:Int, visits:List[Visit])
case class PageVisitRow(languageCode:String, page_id:Long, visit_time: Timestamp, count:Int)

class PagecountProcessor(val languages: List[String], val parser: WikipediaElementParser[WikipediaPagecount],
                         val cfg: Config, val saveToCassandra:Boolean, val legacyPageCount: Boolean)
  extends Serializable with JsonWriter with CsvWriter {

  private def createSparkConf(cfg: Config, saveToCassandra: Boolean) = {
    if (saveToCassandra)
      new SparkConf().setAppName("Wikipedia pagecount processor")
                      .set("spark.cassandra.connection.host", cfg.getString("cassandra.db.host"))
                      .set("spark.cassandra.connection.port", cfg.getInt("cassandra.db.port").toString)
                      .set("spark.cassandra.auth.username", cfg.getString("cassandra.db.username"))
                      .set("spark.cassandra.auth.password", cfg.getString("cassandra.db.password"))
    else
      new SparkConf().setAppName("Wikipedia pagecount processor")
  }

  private def createWriter(cfg: Config, saveToCassandra:Boolean) = {
    if (saveToCassandra)
      new CassandraPagecountWriter(session,
        cfg.getString("cassandra.db.keyspace"), cfg.getString("cassandra.db.tableVisits"),
        cfg.getString("cassandra.db.tableMeta"))
    else
      new ParquetPagecountWriter(session, cfg.getString("outputPath"))
  }

  lazy val sparkConf = createSparkConf(cfg, saveToCassandra)
       
  lazy val session = SparkSession.builder.config(sparkConf).getOrCreate()

  lazy val resultsWriter = createWriter(cfg, saveToCassandra)

  val hourParser = new WikipediaHourlyVisitsParser
  
  def dateRange(from:LocalDate, to:LocalDate, step:Period):Iterator[LocalDate] = {
     if (from.isAfter(to))
       throw new IllegalArgumentException("start date must be before end date")
     Iterator.iterate(from)(_.plus(step)).takeWhile(!_.isAfter(to))

  }
  
  def parseLinesToDf(input:RDD[String], minDailyVisits:Int, minDailyVisitsHourSplit:Int, date:LocalDate):Dataset[PageVisits] = {
    import session.implicits._
    val rdd = if (legacyPageCount) parseLinesLegacy(input, minDailyVisits, minDailyVisitsHourSplit, date)
    else parseLines(input, minDailyVisits, minDailyVisitsHourSplit, date)
    session.createDataFrame(rdd).as[PageVisits]
  }
  
  def getPageVisit(p:WikipediaPagecount, minDailyVisitsHourSplit:Int, date:LocalDate): List[Visit] = {
    if (p.dailyVisits >= minDailyVisitsHourSplit) 
      hourParser.parseField(p.hourlyVisits, date)
                .map(h => Visit(Timestamp.valueOf(h.time), h.visits, "Hour"))
    else List(Visit(Timestamp.valueOf(date.atStartOfDay), p.dailyVisits, "Day"))  
  }

  def parseLinesLegacy(input:RDD[String], minDailyVisits:Int, minDailyVisitsHourSplit:Int, date:LocalDate):RDD[PageVisits] = {

     parser.getRDD(input.filter(!_.startsWith("#")))
           .filter(w => w.dailyVisits > minDailyVisits)
           .map(p => PageVisits(p.languageCode, p.title, p.namespace, getPageVisit(p, minDailyVisitsHourSplit, date)))
  }

  protected def aggHourlyVisits(input:String): String = {
    val aggRegex = """([A-X])(\d+)""".r
    val resMap = aggRegex.findAllIn(input).matchData.toList
                                          .groupBy(_.group(1))
                                          .mapValues(v => v.foldLeft(0)(_ + _.group(2).toInt))
    // resMap: Map[String,Int] = Map(E -> 393, X -> 457, N -> 331, T -> 469, J -> 22...)
    resMap.toSeq.sortBy(_._1).foldLeft("")((a,b) => a + (b._1 + b._2.toString))
  }

  def parseLines(input:RDD[String], minDailyVisits:Int, minDailyVisitsHourSplit:Int, date:LocalDate):RDD[PageVisits] = {
    import session.implicits._
    val hourlyVisitUdf = udf((s:String) => aggHourlyVisits(s))
    val visitsRow = session.createDataFrame(parser.getRDD(input.filter(!_.startsWith("#")))).as[WikipediaPagecount]
    // aggregate results per source
    val visitsAgg = visitsRow.groupBy("title", "languageCode", "namespace")
                             .agg(sum("dailyVisits") as "totalDailyVisits",
                                  concat_ws("", collect_list("hourlyVisits")) as "aggHourlyVisits")
                            // now all the visits modalities are counted, we can filter
                            .where($"totalDailyVisits" > minDailyVisits)
                            .select($"title", $"languageCode", $"namespace", $"totalDailyVisits", hourlyVisitUdf($"aggHourlyVisits") as "mergedHourlyVisits")
    val pageCount = visitsAgg.withColumn("dailyVisits",
                                          visitsAgg.col("totalDailyVisits").cast(DataTypes.IntegerType))
                            .drop("totalDailyVisits")
                            .withColumnRenamed("mergedHourlyVisits", "hourlyVisits")
                            .withColumn("source", lit("web")).as[WikipediaPagecount]

    // finally...
    pageCount.rdd.map(p => PageVisits(p.languageCode, p.title, p.namespace, getPageVisit(p, minDailyVisitsHourSplit, date)))
  }

  def mergePagecount(pageDf:Dataset[WikipediaPageLang], pagecountDf:Dataset[PageVisits]): Dataset[PageVisitsIdFull] = {
    import session.implicits._
    pagecountDf.join(pageDf, Seq("title", "namespace", "languageCode"))
               .select("title", "namespace", "id", "visits", "languageCode")
               .as[PageVisitsIdFull]
  }
  
  def getPageDataFrame(fileName:String):Dataset[WikipediaPageLang] = {
    import session.implicits._
    if (fileName.endsWith("sql.bz2") || fileName.endsWith("sql.gz")) { // seems like we are reading a table dump 
      val pageParser = new DumpParser
      pageParser.processFileToDf(session, fileName, WikipediaDumpType.Page).select("id", "namespace", "title").as[WikipediaPageLang]
    } else { // otherwise try to import from a parquet file
      session.read.parquet(fileName).as[WikipediaPageLang]
    }
  }

  def getResult(pageDumpPath: String, keepRedirects: Boolean, dfVisits: Dataset[PageVisits]) = {
    import session.implicits._
    val pgDf = getPageDataFrame(pageDumpPath)
      .filter(p => keepRedirects || !p.isRedirect)

    // join page and page count
    val pcDfId = mergePagecount(pgDf, dfVisits)
      .groupBy("id", "languageCode")
      .agg(flatten(collect_list("visits")).alias("visits")).as[PageVisitsId]
    pcDfId.flatMap(p => p.visits.map(v => PageVisitRow(p.languageCode, p.id, v.time, v.count)))
  }

  def getEarliestDate(current:Timestamp, newDate: LocalDate):Timestamp = {
    val newTime = newDate.atStartOfDay
    if (newTime.isBefore(current.toLocalDateTime))
      Timestamp.valueOf(newTime)
    else
      current
  }
  
  def getLatestDate(current:Timestamp, newDate: LocalDate):Timestamp = {
    val newTime = newDate.plusDays(1).atStartOfDay // check the day after at 0:00 to get integer number of days
    if (newTime.isAfter(current.toLocalDateTime))
      Timestamp.valueOf(newTime)
    else
      current
  }

  def saveResult(data: Dataset[PageVisitRow], startDate: LocalDate, endDate: LocalDate): Unit = {
    resultsWriter.updateMeta(startDate, endDate)
    resultsWriter.writeData(data)
  }
  
}

object PagecountProcessor {
  
  val legacyDateFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd")
  val dateFormatter = DateTimeFormatter.ofPattern("yyyyMMdd")
  val flatten = udf((xs: Seq[Seq[Visit]]) => xs.flatten) // helper function


  def getParser(legacyPagecount:Boolean, languages:List[String]) = {
    val eltFilter = new ElementFilter[WikipediaPagecount] {
      override def filterElt(t: WikipediaPagecount): Boolean = languages.contains(t.languageCode)
    }
    if (legacyPagecount)
      new WikipediaPagecountLegacyParser(eltFilter)
    else
      new WikipediaPagecountParser(eltFilter)
  }

  def formatFilename(basePath:String, date:LocalDate, legacyPageCount:Boolean) = {
    if (legacyPageCount)
      Paths.get(basePath, "pagecounts-" + date.format(legacyDateFormatter) + ".bz2").toString
    else
      Paths.get(basePath, "pageviews-" + date.format(dateFormatter) + "-user.bz2").toString
  }


  def main(args:Array[String]):Unit = {
    val cfgBase = new PagecountConf(args)
    val cfgDefault = ConfigFactory.parseString("cassandra.db.port=9042,pagecountProcessor.keepRedirects=false")
    val cfg = ConfigFactory.parseFile(new File(cfgBase.cfgFile())).withFallback(cfgDefault)
    val languages = cfgBase.langList()
    val legacyPageCount = cfgBase.legacyPagecount()
    val saveToCassandra = cfgBase.outputPath.isEmpty
    val cfgFinal = if (saveToCassandra) cfg else cfg.withValue("outputPath", ConfigValueFactory.fromAnyRef(cfgBase.outputPath()))
    val pgCountProcessor = new PagecountProcessor(languages, getParser(legacyPageCount, languages),
                                                  cfgFinal, saveToCassandra, legacyPageCount)
    
    val range = pgCountProcessor.dateRange(cfgBase.startDate(), cfgBase.endDate(), Period.ofDays(1))
    val files = range.map(d => (d, formatFilename(cfgBase.basePath(), d, legacyPageCount))).toMap
    val pgInputRdd = files.mapValues(p => pgCountProcessor.session.sparkContext.textFile(p))
    
    import pgCountProcessor.session.implicits._
    val pcRdd = pgInputRdd.transform((d, p) => pgCountProcessor.parseLinesToDf(p, cfgFinal.getInt("pagecountProcessor.minDailyVisits"), cfg.getInt("pagecountProcessor.minDailyVisitsHourlySplit"), d))
    val dfVisits = pcRdd.values.reduce((p1, p2) => p1.union(p2)) // group all rdd's into one

    val pgVisitRows = pgCountProcessor.getResult(cfgBase.pageDump(),
                                                  cfgFinal.getBoolean("pagecountProcessor.keepRedirects"), dfVisits)

    pgCountProcessor.saveResult(pgVisitRows, cfgBase.startDate(), cfgBase.endDate())
  }
}
