package ch.epfl.lts2.wikipedia

import java.io.File
import java.nio.file.Paths
import java.sql.Timestamp
import java.time._
import java.time.format.DateTimeFormatter

import com.typesafe.config.ConfigFactory
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Dataset, SparkSession}
import org.rogach.scallop._

    

class PagecountConf(args: Seq[String]) extends ScallopConf(args) with Serialization {
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

class PagecountProcessor(val dbHost:String, val dbPort:Int, val dbUsername:String, val dbPassword:String,
                         val languages:List[String]) extends Serializable with JsonWriter with CsvWriter {

  lazy val sconf = new SparkConf().setAppName("Wikipedia pagecount processor")
                                  .set("spark.cassandra.connection.host", dbHost)
                                  .set("spark.cassandra.connection.port", dbPort.toString)
                                  .set("spark.cassandra.auth.username", dbUsername)
                                  .set("spark.cassandra.auth.password", dbPassword)
       
  lazy val session = SparkSession.builder.config(sconf).getOrCreate()
  val parser = new WikipediaPagecountParser(languages)
  val hourParser = new WikipediaHourlyVisitsParser
  
  def dateRange(from:LocalDate, to:LocalDate, step:Period):Iterator[LocalDate] = {
     if (from.isAfter(to))
       throw new IllegalArgumentException("start date must be before end date")
     Iterator.iterate(from)(_.plus(step)).takeWhile(!_.isAfter(to))
  }
  
  def parseLinesToDf(input:RDD[String], minDailyVisits:Int, minDailyVisitsHourSplit:Int, date:LocalDate):Dataset[PageVisits] = {
    import session.implicits._
    val rdd = parseLines(input, minDailyVisits, minDailyVisitsHourSplit, date)
    session.createDataFrame(rdd).as[PageVisits]
  }
  
  def getPageVisit(p:WikipediaPagecount, minDailyVisitsHourSplit:Int, date:LocalDate): List[Visit] = {
    if (p.dailyVisits >= minDailyVisitsHourSplit) 
      hourParser.parseField(p.hourlyVisits, date)
                .map(h => Visit(Timestamp.valueOf(h.time), h.visits, "Hour"))
    else List(Visit(Timestamp.valueOf(date.atStartOfDay), p.dailyVisits, "Day"))  
  }
  
  def parseLines(input:RDD[String], minDailyVisits:Int, minDailyVisitsHourSplit:Int, date:LocalDate):RDD[PageVisits] = {
    parser.getRDD(input.filter(!_.startsWith("#")))
                  .filter(w => w.dailyVisits > minDailyVisits)
                  .map(p => PageVisits(p.languageCode, p.title, p.namespace, getPageVisit(p, minDailyVisitsHourSplit, date)))
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
  
  def writeToDb(data:Dataset[PageVisitRow], keyspace:String, tableVisits:String) = {
    data.write
         .format("org.apache.spark.sql.cassandra")
         .option("confirm.truncate","true")
         .option("keyspace", keyspace)
         .option("table", tableVisits)
         .mode("append")
         .save()
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

  def updateMeta(path:String, startDate:LocalDate, endDate:LocalDate) = {
    import session.implicits._


    val newData = PagecountMetadata(Timestamp.valueOf(startDate.atStartOfDay), Timestamp.valueOf(endDate.atStartOfDay))
    val updatedData = session.sparkContext.parallelize(Seq(newData)).toDF.as[PagecountMetadata]
    updatedData.write.mode("append").option("compression", "gzip").parquet(path)
  }

  def updateMeta(keyspace:String, tableMeta:String, startDate:LocalDate, endDate:LocalDate) = {
    import session.implicits._

    val newData = PagecountMetadata(Timestamp.valueOf(startDate.atStartOfDay), Timestamp.valueOf(endDate.atStartOfDay))
    val updatedData = session.sparkContext.parallelize(Seq(newData)).toDF.as[PagecountMetadata]
    updatedData.write
              .format("org.apache.spark.sql.cassandra")
              .option("keyspace", keyspace)
              .option("table", tableMeta)
              .mode("append")
              .save()
  }
  
}

object PagecountProcessor {
  
  val dateFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd")
  val flatten = udf((xs: Seq[Seq[Visit]]) => xs.flatten) // helper function
  
  def main(args:Array[String]):Unit = {
    val cfgBase = new PagecountConf(args)
    val cfgDefault = ConfigFactory.parseString("cassandra.db.port=9042,pagecountProcessor.keepRedirects=false")
    val cfg = ConfigFactory.parseFile(new File(cfgBase.cfgFile())).withFallback(cfgDefault)
    val languages = cfgBase.langList()
    
    val pgCountProcessor = new PagecountProcessor(cfg.getString("cassandra.db.host"), cfg.getInt("cassandra.db.port"),
                                                  cfg.getString("cassandra.db.username"), cfg.getString("cassandra.db.password"),
                                                  languages)
    
    val range = pgCountProcessor.dateRange(cfgBase.startDate(), cfgBase.endDate(), Period.ofDays(1))
    val files = range.map(d => (d, Paths.get(cfgBase.basePath(), "pagecounts-" + d.format(dateFormatter) + ".bz2").toString)).toMap
    val pgInputRdd = files.mapValues(p => pgCountProcessor.session.sparkContext.textFile(p))
    
    import pgCountProcessor.session.implicits._
    val pcRdd = pgInputRdd.transform((d, p) => pgCountProcessor.parseLinesToDf(p, cfg.getInt("pagecountProcessor.minDailyVisits"), cfg.getInt("pagecountProcessor.minDailyVisitsHourlySplit"), d))
    val dfVisits = pcRdd.values.reduce((p1, p2) => p1.union(p2)) // group all rdd's into one
    
    
    
    val pgDf = pgCountProcessor.getPageDataFrame(cfgBase.pageDump())
                               .filter(p => cfg.getBoolean("pagecountProcessor.keepRedirects") || !p.isRedirect)
                               
    // join page and page count
    val pcDfId = pgCountProcessor.mergePagecount(pgDf, dfVisits)
                     .groupBy("id", "languageCode")
                     .agg(flatten(collect_list("visits")).alias("visits")).as[PageVisitsId]
    val pgVisitRows = pcDfId.flatMap(p => p.visits.map(v => PageVisitRow(p.languageCode, p.id, v.time, v.count)))

    if (cfgBase.outputPath.isEmpty) { // save output to database
      pgCountProcessor.writeToDb(pgVisitRows, cfg.getString("cassandra.db.keyspace"), cfg.getString("cassandra.db.tableVisits"))
      if (cfg.hasPath("cassandra.db.tableMeta"))
        pgCountProcessor.updateMeta(cfg.getString("cassandra.db.keyspace"), cfg.getString("cassandra.db.tableMeta"), cfgBase.startDate(), cfgBase.endDate())
    } else { // save to file
      val pathMeta = Paths.get(cfgBase.outputPath(), Constants.META_DIR).toString
      pgCountProcessor.updateMeta(pathMeta, cfgBase.startDate(), cfgBase.endDate())
      val pathPage = Paths.get(cfgBase.outputPath(), Constants.PGCOUNT_DIR).toString
      pgVisitRows.write.mode("append").option("compression", "gzip").parquet(pathPage)
    }
  }
}
