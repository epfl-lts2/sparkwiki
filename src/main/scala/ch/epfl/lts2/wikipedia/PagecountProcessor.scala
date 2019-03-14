package ch.epfl.lts2.wikipedia

import java.time._
import java.time.format.DateTimeFormatter
import java.sql.Timestamp
import java.nio.file.Paths
import org.rogach.scallop._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SQLContext, Row, SparkSession, Dataset}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.cassandra._
import com.datastax.spark.connector._

    

class PagecountConf(args: Seq[String]) extends ScallopConf(args) with Serialization {
  val basePath = opt[String](required = true, name="basePath")
  val startDate = opt[LocalDate](required = true, name="startDate")(singleArgConverter[LocalDate](LocalDate.parse(_)))
  val endDate = opt[LocalDate](required = true, name="endDate")(singleArgConverter[LocalDate](LocalDate.parse(_)))
  val pageDump = opt[String](required=true, name="pageDump")
  val dbHost = opt[String](required=true, name="dbHost")
  val dbPort = opt[Int](required=true, name="dbPort", default=Some[Int](9042))
  val keySpace = opt[String](required=true, name="keySpace")
  val table = opt[String](required=true, name="table")
  val tableMeta = opt[String](name="tableMeta")
  val minDailyVisits = opt[Int](required=true, name="minDailyVisits", default=Some[Int](100))
  val minDailVisitsHourSplit = opt[Int](required=true, name="minDailyVisitsHourSplit", default=Some[Int](10000))
  val keepRedirects = toggle(name="keepRedirects", default=Some(false))
  verify()
}


case class Visit(time:Timestamp, count:Int, timeResolution: String)
case class PageVisits(title:String, namespace:Int, visits:List[Visit])
case class PageVisitsIdFull(title: String, namespace:Int, id:Int, visits:List[Visit])
case class PageVisitsId(id:Int, visits:List[Visit])
case class PageVisitRow(page_id:Long, visit_time: Timestamp, count:Int)

class PagecountProcessor(val dbHost:String, val dbPort:Int) extends Serializable with JsonWriter with CsvWriter {
  lazy val sconf = new SparkConf().setAppName("Wikipedia pagecount processor")
        .set("spark.cassandra.connection.host", dbHost) // TODO use auth 
        .set("spark.cassandra.connection.port", dbPort.toString)
       
  lazy val session = SparkSession.builder.config(sconf).getOrCreate()
  val parser = new WikipediaPagecountParser
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
                  .map(p => PageVisits(p.title, p.namespace, getPageVisit(p, minDailyVisitsHourSplit, date))) 
  }

  def mergePagecount(pageDf:Dataset[WikipediaPage], pagecountDf:Dataset[PageVisits]): Dataset[PageVisitsIdFull] = {
    import session.implicits._
    pagecountDf.join(pageDf, Seq("title", "namespace"))
               .select("title", "namespace", "id", "visits")
               .as[PageVisitsIdFull]
  }
  
  def getPageDataFrame(fileName:String):Dataset[WikipediaPage] = {
    import session.implicits._
    if (fileName.endsWith("sql.bz2") || fileName.endsWith("sql.gz")) { // seems like we are reading a table dump 
      val pageParser = new DumpParser
      pageParser.processFileToDf(session, fileName, WikipediaDumpType.Page).select("id", "namespace", "title").as[WikipediaPage]
    } else { // otherwise try to import from a parquet file
      session.read.parquet(fileName).as[WikipediaPage]
    }
  }
  
  def writeToDb(data:Dataset[PageVisitRow], keyspace:String, table:String) = {
    data.write
         .format("org.apache.spark.sql.cassandra")
         .option("confirm.truncate","true")
         .option("keyspace", keyspace)
         .option("table", table)
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
  
  def updateMeta(keyspace:String, tableMeta:String, startDate:LocalDate, endDate:LocalDate) = {
    import session.implicits._
    val current = session.read
           .format("org.apache.spark.sql.cassandra")
           .options(Map("table"->tableMeta, "keyspace"->keyspace))
           .load().as[PagecountMetadata]
           .first()
           
    val updated = PagecountMetadata(getEarliestDate(current.start_time, startDate), getLatestDate(current.end_time, endDate))
    val updatedData = session.sparkContext.parallelize(Seq(updated)).toDF.as[PagecountMetadata]
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
    val cfg = new PagecountConf(args)
    
    val pgCountProcessor = new PagecountProcessor(cfg.dbHost(), cfg.dbPort())
    
    val range = pgCountProcessor.dateRange(cfg.startDate(), cfg.endDate(), Period.ofDays(1))
    val files = range.map(d => (d, Paths.get(cfg.basePath(), "pagecounts-" + d.format(dateFormatter) + ".bz2").toString)).toMap
    val pgInputRdd = files.mapValues(p => pgCountProcessor.session.sparkContext.textFile(p))
    
    import pgCountProcessor.session.implicits._
    val pcRdd = pgInputRdd.transform((d, p) => pgCountProcessor.parseLinesToDf(p, cfg.minDailyVisits(), cfg.minDailVisitsHourSplit(), d))
    val dfVisits = pcRdd.values.reduce((p1, p2) => p1.union(p2)) // group all rdd's into one
    
    
    
    val pgDf = pgCountProcessor.getPageDataFrame(cfg.pageDump())
                               .filter(p => cfg.keepRedirects() || !p.isRedirect)
                               
    // join page and page count
    val pcDfId = pgCountProcessor.mergePagecount(pgDf, dfVisits)
                     .groupBy("id")
                     .agg(flatten(collect_list("visits")).alias("visits")).as[PageVisitsId]
    val pgVisitRows = pcDfId.flatMap(p => p.visits.map(v => PageVisitRow(p.id, v.time, v.count)))
    //pcDfId.show()
    //pgCountProcessor.writeCsv(pgVisitRows.map(p => (p.page_id, p.visit_time.toString, p.count)).toDF(), cfg.outputPath())
    //pgCountProcessor.writeJson(pcDfId, cfg.outputPath(), true)
    pgCountProcessor.writeToDb(pgVisitRows, cfg.keySpace(), cfg.table())
    if (cfg.tableMeta.isSupplied)
      pgCountProcessor.updateMeta(cfg.keySpace(), cfg.tableMeta(), cfg.startDate(), cfg.endDate())
  }
}
