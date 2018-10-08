package ch.epfl.lts2.wikipedia

import java.time._
import java.time.format.DateTimeFormatter
import java.nio.file.Paths
import org.rogach.scallop._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SQLContext, Row, DataFrame, SparkSession, Dataset}
import org.apache.spark.{SparkConf, SparkContext}


class PagecountConf(args: Seq[String]) extends ScallopConf(args) {
  val basePath = opt[String](required = true, name= "basePath")
  val outputPath = opt[String](required = true, name="outputPath")
  val startDate = opt[LocalDate](required = true, name="startDate")(singleArgConverter[LocalDate](LocalDate.parse(_)))
  val endDate = opt[LocalDate](required = true, name="endDate")(singleArgConverter[LocalDate](LocalDate.parse(_)))
  val pageDump = opt[String](name="pageDump")
  val minDailyVisits = opt[Int](name="minDailyVisits", default=Some[Int](100))
  val minDailVisitsHourSplit = opt[Int](name="minDailyVisitsHourSplit", default=Some[Int](10000))
  verify()
}

case class PageHourlyVisit(time:Long, title:String, namespace:Int, visits:Int)
case class PageDailyVisit(time:Long, title:String, namespace:Int, visits:Int)

class PagecountProcessor extends Serializable with CsvWriter {
  lazy val sconf = new SparkConf().setAppName("Wikipedia pagecount processor").setMaster("local[*]")
  lazy val session = SparkSession.builder.config(sconf).getOrCreate()
  val parser = new WikipediaPagecountParser("en.z")
  val hourParser = new WikipediaHourlyVisitsParser
  
  
  def dateRange(from:LocalDate, to:LocalDate, step:Period):Iterator[LocalDate] = {
     Iterator.iterate(from)(_.plus(step)).takeWhile(!_.isAfter(to))
  }
  
  def parseLinesToDf(input:RDD[String], minDailyVisits:Int, minDailyVisitsHourSplit:Int, date:LocalDate):(DataFrame, DataFrame) = {    
    val (rddD, rddH) = parseLines(input, minDailyVisits, minDailyVisitsHourSplit, date)
    (session.createDataFrame(rddD), session.createDataFrame(rddH))
  }
  
  def parseLines(input:RDD[String], minDailyVisits:Int, minDailyVisitsHourSplit:Int, date:LocalDate):(RDD[PageDailyVisit], RDD[PageHourlyVisit]) = {
    val rdd = parser.getRDD(input.filter(!_.startsWith("#")))
                    .filter(w => w.dailyVisits > minDailyVisits).cache()
    val rddDaily = rdd.filter(w => w.dailyVisits < minDailyVisitsHourSplit)
                      .map(p => PageDailyVisit(date.atTime(0, 0).toInstant(ZoneOffset.UTC).toEpochMilli, p.title, p.namespace, p.dailyVisits))
    val rddHourly = rdd.filter(w => w.dailyVisits >= minDailyVisitsHourSplit)
                    .map(p => (p, hourParser.parseField(p.hourlyVisits, date)))
                    .flatMap{ case (k, v) => v.map((k, _)) }
                    .map(p => PageHourlyVisit(p._2.time.toInstant(ZoneOffset.UTC).toEpochMilli, p._1.title, p._1.namespace, p._2.visits))
                    
    (rddDaily, rddHourly)
  }
  
  def mergePagecount(pageDf:DataFrame, pagecountDf:DataFrame): DataFrame = {
    pagecountDf.join(pageDf, Seq("title", "namespace"))
               .select("time", "title", "namespace", "id", "visits")
  }
  
  def getPageDataFrame(fileName:String):DataFrame = {
     if (fileName.endsWith("sql.bz2") || fileName.endsWith("sql.gz")) { // seems like we are reading a table dump 
        val pageParser = new DumpParser
        pageParser.processFileToDf(session, fileName, WikipediaDumpType.Page).select("id", "namespace", "title")
      } else { // otherwise try to import from a parquet file
        session.read.parquet(fileName)
      }
  }
}

object PagecountProcessor {
  val pgCountProcessor = new PagecountProcessor
  val dateFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd")
  
  def main(args:Array[String]) = {
    val cfg = new PagecountConf(args)
    
    val range = pgCountProcessor.dateRange(cfg.startDate(), cfg.endDate(), Period.ofDays(1))
    val files = range.map(d => (d, Paths.get(cfg.basePath(), "pagecounts-" + d.format(dateFormatter) + ".bz2").toString)).toMap
    val pgInputRdd = files.mapValues(p => pgCountProcessor.session.sparkContext.textFile(p))
    
    
    val pcDf = pgInputRdd.transform((d, p) => pgCountProcessor.parseLinesToDf(p, cfg.minDailyVisits(), cfg.minDailVisitsHourSplit(), d))
    
    if (cfg.pageDump.supplied) { 
      val pgDf = pgCountProcessor.getPageDataFrame(cfg.pageDump())  
      
      // join page and page count
      val pcDfDailyId = pcDf.mapValues(pcdf => pgCountProcessor.mergePagecount(pgDf, pcdf._1))
      val pcDfHourlyId = pcDf.mapValues(pcdf => pgCountProcessor.mergePagecount(pgDf, pcdf._2))
      
      pcDfDailyId.map(pc_id => pgCountProcessor.writeCsv(pc_id._2, Paths.get(cfg.outputPath(), "daily", pc_id._1.format(dateFormatter)).toString, true))
      val dummy = pcDfHourlyId.map(pc_id => pgCountProcessor.writeCsv(pc_id._2, Paths.get(cfg.outputPath(), "hourly", pc_id._1.format(dateFormatter)).toString, true))
    }
  }
}