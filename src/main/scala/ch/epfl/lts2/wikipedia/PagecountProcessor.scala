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
  val minDailyVisit = opt[Int](name="minDailyVisit", default=Some[Int](100))
  verify()
}

case class PageHourlyVisit(date:String, time:String, title:String, namespace:Int, visits:Int)

class PagecountProcessor extends Serializable with CsvWriter {
  lazy val sconf = new SparkConf().setAppName("Wikipedia pagecount processor").setMaster("local[*]")
  lazy val session = SparkSession.builder.config(sconf).getOrCreate()
  val parser = new WikipediaPagecountParser
  val hourParser = new WikipediaHourlyVisitsParser
  
  
  def dateRange(from: LocalDate, to: LocalDate, step: Period) : Iterator[LocalDate] = {
     Iterator.iterate(from)(_.plus(step)).takeWhile(!_.isAfter(to))
  }
  
  def parseLines(input: RDD[String], minDailyVisits:Int, date:LocalDate):DataFrame = {
    val rdd = parser.getRDD(input.filter(!_.startsWith("#")))
                    .filter(w => w.dailyVisits > minDailyVisits)
                    .map(p => (p, hourParser.parseField(p.hourlyVisits, date)))
                    .flatMap{ case (k, v) => v.map((k, _)) }
    val fRdd = rdd.map(p => PageHourlyVisit(p._2.time.format(DateTimeFormatter.ofPattern("yyyy-MM-dd")), p._2.time.format(DateTimeFormatter.ofPattern("HH:mm:ss")), p._1.title, p._1.namespace, p._2.visits))
    session.createDataFrame(fRdd)
  }
  
  def mergePagecount(pageDf:DataFrame, pagecountDf:DataFrame): DataFrame = {
    pagecountDf.join(pageDf, Seq("title", "namespace"))
               .select("date", "time", "title", "namespace", "id", "visits")
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
    
    
    val pcDf = pgInputRdd.transform((d, p) => pgCountProcessor.parseLines(p, cfg.minDailyVisit(), d))
    
    if (cfg.pageDump.supplied) { 
      val pgDf = pgCountProcessor.getPageDataFrame(cfg.pageDump())  
      
      // join page and page count
      val pcDf_id = pcDf.mapValues(pcdf => pgCountProcessor.mergePagecount(pgDf, pcdf))
      val dummy = pcDf_id.map(pc_id => pgCountProcessor.writeCsv(pc_id._2, Paths.get(cfg.outputPath(), pc_id._1.format(dateFormatter)).toString))
    }
  }
}