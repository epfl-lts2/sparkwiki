package ch.epfl.lts2.wikipedia

import java.time._
import java.time.format.DateTimeFormatter
import java.nio.file.Paths
import org.rogach.scallop._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SQLContext, Row, DataFrame, SparkSession, Dataset}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.functions._


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


case class Visit(time:Long, count:Int, timeResolution: String)
case class PageVisits(title:String, namespace:Int, visits:List[Visit])

class PagecountProcessor extends Serializable with JsonWriter {
  lazy val sconf = new SparkConf().setAppName("Wikipedia pagecount processor")
  lazy val session = SparkSession.builder.config(sconf).getOrCreate()
  val parser = new WikipediaPagecountParser
  val hourParser = new WikipediaHourlyVisitsParser
  
  def dateRange(from:LocalDate, to:LocalDate, step:Period):Iterator[LocalDate] = {
     Iterator.iterate(from)(_.plus(step)).takeWhile(!_.isAfter(to))
  }
  
  def parseLinesToDf(input:RDD[String], minDailyVisits:Int, minDailyVisitsHourSplit:Int, date:LocalDate):DataFrame = {    
    val rdd = parseLines(input, minDailyVisits, minDailyVisitsHourSplit, date)
    session.createDataFrame(rdd)
  }
  
  def getPageVisit(p:WikipediaPagecount, minDailyVisitsHourSplit:Int, date:LocalDate): List[Visit] = {
    if (p.dailyVisits >= minDailyVisitsHourSplit) 
      hourParser.parseField(p.hourlyVisits, date)
                .map(h => Visit(h.time.toInstant(ZoneOffset.UTC).toEpochMilli, h.visits, "Hour"))
    else List(Visit(date.atTime(0, 0).toInstant(ZoneOffset.UTC).toEpochMilli, p.dailyVisits, "Day"))  
  }
  
  def parseLines(input:RDD[String], minDailyVisits:Int, minDailyVisitsHourSplit:Int, date:LocalDate):RDD[PageVisits] = {
    parser.getRDD(input.filter(!_.startsWith("#")))
                  .filter(w => w.dailyVisits > minDailyVisits)
                  .map(p => PageVisits(p.title, p.namespace, getPageVisit(p, minDailyVisitsHourSplit, date))) 
  }

  def mergePagecount(pageDf:DataFrame, pagecountDf:DataFrame): DataFrame = {
    pagecountDf.join(pageDf, Seq("title", "namespace"))
               .select("title", "namespace", "id", "visits")
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
  val flatten = udf((xs: Seq[Seq[Visit]]) => xs.flatten) // helper function
  
  def main(args:Array[String]) = {
    import pgCountProcessor.session.implicits._
    val cfg = new PagecountConf(args)
    
    val range = pgCountProcessor.dateRange(cfg.startDate(), cfg.endDate(), Period.ofDays(1))
    val files = range.map(d => (d, Paths.get(cfg.basePath(), "pagecounts-" + d.format(dateFormatter) + ".bz2").toString)).toMap
    val pgInputRdd = files.mapValues(p => pgCountProcessor.session.sparkContext.textFile(p))
    
    
    val pcRdd = pgInputRdd.transform((d, p) => pgCountProcessor.parseLinesToDf(p, cfg.minDailyVisits(), cfg.minDailVisitsHourSplit(), d))
    val dfVisits = pcRdd.values.reduce((p1, p2) => p1.union(p2)) // group all rdd's into one
    
    if (cfg.pageDump.supplied) { 
      val pgDf = pgCountProcessor.getPageDataFrame(cfg.pageDump())
                                 
      // join page and page count
      val pcDfId = pgCountProcessor.mergePagecount(pgDf, dfVisits)
                       .groupBy("id")
                       .agg(flatten(collect_list("visits")).alias("visits"))
      
      //pcDfId.show()
      pgCountProcessor.writeJson(pcDfId, cfg.outputPath(), true)
    }
  }
}