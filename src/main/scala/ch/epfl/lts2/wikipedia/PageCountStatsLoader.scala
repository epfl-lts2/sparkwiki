package ch.epfl.lts2.wikipedia

import org.apache.spark.sql.{Dataset, Row, SQLContext, SparkSession}
import java.time._
import java.time.format.DateTimeFormatter
import java.sql.Timestamp
import com.google.common.collect._
import java.nio.file.Paths

import com.google.common.collect.RangeSet
import org.apache.spark.sql.functions._

class UnavailableVisitDataException(message:String) extends Exception(message)

case class PageStatRow(page_id:Long, mean:Double, variance:Double)
case class PageVisitElapsed(page_id:Long, elapsed_hours:Int, count:Double)
case class PagecountMetadata(start_time:Timestamp, end_time:Timestamp)

trait DateChecker {
  def checkDateRange(visitsMeta:Array[PagecountMetadata], startDate:LocalDate, endDate:LocalDate):Boolean = {
    val dateDomain = new DiscreteDomainLocalDate
    val range = Range.closed(startDate, endDate)
    val rangesMeta = visitsMeta.map(m => Range.closed(m.start_time.toLocalDateTime.toLocalDate, m.end_time.toLocalDateTime.toLocalDate))
    val rs = rangesMeta.foldLeft(TreeRangeSet.create.asInstanceOf[RangeSet[LocalDate]])((acc, r) => {
      acc.add(r.canonical(dateDomain))
      acc
    })
    rs.encloses(range)
  }
}

trait PageCountStatsLoader extends DateChecker with Serializable {
  val zipper = udf[Seq[(Timestamp, Int)], Seq[Timestamp], Seq[Int]](_.zip(_))



  def getVisits():Dataset[PageVisitRow]

  def getVisitsPeriod(startDate:LocalDate, endDate:LocalDate):Dataset[PageVisitRow] = {
    val startTime = Timestamp.valueOf(startDate.atStartOfDay.minusNanos(1))
    val endTime = Timestamp.valueOf(endDate.plusDays(1).atStartOfDay.minusNanos(1))
    if (!checkVisitsDataAvailable(startDate, endDate))
      throw new UnavailableVisitDataException("Missing visit data for period" + startDate.format(DateTimeFormatter.ISO_LOCAL_DATE) + " - " + endDate.format(DateTimeFormatter.ISO_LOCAL_DATE))
    getVisits.filter(p => p.visit_time.after(startTime) && p.visit_time.before(endTime))
  }

  def getVisitsTimeSeriesGroup(startDate:LocalDate, endDate:LocalDate, language:String):Dataset[PageVisitGroup]

  def loadMetadata():Dataset[PagecountMetadata]



  def checkVisitsDataAvailable(startDate:LocalDate, endDate:LocalDate):Boolean ={
    val visitsMeta = loadMetadata().collect()
    checkDateRange(visitsMeta, startDate, endDate)
  }
  
}

class PageCountStatsLoaderCassandra(session:SparkSession, keySpace:String, tableVisits:String, tableMeta:String) extends PageCountStatsLoader {

  def getVisits():Dataset[PageVisitRow] = {
    import session.implicits._
    session.read
           .format("org.apache.spark.sql.cassandra")
           .options(Map("table"->tableVisits, "keyspace"->keySpace))
           .load().as[PageVisitRow]
  }

  def getVisitsTimeSeriesGroup(startDate:LocalDate, endDate:LocalDate, language:String):Dataset[PageVisitGroup] = {
    import session.implicits._

    val input = getVisitsPeriod(startDate, endDate)
    input.filter(r => r.languageCode == language)
         .groupBy("page_id")
         .agg(collect_list("visit_time") as "visit_time", collect_list("count") as "count")
         .withColumn("visits", zipper(col("visit_time"), col("count")))
         .drop("visit_time")
         .drop("count")
         .as[PageVisitGroup]
  }

  def loadMetadata():Dataset[PagecountMetadata] = {
    import session.implicits._
    session.read
           .format("org.apache.spark.sql.cassandra")
           .options(Map("table"->tableMeta, "keyspace"->keySpace))
           .load().as[PagecountMetadata]

  }


}

class PageCountStatsLoaderParquet(session:SparkSession, parquetPath:String) extends PageCountStatsLoader {

  val pgcountPath = Paths.get(parquetPath, Constants.PGCOUNT_DIR).toString
  val metaPath = Paths.get(parquetPath, Constants.META_DIR).toString
  override def getVisits(): Dataset[PageVisitRow] = {
    import session.implicits._
    session.read.parquet(pgcountPath).as[PageVisitRow]
  }

  def getVisitsTimeSeriesGroup(startDate: LocalDate, endDate: LocalDate, language: String): Dataset[PageVisitGroup] = {
    import session.implicits._

    val input = getVisitsPeriod(startDate, endDate)
    input.filter(r => r.languageCode == language)
         .groupBy("page_id")
         .agg(collect_list("visit_time") as "visit_time", collect_list("count") as "count")
         .withColumn("visits", zipper(col("visit_time"), col("count")))
         .drop("visit_time")
         .drop("count")
         .as[PageVisitGroup]
  }


  override def loadMetadata(): Dataset[PagecountMetadata] = {
    import session.implicits._
    session.read.parquet(metaPath).as[PagecountMetadata]
  }
}