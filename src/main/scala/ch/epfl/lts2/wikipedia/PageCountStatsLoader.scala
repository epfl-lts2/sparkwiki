package ch.epfl.lts2.wikipedia

import org.apache.spark.sql.{Dataset, Row, SQLContext, SparkSession}
import java.time._
import java.time.format.DateTimeFormatter
import java.sql.Timestamp
import com.google.common.collect._

import com.google.common.collect.RangeSet
import org.apache.spark.sql.functions._

class UnavailableVisitDataException(message:String) extends Exception(message)

trait PageCountStatsLoader {
  val zipper = udf[Seq[(Timestamp, Int)], Seq[Timestamp], Seq[Int]](_.zip(_))

  def getPeriodHours(startDate:LocalDate, endDate:LocalDate):Int = {
    val visitsPeriod = Duration.between(startDate.atStartOfDay, endDate.plusDays(1).atStartOfDay)
    visitsPeriod.toHours.toInt
  }

  def getVisits(session:SparkSession, keySpace:String, tableVisits:String):Dataset[PageVisitRow] = {
    import session.implicits._
    session.read
           .format("org.apache.spark.sql.cassandra")
           .options(Map("table"->tableVisits, "keyspace"->keySpace))
           .load().as[PageVisitRow]
  }
  
  def getVisitsPeriod(session:SparkSession, keySpace:String, tableVisits:String, tableMeta:String, startDate:LocalDate, endDate:LocalDate):Dataset[PageVisitRow] = {
    val startTime = Timestamp.valueOf(startDate.atStartOfDay.minusNanos(1))
    val endTime = Timestamp.valueOf(endDate.plusDays(1).atStartOfDay.minusNanos(1))
    if (!checkVisitsDataAvailable(session, keySpace, tableMeta, startDate, endDate))
      throw new UnavailableVisitDataException("Missing visit data for period" + startDate.format(DateTimeFormatter.ISO_LOCAL_DATE) + " - " + endDate.format(DateTimeFormatter.ISO_LOCAL_DATE))
    getVisits(session, keySpace, tableVisits).filter(p => p.visit_time.after(startTime) && p.visit_time.before(endTime))
  }
  
  def getVisitsTimeSeriesGroup(session:SparkSession, keySpace:String, tableVisits:String, tableMeta:String, startDate:LocalDate, endDate:LocalDate):Dataset[PageVisitGroup] = {
    import session.implicits._
    
    val input = getVisitsPeriod(session, keySpace, tableVisits, tableMeta, startDate, endDate)
    input.groupBy("page_id")
         .agg(collect_list("visit_time") as "visit_time", collect_list("count") as "count")
         .withColumn("visits", zipper(col("visit_time"), col("count")))
         .drop("visit_time")
         .drop("count")
         .as[PageVisitGroup]
  }
  
  def getStatsFromTable(session:SparkSession, keySpace:String, tableStats:String):Dataset[PageStatRow] = {
    import session.implicits._
    session.read
           .format("org.apache.spark.sql.cassandra")
           .options(Map("table"->tableStats, "keyspace"->keySpace))
           .load().as[PageStatRow]
  }
  
  
  def loadMetadata(session:SparkSession, keySpace:String, tableMeta:String):Dataset[PagecountMetadata] = {
    import session.implicits._
    session.read
           .format("org.apache.spark.sql.cassandra")
           .options(Map("table"->tableMeta, "keyspace"->keySpace))
           .load().as[PagecountMetadata]

  }

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
  def checkVisitsDataAvailable(session:SparkSession, keySpace:String, tableMeta:String, startDate:LocalDate, endDate:LocalDate):Boolean ={
    val visitsMeta = loadMetadata(session, keySpace, tableMeta).collect()
    checkDateRange(visitsMeta, startDate, endDate)
  }
  
}