package ch.epfl.lts2.wikipedia

import org.apache.spark.sql.{SQLContext, Row, SparkSession, Dataset}
import java.time._
import java.sql.Timestamp
import org.apache.spark.sql.functions._
trait PageCountStatsLoader {
  val zipper = udf[Seq[(Timestamp, Int)], Seq[Timestamp], Seq[Int]](_.zip(_))
  def getVisits(session:SparkSession, keySpace:String, tableVisits:String):Dataset[PageVisitRow] = {
    import session.implicits._
    session.read
           .format("org.apache.spark.sql.cassandra")
           .options(Map("table"->tableVisits, "keyspace"->keySpace))
           .load().as[PageVisitRow]
  }
  
  def getVisitsPeriod(session:SparkSession, keySpace:String, tableVisits:String, startDate:LocalDate, endDate:LocalDate):Dataset[PageVisitRow] = {
    import session.implicits._
    val startTime = Timestamp.valueOf(startDate.atStartOfDay.minusNanos(1))
    val endTime = Timestamp.valueOf(endDate.plusDays(1).atStartOfDay.minusNanos(1))
    getVisits(session, keySpace, tableVisits).filter(p => p.visit_time.after(startTime) && p.visit_time.before(endTime))
  }
  
  def getVisitsTimeSeriesGroup(session:SparkSession, keySpace:String, tableVisits:String, startDate:LocalDate, endDate:LocalDate):Dataset[PageVisitGroup] = {
    import session.implicits._
    
    val input = getVisitsPeriod(session, keySpace, tableVisits, startDate, endDate)
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
  
  
  def loadMetadata(session:SparkSession, keySpace:String, tableMeta:String):PagecountMetadata = {
    import session.implicits._
    session.read
           .format("org.apache.spark.sql.cassandra")
           .options(Map("table"->tableMeta, "keyspace"->keySpace))
           .load().as[PagecountMetadata]
           .first()
  }

  
}