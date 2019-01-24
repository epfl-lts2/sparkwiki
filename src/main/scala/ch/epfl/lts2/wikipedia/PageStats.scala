package ch.epfl.lts2.wikipedia

import java.time._
import java.sql.Timestamp
import org.rogach.scallop._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SQLContext, Row, SparkSession, Dataset}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.cassandra
import com.datastax.spark.connector._
import breeze.linalg._
import breeze.numerics._
import breeze.stats._

case class PageStatRow(page_id:Long, mean:Double, variance:Double)
case class PageVisitElapsed(page_id:Long, elapsed_hours:Int, count:Double)
case class PageVisitElapsedGroup(page_id:Long, visits:List[(Int, Double)])
case class PagecountMetadata(start_time:Timestamp, end_time:Timestamp)

class PageStatsConf(args:Seq[String]) extends ScallopConf(args) with Serialization {
  val dbHost = opt[String](required=true, name="dbHost")
  val dbPort = opt[Int](required=true, name="dbPort", default=Some[Int](9042))
  val keySpace = opt[String](required=true, name="keySpace")
  val tableVisits = opt[String](required=true, name="tableVisits")
  val tableStats = opt[String](required=true, name="tableStats")
  val tableMeta = opt[String](required=true, name="tableMeta")
  verify()
}

class PageStats(val dbHost:String, val dbPort:Int) extends Serializable {
  lazy val sconf = new SparkConf().setAppName("Wikipedia pagestats computation")
        .set("spark.cassandra.connection.host", dbHost) // TODO use auth 
        .set("spark.cassandra.connection.port", dbPort.toString)
       
  lazy val session = SparkSession.builder.config(sconf).getOrCreate()
  val zipper = udf[Seq[(Int, Double)], Seq[Int], Seq[Double]](_.zip(_))

  
  def loadMetadata(keySpace:String, tableMeta:String):PagecountMetadata = {
    import session.implicits._
    session.read
           .format("org.apache.spark.sql.cassandra")
           .options(Map("table"->tableMeta, "keyspace"->keySpace))
           .load().as[PagecountMetadata]
           .first()
  }
  
  def getVisits(keySpace:String, tableVisits:String):Dataset[PageVisitRow] = {
    import session.implicits._
    session.read
           .format("org.apache.spark.sql.cassandra")
           .options(Map("table"->tableVisits, "keyspace"->keySpace))
           .load().as[PageVisitRow]
  }
  
  def computeStats(p:PageVisitElapsedGroup, totalHours:Int):PageStatRow = {
    val vb = new VectorBuilder(p.visits.map(f => f._1).toArray, p.visits.map(f => f._2).toArray, p.visits.size, totalHours)
    val m = meanAndVariance(vb.toDenseVector)
    PageStatRow(p.page_id, m.mean, m.variance)
  }
  
  def writeStats(data:Dataset[PageStatRow], keyspace:String, table:String) = {
   
    data.write
         .format("org.apache.spark.sql.cassandra")
         .option("confirm.truncate","true")
         .option("keyspace", keyspace)
         .option("table", table)
         .mode("append")
         .save()
  }
  
  def updateStats(keySpace:String, tableVisits:String, tableStats:String, tableMeta:String) = {
    import session.implicits._
    val meta = loadMetadata(keySpace, tableMeta) 
    
    val visitsPeriod = Duration.between(meta.start_time.toInstant, meta.end_time.toInstant)
    val totalHours = visitsPeriod.toHours.toInt
    
    val visitData = getVisits(keySpace, tableVisits).map(p => PageVisitElapsed(p.page_id, Duration.between(meta.start_time.toInstant, p.visit_time.toInstant).toHours.toInt, p.count))
    
    val visitGrp = visitData.groupBy("page_id")
                         .agg(collect_list("elapsed_hours") as "elapsed_hours", collect_list("count") as "count")
                         .withColumn("visits", zipper(col("elapsed_hours"), col("count")))
                         .drop("elapsed_hours")
                         .drop("count")
                         .as[PageVisitElapsedGroup]
    
    val statsData = visitGrp.map(p => computeStats(p, totalHours))
    
    writeStats(statsData, keySpace, tableStats)
  }
}

object PageStats {
  def main(args:Array[String]) = {
    val cfg = new PageStatsConf(args)
    val ps = new PageStats(cfg.dbHost(), cfg.dbPort())
    
    ps.updateStats(cfg.keySpace(), cfg.tableVisits(), cfg.tableStats(), cfg.tableMeta())
  }
}