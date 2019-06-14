package ch.epfl.lts2.wikipedia

import java.io.File
import java.sql.Timestamp
import java.time._

import breeze.linalg._
import breeze.stats._
import com.typesafe.config.ConfigFactory
import org.apache.spark.SparkConf
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Dataset, SparkSession}

case class PageStatRow(page_id:Long, mean:Double, variance:Double)
case class PageVisitElapsed(page_id:Long, elapsed_hours:Int, count:Double)
case class PagecountMetadata(start_time:Timestamp, end_time:Timestamp)


class PageStats(val dbHost:String, val dbPort:Int, val dbUsername:String, val dbPassword:String) extends PageCountStatsLoader with Serializable {
  lazy val sconf = new SparkConf().setAppName("Wikipedia pagestats computation")
        .set("spark.cassandra.connection.host", dbHost)
        .set("spark.cassandra.connection.port", dbPort.toString)
        .set("spark.cassandra.auth.username", dbUsername)
        .set("spark.cassandra.auth.password", dbPassword)
       
  lazy val session = SparkSession.builder.config(sconf).getOrCreate()
  

  
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
    val meta = loadMetadata(session, keySpace, tableMeta).collect()
    val min_start_time = meta.map(m => m.start_time.toInstant).min
    val max_end_time = meta.map(m => m.end_time.toInstant).max
    val visitsPeriod = Duration.between(min_start_time, max_end_time)
    val totalHours = visitsPeriod.toHours.toInt
    
    val visitData = getVisits(session, keySpace, tableVisits).map(p => PageVisitElapsed(p.page_id, Duration.between(min_start_time, p.visit_time.toInstant).toHours.toInt, p.count))
    
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
    val cfgBase = new ConfigFileOpt(args)
    val cfgDefault = ConfigFactory.parseString("cassandra.db.port=9042")
    val cfg = ConfigFactory.parseFile(new File(cfgBase.cfgFile())).withFallback(cfgDefault)
    val ps = new PageStats(cfg.getString("cassandra.db.host"), cfg.getInt("cassandra.db.port"),
                          cfg.getString("cassandra.db.username"), cfg.getString("cassandra.db.password"))
    
    ps.updateStats(cfg.getString("cassandra.db.keyspace"), cfg.getString("cassandra.db.tableVisits"),
                   cfg.getString("cassandra.db.tableStats"), cfg.getString("cassandra.db.tableMeta"))
  }
}