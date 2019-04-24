package ch.epfl.lts2.wikipedia

import java.io.{File, FileWriter, PrintWriter}
import java.nio.file.Paths
import java.time._

import breeze.linalg.VectorBuilder
import org.rogach.scallop._
import com.typesafe.config.ConfigFactory
import org.apache.spark.SparkConf
import org.apache.spark.sql.{Dataset, SparkSession}
import com.github.tototoshi.csv._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

class TimeSeriesExtractorConf(args:Seq[String]) extends ScallopConf(args) with Serialization {
  val cfgFile = opt[String](name="config", required=true)
  val outputPath = opt[String](name="outputPath", required=true)
  val startDate = opt[LocalDate](required = true, name="startDate")(singleArgConverter[LocalDate](LocalDate.parse(_)))
  val endDate = opt[LocalDate](required = true, name="endDate")(singleArgConverter[LocalDate](LocalDate.parse(_)))
  val pageIds = trailArg[List[Long]](required = true)
  verify()
}

class TimeSeriesExtractor(dbHost:String, dbPort:Int, dbUsername:String, dbPassword:String,
                          keySpace: String, tableVisits:String, tableMeta:String)
  extends PageCountStatsLoader with CsvWriter with Serializable {

  lazy val sparkConfig: SparkConf = new SparkConf().setAppName("Wikipedia activity detector")
    .set("spark.cassandra.connection.host", dbHost)
    .set("spark.cassandra.connection.port", dbPort.toString)
    .set("spark.cassandra.auth.username", dbUsername)
    .set("spark.cassandra.auth.password", dbPassword)

  lazy val session: SparkSession = SparkSession.builder.config(sparkConfig).getOrCreate()

  def getTimeSeries(startDate:LocalDate, endDate:LocalDate, pageIds:Seq[Long]): Dataset[PageVisitGroup] = {
    getVisitsTimeSeriesGroup(session, keySpace, tableVisits, tableMeta, startDate, endDate)
              .filter(p => pageIds.contains(p.page_id))
  }



  def writeOutput(visits:Dataset[PageVisitGroup], outputPath:String, startTime:LocalDateTime, totalHours:Int) = {
    import session.implicits._
    val visitVec = visits.map(p => PageVisitElapsedGroup(p.page_id, p.visits.map(v => (Duration.between(startTime, v._1.toLocalDateTime).toHours.toInt, v._2.toDouble))))
                    .map(p => (p.page_id, new VectorBuilder(p.visits.map(f => f._1).toArray, p.visits.map(f => f._2).toArray, p.visits.size, totalHours).toDenseVector.toArray))


    visitVec.foreach(v => writeCsv(session.sparkContext.parallelize(v._2.toList).toDF, Paths.get(outputPath, v._1+".csv").toString, false, true))
  }
}

object TimeSeriesExtractor {
  def main(args:Array[String]) = {
    val cfgBase = new TimeSeriesExtractorConf(args)
    val cfgDefault = ConfigFactory.parseString("cassandra.db.port=9042")
    val cfg = ConfigFactory.parseFile(new File(cfgBase.cfgFile())).withFallback(cfgDefault)
    val tse = new TimeSeriesExtractor(cfg.getString("cassandra.db.host"), cfg.getInt("cassandra.db.port"),
      cfg.getString("cassandra.db.username"), cfg.getString("cassandra.db.password"),
      cfg.getString("cassandra.db.keyspace"), cfg.getString("cassandra.db.tableVisits"),
      cfg.getString("cassandra.db.tableMeta"))

    val startTime = cfgBase.startDate().atStartOfDay
    val visitsPeriod = Duration.between(startTime, cfgBase.endDate().plusDays(1).atStartOfDay)
    val totalHours = visitsPeriod.toHours.toInt
    val visits = tse.getTimeSeries(cfgBase.startDate(), cfgBase.endDate(), cfgBase.pageIds())

    tse.writeOutput(visits, cfgBase.outputPath(), startTime, totalHours)

  }
}
