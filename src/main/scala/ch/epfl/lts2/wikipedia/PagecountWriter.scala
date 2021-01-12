package ch.epfl.lts2.wikipedia

import org.apache.spark.sql.{Dataset, SparkSession}

import java.nio.file.Paths
import java.sql.Timestamp
import java.time.LocalDate

abstract class PagecountWriter extends Serializable {
  def updateMeta(startDate:LocalDate, endDate:LocalDate)
  def writeData(data:Dataset[PageVisitRow])
}

class CassandraPagecountWriter(val session: SparkSession,
                               val keySpace: String, val tableVisits:String, val tableMeta:String)
  extends PagecountWriter {

  override def updateMeta(startDate: LocalDate, endDate: LocalDate): Unit = {
    import session.implicits._

    val newData = PagecountMetadata(Timestamp.valueOf(startDate.atStartOfDay), Timestamp.valueOf(endDate.atStartOfDay))
    val updatedData = session.sparkContext.parallelize(Seq(newData)).toDF.as[PagecountMetadata]
    updatedData.write
      .format("org.apache.spark.sql.cassandra")
      .option("keyspace", keySpace)
      .option("table", tableMeta)
      .mode("append")
      .save()
  }

  override def writeData(data: Dataset[PageVisitRow]): Unit = {
    data.write
      .format("org.apache.spark.sql.cassandra")
      .option("confirm.truncate","true")
      .option("keyspace", keySpace)
      .option("table", tableVisits)
      .mode("append")
      .save()
  }
}

class ParquetPagecountWriter(val session: SparkSession, val basePath: String) extends PagecountWriter {

  override def updateMeta(startDate: LocalDate, endDate: LocalDate): Unit = {
    import session.implicits._
    val pathMeta = Paths.get(basePath, Constants.META_DIR).toString

    val newData = PagecountMetadata(Timestamp.valueOf(startDate.atStartOfDay), Timestamp.valueOf(endDate.atStartOfDay))
    val updatedData = session.sparkContext.parallelize(Seq(newData)).toDF.as[PagecountMetadata]
    updatedData.write.mode("append").option("compression", "gzip").parquet(pathMeta)
  }

  override def writeData(data: Dataset[PageVisitRow]): Unit = {
    val pathPage = Paths.get(basePath, Constants.PGCOUNT_DIR).toString
    data.write.mode("append").option("compression", "gzip").parquet(pathPage)
  }
}
