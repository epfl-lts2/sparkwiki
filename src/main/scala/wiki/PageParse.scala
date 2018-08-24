package wiki
import java.io.File

import com.opencsv.CSVParser
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.{SparkConf, SparkContext}

import org.apache.spark.sql.{SQLContext, Row, DataFrame}

import scala.RuntimeException

object PageParse {
  def main(args: Array[String]) {
    var pageFilePath = args(0)
    println("Using %s".format(pageFilePath))
    val conf = new SparkConf().setAppName("Wikipedia dump parser").setMaster("local[2]")
    val sc = new SparkContext(conf)
    val file = sc.textFile(pageFilePath, 4)
  }

}

