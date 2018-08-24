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
  
  def parseLine(line: String):List[String] = {
    val grp = """\((.*?)\)""".r
    val spl = line.split(" VALUES ")(1)
    grp.findAllIn(spl).toList
    
  }
    
  
  def main(args: Array[String]) {
    var filePath = args(0)
    var fileType = args(1)
    println("Reading %s".format(filePath))
    
    val conf = new SparkConf().setAppName("Wikipedia dump parser").setMaster("local[2]")
    val sc = new SparkContext(conf)
    
    val lines = sc.textFile(filePath, 4)
    val sqlLines = lines.filter(l => l.startsWith("INSERT INTO `%s` VALUES".format(fileType)))
    val records = sqlLines.map(l => parseLine(l))
    val rec_len = records.map(_.length)
    println("File contains %d lines, %d inserts and %d records".format(lines.count(), sqlLines.count(), rec_len.sum().toInt))
    
  }

}

