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
    val spl = line.split(" VALUES ")(1).trim
    val splt = spl.substring(1, spl.length - 2)
    splt.split("\\),\\(").toList
    
  }
    
  
  def main(args: Array[String]) {
    var filePath = args(0)
    var fileType = args(1) // TODO detect file type from CREATE TABLE statement
    println("Reading %s".format(filePath))
    
    val conf = new SparkConf().setAppName("Wikipedia dump parser").setMaster("local[2]")
    val sc = new SparkContext(conf)
    
    val lines = sc.textFile(filePath, 4)
    val sqlLines = lines.filter(l => l.startsWith("INSERT INTO `%s` VALUES".format(fileType)))
    val records = sqlLines.flatMap(l => parseLine(l))
    val page_records = records.map(l => new WikipediaPage(l))
    val page_csv = page_records.map(p => p.toCsv)
    println("File contains %d lines, %d inserts and %d records".format(lines.count(), sqlLines.count(), page_records.count()))
    
  }

}

