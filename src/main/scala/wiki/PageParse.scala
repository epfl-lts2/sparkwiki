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
  
  def splitSqlInsertLine(line: String):List[String] = {
    val spl = line.split(" VALUES ")(1).trim
    val splt = spl.substring(1, spl.length - 2)
    splt.split("\\),\\(").toList
    
  }
    
  
  def main(args: Array[String]) {
    val filePath = args(0)
    val fileType = args(1) // TODO detect file type from CREATE TABLE statement
    val outputPath = args(2)
    println("Reading %s".format(filePath))
    
    val conf = new SparkConf().setAppName("Wikipedia dump parser").setMaster("local[*]")
    val sc = new SparkContext(conf)
    
    val wp = new WikipediaPageParser
    val lines = sc.textFile(filePath, 4)
    val sqlLines = lines.filter(l => l.startsWith("INSERT INTO `%s` VALUES".format(fileType)))
    val records = sqlLines.flatMap(l => splitSqlInsertLine(l))
    val page_records = records.map(l => wp.parseLine(l)).filter(w => w.namespace == 0) // keep only namespace 0
    val page_csv = page_records.map(p => p.toCsv)
    page_csv.saveAsTextFile(outputPath)
    println("File contains %d lines, %d inserts and %d records".format(lines.count(), sqlLines.count(), page_records.count()))
    
  }

}

