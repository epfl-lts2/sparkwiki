package wiki
import java.io.File

import com.opencsv.CSVParser
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.{SparkConf, SparkContext}

import org.apache.spark.sql.{SQLContext, Row, DataFrame, SparkSession}

import scala.RuntimeException
import org.rogach.scallop._

class Conf(args: Seq[String]) extends ScallopConf(args) {
  val dumpFilePath = opt[String](required = true, name= "dumpFilePath")
  val dumpType = opt[String](required = true, name="dumpType")
  val outputPath = opt[String](required = true, name="outputPath")
  verify()
}

object DumpParser {
  
  def splitSqlInsertLine(line: String):List[String] = {
    val spl = line.split(" VALUES ")(1).trim
    val splt = spl.substring(1, spl.length - 2)
    splt.split("\\),\\(").toList
    
  }
    
  def writeCsv(df:DataFrame, outputPath:String) = {
    df.write.option("delimiter", "\t")
            .option("header", false)
            .option("quote", "")
            .csv(outputPath)
  }
  
  def readPages(lines:RDD[String], outputPath:String, session: SparkSession) = {
    val wp = new WikipediaPageParser
    
    // keep only namespace 0 and remove dummies for now
    val page_records = lines.map(l => wp.parseLine(l)).filter(w => w.namespace == 0 && w.id > 0)
    
    val page_df = session.createDataFrame(page_records).select("id", "namespace", "title", "isRedirect", "isNew")
    writeCsv(page_df, outputPath)
  }
  
  def readPageLinks(lines:RDD[String], outputPath:String, session:SparkSession) = {
    val wp = new WikipediaPageLinkParser
    val pl_records = lines.map(l => wp.parseLine(l)).filter(w => w.namespace == 0 && w.fromNamespace == 0)
    
    val pl_df = session.createDataFrame(pl_records)
    writeCsv(pl_df, outputPath)
  }
  
  def main(args: Array[String]) {
    val conf = new Conf(args) // TODO detect type from CREATE TABLE statement
    println("Reading %s".format(conf.dumpFilePath()))
    val dumpType = conf.dumpType()
    val sconf = new SparkConf().setAppName("Wikipedia dump parser").setMaster("local[*]")
    val session = SparkSession.builder.config(sconf).getOrCreate()
    val sctx = session.sparkContext
    
    
    val lines = sctx.textFile(conf.dumpFilePath(), 4)
    
    val sqlLines = lines.filter(l => l.startsWith("INSERT INTO `%s` VALUES".format(dumpType)))
    val records = sqlLines.flatMap(l => splitSqlInsertLine(l))
    dumpType match {
      case "page" => readPages(records, conf.outputPath(), session)
      case "pagelinks" => readPageLinks(records, conf.outputPath(), session)
    }
    
  }

}

