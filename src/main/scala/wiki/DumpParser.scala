package wiki
import java.io.File

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.{SparkConf, SparkContext}

import org.apache.spark.sql.{SQLContext, Row, DataFrame, SparkSession}

import scala.RuntimeException
import org.rogach.scallop._

class ParserConf(args: Seq[String]) extends ScallopConf(args) {
  val dumpFilePath = opt[String](required = true, name= "dumpFilePath")
  val dumpType = opt[String](required = true, name="dumpType")
  val outputPath = opt[String](required = true, name="outputPath")
  val outputFormat = opt[String](name="outputFormat", default=Some("csv"))
  verify()
}

object DumpParser {
  
  def splitSqlInsertLine(line: String):String = {
    line.split(" VALUES ")(1).trim
  }
    
  def writeCsv(df:DataFrame, outputPath:String) = {
    df.write.option("delimiter", "\t")
            .option("header", false)
            .option("quote", "")
            .option("compression", "gzip")
            .csv(outputPath)
  }
  
  def writeParquet(df:DataFrame, outputPath: String) =  {
    df.write.option("compression", "gzip").parquet(outputPath)
  }
  
  
  def main(args: Array[String]) {
    val conf = new ParserConf(args) // TODO detect type from CREATE TABLE statement
    println("Reading %s".format(conf.dumpFilePath()))
    val dumpType = conf.dumpType()
    val outputFormat = conf.outputFormat()
    val sconf = new SparkConf().setAppName("Wikipedia dump parser").setMaster("local[*]")
    val session = SparkSession.builder.config(sconf).getOrCreate()
    val sctx = session.sparkContext
    
    
    val lines = sctx.textFile(conf.dumpFilePath(), 4)
    
    val sqlLines = lines.filter(l => l.startsWith("INSERT INTO `%s` VALUES".format(dumpType)))
    val records = sqlLines.map(l => splitSqlInsertLine(l))
    val parser = dumpType match {
      case "page" => new WikipediaPageParser
      case "pagelinks" => new WikipediaPageLinkParser
      case "redirect" => new WikipediaRedirectParser
      case "category" => new WikipediaCategoryParser
      case "categorylinks" => new WikipediaCategoryLinkParser
    }
    
    val df = parser.getDataFrame(session, records)
    outputFormat match {
      case "parquet" => writeParquet(df, conf.outputPath())
      case _ => writeCsv(df, conf.outputPath())
    }
    
    
    
  }

}

