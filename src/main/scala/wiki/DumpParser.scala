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
  val neo4j = opt[Boolean](name="neo4j")
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
    val parser = dumpType match {
      case "page" => new WikipediaPageParser
      case "pagelinks" => new WikipediaPageLinkParser
      case "redirect" => new WikipediaRedirectParser
    }
    
    val df = parser.getDataFrame(session, records)
    writeCsv(df, conf.outputPath())
    
    
  }

}

