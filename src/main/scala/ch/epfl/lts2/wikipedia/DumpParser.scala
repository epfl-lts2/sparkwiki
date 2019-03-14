package ch.epfl.lts2.wikipedia
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.rogach.scallop._

class ParserConf(args: Seq[String]) extends ScallopConf(args) with Serialization {
  val dumpFilePath = opt[String](required = true, name= "dumpFilePath")
  val dumpType = opt[String](required = true, name="dumpType")
  val outputPath = opt[String](required = true, name="outputPath")
  val outputFormat = opt[String](name="outputFormat", default=Some("csv"))
  verify()
}

class DumpParser extends Serializable  with CsvWriter {
  
  def splitSqlInsertLine(line: String):String = {
    line.split(" VALUES ")(1).trim
  }
    
  
  def writeParquet(df:DataFrame, outputPath: String) =  {
    df.write.mode("overwrite").option("compression", "gzip").parquet(outputPath)
  }
  
  def processToDf(session: SparkSession, input:RDD[String], dumpType:WikipediaDumpType.Value):DataFrame = {
    
    val sqlLines = input.filter(l => l.startsWith("INSERT INTO `%s` VALUES".format(dumpType)))
    val records = sqlLines.map(l => splitSqlInsertLine(l))
    val parser = dumpType match {
      case WikipediaDumpType.Page => new WikipediaPageParser
      case WikipediaDumpType.PageLinks => new WikipediaPageLinkParser
      case WikipediaDumpType.Redirect => new WikipediaRedirectParser
      case WikipediaDumpType.Category => new WikipediaCategoryParser
      case WikipediaDumpType.CategoryLinks => new WikipediaCategoryLinkParser
    }
    
    parser.getDataFrame(session, records)
  }
  
  def processFileToDf(session: SparkSession, inputFilename:String, dumpType:WikipediaDumpType.Value):DataFrame = {
    val lines = session.sparkContext.textFile(inputFilename, 4)
    processToDf(session, lines, dumpType)
  }
  
  def process(session: SparkSession, inputFilename:String, dumpType:WikipediaDumpType.Value, outputPath:String, outputFormat:String) = {
    val df = processFileToDf(session, inputFilename, dumpType)
    outputFormat match {
      case "parquet" => writeParquet(df, outputPath)
      case _ => writeCsv(df, outputPath)
    }

  }
}

object DumpParser 
{
  val dumpParser = new DumpParser
  // main 
  def main(args:Array[String]) = 
  {
    val conf = new ParserConf(args) // TODO detect type from CREATE TABLE statement
    println("Reading %s".format(conf.dumpFilePath()))
    val dumpType = conf.dumpType()
    val outputFormat = conf.outputFormat()
    val sconf = new SparkConf().setAppName("Wikipedia dump parser")
    val session = SparkSession.builder.config(sconf).getOrCreate()
    assert(WikipediaNamespace.Page == 0)
    assert(WikipediaNamespace.Category == 14)
    val dumpEltType = dumpType match {
      case "page" => WikipediaDumpType.Page
      case "pagelinks" => WikipediaDumpType.PageLinks
      case "redirect" => WikipediaDumpType.Redirect
      case "category" => WikipediaDumpType.Category
      case "categorylinks" => WikipediaDumpType.CategoryLinks
    }
    dumpParser.process(session, conf.dumpFilePath(), dumpEltType, conf.outputPath(), conf.outputFormat())

  }
}

