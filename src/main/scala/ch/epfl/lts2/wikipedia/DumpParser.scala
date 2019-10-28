package ch.epfl.lts2.wikipedia
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.lit
import org.rogach.scallop._
import java.nio.file.Paths

class ParserConf(args: Seq[String]) extends ScallopConf(args) with Serialization {
  val dumpFilePaths = opt[List[String]](required = true, name= "dumpFilePaths")
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

  def splitFilename(fileName:String): DumpInfo = {
    // filename has format: enwiki-20180801-langlinks.sql.bz2
    val p = Paths.get(fileName)
    val spl = p.getFileName.toString.split('-')
    val dt = spl(2).split('.')
    DumpInfo(spl(0).stripSuffix("wiki"), spl(1), dt(0))
  }

  def process(session: SparkSession, inputFilenames:List[String], dumpType:WikipediaDumpType.Value, outputPath:String, outputFormat:String) = {
    val df = inputFilenames.map(f => processFileToDf(session, f, dumpType).withColumn("languageCode", lit(splitFilename(f).langCode)))
                           .reduce((p1, p2) => p1.union(p2))

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
    val conf = new ParserConf(args)
    val dumpType = conf.dumpType()
    val outputFormat = conf.outputFormat()
    val sconf =  new SparkConf().setAppName("Wikipedia dump parser")
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

    dumpParser.process(session, conf.dumpFilePaths(), dumpEltType, conf.outputPath(), conf.outputFormat())

  }
}

