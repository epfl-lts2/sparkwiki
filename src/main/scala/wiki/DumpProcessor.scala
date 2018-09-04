package wiki
import org.rogach.scallop._
import org.apache.spark.sql.{SQLContext, Row, DataFrame, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

class ProcessorConf(args:Seq[String]) extends ScallopConf(args) {
  val dumpPath = opt[String](required = true, name="dumpPath")
  val outputPath = opt[String](required = true, name="outputPath")
  val namePrefix = opt[String](required = true, name="namePrefix")
  verify()
}

class DumpProcessor {
  def main(args:Array[String]) = {
    val conf = new ProcessorConf(args)
    val dumpParser = new DumpParser
    val sconf = new SparkConf().setAppName("Wikipedia dump processor").setMaster("local[*]")
    val session = SparkSession.builder.config(sconf).getOrCreate()
    
    val pageFile = conf.namePrefix() + "-page.sql.bz2"
    val pageOutput = conf.outputPath() + "/page"
    dumpParser.process(session, pageFile, "page", pageOutput, "csv")
    dumpParser.process(session, pageFile, "page", pageOutput+".p", "parquet") // need parquet for joins
    
    val pageLinksFile = conf.namePrefix() + "-pagelinks.sql.bz2"
    val pageLinksOutput = conf.outputPath() + "/pagelinks"
    dumpParser.process(session, pageLinksFile, "pagelinks", pageOutput+".p", "parquet") // need parquet for joins
  }
}