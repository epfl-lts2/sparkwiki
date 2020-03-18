package ch.epfl.lts2.wikipedia

import java.nio.file.Paths

import ch.epfl.lts2.wikipedia.DumpProcessor.dp
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.rogach.scallop.{ScallopConf, Serialization}

class LangProcessorConf(args:Seq[String]) extends ScallopConf(args) with Serialization {
  val dumpPath = opt[String](required = true, name="dumpPath")
  val outputPath = opt[String](required = true, name="outputPath")
  val namePrefix = opt[String](required = true, name="namePrefix")
  val toLang = opt[String](required = true, name="toLang")
  verify()
}

class DumpLangProcessor extends Serializable {
  lazy val sconf = new SparkConf().setAppName("Wikipedia dump processor")
  lazy val session = SparkSession.builder.config(sconf).getOrCreate()

}

object DumpLangProcessor {

  val dlp = new DumpLangProcessor

  def main(args:Array[String]) = {
    import dlp.session.implicits._

    val conf = new LangProcessorConf(args)
    val dumpParser = new DumpParser
    //val sparkWikiParser = new SparkWikiParser


    val langLinksFile = Paths.get(conf.dumpPath(), conf.namePrefix() + "-langlinks.sql.bz2").toString
    val langLinksOutput = Paths.get(conf.outputPath(), conf.toLang.getOrElse("en")+"-langlinks").toString
    val langLinksDf = dumpParser.processFileToDf(dp.session, langLinksFile, WikipediaDumpType.LangLinks, conf.toLang.getOrElse("en") ).as[WikipediaLangLink]

    val normal_pages_langLinks = langLinksDf.filter( t => !t.title.contains(":") && t.title != "" )

    dumpParser.writeCsv(normal_pages_langLinks.select("from", "title", "lang"), langLinksOutput)

  }
}