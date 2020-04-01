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
  val langList = opt[List[String]](required=false, name="languages", default=Some(List()))
  verify()
}

class DumpLangProcessor extends Serializable {
  lazy val sconf = new SparkConf().setAppName("Wikipedia dump processor")
  lazy val session = SparkSession.builder.config(sconf).getOrCreate()
  def customFilter(t:WikipediaLangLink):Boolean = {
    !t.title.contains(":")
  }
}

object DumpLangProcessor {

  val dlp = new DumpLangProcessor


  def main(args:Array[String]) = {
    import dlp.session.implicits._

    val conf = new LangProcessorConf(args)
    val dumpParser = new DumpParser
    val languages = conf.langList()
    val custFilter = if (languages.isEmpty)
      new ElementFilter[WikipediaLangLink] {
        override def filterElt(t: WikipediaLangLink): Boolean = !t.title.contains(":")
      }
    else
      new ElementFilter[WikipediaLangLink] {
        override def filterElt(t: WikipediaLangLink): Boolean = !t.title.contains(":") && languages.contains(t.lang)
      }


    val langLinksFile = Paths.get(conf.dumpPath(), conf.namePrefix() + "-langlinks.sql.bz2").toString
    val langLinksOutput = Paths.get(conf.outputPath(), "langlinks").toString
    val langLinksDf = dumpParser.processFileToDf(dp.session, langLinksFile, WikipediaDumpType.LangLinks, custFilter).as[WikipediaLangLink]


    dumpParser.writeCsv(langLinksDf.select("from", "title", "lang"), langLinksOutput)

  }
}