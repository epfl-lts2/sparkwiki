package ch.epfl.lts2.wikipedia
import java.nio.file.Paths
import org.rogach.scallop._
import org.apache.spark.sql.{SQLContext, Row, DataFrame, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

class ProcessorConf(args:Seq[String]) extends ScallopConf(args) {
  val dumpPath = opt[String](required = true, name="dumpPath")
  val outputPath = opt[String](required = true, name="outputPath")
  val namePrefix = opt[String](required = true, name="namePrefix")
  verify()
}

object DumpProcessor  {
  def main(args:Array[String]) = {
    val conf = new ProcessorConf(args)
    val dumpParser = new DumpParser
    val sconf = new SparkConf().setAppName("Wikipedia dump processor").setMaster("local[*]")
    val session = SparkSession.builder.config(sconf).getOrCreate()
    
    val pageFile = Paths.get(conf.dumpPath(), conf.namePrefix() + "-page.sql.bz2").toString
    val pageOutput = Paths.get(conf.outputPath(), "page")
    val pageDf = dumpParser.processToDf(session, pageFile, WikipediaDumpType.Page)
    
    
    val pageLinksFile = Paths.get(conf.dumpPath(), conf.namePrefix() + "-pagelinks.sql.bz2").toString
    val pageLinksOutput = Paths.get(conf.outputPath(), "pagelinks").toString
    val pageLinksDf = dumpParser.processToDf(session, pageLinksFile, WikipediaDumpType.PageLinks)
    
    val categoryLinksFile = Paths.get(conf.dumpPath(), conf.namePrefix() + "-categorylinks.sql.bz2").toString
    val categoryLinksOutput = Paths.get(conf.outputPath(), "categorylinks").toString
    val categoryLinksDf = dumpParser.processToDf(session, categoryLinksFile, WikipediaDumpType.CategoryLinks)
    
    val redirectFile = Paths.get(conf.dumpPath(), conf.namePrefix() + "-redirect.sql.bz2").toString
    val redirectOutput = Paths.get(conf.outputPath(), "redirect").toString
    val redirectDf = dumpParser.processToDf(session, redirectFile, WikipediaDumpType.Redirect)
    
    import session.implicits._
    val pagelinks_id = pageLinksDf.join(pageDf, Seq("title", "namespace"))
                                  .select("from", "id", "title", "fromNamespace", "namespace")
    
    dumpParser.writeCsv(pagelinks_id, pageLinksOutput)
                                  
    val redirect_id = redirectDf.withColumn("id", redirectDf.col("from"))
                              .join(pageDf.drop(pageDf.col("title")), "id")
                              .select("from", "targetNamespace", "title")
                              .withColumn("namespace", redirectDf.col("targetNamespace"))
                              .join(pageDf, Seq("title", "namespace")).select("from", "id", "title")
                              
    dumpParser.writeCsv(redirect_id, redirectOutput)
    
    val cat_pages = pageDf.filter($"namespace" === WikipediaNamespace.Category)
                          .select("id", "title", "isRedirect", "isNew")
    val catlinks_id = categoryLinksDf.withColumn("id", categoryLinksDf.col("from"))
                              .join(pageDf, "id") // restrict to existing pages
                              .select("from", "to", "ctype")
                              .withColumn("title", categoryLinksDf.col("to"))
                              .join(cat_pages.select("id", "title"), "title")
                              .select("from", "title", "id", "ctype")
    dumpParser.writeCsv(catlinks_id, categoryLinksOutput)
    dumpParser.writeCsv(cat_pages, pageOutput.resolve("category_pages").toString)
    
    val normal_pages = pageDf.filter($"namespace" === WikipediaNamespace.Page)
                             .select("id", "title", "isRedirect", "isNew")
    
    dumpParser.writeCsv(normal_pages, pageOutput.resolve("normal_pages").toString)
    
    
  }
}
