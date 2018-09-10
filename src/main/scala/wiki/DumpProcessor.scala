package ch.epfl.lts2.wiki
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

class DumpProcessor extends App {
  def writeCsv(df:DataFrame, outputPath:String) = {
    df.write.option("delimiter", "\t")
            .option("header", false)
            .option("quote", "")
            .option("compression", "gzip")
            .csv(outputPath)
  }
  
  override def main(args:Array[String]) = {
    val conf = new ProcessorConf(args)
    val dumpParser = new DumpParser
    val sconf = new SparkConf().setAppName("Wikipedia dump processor").setMaster("local[*]")
    val session = SparkSession.builder.config(sconf).getOrCreate()
    
    val pageFile = conf.namePrefix() + "-page.sql.bz2"
    val pageOutput = Paths.get(conf.outputPath(), "page")
    val pageDf = dumpParser.processToDf(session, pageFile, WikipediaDumpType.Page)
    
    
    val pageLinksFile = conf.namePrefix() + "-pagelinks.sql.bz2"
    val pageLinksOutput = Paths.get(conf.outputPath(), "pagelinks").toString
    val pageLinksDf = dumpParser.processToDf(session, pageLinksFile, WikipediaDumpType.PageLinks)
    
    val categoryLinksFile = conf.namePrefix() + "-categorylinks.sql.bz2"
    val categoryLinksOutput = Paths.get(conf.outputPath(), "categorylinks").toString
    val categoryLinksDf = dumpParser.processToDf(session, categoryLinksFile, WikipediaDumpType.CategoryLinks)
    
    val redirectFile = conf.namePrefix() + "-redirect.sql.bz2"
    val redirectOutput = Paths.get(conf.outputPath(), "redirect").toString
    val redirectDf = dumpParser.processToDf(session, redirectFile, WikipediaDumpType.Redirect)
    
    import session.implicits._
    val pagelinks_id = pageLinksDf.join(pageDf, Seq("title", "namespace"))
                                  .select("from", "id", "title", "fromNamespace", "namespace")
                                  
    val redirect_id = redirectDf.withColumn("id", redirectDf.col("from"))
                              .join(pageDf.drop(pageDf.col("title")), "id")
                              .select("from", "targetNamespace", "title")
                              .withColumn("namespace", redirectDf.col("targetNamespace"))
                              .join(pageDf, Seq("title", "namespace")).select("from", "id", "title")
                              
    val cat_pages = pageDf.filter($"namespace" === WikipediaNamespace.Category)
                          .select("id", "title", "isRedirect", "isNew")
    val catlinks_id = categoryLinksDf.withColumn("id", categoryLinksDf.col("from"))
                              .join(pageDf, "id") // restrict to existing pages
                              .select("from", "to", "ctype")
                              .withColumn("title", categoryLinksDf.col("to"))
                              .join(cat_pages.select("id", "title"), "title")
                              .select("from", "title", "id", "ctype")  
    
    val normal_pages = pageDf.filter($"namespace" === WikipediaNamespace.Page)
                             .select("id", "title", "isRedirect", "isNew")
    
    writeCsv(normal_pages, pageOutput.resolve("normal_pages").toString)
    writeCsv(cat_pages, pageOutput.resolve("category_pages").toString)
    writeCsv(pagelinks_id, pageLinksOutput)
    writeCsv(redirect_id, redirectOutput)
    writeCsv(catlinks_id, categoryLinksOutput)
    
  }
}