package ch.epfl.lts2.wikipedia
import java.nio.file.Paths

import org.apache.spark.SparkConf
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.rogach.scallop._


class ProcessorConf(args:Seq[String]) extends ScallopConf(args) with Serialization {
  val dumpPath = opt[String](required = true, name="dumpPath")
  val outputPath = opt[String](required = true, name="outputPath")
  val namePrefix = opt[String](required = true, name="namePrefix")
  verify()
}


class DumpProcessor extends Serializable {
  lazy val sconf = new SparkConf().setAppName("Wikipedia dump processor")
  lazy val session = SparkSession.builder.config(sconf).getOrCreate()
  
  def mergePageLink(pageDf:Dataset[WikipediaPage], pageLinkDf:Dataset[WikipediaPageLink]): Dataset[MergedPageLink] = {
    import session.implicits._
    pageLinkDf.join(pageDf, Seq("title", "namespace"))
              .select("from", "id", "title", "fromNamespace", "namespace")
              .as[MergedPageLink]
  }
  
  def mergeRedirect(pageDf:Dataset[WikipediaPage], redirectDf: Dataset[WikipediaRedirect]):Dataset[MergedRedirect] = {
    import session.implicits._
    redirectDf.withColumn("id", redirectDf.col("from"))
                .join(pageDf.drop(pageDf.col("title")), "id")
                .select("from", "targetNamespace", "title")
                .withColumn("namespace", redirectDf.col("targetNamespace"))
                .join(pageDf, Seq("title", "namespace")).select("from", "id", "title")
                .as[MergedRedirect]
  }
  
  def mergeCategoryLinks(pageDf: Dataset[WikipediaPage], categoryPageDf: Dataset[WikipediaSimplePage], catLinkDf: Dataset[WikipediaCategoryLink]): Dataset[MergedCatlink] = {
    import session.implicits._
    catLinkDf.withColumn("id", catLinkDf.col("from"))
                              .join(pageDf, "id") // restrict to existing pages
                              .select("from", "to", "ctype")
                              .withColumn("title", catLinkDf.col("to"))
                              .join(categoryPageDf.select("id", "title"), "title")
                              .select("from", "title", "id", "ctype")
                              .as[MergedCatlink]
  }
  
  def getPagesByNamespace(pageDf: Dataset[WikipediaPage], ns: Int, keepRedirect: Boolean): Dataset[WikipediaSimplePage] = {
    import session.implicits._
    pageDf.filter(p => p.namespace == ns && (keepRedirect || !p.isRedirect))
          .select("id", "title", "isRedirect", "isNew", "lang").as[WikipediaSimplePage]
  }
  
  def resolvePageRedirects(pgLinksIdDf:Dataset[MergedPageLink], redirectDf:Dataset[MergedRedirect], pages:Dataset[WikipediaSimplePage]):DataFrame = {
    import session.implicits._
    val linksDf = pgLinksIdDf.withColumn("inter", pgLinksIdDf.col("id"))
                   .join(redirectDf.withColumn("inter", redirectDf.col("from")).withColumnRenamed("from", "from_r").withColumnRenamed("id", "to_r"), Seq("inter"), "left")
                   .withColumn("dest", when(col("to_r").isNotNull, col("to_r")).otherwise(col("id")))
                   .select("from", "dest")
                   .filter($"from" !== $"dest") // remove self-links
                   .distinct // redirect removal will cause duplicates -> remove them
                   
    // some redirect pages have regular links -> remove them
    linksDf.withColumn("id", linksDf.col("from"))
           .join(pages, "id")
           .filter($"isRedirect" === false)
           .select("from", "dest")
               
  }
                          
}

object DumpProcessor  {
  val dp = new DumpProcessor
  
  
  def main(args:Array[String]) = {
    import dp.session.implicits._
    val conf = new ProcessorConf(args)
    val dumpParser = new DumpParser
    
    
    val pageFile = Paths.get(conf.dumpPath(), conf.namePrefix() + "-page.sql.bz2").toString
    val pageOutput = Paths.get(conf.outputPath(), "page")
    val pageDf = dumpParser.processFileToDf(dp.session, pageFile, WikipediaDumpType.Page).as[WikipediaPage]
    
    val pageLinksFile = Paths.get(conf.dumpPath(), conf.namePrefix() + "-pagelinks.sql.bz2").toString
    val pageLinksOutput = Paths.get(conf.outputPath(), "pagelinks").toString
    val pageLinksDf = dumpParser.processFileToDf(dp.session, pageLinksFile, WikipediaDumpType.PageLinks).as[WikipediaPageLink]
    
    val categoryLinksFile = Paths.get(conf.dumpPath(), conf.namePrefix() + "-categorylinks.sql.bz2").toString
    val categoryLinksOutput = Paths.get(conf.outputPath(), "categorylinks").toString
    val categoryLinksDf = dumpParser.processFileToDf(dp.session, categoryLinksFile, WikipediaDumpType.CategoryLinks).as[WikipediaCategoryLink]
    
    val redirectFile = Paths.get(conf.dumpPath(), conf.namePrefix() + "-redirect.sql.bz2").toString
    val redirectOutput = Paths.get(conf.outputPath(), "redirect").toString
    val redirectDf = dumpParser.processFileToDf(dp.session, redirectFile, WikipediaDumpType.Redirect).as[WikipediaRedirect]
    
    
    val pagelinks_id = dp.mergePageLink(pageDf, pageLinksDf)
    val normal_pages = dp.getPagesByNamespace(pageDf, WikipediaNamespace.Page, false)
    val cat_pages = dp.getPagesByNamespace(pageDf, WikipediaNamespace.Category, false)
    //dumpParser.writeCsv(pagelinks_id.toDF, pageLinksOutput)
    
    val redirect_id = dp.mergeRedirect(pageDf, redirectDf)                          
    //dumpParser.writeCsv(redirect_id.toDF, redirectOutput)
    val pglinks_noredirect = dp.resolvePageRedirects(pagelinks_id, redirect_id, normal_pages.union(cat_pages))
    dumpParser.writeCsv(pglinks_noredirect, pageLinksOutput)
   
    val catlinks_id = dp.mergeCategoryLinks(pageDf, cat_pages, categoryLinksDf)
    dumpParser.writeCsv(catlinks_id.toDF, categoryLinksOutput)
    dumpParser.writeCsv(cat_pages.toDF, pageOutput.resolve("category_pages").toString)
    
    
    dumpParser.writeCsv(normal_pages.toDF, pageOutput.resolve("normal_pages").toString)
    
    
  }
}
