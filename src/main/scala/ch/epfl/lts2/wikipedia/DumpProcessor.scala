package ch.epfl.lts2.wikipedia
import java.nio.file.Paths
import org.rogach.scallop._
import org.apache.spark.sql.{SQLContext, Row, DataFrame, SparkSession, Dataset}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.functions.lit

class ProcessorConf(args:Seq[String]) extends ScallopConf(args) {
  val dumpPath = opt[String](required = true, name="dumpPath")
  val outputPath = opt[String](required = true, name="outputPath")
  val namePrefix = opt[String](required = true, name="namePrefix")
  val outputFormat = opt[String](required = true, name = "outputFormat", default=Some("csv")) 
  verify()
}

class DumpProcessor extends Serializable {
  lazy val sconf = new SparkConf().setAppName("Wikipedia dump processor")
  lazy val session = SparkSession.builder.config(sconf).getOrCreate()
  
  def mergePageLink(pageDf:DataFrame, pageLinkDf:DataFrame): DataFrame = {
    pageLinkDf.join(pageDf, Seq("title", "namespace")).select("from", "id", "title", "fromNamespace", "namespace")
  }
  
  def mergeRedirect(pageDf:DataFrame, redirectDf: DataFrame):DataFrame = {
    redirectDf.withColumn("id", redirectDf.col("from"))
                .join(pageDf.drop(pageDf.col("title")), "id")
                .select("from", "targetNamespace", "title")
                .withColumn("namespace", redirectDf.col("targetNamespace"))
                .join(pageDf, Seq("title", "namespace")).select("from", "id", "title")
  }
  
  def mergeCategoryLinks(pageDf: DataFrame, categoryPageDf: DataFrame, catLinkDf: DataFrame): DataFrame = {
    catLinkDf.withColumn("id", catLinkDf.col("from"))
                              .join(pageDf, "id") // restrict to existing pages
                              .select("from", "to", "ctype")
                              .withColumn("title", catLinkDf.col("to"))
                              .join(categoryPageDf.select("id", "title"), "title")
                              .select("from", "title", "id", "ctype")
  }
  
  def getPagesByNamespace(pageDf: DataFrame, ns: Int): DataFrame = {
    import session.implicits._
    pageDf.filter($"namespace" === ns)
          .select("id", "title", "isRedirect", "isNew")
  }
                          
}

object DumpProcessor  {
  val dp = new DumpProcessor
  
  
  def main(args:Array[String]) = {
    val conf = new ProcessorConf(args)
    val dumpParser = new DumpParser
    val outputFormat = conf.outputFormat()
    
    val pageFile = Paths.get(conf.dumpPath(), conf.namePrefix() + "-page.sql.bz2").toString
    val pageOutput = Paths.get(conf.outputPath(), "page")
    val pageDf = dumpParser.processFileToDf(dp.session, pageFile, WikipediaDumpType.Page)
    
    
    val pageLinksFile = Paths.get(conf.dumpPath(), conf.namePrefix() + "-pagelinks.sql.bz2").toString
    val pageLinksOutput = Paths.get(conf.outputPath(), "pagelinks").toString
    val pageLinksDf = dumpParser.processFileToDf(dp.session, pageLinksFile, WikipediaDumpType.PageLinks)
    
    val categoryLinksFile = Paths.get(conf.dumpPath(), conf.namePrefix() + "-categorylinks.sql.bz2").toString
    val categoryLinksOutput = Paths.get(conf.outputPath(), "categorylinks").toString
    val categoryLinksDf = dumpParser.processFileToDf(dp.session, categoryLinksFile, WikipediaDumpType.CategoryLinks)
    
    val redirectFile = Paths.get(conf.dumpPath(), conf.namePrefix() + "-redirect.sql.bz2").toString
    val redirectOutput = Paths.get(conf.outputPath(), "redirect").toString
    val redirectDf = dumpParser.processFileToDf(dp.session, redirectFile, WikipediaDumpType.Redirect)
    
    
   
    val pagelinks_id = dp.mergePageLink(pageDf, pageLinksDf)
    val redirect_id = dp.mergeRedirect(pageDf, redirectDf)  
    val cat_pages = dp.getPagesByNamespace(pageDf, WikipediaNamespace.Category)
    val catlinks_id = dp.mergeCategoryLinks(pageDf, cat_pages, categoryLinksDf)
    val normal_pages = dp.getPagesByNamespace(pageDf, WikipediaNamespace.Page)
    
    outputFormat match {
      case "parquet" => {
        val edgesOutputPath = Paths.get(conf.outputPath(), "edges.p").toString
        val verticesOutputPath = Paths.get(conf.outputPath(), "vertices.p").toString
        val pagelinks_id_gf = pagelinks_id.select("from", "id")
                              .withColumnRenamed("from", "src")
                              .withColumnRenamed("id", "dst")
                              .withColumn("linkType", lit("links_to"))
        pagelinks_id_gf.write.option("compression", "gzip").parquet(edgesOutputPath)
        
        val redirect_id_gf = redirect_id.select("from", "id")
                              .withColumnRenamed("from", "src")
                              .withColumnRenamed("id", "dst")
                              .withColumn("linkType", lit("redirects_to"))
        redirect_id_gf.write.mode("append").option("compression", "gzip").parquet(edgesOutputPath)
        
        val catlinks_id_gf = catlinks_id.select("from", "id")
                              .withColumnRenamed("from", "src")
                              .withColumnRenamed("id", "dst")
                              .withColumn("linkType", lit("belongs_to"))
        catlinks_id_gf.write.mode("append").option("compression", "gzip").parquet(edgesOutputPath)                     
        
        
        
                                        
        cat_pages.withColumn("pageType", lit("Category")).write.option("compression", "gzip").parquet(verticesOutputPath)
        normal_pages.withColumn("pageType", lit("Page")).write.mode("append").option("compression", "gzip").parquet(verticesOutputPath)
      }
      case _ => {
        
        dumpParser.writeCsv(pagelinks_id, pageLinksOutput)
    
                                
        dumpParser.writeCsv(redirect_id, redirectOutput)
    
        
        dumpParser.writeCsv(catlinks_id, categoryLinksOutput)
        dumpParser.writeCsv(cat_pages, pageOutput.resolve("category_pages").toString)
    
            
        dumpParser.writeCsv(normal_pages, pageOutput.resolve("normal_pages").toString)
      }
    }
    
  }
}
