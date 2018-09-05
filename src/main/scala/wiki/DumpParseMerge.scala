package wiki
import java.nio.file.Paths

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{SQLContext, Row, DataFrame, SparkSession}

import scala.RuntimeException
import org.rogach.scallop._

class MergeConf(args: Seq[String]) extends ScallopConf(args) {
  val pagePath = opt[String](name="pagePath", required=true)
  val pageLinksPath = opt[String](name="pageLinksPath")
  val redirectPath = opt[String](name="redirectPath")
  val catlinksPath = opt[String](name="categoryLinksPath")
  val outputPath = opt[String](required=true, name="outputPath")
  
  requireOne(pageLinksPath, redirectPath, catlinksPath)
  verify()
}

object DumpParseMerge {
  def writeCsv(df:DataFrame, outputPath:String) = {
    df.write.option("delimiter", "\t")
            .option("header", false)
            .option("quote", "")
            .option("compression", "gzip")
            .csv(outputPath)
  }
  
  
  def joinPageLinks(session:SparkSession, pages:DataFrame, pageLinkPath:String, outputPath:String) = {
    import session.implicits._
    val normal_pages = pages.filter($"namespace" === 0).select("id", "title")
    val cat_pages = pages.filter($"namespace" === 14).select("id", "title")
    
    val pagelinks = session.read.parquet(pageLinkPath)
    
    // create normal page links (relation LINKS_TO)
    val normal_links = pagelinks.filter($"fromNamespace" === 0 && $"namespace" === 0)
                                .join(normal_pages, "title")
                                .select("from", "id", "title", "fromNamespace", "namespace")
    writeCsv(normal_links, Paths.get(outputPath, "normal_links").toString)

    val cat_links = pagelinks.filter($"fromNamespace" === 14 && $"namespace" === 14)
                             .join(cat_pages, "title")
                             .select("from", "id", "title", "fromNamespace", "namespace")
    writeCsv(cat_links, Paths.get(outputPath, "cat_links").toString)
    
    val n2c_links = pagelinks.filter($"fromNamespace" === 0 && $"namespace" === 14)
                             .join(cat_pages, "title")
                             .select("from", "id", "title", "fromNamespace", "namespace")
    writeCsv(n2c_links, Paths.get(outputPath, "n2c_links").toString)
    
    val c2n_links = pagelinks.filter($"fromNamespace" === 14 && $"namespace" === 0)
                             .join(normal_pages, "title")
                             .select("from", "id", "title", "fromNamespace", "namespace")
    writeCsv(c2n_links, Paths.get(outputPath, "c2n_links").toString)
  }
  
  def joinRedirect(session:SparkSession, pages:DataFrame, redirectPath:String, outputPath:String) = {
    val redirect = session.read.parquet(redirectPath)
    val redirect_id = redirect.join(pages, "title").select("from", "id", "title")
    
    writeCsv(redirect_id, outputPath)
  }
  
  def joinCategory(session:SparkSession, pages:DataFrame, categoryLinksPath:String, outputPath:String) = {
    import session.implicits._
    val catlinks = session.read.parquet(categoryLinksPath)
    val cat_pages = pages.filter($"namespace" === 14).select("id", "title")
    
    // this will only show categories having a matching page (in namespace 14)
    val catlinks_id = catlinks.withColumn("title", catlinks.col("to"))
                          .join(pages, "title")
                          .select("from", "title", "id", "ctype")
    writeCsv(catlinks_id, outputPath)
  }
  
  
  def main(args: Array[String]) {
    val conf = new MergeConf(args)
    
    val sconf = new SparkConf().setAppName("Wikipedia dump merge").setMaster("local[*]")
    val session = SparkSession.builder.config(sconf).getOrCreate()
    val sctx = session.sparkContext
    
    val pages = session.read.parquet(conf.pagePath())
    
    conf.catlinksPath.toOption match {
      case None => {
        conf.pageLinksPath.toOption match {
          case None => joinRedirect(session, pages, conf.redirectPath(), conf.outputPath())
          case _ => joinPageLinks(session, pages, conf.pageLinksPath(), conf.outputPath())
        }
      }
      case _ => {
        joinCategory(session, pages, conf.catlinksPath(), conf.outputPath())
      }
    }    
   
  }
}