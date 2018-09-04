package wiki
import java.io.File

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.{SparkConf, SparkContext}

import org.apache.spark.sql.{SQLContext, Row, DataFrame, SparkSession}

import scala.RuntimeException
import org.rogach.scallop._

class MergeConf(args: Seq[String]) extends ScallopConf(args) {
  val pagePath = opt[String](name="pagePath")
  val categoryPath = opt[String](name="categoryPath")
  val pageLinksPath = opt[String](name="pageLinksPath")
  val redirectPath = opt[String](name="redirectPath")
  val catlinksPath = opt[String](name="categoryLinksPath")
  val outputPath = opt[String](required=true, name="outputPath")
  // TODO improve validation
  requireOne(pageLinksPath, redirectPath, catlinksPath)
  codependent(catlinksPath, pagePath)
  codependent(categoryPath, catlinksPath)
  conflicts(categoryPath, List(pageLinksPath, redirectPath))
  verify()
}

object DumpParseMerge {
  def joinPageLinks(session:SparkSession, pages:DataFrame, pageLinkPath:String, outputPath:String) = {
    val pagelinks = session.read.parquet(pageLinkPath)
    val pagelinks_id = pagelinks.join(pages, "title").select("from", "id", "title")
    
    pagelinks_id.write.option("delimiter", "\t")
            .option("header", false)
            .option("quote", "")
            .option("compression", "gzip")
            .csv(outputPath)
  }
  
  def joinRedirect(session:SparkSession, pages:DataFrame, redirectPath:String, outputPath:String) = {
    val redirect = session.read.parquet(redirectPath)
    val redirect_id = redirect.join(pages, "title").select("from", "id", "title")
    
    redirect_id.write.option("delimiter", "\t")
            .option("header", false)
            .option("quote", "")
            .option("compression", "gzip")
            .csv(outputPath)
  }
  
  def joinCategory(session:SparkSession, category:DataFrame, pages:DataFrame, categoryLinksPath:String, outputPath:String) = {
    val catlinks = session.read.parquet(categoryLinksPath)
    
    // join first on the page dataframe to filter out pages not existing in the dataset
    val catlinks_pg = catlinks.withColumn("id", catlinks.col("from"))
                          .join(pages, "id")
                          .select("from", "to")
    val catlinks_id = catlinks_pg.withColumn("title", catlinks_pg.col("to"))
                          .join(category, "title")
                          .withColumn("page_id", catlinks.col("from"))
                          .select("page_id", "title", "id")
    catlinks_id.write.option("delimiter", "\t")
           .option("header", false)
           .option("quote", "")
           .option("compression", "gzip")
           .csv(outputPath)
  }
  
  
  def main(args: Array[String]) {
    val conf = new MergeConf(args)
    
    val sconf = new SparkConf().setAppName("Wikipedia dump merge").setMaster("local[*]")
    val session = SparkSession.builder.config(sconf).getOrCreate()
    val sctx = session.sparkContext
    
    val pages = session.read.parquet(conf.pagePath())
    
    conf.categoryPath.toOption match {
      case None => {
        conf.pageLinksPath.toOption match {
          case None => joinRedirect(session, pages, conf.redirectPath(), conf.outputPath())
          case _ => joinPageLinks(session, pages, conf.pageLinksPath(), conf.outputPath())
        }
      }
      case _ => {
        val category = session.read.parquet(conf.categoryPath())
        joinCategory(session, category, pages, conf.catlinksPath(), conf.outputPath())
      }
    }    
   
  }
}