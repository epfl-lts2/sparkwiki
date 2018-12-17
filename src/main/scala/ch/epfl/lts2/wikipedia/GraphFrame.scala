package ch.epfl.lts2.wikipedia

import org.graphframes._
import org.apache.spark.sql._
import org.apache.spark.rdd.RDD
import org.apache.spark.graphx._
import org.apache.spark.sql.functions._
import org.apache.spark.{SparkConf, SparkContext}
import org.rogach.scallop._
import org.apache.spark.sql.{SQLContext, Row, DataFrame, SparkSession, Dataset}


case class WikipediaPageInfo(id:Int, title:String, isRedirect:Boolean, isNew:Boolean, pageType:String)
case class WikipediaPageInfoRank(id:Int, title:String, isRedirect:Boolean, isNew:Boolean, pageType:String, pageRank:Double) {
  def this(p: WikipediaPageInfo, pr:Double) = this(p.id, p.title, p.isRedirect, p.isNew, p.pageType, pr)
}
case class WikipediaLink(src:Int, dst:Int, linkType:String)

class GraphFrameOpt(args: Seq[String]) extends ScallopConf(args) {
  val pagePath = opt[String](required = true, name = "pagePath")
  val edgePath = opt[String](required = true, name = "edgePath")
  val outputPagePath = opt[String](required = true, name = "outputPagePath")
  verify()
}

class GraphFrameFilter extends Serializable with ParquetWriter {
  lazy val sconf = new SparkConf().setAppName("Wikipedia GraphFrame")
  lazy val session = SparkSession.builder.config(sconf).getOrCreate()
  
  def readDataFrame(fileName:String):DataFrame = {
      session.read.parquet(fileName)
  }
  
  def getGraph(vertices:DataFrame, edges:DataFrame):Graph[WikipediaPageInfo, String] ={
    import session.implicits._
    val v:RDD[(VertexId, WikipediaPageInfo)] = vertices.as[WikipediaPageInfo].map(p => (p.id.toLong, p)).rdd
    val e = edges.as[WikipediaLink].map(d => Edge(d.src.toLong, d.dst.toLong, d.linkType)).rdd
    Graph(v, e)
  }
}

object GraphFrameFilter {
  val gf = new GraphFrameFilter
  
  def main(args:Array[String]) = {
    
    val cfg = new GraphFrameOpt(args)
    val pgDf = gf.readDataFrame(cfg.pagePath())
    val edDf = gf.readDataFrame(cfg.edgePath())
    
    val graph: Graph[WikipediaPageInfo, String] = gf.getGraph(pgDf, edDf)
    val results = graph.pageRank(1e-4)
    val resGraphVertices = graph.vertices.join(results.vertices).map(p => new WikipediaPageInfoRank(p._2._1, p._2._2))
    gf.writeParquet(gf.session.createDataFrame(resGraphVertices), cfg.outputPagePath())
  }
  
}