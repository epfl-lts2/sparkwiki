package ch.epfl.lts2.wikipedia

import org.graphframes._
import org.apache.spark.sql.{SQLContext, Row, DataFrame, SparkSession, Dataset}
import org.apache.spark.{SparkConf, SparkContext}
import org.rogach.scallop._

class GraphFrameOpt(args: Seq[String]) extends ScallopConf(args) {
  val pagePath = opt[String](required = true, name= "pagePath")
  val edgePath = opt[String](required = true, name= "edgePath")
  verify()
}

class GraphFrameFilter extends Serializable {
  lazy val sconf = new SparkConf().setAppName("Wikipedia GraphFrame").setMaster("local[*]")
  lazy val session = SparkSession.builder.config(sconf).getOrCreate()
  
  def readDataFrame(fileName:String):DataFrame = {
      session.read.parquet(fileName)
  }
  
  def getGraph(vertices:DataFrame, edges:DataFrame):GraphFrame ={
    GraphFrame(vertices, edges)
  }
}

object GraphFrameFilter {
  val gf = new GraphFrameFilter
  
  def main(args:Array[String]) = {
    val cfg = new GraphFrameOpt(args)
    val pgDf = gf.readDataFrame(cfg.pagePath())
    val edDf = gf.readDataFrame(cfg.edgePath())
    
    val graph = gf.getGraph(pgDf, edDf)
    
  
  }
  
}