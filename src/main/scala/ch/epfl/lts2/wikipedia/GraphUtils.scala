package ch.epfl.lts2.wikipedia

import java.io.PrintWriter
import java.sql.Timestamp
import org.apache.spark.graphx._
import scala.reflect.ClassTag
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}



/*
 * 
 * Ported and/or copied from https://github.com/mizvol/WikiBrain/
 * 
 */
object GraphUtils {
   /**
    * Removes singleton (disconnected) vertices from a given graph
    *
    * @param graph Graph to process
    * @tparam VD
    * @tparam ED
    * @return Graph without singleton vertices
    */
  def removeSingletons[VD: ClassTag, ED: ClassTag](graph: Graph[VD, ED]) =
    Graph(graph.triplets.map(et => (et.srcId, et.srcAttr))
      .union(graph.triplets.map(et => (et.dstId, et.dstAttr)))
      .distinct,
      graph.edges)
      
  /**
    * Remove edges with weights, lower than a threshold minWeight
    *
    * @param graph Graph to process
    * @param minWeight Weight threshold
    * @tparam VD
    * @return Graph with edges that have weights higher than the minWeight.
    */
  def removeLowWeightEdges[VD: ClassTag](graph: Graph[VD, Double], minWeight: Double): Graph[VD, Double] = {
    Graph(graph.vertices,
      graph.edges.filter(_.attr > minWeight)
    )
  }
  

  
  /**
    * Get the largest connected component (LCC) of a graph
    * @param g GraphX graph
    * @return LCC graph in GraphX format
    */
  def getLargestConnectedComponent[VD: ClassTag](g: Graph[VD, Double]): Graph[VD, Double] = {
    val cc = g.connectedComponents()
    val ids = cc.vertices.map((v: (Long, Long)) => v._2)
    val largestId = ids.map((_, 1L)).reduceByKey(_ + _).sortBy(-_._2).keys.first
    val largestCC = cc.vertices.filter((v: (Long, Long)) => v._2 == largestId)
    val lccVertices = largestCC.map(_._1).collect()
    g.subgraph(vpred = (id, _) => lccVertices.contains(id))
  }

  def toUndirected[VD: ClassTag](g: Graph[VD, Double]): Graph[VD, Double] = {
    // gather edges (a,b) and (b,a) into a single one
    val ec = g.edges.map(e => if (e.srcId < e.dstId) Edge(e.srcId, e.dstId, e.attr) else Edge(e.dstId, e.srcId, e.attr))
                    .groupBy(e => (e.srcId, e.dstId))
                    .mapValues(v => v.map(_.attr).sum)
                    .map(k => Edge(k._1._1, k._1._2, k._2))
    Graph(g.vertices, ec)
  }

  def toUndirected[VD:ClassTag, ED:ClassTag](g: Graph[VD, ED], op:Iterable[Edge[ED]] => ED):Graph[VD,ED] ={
    val ec = g.edges.map(e => if (e.srcId < e.dstId) Edge(e.srcId, e.dstId, e.attr) else Edge(e.dstId, e.srcId, e.attr))
                    .groupBy(e => (e.srcId, e.dstId))
                    .mapValues(v => op(v)).map(k => Edge(k._1._1, k._1._2, k._2))
    Graph(g.vertices, ec)
  }

  /**
    * Converts GraphX graph to GEXF XML format. Returns unweighted graph.
    * The code is taken from "Spark GraphX in Action" book
    * by Michael S. Malak and Robin East.
    * @param g GraphX graph
    * @return XML string in GEXF format
    */
  private def toGexf[VD, ED](g: Graph[VD, ED]) =
    "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n" +
      "<gexf xmlns=\"http://www.gexf.net/1.2draft\" version=\"1.2\">\n" +
      " <graph mode=\"static\" defaultedgetype=\"directed\">\n" +
      " <nodes>\n" +
      g.vertices.map(v => "      <node id=\"" + v._1 + "\" label=\"" +
        v._2 + "\" />\n").collect.mkString +
      "      </nodes>\n" +
      "      <edges>\n" +
      g.edges.map(e => "        <edge source=\"" + e.srcId +
        "\" target=\"" + e.dstId + "\" label=\"" + e.attr +
        "\" />\n").collect.mkString +
      "        </edges>\n" +
      " </graph>\n" +
      "</gexf>"

  /**
    * Converts GraphX graph to GEXF XML format. Returns weighted graph.
    * @param g GraphX graph
    * @return XML string in GEXF format
    */
  private def toGexfWeighted[VD, ED](g: Graph[VD, ED]) =
    "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n" +
      "<gexf xmlns=\"http://www.gexf.net/1.2draft\" version=\"1.2\">\n" +
      " <graph mode=\"static\" defaultedgetype=\"directed\">\n" +
      " <nodes>\n" +
      g.vertices.map(v => "      <node id=\"" + v._1 + "\" label=\"" +
        v._2 + "\" />\n").collect.mkString +
      "      </nodes>\n" +
      "      <edges>\n" +
      g.edges.map(e => "        <edge source=\"" + e.srcId +
        "\" target=\"" + e.dstId + "\" label=\"" + e.attr + "\" weight=\"" + e.attr +
        "\" />\n").collect.mkString +
      "        </edges>\n" +
      " </graph>\n" +
      "</gexf>"


      
  /**
    * Save GEXF graph to a file
    * @param graph GraphX graph
    * @param weighted Specify if you want a weighted graph
    * @param fileName output file name
    */
  def saveGraph[VD, ED](graph: Graph[VD, ED], weighted: Boolean = true, fileName: String) = {
    
    val pw = new PrintWriter(fileName)

    if (weighted) pw.write(toGexfWeighted(graph))
    else pw.write(toGexf(graph))

    pw.close
  }
  
  /**
    * Save GEXF graph to a file
    * @param graph GraphX graph
    * @param weighted Specify if you want a weighted graph
    * @param fileName output file name
    */
  def saveGraphHdfs[VD, ED](graph: Graph[VD, ED], weighted: Boolean = true, fileName: String) = {
    
    val data = if (weighted) toGexfWeighted(graph) else  toGexf(graph)

    writeHadoop(fileName, data)
  }
  
  private def writeHadoop(filePath: String, data: String) = {
    val path = new Path(filePath)
    val conf = new Configuration()
    
    val fs = FileSystem.get(conf)
    val os = fs.create(path)
    val pw = new PrintWriter(os)
    pw.write(data)
    pw.close
    fs.close
  }
  
}