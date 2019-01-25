package ch.epfl.lts2.wikipedia

import org.scalatest._
import org.scalactic._
import java.time._
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{SQLContext, Row, DataFrame, SparkSession, Dataset}

class PageStatsSpec extends FlatSpec with SparkSessionTestWrapper {
  
   val epsilon = 1e-6f

  implicit val doubleEq = TolerantNumerics.tolerantDoubleEquality(epsilon)
  
  "PageStats" should "compute mean and variance correctly" in {
    val ps = new PageStats("localhost", 9042)
    //case class PageVisitElapsedGroup(page_id:Long, visits:List[(Int, Double)])
    val p = PageVisitElapsedGroup(1, List((1, 1.0), (10, 1.0)))
    val size = 20
    val r = ps.computeStats(p, size)
  
    assert(r.page_id == p.page_id)
    assert(r.mean === 0.1)
    assert(r.variance === 0.0947368421)
    
    val p2 = PageVisitElapsedGroup(2, List((0, 1.0), (3, 1.0), (10, -1.0), (13, 2.0), (16, -5.0)))
    
    val r2 = ps.computeStats(p2, size)
    assert(r2.page_id == p2.page_id)
    assert(r2.mean === -0.1)
    assert(r2.variance === 1.6736842105)
  }
}