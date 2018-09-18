package ch.epfl.lts2.wikipedia

import org.scalatest._
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{SQLContext, Row, DataFrame, SparkSession, Dataset}

class PageCountSpec extends FlatSpec with SparkSessionTestWrapper with TestData {
  "WikipediaPagecountParser" should "parse pagecounts correctly" in {
    val p = new WikipediaPagecountParser
    val pcLines = pageCount.split('\n').filter(p => !p.startsWith("#"))
    
    val rdd = p.getRDD(spark.sparkContext.parallelize(pcLines, 2))
    val resMap = rdd.map(w => (w.title, w)).collect().toMap
   
    assert(resMap.keys.size == 20)
    
    val res16 = resMap("16th_amendment")
    assert(res16.namespace == WikipediaNamespace.Page && res16.dailyVisits == 2 && res16.hourlyVisits == "C1P1")
    val res16c = resMap("16th_ammendment")
    assert(res16c.namespace == WikipediaNamespace.Category && res16c.dailyVisits == 1 && res16c.hourlyVisits == "J1")
    assert(resMap.get("16th_century_in_literature") == None) // book -> filtered out
    
    val res16th = resMap("16th_century")
    assert(res16th.dailyVisits == 258 && res16th.namespace == WikipediaNamespace.Page && 
              res16th.hourlyVisits == "A9B13C9D15E7F14G9H8I8J9K8L4M9N18O9P15Q17R12S10T12U7V15W15X6")
  }
  
}