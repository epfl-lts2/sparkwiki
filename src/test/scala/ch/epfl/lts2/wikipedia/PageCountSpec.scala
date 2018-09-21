package ch.epfl.lts2.wikipedia

import org.scalatest._
import java.time._
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
  
  "WikipediaHourlyVisitParser" should "parse hourly visits fields correctly" in {
    val p = new WikipediaHourlyVisitsParser
    val r = p.parseField("J1")
    assert(r.size == 1)
    val j1 = r.head
    assert(j1.hour == 9 && j1.visits == 1)
    
    val r2 = p.parseField("C1P3")
    assert(r2.size == 2)
    val r2m = r2.map(w => (w.hour, w.visits)).toMap
    assert(r2m(2) == 1 && r2m(15) == 3)
    
    val r3 = p.parseField("A9B13C9D15E7F14G9H8I8J9K8L4M9N18O9P15Q17R12S10T12U7V15W15X6")
    assert(r3.size == 24)
    val r3m = r3.map(w => (w.hour, w.visits)).toMap
    assert(r3m(23) == 6 && r3m(22) == 15 && r3m(0) == 9 && r3m(1) == 13)
  }
  
  "PagecountProcessor" should "generate correct date ranges" in {
    val p = new PagecountProcessor
    val range = p.dateRange(LocalDate.parse("2018-08-01"), LocalDate.parse("2018-08-10"), Period.ofDays(1))
    assert(range.size == 10)
    val r2 = p.dateRange(LocalDate.parse("2017-08-01"), LocalDate.parse("2017-09-01"), Period.ofDays(1))
    assert(r2.size == 32)
  }
  
}