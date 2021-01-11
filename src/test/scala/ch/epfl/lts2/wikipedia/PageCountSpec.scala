package ch.epfl.lts2.wikipedia

import org.scalatest._
import java.time._
import java.sql.Timestamp
import org.apache.spark.sql.functions.lit
import com.typesafe.config.ConfigFactory
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{SQLContext, Row, DataFrame, SparkSession, Dataset}

case class MergedPagecount (title:String, namespace:Int, visits:List[Visit], id:Int)

class PageCountSpec extends FlatSpec with SparkSessionTestWrapper with TestData {
    
  
   "WikipediaHourlyVisitParser" should "parse hourly visits fields correctly" in {
    val p = new WikipediaHourlyVisitsParser
    val d = LocalDate.of(2018, 8, 1)
    val r = p.parseField("J1", d)
    assert(r.size == 1)
    val j1 = r.head
    assert(j1.time.getHour == 9 && j1.visits == 1)
    
    val r2 = p.parseField("C1P3", d)
    assert(r2.size == 2)
    val r2m = r2.map(w => (w.time.getHour, w.visits)).toMap
    assert(r2m(2) == 1 && r2m(15) == 3)
    
    val r3 = p.parseField("A9B13C9D15E7F14G9H8I8J9K8L4M9N18O9P15Q17R12S10T12U7V15W15X6", d)
    assert(r3.size == 24)
    val r3m = r3.map(w => (w.time.getHour, w.visits)).toMap
    assert(r3m(23) == 6 && r3m(22) == 15 && r3m(0) == 9 && r3m(1) == 13)
    
  }
  
  
  "WikipediaPagecountParser" should "parse pagecounts (legacy) correctly" in {
    val langList = List("en")
    val enFilter = new ElementFilter[WikipediaPagecount] {
      override def filterElt(t: WikipediaPagecount): Boolean = langList.contains(t.languageCode)
    }
    val p = new WikipediaPagecountLegacyParser(enFilter)
    
    val pcLines = pageCountLegacy.split('\n').filter(p => !p.startsWith("#"))
    
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
  
  "PagecountProcessor" should "generate correct date ranges" in {
    val p = new PagecountProcessor(List("en"), new WikipediaPagecountLegacyParser(),
                                   ConfigFactory.parseString(""), false, true)
    val range = p.dateRange(LocalDate.parse("2018-08-01"), LocalDate.parse("2018-08-10"), Period.ofDays(1))
    assert(range.size == 10)
    val r2 = p.dateRange(LocalDate.parse("2017-08-01"), LocalDate.parse("2017-09-01"), Period.ofDays(1))
    assert(r2.size == 32)
    assertThrows[IllegalArgumentException](p.dateRange(LocalDate.parse("2017-09-01"), LocalDate.parse("2017-01-01"), Period.ofDays(1)))
  }
  
  it should "update correctly pagecount metadata" in {
    val p = new PagecountProcessor(List("en"), new WikipediaPagecountLegacyParser(),
                                   ConfigFactory.parseString(""), false, true)
    val d1 = LocalDate.of(2018,6,1)
    val d2 = LocalDate.of(2018,5,1)
    val d3 = LocalDate.of(2018,6,30)
    val d3r = LocalDate.of(2018,7,1)
    val midnight = LocalTime.MIDNIGHT
    val tsref1 = Timestamp.valueOf(LocalDateTime.of(d1, midnight))
    val tsref2 = Timestamp.valueOf(LocalDateTime.of(d2, midnight))
    val tsref3 = Timestamp.valueOf(LocalDateTime.of(d3, midnight))
    val tsref3r = Timestamp.valueOf(LocalDateTime.of(d3r, midnight))
    val ts1 = p.getLatestDate(tsref1, d2)
    assert(ts1 == tsref1)
    val ts2 = p.getLatestDate(tsref1, d3)
    assert(ts2 == tsref3r)
    val ts3 = p.getEarliestDate(tsref1, d3)
    assert(ts3 == tsref1)
    val ts4 = p.getEarliestDate(tsref1, d2)
    assert(ts4 == tsref2)
  }
  
  it should "read correctly pagecounts (legacy)" in {
    val p = new PagecountProcessor(List("en"), new WikipediaPagecountLegacyParser(),
                                   ConfigFactory.parseString(""), false, true)
    val rdd = p.parseLines(spark.sparkContext.parallelize(pageCountLegacy2, 2), 100, 2000, LocalDate.of(2018, 8, 1))
    val refTime = Timestamp.valueOf(LocalDate.of(2018,8,1).atStartOfDay)
    val res1 = rdd.filter(f => f.title == "Anarchism").collect()
    val res2 = rdd.filter(f => f.title == "AfghanistanHistory").collect()
    val res3 = rdd.filter(f => f.title == "AnchorageAlaska").collect()
    //spark.createDataFrame(rdd).show()
    assert(res1.size == 1)
    val ref1 = Visit(refTime, 300, "Day")
    assert(res1(0).namespace == WikipediaNamespace.Category && res1(0).visits.size == 1 && res1(0).visits.contains(ref1))
    assert(res2.size == 1)
    val ref2 = Visit(refTime, 200, "Day")
    assert(res2(0).namespace == WikipediaNamespace.Page && res2(0).visits.size == 1 && res2(0).visits.contains(ref2))
    assert(res3.size == 1)
    assert(res3(0).namespace == WikipediaNamespace.Page && res3(0).visits.size == 5)
    res3(0).visits.map(p => assert(p.count == 600 && p.timeResolution == "Hour"))
  }

  it should "filter according to minimum daily visits correctly" in {
    import spark.implicits._
    val p = new PagecountProcessor(List("en"), new WikipediaPagecountLegacyParser(),
                                   ConfigFactory.parseString(""), false, true)
    val pcDf = p.parseLinesToDf(spark.sparkContext.parallelize(pageCountLegacy2, 2), 150, 2000, LocalDate.of(2018, 8, 1))
    assert(pcDf.count() == 3)
    val res = pcDf.filter(p => p.title == "AccessibleComputing").collect()
    assert(res.isEmpty)

  }

  it should "merge page dataframe correctly" in {
    import spark.implicits._
    val p = new PagecountProcessor(List("en"), new WikipediaPagecountLegacyParser(),
                                   ConfigFactory.parseString(""), false, true)
    val pcDf = p.parseLinesToDf(spark.sparkContext.parallelize(pageCountLegacy2, 2), 100, 2000, LocalDate.of(2018, 8, 1))
    val dp = new DumpParser
    
    val df = dp.processToDf(spark, spark.sparkContext.parallelize(Seq(sqlPage), 2), WikipediaDumpType.Page)
    val res = p.mergePagecount(df.withColumn("languageCode", lit("en")).as[WikipediaPageLang], pcDf).as[MergedPagecount] //FIXME
    
    val res1 = res.filter(p => p.title == "Anarchism").collect()
    val res2 = res.filter(p => p.title == "AfghanistanHistory").collect()
    val res3 = res.filter(f => f.title == "AnchorageAlaska").collect()
    assert(res1.size == 1)
    assert(res1(0).id == 12)
    
    assert(res2.size == 1)
    assert(res2(0).id == 13)
    
    assert(res3.size == 1)
    assert(res3(0).id == 258)
    
  }
}