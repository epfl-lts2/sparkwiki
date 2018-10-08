package ch.epfl.lts2.wikipedia

import org.scalatest._
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{SQLContext, Row, DataFrame, SparkSession}



class DumpParserSpec extends FlatSpec with SparkSessionTestWrapper with TestData {
  
  
  "DumpParser" should "split sql insert statements correctly" in {
  
    val dp = new DumpParser
    
    
    val res = dp.splitSqlInsertLine(sqlPage)
    assert(expectPage == res)
    
    assert(expectPageLong == dp.splitSqlInsertLine(sqlPageLong))
    assertThrows[ArrayIndexOutOfBoundsException](dp.splitSqlInsertLine(expectPage)) // no insert statement -> throw
  }
  
  
  
  it should "parse page insert statement correctly into dataframe" in {
    import spark.implicits._
    val dp = new DumpParser
    val df = dp.processToDf(spark, spark.sparkContext.parallelize(Seq(sqlPage), 2), WikipediaDumpType.Page)
    
    val res = df.as[WikipediaPage].collect().map(f => (f.id, f)).toMap
    assert(res.keys.size == 4)
    val r10 = res(10)
    assert(r10.namespace == 0 && r10.title == "AccessibleComputing" && !r10.isNew && r10.isRedirect)
    val r12 = res(12)
    assert(r12.namespace == 14 & r12.title == "Anarchism" && !r12.isNew && !r12.isRedirect)
    val r13 = res(13)
    assert(r13.namespace == 0 && r13.title == "AfghanistanHistory" && !r13.isNew && r13.isRedirect)
    val r258 = res(258)
    assert(r258.namespace == 0 && r258.title == "AnchorageAlaska" && !r258.isNew && r258.isRedirect)
  }

  
  it should "parse long page insert statement correctly into dataframe" in {
    import spark.implicits._
    val dp = new DumpParser
    val df = dp.processToDf(spark, spark.sparkContext.parallelize(Seq(sqlPageLong), 2), WikipediaDumpType.Page)
    
    val res = df.as[WikipediaPage].collect().map(f => (f.id, f)).toMap
    assert(res.keys.size == 9) // namespace == 1 are filtered out, only 0 and 14 are kept
    val pnrCat = res(45533)
    assert(pnrCat.namespace == 14 && pnrCat.title == "Pseudorandom_number_generator" && !pnrCat.isNew && !pnrCat.isRedirect)
    val socDar = res(45541)
    assert(socDar.namespace == 0 && socDar.title == "Social_Darwinism" && !socDar.isNew && !socDar.isRedirect)
    val refuge = res(45546)
    assert(refuge.namespace == 0 && refuge.title == "Refugees" && !refuge.isNew && refuge.isRedirect)
  }
  
    
  it should "parse pagelinks insert statements correctly" in {
    import spark.implicits._
    val dp = new DumpParser
    val df = dp.processToDf(spark, spark.sparkContext.parallelize(Seq(sqlPageLinks), 2), WikipediaDumpType.PageLinks)
    val res = df.as[WikipediaPageLink].collect().map(f => (f.from, f)).toMap
    
    assert(res.keys.size == 44)
    res.values.map(v => assert(v.fromNamespace == 0 && v.namespace == 0 && v.title == "1000_in_Japan"))
  }
  
    
  it should "parse redirect insert statements correctly" in {
    import spark.implicits._
    val dp = new DumpParser
    val df = dp.processToDf(spark, spark.sparkContext.parallelize(Seq(sqlRedirect), 2), WikipediaDumpType.Redirect)
    val res = df.as[WikipediaRedirect].collect().map(f => (f.from, f)).toMap
    assert(res.keys.size == 11)
    res.values.map(v => assert(v.targetNamespace == 0))
    val am = res(24)
    assert(am.title == "Amoeba")
    val haf = res(13)
    assert(haf.title == "History_of_Afghanistan")
    val at = res(23)
    assert(at.title == "Assistive_technology")
  }
  
  
  it should "parse category insert statements correctly" in {
    import spark.implicits._
    val dp = new DumpParser
    val df = dp.processToDf(spark, spark.sparkContext.parallelize(Seq(sqlCategory), 2), WikipediaDumpType.Category)
    val res = df.as[WikipediaCategory].collect().map(f => (f.id, f)).toMap
    assert(res.keys.size == 17)
    val sdg = res(388205)
    assert(sdg.title == "Swiss_death_metal_musical_groups")
    val pbg = res(388207)
    assert(pbg.title == "Peruvian_black_metal_musical_groups")
    val cs = res(388222)
    assert(cs.title == "Culture_in_Cheshire")
  }
    
  it should "parse categorylinks insert statements correctly" in {
    import spark.implicits._
    val dp = new DumpParser
    val df = dp.processToDf(spark, spark.sparkContext.parallelize(Seq(sqlCatLink), 2), WikipediaDumpType.CategoryLinks)
    val res = df.as[WikipediaCategoryLink].collect().map(f => (f.from, f))
    assert(res.size == 7)
    assert(res.filter(p => p._2.to == "Oxford_University_Press_people").size == 1)
    assert(res.filter(p => p._2.to == "English_male_short_story_writers").size == 1)
    assert(res.filter(p => p._2.to == "Pages_using_citations_with_format_and_no_URL").size == 1)
    
  } 
    
}