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

  it should "parse page insert statement (without restriction field) correctly into dataframe" in {
    import spark.implicits._
    val dp = new DumpParser
    val df = dp.processToDf(spark, spark.sparkContext.parallelize(Seq(sqlPageTest), 2),
      WikipediaDumpType.Page)

    val res = df.as[WikipediaPage].collect().map(f => (f.id, f)).toMap
    assert(res.keys.size == 7)
    val r8 = res(8)
    assert(r8.namespace == 0 && r8.title == "Ilyanep" && !r8.isNew && !r8.isRedirect)
    val r18 = res(18)
    assert(r18.namespace == 14 && r18.title == "Aboutsite" && r18.isNew && !r18.isRedirect)
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

  it should "split filenames correctly" in {
    val dp = new DumpParser
    val di = dp.splitFilename("enwiki-20180801-langlinks.sql.bz2")
    assert(di.langCode == "en" && di.dateCode == "20180801" & di.dumpType == "langlinks")
    val dii = dp.splitFilename("frwiki-20190901-page.sql.bz2")
    assert(dii.langCode == "fr" && dii.dateCode == "20190901" & dii.dumpType == "page")
    val dii_small = dp.splitFilename("frwiki-20190901-page.small.sql.bz2")
    assert(dii_small.langCode == "fr" && dii_small.dateCode == "20190901" & dii_small.dumpType == "page")
  }

  it should "parse langlinks insert statements correctly" in {
    import spark.implicits._
    val dp = new DumpParser
    val frFilter = new ElementFilter[WikipediaLangLink] {
      override def filterElt(t: WikipediaLangLink): Boolean = t.lang == "fr"
    }
    val esFilter = new ElementFilter[WikipediaLangLink] {
      override def filterElt(t: WikipediaLangLink): Boolean = t.lang == "es"
    }
    val dfr = dp.processToDf(spark, spark.sparkContext.parallelize(Seq(langLinksFr), 2), WikipediaDumpType.LangLinks, frFilter)
    val resfr = dfr.as[WikipediaLangLink].collect()
    assert(resfr.length == 15)

    val dfr2 = dp.processToDf(spark, spark.sparkContext.parallelize(Seq(langLinksFr2), 2), WikipediaDumpType.LangLinks, frFilter)
    val resfr2 = dfr2.as[WikipediaLangLink].collect()
    assert(resfr2.length == 2) // test if empty entries are filtered out correctly

    val desEmpt = dp.processToDf(spark, spark.sparkContext.parallelize(Seq(langLinksEs), 2), WikipediaDumpType.LangLinks, frFilter)
    val resesEmpt = desEmpt.as[WikipediaLangLink].collect()
    assert(resesEmpt.isEmpty)

    val des = dp.processToDf(spark, spark.sparkContext.parallelize(Seq(langLinksEs), 2), WikipediaDumpType.LangLinks, esFilter)
    val reses = des.as[WikipediaLangLink].collect()
    assert(reses.length == 11)
  }
    
}