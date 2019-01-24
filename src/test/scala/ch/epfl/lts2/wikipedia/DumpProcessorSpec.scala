package ch.epfl.lts2.wikipedia

import org.scalatest._
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{SQLContext, Row, DataFrame, SparkSession, Dataset}


class DumpProcessorSpec extends FlatSpec with SparkSessionTestWrapper with TestData {
  /*val sqlPage = "INSERT INTO `page` VALUES (10,0,'AccessibleComputing','',0,1,0,0.33167112649574004,'20180709171712','20180410125914',834079434,122,'wikitext',NULL),"+
                "(12,14,'Anarchism','',5252,0,0,0.786172332974311,'20180730175243','20180730175339',851684166,188642,'wikitext',NULL),"+
                "(13,0,'AfghanistanHistory','',5,1,0,0.0621502865684687,'20180726011011','20180410125914',783865149,90,'wikitext',NULL);  ";
  */
  
  spark.sparkContext.setLogLevel("WARN")
  
  "DumpProcessor" should "filter pages according to namespace correctly" in {
    val dproc = new DumpProcessor
    val dp = new DumpParser
    import spark.implicits._
    val pages = dp.processToDf(spark, spark.sparkContext.parallelize(Seq(sqlPage), 1), WikipediaDumpType.Page).as[WikipediaPage]
    assert(pages.count == 4)
    val normal_pages = dproc.getPagesByNamespace(pages, WikipediaNamespace.Page, true)
    val category_pages = dproc.getPagesByNamespace(pages, WikipediaNamespace.Category, true)
    assert(normal_pages.count == 3)
    assert(category_pages.count == 1)
  }
  
   /*
   * # pagelinks
`pl_from` int(8) unsigned NOT NULL DEFAULT '0',
`pl_namespace` int(11) NOT NULL DEFAULT '0',
`pl_title` varbinary(255) NOT NULL DEFAULT '',
`pl_from_namespace` int(11) NOT NULL DEFAULT '0'*/
  val sqlPageLinkDummy = "INSERT INTO `pagelinks` VALUES (10,14,'Anarchism',0),(12,0,'AfghanistanHistory',14),(13,0,'Nonexisting_Page',0);"
  it should "join page and pagelinks correctly" in {
    import spark.implicits._
    val dproc = new DumpProcessor
    val dp = new DumpParser
  
  
    val pages = dp.processToDf(spark, spark.sparkContext.parallelize(Seq(sqlPage), 1), WikipediaDumpType.Page)
    
    
    val pageLink = dp.processToDf(spark, spark.sparkContext.parallelize(Seq(sqlPageLinkDummy), 1), WikipediaDumpType.PageLinks)
    assert(pageLink.count == 3)
    
    val jdf = dproc.mergePageLink(pages.as[WikipediaPage], pageLink.as[WikipediaPageLink])
    val jdp = jdf.collect().map(w => (w.from, w)).toMap
    assert(jdp.keys.size == 2) // should ignore references to non-existing pages
    val pl1 = jdp(10)
    assert(pl1.fromNamespace == 0 && pl1.id == 12 && pl1.namespace == 14)
    val pl2 = jdp(12)
    assert(pl2.fromNamespace == 14 && pl2.id == 13 && pl2.namespace == 0)
  }
  
  
  /* TABLE `redirect` (
  `rd_from` int(8) unsigned NOT NULL DEFAULT '0',
  `rd_namespace` int(11) NOT NULL DEFAULT '0',
  `rd_title` varbinary(255) NOT NULL DEFAULT '',
  `rd_interwiki` varbinary(32) DEFAULT NULL,
  `rd_fragment` varbinary(255) DEFAULT NULL,
   */
  val sqlRedirectDummy = "INSERT INTO `redirect` VALUES (12,0,'AccessibleComputing','n/a','n/a'),(13,0,'Non_existing_page',NULL,NULL);"
  it should "join page and redirect correctly" in {
    import spark.implicits._
    val dproc = new DumpProcessor
    val dp = new DumpParser
  
  
    val pages = dp.processToDf(spark, spark.sparkContext.parallelize(Seq(sqlPage), 1), WikipediaDumpType.Page).as[WikipediaPage]
    val red = dp.processToDf(spark, spark.sparkContext.parallelize(Seq(sqlRedirectDummy), 1), WikipediaDumpType.Redirect).as[WikipediaRedirect]
    assert(red.count == 2)
    val rds = dproc.mergeRedirect(pages, red).collect().map(w => (w.from, w)).toMap
    assert(rds.keys.size == 1)
    val r1 = rds(12)
    assert(r1.id == 10)
  }
  
   /* TABLE `categorylinks` (
  `cl_from` int(8) unsigned NOT NULL DEFAULT '0',
  `cl_to` varbinary(255) NOT NULL DEFAULT '',
  `cl_sortkey` varbinary(230) NOT NULL DEFAULT '',
  `cl_timestamp` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  `cl_sortkey_prefix` varbinary(255) NOT NULL DEFAULT '',
  `cl_collation` varbinary(32) NOT NULL DEFAULT '',
  * `cl_type` enum('page','subcat','file') NOT NULL DEFAULT 'page'
  */
  val catLinksSqlDummy = "INSERT INTO `categorylinks` VALUES (10,'Anarchism','','2018-08-01 02:00:00','','','page'),(13,'Dummy_category','','1970-01-01 00:00:00','','','subcat'); "
  it should "join page and categorylinks correctly" in {
    import spark.implicits._
    val dproc = new DumpProcessor
    val dp = new DumpParser
  
  
    val pages = dp.processToDf(spark, spark.sparkContext.parallelize(Seq(sqlPage), 1), WikipediaDumpType.Page).as[WikipediaPage]
    val cl = dp.processToDf(spark, spark.sparkContext.parallelize(Seq(catLinksSqlDummy), 1), WikipediaDumpType.CategoryLinks).as[WikipediaCategoryLink]
    assert(cl.count == 2)
    
    val cp = dproc.getPagesByNamespace(pages, WikipediaNamespace.Category, true)
    val cldf = dproc.mergeCategoryLinks(pages, cp, cl).collect().map(w => (w.from, w)).toMap
    assert(cldf.keys.size == 1)
    val cl1 = cldf(10)
    assert(cl1.id == 12 && cl1.title == "Anarchism" && cl1.ctype == "page")
  }
  
}