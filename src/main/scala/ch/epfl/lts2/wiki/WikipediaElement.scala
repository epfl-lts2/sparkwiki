package ch.epfl.lts2.wiki
import java.text.SimpleDateFormat
import java.sql.Timestamp
import java.util.Date
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._

object WikipediaDumpType extends Enumeration {
  val Page, PageLinks, Redirect, Category, CategoryLinks = Value
}

object WikipediaNamespace extends Enumeration {
  // cf https://en.wikipedia.org/wiki/Wikipedia:Namespace
  val Page:Int = 0
  val Category:Int = 14
  val Portal:Int = 100
  val Book:Int = 108
}

abstract class WikipediaElement extends Serializable

trait WikipediaElementParser[T <: WikipediaElement with Product] {
  def parseLine(lineInput:String): List[T]
  def filterElt(t: T): Boolean
  def getDataFrame(session: SparkSession, lines: RDD[String]): DataFrame
}


case class WikipediaPage(id:Int, namespace:Int, title:String, restriction:String, counter:Int, 
                          isRedirect:Boolean, isNew:Boolean, random:Double, touched:Timestamp, linksUpdated:String,
                          latest:Int, len:Int, contentModel:String, lang:String) extends WikipediaElement 
                          
case class WikipediaPageLink(from:Int, namespace:Int, title:String, fromNamespace:Int) extends WikipediaElement 

case class WikipediaRedirect(from:Int, targetNamespace:Int, title:String, interwiki:String, fragment:String) extends WikipediaElement 

case class WikipediaCategory(id:Int, title:String, pages:Int, subcats:Int, files:Int) extends WikipediaElement

case class WikipediaCategoryLink(from:Int, to:String, sortKey:String, timestamp:Timestamp, 
                                  sortkeyPrefix:String, collation:String, ctype:String) extends WikipediaElement

class WikipediaPageParser extends Serializable with WikipediaElementParser[WikipediaPage]  {
  
  /*
   * # page
`page_id` int(8) unsigned NOT NULL AUTO_INCREMENT,
`page_namespace` int(11) NOT NULL DEFAULT '0',
`page_title` varbinary(255) NOT NULL DEFAULT '',
`page_restrictions` tinyblob NOT NULL,
`page_counter` bigint(20) unsigned NOT NULL DEFAULT '0',
`page_is_redirect` tinyint(1) unsigned NOT NULL DEFAULT '0',
`page_is_new` tinyint(1) unsigned NOT NULL DEFAULT '0',
`page_random` double unsigned NOT NULL DEFAULT '0',
`page_touched` varbinary(14) NOT NULL DEFAULT '',
`page_links_updated` varbinary(14) DEFAULT NULL,
`page_latest` int(8) unsigned NOT NULL DEFAULT '0',
`page_len` int(8) unsigned NOT NULL DEFAULT '0',
`page_content_model` varbinary(32) DEFAULT NULL,
`page_lang` varbinary(35) DEFAULT NULL,
   */
  val pageRegex = """\((\d+),(\d+),'(.*?)','(.*?)',(\d+),([01]),([01]),([\d\.]+?),'(\d{14})',(.*?),(\d+),(\d+),(.*?),(.*?)\)""".r
  val timestampFormat = new SimpleDateFormat("yyyyMMddHHmmss") 
  def parseLine(lineInput:String):List[WikipediaPage] = {
  
    val r = pageRegex.findAllIn(lineInput).matchData.toList
    r.map(m =>  WikipediaPage(m.group(1).toInt, m.group(2).toInt, m.group(3), m.group(4), m.group(5).toInt,
                        m.group(6).toInt == 1, m.group(7).toInt == 1, m.group(8).toDouble, 
                        new Timestamp(timestampFormat.parse(m.group(9)).getTime), m.group(10), m.group(11).toInt, 
                        m.group(12).toInt, m.group(13), m.group(14)))
      
  }
    
  
  def filterElt(t: WikipediaPage):Boolean = (t.namespace == WikipediaNamespace.Page || 
                                             t.namespace == WikipediaNamespace.Category)
  def getDataFrame(session:SparkSession, lines: RDD[String]):DataFrame = {
    session.createDataFrame(lines.flatMap(l => parseLine(l)).filter(filterElt))
              .select("id", "namespace", "title", "isRedirect", "isNew")
  }
}

class WikipediaPageLinkParser extends Serializable with WikipediaElementParser[WikipediaPageLink] {
  /*
   * # pagelinks
`pl_from` int(8) unsigned NOT NULL DEFAULT '0',
`pl_namespace` int(11) NOT NULL DEFAULT '0',
`pl_title` varbinary(255) NOT NULL DEFAULT '',
`pl_from_namespace` int(11) NOT NULL DEFAULT '0'*/
  val plRegex = """\((\d+),(\d+),'(.*?)',(\d+)\)""".r
  
  def parseLine(lineInput: String):List[WikipediaPageLink] = {
    val r = plRegex.findAllIn(lineInput).matchData.toList
    r.map(m => WikipediaPageLink(m.group(1).toInt, m.group(2).toInt, m.group(3), m.group(4).toInt)) 
  }
  
  
  def filterElt(t:WikipediaPageLink): Boolean = (t.namespace == WikipediaNamespace.Page || t.namespace == WikipediaNamespace.Category) && 
                                                (t.fromNamespace == WikipediaNamespace.Page || t.fromNamespace == WikipediaNamespace.Category)
  def getDataFrame(session:SparkSession, lines: RDD[String]):DataFrame = {
    session.createDataFrame(lines.flatMap(l => parseLine(l)).filter(filterElt))
  }
}

class WikipediaRedirectParser extends Serializable with WikipediaElementParser[WikipediaRedirect] {
  /* TABLE `redirect` (
  `rd_from` int(8) unsigned NOT NULL DEFAULT '0',
  `rd_namespace` int(11) NOT NULL DEFAULT '0',
  `rd_title` varbinary(255) NOT NULL DEFAULT '',
  `rd_interwiki` varbinary(32) DEFAULT NULL,
  `rd_fragment` varbinary(255) DEFAULT NULL,
   */
  val redirectRegex = """\((\d+),(\d+),'(.*?)',(.*?),(.*?)\)""".r
  
  def parseLine(lineInput: String):List[WikipediaRedirect] = {
    val r = redirectRegex.findAllIn(lineInput).matchData.toList
    r.map(m => WikipediaRedirect(m.group(1).toInt, m.group(2).toInt, m.group(3), m.group(4), m.group(5)))
  }
  
  
  def filterElt(t: WikipediaRedirect):Boolean = t.targetNamespace == WikipediaNamespace.Page || t.targetNamespace == WikipediaNamespace.Category
  def getDataFrame(session:SparkSession, lines: RDD[String]):DataFrame = {
    session.createDataFrame(lines.flatMap(l => parseLine(l)).filter(filterElt))
  }
}

class WikipediaCategoryParser extends Serializable with WikipediaElementParser[WikipediaCategory] {
  /*TABLE `category` (
  `cat_id` int(10) unsigned NOT NULL AUTO_INCREMENT,
  `cat_title` varbinary(255) NOT NULL DEFAULT '',
  `cat_pages` int(11) NOT NULL DEFAULT '0',
  `cat_subcats` int(11) NOT NULL DEFAULT '0',
  `cat_files` int(11) NOT NULL DEFAULT '0'*/
  val categoryRegex = """\((\d+),'(.*?)',(\d+),(\d+),(\d+)\)""".r
  def parseLine(lineInput: String):List[WikipediaCategory] = {
    val r = categoryRegex.findAllIn(lineInput).matchData.toList
    r.map(m => WikipediaCategory(m.group(1).toInt, m.group(2), m.group(3).toInt, m.group(4).toInt, m.group(5).toInt))
  }
  
  def filterElt(t: WikipediaCategory):Boolean = true
  def getDataFrame(session:SparkSession, lines:RDD[String]):DataFrame = {
    session.createDataFrame(lines.flatMap(l => parseLine(l)).filter(filterElt))
  }
}

class WikipediaCategoryLinkParser extends Serializable with WikipediaElementParser[WikipediaCategoryLink] {
  /* TABLE `categorylinks` (
  `cl_from` int(8) unsigned NOT NULL DEFAULT '0',
  `cl_to` varbinary(255) NOT NULL DEFAULT '',
  `cl_sortkey` varbinary(230) NOT NULL DEFAULT '',
  `cl_timestamp` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  `cl_sortkey_prefix` varbinary(255) NOT NULL DEFAULT '',
  `cl_collation` varbinary(32) NOT NULL DEFAULT '',
  * `cl_type` enum('page','subcat','file') NOT NULL DEFAULT 'page'
  */

  val categoryLinkRegex = """\((\d+),'(.*?)','(.*?)','(.*?)','(.*?)','(.*?)','(.*?)'\)""".r
  val timestampFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss") 
  def parseLine(lineInput:String):List[WikipediaCategoryLink] = {
    val r = categoryLinkRegex.findAllIn(lineInput).matchData.toList
    r.map(m => WikipediaCategoryLink(m.group(1).toInt, m.group(2), m.group(3), 
                new Timestamp(timestampFormat.parse(m.group(4)).getTime), m.group(5), m.group(6), m.group(7)))
  }
  def filterElt(t: WikipediaCategoryLink):Boolean = true
  def getDataFrame(session:SparkSession, lines:RDD[String]):DataFrame = {
    session.createDataFrame(lines.flatMap(l => parseLine(l)).filter(filterElt)).select("from", "to", "ctype")
  }
}
