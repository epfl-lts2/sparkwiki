package wiki
import java.text.SimpleDateFormat
import java.sql.Timestamp
import java.util.Date
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import scala.reflect.ClassTag

abstract class WikipediaElement extends Serializable

trait WikipediaElementParser[T <: WikipediaElement with Product] {
  def parseLine(lineInput:String): T
  def filterElt(t: T): Boolean
  def getDataFrame(session: SparkSession, lines: RDD[String]): DataFrame
}


case class WikipediaPage(id:Int, namespace:Int, title:String, restriction:String, counter:Int, 
                          isRedirect:Boolean, isNew:Boolean, random:Double, touched:Timestamp, linksUpdated:String,
                          latest:Int, len:Int, contentModel:String, lang:String) extends WikipediaElement {
  
}

case class WikipediaPageLink(from:Int, namespace:Int, title:String, fromNamespace:Int) extends WikipediaElement {
  
}

case class WikipediaRedirect(from:Int, targetNamespace:Int, title:String, interwiki:String, fragment:String) extends WikipediaElement {
  
}

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
  val pageRegex = """(\d+),(\d+),'(.*?)','(.*?)',(\d+),([01]),([01]),([\d\.]+?),'(\d{14})',(.*?),(\d+),(\d+),(.*?),(.*)""".r
  val timestampFormat = new SimpleDateFormat("yyyyMMddHHmmss") 
  def parseLine(lineInput:String):WikipediaPage = {
  
       lineInput match {
        case pageRegex(id_r, namespace_r, title_r, restriction_r, counter_r, isRedirect_r, isNew_r, random_r, 
            touched_r, linksUpdated_r, latest_r, len_r, contentModel_r, lang_r) => {
              val id_s = id_r.toInt
              val namespace_s = namespace_r.toInt
              val counter_s = counter_r.toInt
              val isRedirect_s = isRedirect_r.toInt == 1
              val isNew_s = isNew_r.toInt == 1
              val random_s = random_r.toDouble
              val touched_s = timestampFormat.parse(touched_r)
              val latest_s = latest_r.toInt
              val len_s = len_r.toInt
              
              
              WikipediaPage(id_s, namespace_s, title_r, restriction_r, counter_s, isRedirect_s, isNew_s, random_s,
                  new Timestamp(touched_s.getTime), linksUpdated_r, latest_s, len_s, contentModel_r, lang_r)
         }
        case _ =>  {
          println("Parse error %s".format(lineInput))
          WikipediaPage(-1, -1, "", "", 0, false, false, 0,
                  new Timestamp(timestampFormat.parse("19700101000000").getTime), "", 0, 0, "", "")
        }
        
      }
  }
  
  def filterElt(t: WikipediaPage):Boolean = t.namespace == 0 && t.id > 0
  def getDataFrame(session:SparkSession, lines: RDD[String]):DataFrame = {
    session.createDataFrame(lines.map(l => parseLine(l)).filter(filterElt)).select("id", "namespace", "title", "isRedirect", "isNew")
  }
}

class WikipediaPageLinkParser extends Serializable with WikipediaElementParser[WikipediaPageLink] {
  /*
   * # pagelinks
`pl_from` int(8) unsigned NOT NULL DEFAULT '0',
`pl_namespace` int(11) NOT NULL DEFAULT '0',
`pl_title` varbinary(255) NOT NULL DEFAULT '',
`pl_from_namespace` int(11) NOT NULL DEFAULT '0'*/
  val plRegex = """(\d+),(\d+),'(.*?)',(\d+)""".r
  
  def parseLine(lineInput: String):WikipediaPageLink = {
    lineInput match {
      case plRegex(from_r, namespace_r, title_r, from_ns_r) => {
        val from_s = from_r.toInt
        val namespace_s = namespace_r.toInt
        val from_ns_s = from_ns_r.toInt
        
        WikipediaPageLink(from_s, namespace_s, title_r, from_ns_s)
      }
      case _ => {
        println("Parse error %s".format(lineInput))
        WikipediaPageLink(-1, -1, "", -1)
      }
    }
  }
  
  
  def filterElt(t:WikipediaPageLink): Boolean = t.namespace == 0 && t.fromNamespace == 0
  def getDataFrame(session:SparkSession, lines: RDD[String]):DataFrame = {
    session.createDataFrame(lines.map(l => parseLine(l)).filter(filterElt))
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
  val redirectRegex = """(\d+),(\d+),'(.*?)',(.*?),(.*?)""".r
  
  def parseLine(lineInput: String):WikipediaRedirect = {
    lineInput match {
      case redirectRegex(from_r, targetNamespace_r, title_r, interwiki_r, fragment_r) => {
        val from_s = from_r.toInt
        val targetNamespace_s = targetNamespace_r.toInt
        WikipediaRedirect(from_s, targetNamespace_s, title_r, interwiki_r, fragment_r)
      }
      case _ => {
        println("Parse error %s".format(lineInput))
        WikipediaRedirect(-1, -1, "", "", "")
      }
    }
  }
  
  
  def filterElt(t: WikipediaRedirect):Boolean = t.targetNamespace == 0
  def getDataFrame(session:SparkSession, lines: RDD[String]):DataFrame = {
    session.createDataFrame(lines.map(l => parseLine(l)).filter(filterElt))
  }
}