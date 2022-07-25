package ch.epfl.lts2.wikipedia
import java.text.SimpleDateFormat
import java.sql.Timestamp
import java.net.URLDecoder
import java.nio.charset.StandardCharsets
import java.time._
import java.util.Date
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._


trait ElementFilter[T <: WikipediaElement with Product] extends Serializable {
  def filterElt(t: T): Boolean
}

sealed class DefaultElementFilter[T <: WikipediaElement with Product] extends ElementFilter[T] {
  def filterElt(t:T):Boolean = true
}


trait WikipediaElementParserBase[T <: WikipediaElement with Product] {
  def parseLine(lineInput:String): List[T]
  def getRDD(lines: RDD[String]): RDD[T]
  def getDataFrame(session:SparkSession, data: RDD[String]):DataFrame
  def defaultFilterElt(t: T):Boolean
  def filterElt(t: T): Boolean
}

abstract class WikipediaElementParser[T <: WikipediaElement with Product](elementFilter: ElementFilter[T])
  extends WikipediaElementParserBase[T] with Serializable {
  override def filterElt(t: T): Boolean = elementFilter.filterElt(t) && defaultFilterElt(t)
}


class WikipediaPageParser(elementFilter: ElementFilter[WikipediaPage] = new DefaultElementFilter[WikipediaPage]) extends WikipediaElementParser[WikipediaPage](elementFilter) {
  
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
  val pageRegex = """\((\d+),(\d+),'(.*?)'(?:,'.*?')?,([01]),([01]),([\d\.]+?),'(\d{14})',(.*?),(\d+),(\d+),(.*?),(.*?)\)""".r
  val timestampFormat = new SimpleDateFormat("yyyyMMddHHmmss") 
  def parseLine(lineInput:String):List[WikipediaPage] = {
  
    val r = pageRegex.findAllIn(lineInput).matchData.toList
    r.map(m =>  WikipediaPage(m.group(1).toInt, m.group(2).toInt, m.group(3), m.group(4),
                        m.group(5).toInt == 1, m.group(6).toInt == 1, m.group(7).toDouble, 
                        new Timestamp(timestampFormat.parse(m.group(8)).getTime), m.group(9), m.group(10).toInt, 
                        m.group(11).toInt, m.group(12)))
      
  }


  def defaultFilterElt(t: WikipediaPage): Boolean =  (t.namespace == WikipediaNamespace.Page ||
                                                      t.namespace == WikipediaNamespace.Category)
                                             
  def getRDD(lines: RDD[String]): RDD[WikipediaPage] = {
    lines.flatMap(l => parseLine(l)).filter(filterElt)
  }
  def getDataFrame(session:SparkSession, data:RDD[String]):DataFrame = session.createDataFrame(getRDD(data))
 
}

class WikipediaPageLinkParser(elementFilter: ElementFilter[WikipediaPageLink] = new DefaultElementFilter[WikipediaPageLink])
  extends WikipediaElementParser[WikipediaPageLink](elementFilter) {
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
  
  
  def defaultFilterElt(t:WikipediaPageLink): Boolean = (t.namespace == WikipediaNamespace.Page || t.namespace == WikipediaNamespace.Category) &&
                                                (t.fromNamespace == WikipediaNamespace.Page || t.fromNamespace == WikipediaNamespace.Category)
  def getRDD(lines: RDD[String]): RDD[WikipediaPageLink] = {
    lines.flatMap(l => parseLine(l)).filter(filterElt)
  }
  def getDataFrame(session:SparkSession, data:RDD[String]):DataFrame = session.createDataFrame(getRDD(data))
}

class WikipediaLangLinkParser(filter: ElementFilter[WikipediaLangLink] = new DefaultElementFilter[WikipediaLangLink])
  extends WikipediaElementParser[WikipediaLangLink](filter) {
  /* CREATE TABLE `langlinks` (
    `ll_from` int(8) unsigned NOT NULL DEFAULT '0',
  `ll_lang` varbinary(20) NOT NULL DEFAULT '',
  `ll_title` varbinary(255) NOT NULL DEFAULT '',
  ) */
  val llRegex = """\((\d+),'(.*?)','(.*?)'\)""".r

  def parseLine(lineInput: String):List[WikipediaLangLink] = {
    val r = llRegex.findAllIn(lineInput).matchData.toList
    r.map(m => WikipediaLangLink(m.group(1).toInt, m.group(2), m.group(3).replace(" ","_")))
  }

  def defaultFilterElt(t:WikipediaLangLink): Boolean = !t.title.isEmpty

  def getRDD(lines: RDD[String]): RDD[WikipediaLangLink] = {
    lines.flatMap(l => parseLine(l)).filter(filterElt)
  }
  def getDataFrame(session:SparkSession, data:RDD[String]):DataFrame = session.createDataFrame(getRDD(data))
}

class WikipediaRedirectParser(elementFilter: ElementFilter[WikipediaRedirect] = new DefaultElementFilter[WikipediaRedirect])
  extends  WikipediaElementParser[WikipediaRedirect](elementFilter) {
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
  
  
  def defaultFilterElt(t: WikipediaRedirect):Boolean = t.targetNamespace == WikipediaNamespace.Page || t.targetNamespace == WikipediaNamespace.Category
  def getRDD(lines: RDD[String]):RDD[WikipediaRedirect] = {
    lines.flatMap(l => parseLine(l)).filter(filterElt)
  }
  def getDataFrame(session:SparkSession, data:RDD[String]):DataFrame = session.createDataFrame(getRDD(data))
}

class WikipediaCategoryParser(elementFilter: ElementFilter[WikipediaCategory] = new DefaultElementFilter[WikipediaCategory])
  extends WikipediaElementParser[WikipediaCategory](elementFilter) {
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
  
  def defaultFilterElt(t: WikipediaCategory):Boolean = true
  def getRDD(lines:RDD[String]):RDD[WikipediaCategory] = {
    lines.flatMap(l => parseLine(l)).filter(filterElt)
  }
  def getDataFrame(session:SparkSession, data:RDD[String]):DataFrame = session.createDataFrame(getRDD(data))
}

class WikipediaCategoryLinkParser(elementFilter: ElementFilter[WikipediaCategoryLink] = new DefaultElementFilter[WikipediaCategoryLink])
  extends WikipediaElementParser[WikipediaCategoryLink](elementFilter) {
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
  def defaultFilterElt(t: WikipediaCategoryLink):Boolean = true
  def getRDD(lines:RDD[String]):RDD[WikipediaCategoryLink] = {
    lines.flatMap(l => parseLine(l)).filter(filterElt)
  }
  def getDataFrame(session:SparkSession, data:RDD[String]):DataFrame = session.createDataFrame(getRDD(data))
}

class WikipediaPagecountParser(elementFilter: ElementFilter[WikipediaPagecount] = new DefaultElementFilter[WikipediaPagecount])
  extends WikipediaElementParser[WikipediaPagecount](elementFilter) {
  val pageCountRegex = """^([a-z]{2}\.[a-z]+) (.*?) (\d+|null) (.*?) (\d+) ((?:[A-Z]\d+)+)$""".r
  val titleNsRegex = """(.*?):(.*?)""".r

  override def parseLine(lineInput: String): List[WikipediaPagecount] = {
    val r = pageCountRegex.findAllIn(lineInput).matchData.toList
    r.map(m => {
      // extract lang code
      val langCode = m.group(1).split('.')(0)
      // get namespace
      val (title, nsStr) = m.group(2) match {
        case titleNsRegex(nsStr, title) => (title, nsStr)
        case _ => (m.group(2), "Page")
      }
      val ns = nsStr match {
        case "Page" => WikipediaNamespace.Page
        case "Category" => WikipediaNamespace.Category
        case "Book" => WikipediaNamespace.Book
        case _ => WikipediaNamespace.Dummy
      }
      WikipediaPagecount(langCode, m.group(2), ns, m.group(4), m.group(5).toInt, m.group(6))
    })
  }
  override def defaultFilterElt(t: WikipediaPagecount): Boolean = true

  override def getRDD(lines: RDD[String]): RDD[WikipediaPagecount] = lines.flatMap(l => parseLine(l)).filter(filterElt)
  def getDataFrame(session:SparkSession, data:RDD[String]):DataFrame = session.createDataFrame(getRDD(data))
}

class WikipediaPagecountLegacyParser(elementFilter: ElementFilter[WikipediaPagecount] = new DefaultElementFilter[WikipediaPagecount])
  extends WikipediaElementParser[WikipediaPagecount](elementFilter) {
  val pageCountRegex = """^([a-z]{2}\.[a-z]) (.*?) (\d+) ((?:[A-Z]\d+)+)$""".r
  val titleNsRegex = """(.*?):(.*?)""".r
  val urlEncodeRegex = """^%[A-F\d]{2}""".r
  def decodeTitle(t: String):String = {
   val d = t match {
     case urlEncodeRegex(_*) => URLDecoder.decode(t, StandardCharsets.UTF_8.name())
     case _ => t
    }
    d
  }
  def parseLine(lineInput:String): List[WikipediaPagecount] = {
    val r = pageCountRegex.findAllIn(lineInput).matchData.toList
    r.map(m => {
      // for some locales, the title of the page in pagecount data is html encoded -> decode it s.t. it can be matched to pages


      val extTitle = decodeTitle(m.group(2))
      val (title, nsStr) = extTitle match {
        case titleNsRegex(nsStr, title) => (title, nsStr)
        case _ => (extTitle, "Page")
      }

      val ns = nsStr match {
        case "Page" => WikipediaNamespace.Page
        case "Category" => WikipediaNamespace.Category
        case "Book" => WikipediaNamespace.Book
        case _ => WikipediaNamespace.Dummy
      }
      // extract lang code
      val langCode = m.group(1).split('.')(0)
      // In 'old' pagecounts, there is no source so return "web"
      WikipediaPagecount(langCode, title, ns, "web", m.group(3).toInt, m.group(4)) // language = 1st two chars of project name

    })
  }
  
  def defaultFilterElt(t: WikipediaPagecount):Boolean = (t.namespace == WikipediaNamespace.Page || t.namespace == WikipediaNamespace.Category)

                                                              
  def getRDD(lines:RDD[String]):RDD[WikipediaPagecount] = {
    lines.flatMap(l => parseLine(l)).filter(filterElt)
  }
  def getDataFrame(session:SparkSession, data:RDD[String]):DataFrame = session.createDataFrame(getRDD(data))
}

class WikipediaHourlyVisitsParser extends Serializable {
  val visitRegex = """([A-Z])(\d+)""".r
  def parseField(input:String, date:LocalDate): List[WikipediaHourlyVisit] = {
    val r = visitRegex.findAllIn(input).matchData.toList
    r.map(m => 
      {
        val hour = m.group(1).charAt(0).toInt - 'A'.toInt
        WikipediaHourlyVisit(LocalDateTime.of(date, LocalTime.of(hour, 0, 0)), m.group(2).toInt)
        })
  }
}