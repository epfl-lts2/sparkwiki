package wiki
import java.text.SimpleDateFormat
import java.util.Date

abstract class WikipediaElement {
  def toCsv():String
}

case object DummyElement extends WikipediaElement {
  def toCsv():String = ""
}
case class WikipediaPage(id:Int, namespace:Int, title:String, restriction:String, counter:Int, 
                          isRedirect:Boolean, isNew:Boolean, random:Double, touched:Date, linksUpdated:String,
                          latest:Int, len:Int, contentModel:String, lang:String) extends WikipediaElement {
  def toCsv():String = {
    val sb = new StringBuilder
    sb.append(id)
    sb += ','
    sb.append(namespace)
    sb += ','
    sb ++= title
    sb += ','
    sb.append(isRedirect)
   
    sb.toString()
  }
}

case class WikipediaPageLink(from:Int, namespace:Int, title:String, fromNamespace:Int) extends WikipediaElement {
  def toCsv():String = {
    val sb = new StringBuilder
    sb.append(from)
    sb += ','
    sb.append(fromNamespace)
    sb += ','
    sb.append(namespace)
    sb += ','
    sb ++=title
    sb.toString
  }
}

class WikipediaPageParser extends Serializable  {
  
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
                  touched_s, linksUpdated_r, latest_s, len_s, contentModel_r, lang_r)
         }
        case _ =>  {
          println("Parse error %s".format(lineInput))
          WikipediaPage(-1, -1, "", "", 0, false, false, 0,
                  timestampFormat.parse("19700101000000"), "", 0, 0, "", "")
        }
        
      }
  }
}

class WikipediaPageLinkParser extends Serializable {
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
}