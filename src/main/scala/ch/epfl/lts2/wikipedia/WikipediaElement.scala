package ch.epfl.lts2.wikipedia
import java.text.SimpleDateFormat
import java.sql.Timestamp
import java.util.Date
import java.time.LocalDateTime
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._

object WikipediaDumpType extends Enumeration {
  // values must match the table name !
  val Page = Value("page")
  val PageLinks = Value("pagelinks")
  val Redirect = Value("redirect")
  val Category = Value("category")
  val CategoryLinks = Value("categorylinks")
  val LangLinks = Value("langlinks")
}

object WikipediaNamespace extends Enumeration {
  // cf https://en.wikipedia.org/wiki/Wikipedia:Namespace
  val Dummy:Int = -1
  val Page:Int = 0
  val Category:Int = 14
  val Portal:Int = 100
  val Book:Int = 108
}

abstract class WikipediaElement extends Serializable


case class WikipediaPage(id:Int, namespace:Int, title:String, restriction:String, counter:Int, 
                          isRedirect:Boolean, isNew:Boolean, random:Double, touched:Timestamp, linksUpdated:String,
                          latest:Int, len:Int, contentModel:String, lang:String) extends WikipediaElement 
                          
                          
case class WikipediaPageLink(from:Int, namespace:Int, title:String, fromNamespace:Int) extends WikipediaElement 


case class WikipediaRedirect(from:Int, targetNamespace:Int, title:String, interwiki:String, fragment:String) extends WikipediaElement 


case class WikipediaCategory(id:Int, title:String, pages:Int, subcats:Int, files:Int) extends WikipediaElement


case class WikipediaCategoryLink(from:Int, to:String, sortKey:String, timestamp:Timestamp, 
                                  sortkeyPrefix:String, collation:String, ctype:String) extends WikipediaElement

case class WikipediaPagecount(project:String, title:String, namespace:Int, dailyVisits:Int, hourlyVisits:String) extends WikipediaElement

case class WikipediaHourlyVisit(time:LocalDateTime, visits:Int) extends WikipediaElement

case class WikipediaLangLink(from:Int, targetTitle:String, targetLang:String) extends WikipediaElement
