package wiki

abstract class WikipediaElement {
  def toCsv():String
}

class WikipediaPage(input: String) extends WikipediaElement {
  
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
  val pageRegex = """(\d+),(\d+),'(.*?)','(.*?)',(\d+),([01]),([01]),([\d\.]+?),'(\d{14})',(.*?),(\d+),(\d+),(.*?)""".r
  /*val (id:Int,
       namespace:Int,
       title:String,
       restriction: String,
       counter:Int,
       isRedirect:Boolean,
       isNew:Boolean,
       random:Double,
       touched:Int,
       linksUpdated:String,
       len:Int,
       contentModel:Int,
       lang:String)*/
      //val input_clean = input.substring(1, input.size - 2)
      val data = {
  
       input match {
        case pageRegex(id_r, namespace_r, title_r, restriction_r, counter_r, isRedirect_r, isNew_r, random_r, 
            touched_r, linksUpdated_r, len_r, contentModel_r, lang_r) => {
              val id_s = id_r.toInt
              val namespace_s = namespace_r.toInt
              val counter_s = counter_r.toInt
              val isRedirect_s = isRedirect_r.toInt == 1
              val isNew_s = isNew_r.toInt == 1
              val random_s = random_r.toDouble
              //val touched_s = touched_r.toLong
              val len_s = len_r.toInt
              val contentModel_s = contentModel_r.toInt
              println("Page %d %s".format(id_s, title_r))
              (id_s, namespace_s, title_r, restriction_r, counter_s, isRedirect_s, isNew_s, random_s,
                  touched_r, linksUpdated_r, len_s, contentModel_s, lang_r)
         }
      }
  }
  
  def toCsv():String = {
    val sb = new StringBuilder
    sb.append(data._1)
    sb += ','
    sb.append(data._2)
    sb += ','
    sb ++= data._3
    sb.toString()
  }
}