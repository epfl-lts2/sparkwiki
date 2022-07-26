package ch.epfl.lts2.wikipedia

import org.scalatest._

class WikipediaParserSpec extends FlatSpec {
  "WikipediaPageParser" should "match string from sql dump correctly" in {
    val strTest = "(3,2,'Jon_Harald_Søby',0,0,0.28238286483,'20180813135855','20211107055312',119096,166,'wikitext',NULL)," +
      "(8,2,'Ilyanep',0,0,0.226528387236,'20150602173418',NULL,2292,468,'wikitext',NULL)"
    val pageParser = new WikipediaPageParser()
    val res = pageParser.parseLine(strTest)
    assert(res.size == 2)
    val res0 = res(0)
    assert(res0.title == "Jon_Harald_Søby" && res0.namespace == 2 && !res0.isRedirect && !res0.isNew)
    val res1 = res(1)
    assert(res1.title == "Ilyanep" && res1.namespace == 2 && !res1.isRedirect && !res1.isNew)
  }
}
