package com.gravity.interests.jobs.intelligence

import org.junit.{Assert, Test}
import scalaz.Digit._1

/*             )\._.,--....,'``.
.b--.        /;   _.. \   _\  (`._ ,.
`=,-,-'~~~   `----(,_..'--(,_..'`-.;.'  */
class SectionPathTest {

  @Test def testSectionPath() {

    val path = SectionPath(Seq("yahoo_news_ca","entertainment","movies"))
    val path2 = SectionPath(Seq("yahoo_news_uk","entertainment","movies"))


    println(path.sections)
    println(path2.sections)

    Assert.assertEquals(path.sectionStrings, Seq("yahoo_news_ca", "yahoo_news_ca|entertainment", "yahoo_news_ca|entertainment|movies"))
    Assert.assertEquals(path2.sectionStrings, Seq("yahoo_news_uk", "yahoo_news_uk|entertainment", "yahoo_news_uk|entertainment|movies"))
    println(path.sectionStrings)
    println(path2.sectionStrings)

    Assert.assertEquals(path.sectionKeys("mysiteguid"),Set(SectionKey(-5575991411245112500l,-5918550096981894744l), SectionKey(-5575991411245112500l,-3613865305532583828l), SectionKey(-5575991411245112500l,1313773900347321360l)))
    Assert.assertEquals(path2.sectionKeys("mysiteguid"),Set(SectionKey(-5575991411245112500l,-8919348567366714953l), SectionKey(-5575991411245112500l,8881292604647399212l), SectionKey(-5575991411245112500l,-5430435586027631111l)))


    println(path.sectionKeys("mysiteguid"))
    println(path2.sectionKeys("mysiteguid"))

  }

}
