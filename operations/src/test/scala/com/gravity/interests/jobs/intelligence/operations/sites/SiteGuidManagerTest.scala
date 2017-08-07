package com.gravity.interests.jobs.intelligence.operations.sites

import com.gravity.interests.jobs.intelligence.SiteKey
import com.gravity.interests.jobs.intelligence.hbase.HBaseTestEnvironment
import com.gravity.test.operationsTesting
import com.gravity.utilities.analytics.articles.ArticleWhitelist
import com.gravity.utilities.{BaseScalaTest, HashUtils}

import scalaz.Scalaz._

/*             )\._.,--....,'``.
.b--.        /;   _.. \   _\  (`._ ,.
`=,-,-'~~~   `----(,_..'--(,_..'`-.;.'  */
class SiteGuidManagerTest extends BaseScalaTest with operationsTesting {
  val sm = new SiteGuidManager {}

  test("Guid Knowledge") {
    val wordPress = ArticleWhitelist.siteGuid(_.WORDPRESS)
    val yahooNews = ArticleWhitelist.siteGuid(_.YAHOO_NEWS)
    val techCrunch = ArticleWhitelist.siteGuid(_.TECHCRUNCH)
    val nuSite1 = HashUtils.md5("mysite.com")
    val nuSite2 = HashUtils.md5("ofsejfojseofjo.com")
    val urlTests = Seq(
      wordPress -> "http://chris.wordpress.com/myblog",
      wordPress -> "http://joe.wordpress.com/hisblog",
      techCrunch -> "http://www.techcrunch.com/myarticle",
      nuSite1 -> "http://hekktor.mysite.com/myarticle/myarticle.html",
      nuSite1 -> "http://josephine.hekktor.mysite.com/myarticle/myarticle.html",
      nuSite2 -> "http://ofsejfojseofjo.com/hi"
    )

    for {
      (siteGuid, url) <- urlTests
      siteGuidVal = sm.findOrGenerateSiteGuidByUrl(url)

    } {
      println("Did url " + url)
      siteGuidVal.fold(fails => {
        fails.foreach(println)
      }, success => {
        success should be (siteGuid)
        success.println
      })

      println("Sectional results")
      sm.extractSectionNameFromUrl(url,"hi").println
    }

    val skNotExists = SiteKey(3030434l)
    val skExists = SiteKey(wordPress)

    sm.fetchSiteGuidBySiteKey(skExists).println
    sm.fetchSiteGuidBySiteKey(skNotExists).println
  }
}
