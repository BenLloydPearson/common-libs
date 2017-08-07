package com.gravity.interests.jobs.intelligence.operations

import com.gravity.interests.jobs.intelligence._
import com.gravity.interests.jobs.intelligence.hbase.HBaseTestEnvironment
import com.gravity.test.operationsTesting
import com.gravity.utilities.BaseScalaTest
import com.gravity.utilities.cache.PermaCacher
import com.gravity.utilities.time.DateHour
import org.joda.time.DateTime


/*             )\._.,--....,'``.
.b--.        /;   _.. \   _\  (`._ ,.
`=,-,-'~~~   `----(,_..'--(,_..'`-.;.'  */


class SiteServiceTest extends BaseScalaTest with operationsTesting {


  test("Visitors Table") {
    val siteGuid = "MYRANDOM39uf90fe90fjef"
    val userGuid = "MYRANDOM39uf90fe90fste"

    withClue("user has already visited!") {
      SiteService.hasUserVisited(userGuid, siteGuid) should be (false)
    }

    SiteService.userHasVisited(userGuid, siteGuid, DateHour.currentHour, hasVisited = true)
    withClue("user could not be marked as having visited!") {
      SiteService.hasUserVisited(userGuid, siteGuid) should be (true)
    }

    SiteService.userHasVisited(userGuid, siteGuid, DateHour.currentHour, hasVisited = false)
    withClue("user could not be marked as having not visited!") {
      SiteService.hasUserVisited(userGuid, siteGuid) should be (false)
    }
  }

  test("Visitors Table Bulk") {
    val siteGuid = "MYRANDOM39uf90fe90hjef"
    val userGuids = for (i <- 0 until 2603) yield i.toString
    val hour = DateHour.currentHour
    val keys = userGuids.map(userGuid => UserSiteHourKey(userGuid, siteGuid, hour)).toSet

    SiteService.usersHaveNotVisited(keys)

    val initialVisit = SiteService.haveUsersVisited(keys, keys.size)

    keys.foreach(key => {
      withClue(s"user $key has already visited!") {
        initialVisit.getOrElse(key, false) should be (false)
      }
    })

    SiteService.usersHaveVisited(keys)

    val secondVisit = SiteService.haveUsersVisited(keys, keys.size)

    println("size of second visit: " + secondVisit.size)

    keys.foreach(key => {
      withClue(s"user $key could not be marked as having visited!") {
        secondVisit.getOrElse(key, false) should be (true)
      }
    })

    SiteService.usersHaveNotVisited(keys)

    val thirdVisit = SiteService.haveUsersVisited(keys, keys.size)

    keys.foreach(key => {
      withClue(s"user $key could not be marked as having not visited") {
        thirdVisit.getOrElse(key, false) should be (false)
      }
    })
  }
  
  test("Recent Article Pool") {
    val siteGuid = "MYRANDOM39uf90fe90fjef"

    val article1 = ArticleKey("http://" + siteGuid + "/article1.html")
    val article1Date = new DateTime().minusHours(6)

    val article2 = ArticleKey("http://" + siteGuid + "/article2.html")
    val article2Date = new DateTime().minusHours(3)
    val article3 = ArticleKey("http://" + siteGuid + "/article3.html")
    val article3Date = new DateTime().minusHours(2)
    val article4 = ArticleKey("http://" + siteGuid + "/article4.html")
    val article4Date = new DateTime().minusHours(11)

    Schema.Sites.put(SiteKey(siteGuid)).valueMap(_.recentArticles,Map(
      PublishDateAndArticleKey(article1Date,article1) -> article1,
      PublishDateAndArticleKey(article2Date,article2) -> article2,
      PublishDateAndArticleKey(article3Date,article3) -> article3,
      PublishDateAndArticleKey(article4Date,article4) -> article4
    )).execute()

    val map = Schema.Sites.query2.withKey(SiteKey(siteGuid)).withFamilies(_.recentArticles).singleOption().get.family(_.recentArticles)

    map.size should be >= 4
    println(map)
  }

  test("Ignore domain check table value is respected") {
    withSites(2) { msc =>
      val domainCheckTrueSiteGuid = msc.siteRows(0).guid
      val domainCheckTrueSiteKey = SiteKey(domainCheckTrueSiteGuid)
      Schema.Sites.put(domainCheckTrueSiteKey).value(_.ignoreDomainCheck, true).execute()

      val domainCheckFalseSiteGuid = msc.siteRows(1).guid
      val domainCheckFalseSiteKey = SiteKey(domainCheckFalseSiteGuid)
      Schema.Sites.put(domainCheckFalseSiteKey).value(_.ignoreDomainCheck, false).execute()

      assert(SiteService.ignoreDomainCheck(domainCheckTrueSiteKey))
      assert(!SiteService.ignoreDomainCheck(domainCheckFalseSiteKey))
    }
  }
}