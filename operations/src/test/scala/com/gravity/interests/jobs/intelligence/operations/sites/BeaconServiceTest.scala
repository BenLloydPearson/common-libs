package com.gravity.interests.jobs.intelligence.operations.sites

//import com.gravity.domain.BeaconEvent
//import com.gravity.interests.jobs.intelligence._
//import com.gravity.interests.jobs.intelligence.operations.{BeaconGushService, BeaconGushedHBasePersistence}
//import com.gravity.test.operationsTesting
//import com.gravity.utilities.analytics.DateMidnightRange
//import com.gravity.utilities.grvtime._
//import com.gravity.utilities.{BaseScalaTest, HashUtils}
//import org.joda.time.DateTime
//
//import scalaz.{Failure, Success}


/*             )\._.,--....,'``.
.b--.        /;   _.. \   _\  (`._ ,.
`=,-,-'~~~   `----(,_..'--(,_..'`-.;.'  */


//class BeaconServiceTest extends BaseScalaTest with operationsTesting {
//
//  test("testWriteMetrics") {
//    val beaconService = BeaconGushService
//    def printSite(guid: String) {
//      val key = SiteKey(guid)
//
//      Schema.Sites.query2.withKey(key).withAllColumns.singleOption() match {
//        case Some(siteRow) => {
//          siteRow.prettyPrint()
//        }
//        case None => println("No site found for guid: " + guid)
//      }
//    }
//
//
//    val date = new DateTime()
//    val earlierDate = date.minusHours(2)
//    val isSameDay = date.getDayOfYear == earlierDate.getDayOfYear
//    val siteGuid = HashUtils.md5("TESTWRITEMETRICSGUID")
//    val userGuid = HashUtils.md5("TESTWRITEMETRICSUSER")
//
//    println("MADE SITE")
//    makeSite(siteGuid,"Write Metrics Site")
//
//    val siteMap = Schema.Sites.query2.withFamilies(_.meta).scanToIterable(site=>(site.siteGuid.getOrElse("") -> site)).toMap
//
//    println("SITEMAP")
//    siteMap.foreach{kv=>
//      println(kv)
//    }
//
//    val numViewsThisHour = 6L
//    val numViewsEarlier = 6L
//    val numViews = numViewsThisHour + numViewsEarlier
//    val numViewsToday = if (isSameDay) numViews else numViewsThisHour
//    val numViewsEarlierIfDifferentDay = if (isSameDay) numViews else numViewsEarlier
//    val numViewsArticle2 = 6L
//
//    val url = "http://cats.com/grooming.html"
//    val url2 = "http://cats.com/petting.html"
//    val someReferrer = "http://cats.com/wildcat.html"
//    val referrer = "http://kittens.com/schlooming.html"
//    val title = "cat grooming and thi"
//    val title2 = "cat petting"
//    val publishBeacon = makePublishBeacon(earlierDate, siteGuid, url, title, "cats, fur", "THEY DO IT WITH THEIR TONGUES", date, "tags, metaandsuch", "really. with their tongues")
//    val publishBeacon2 = makePublishBeacon(date, siteGuid, url2, title2, "cats, petting", "THEY DO IT WITH THEIR TONGUES", date, "tags, metaandsuch", "hands!")
//    val urlKey : ArticleKey = ArticleKey(url)
//    val urlKey2 : ArticleKey = ArticleKey(url2)
//    val clickKeys1 = Set(ClickStreamKey(date, url), ClickStreamKey(earlierDate, url))
//    val clickKeys2 = Set(ClickStreamKey(date, url2), ClickStreamKey(earlierDate, url2))
//    var userKey = UserSiteKey(userGuid, siteGuid)
//    var publishKey = ArticleKey(1)
//    var publishKey2 = ArticleKey(1)
//
////    SiteService.userHasVisited(userGuid, siteGuid, DateHour(date), false)
////    SiteService.userHasVisited(userGuid, siteGuid, DateHour(earlierDate), false)
//
//    val mb = BeaconEvent.fromTokenizedString(makeBeaconString(date, siteGuid, url, userGuid, someReferrer))
//    val mb2 = BeaconEvent.fromTokenizedString(makeBeaconString(date, siteGuid, url2, userGuid, someReferrer))
//    val mb_earlier = BeaconEvent.fromTokenizedString(makeBeaconString(earlierDate, siteGuid, url, userGuid, someReferrer))
//    beaconService.extractMetrics(publishBeacon) match {
//      case Success(result) => {
//        println(result)
//        beaconService.writeMetrics(result, true, false)
//        publishKey = ArticleKey(result.url)
//      }
//      case Failure(f) => {
//        println(f)
//      }
//    }
//    beaconService.extractMetrics(publishBeacon2) match {
//      case Success(result) => {
//        println(result)
//        beaconService.writeMetrics(result, true, false)
//        publishKey2 = ArticleKey(result.url)
//      }
//      case Failure(f) => {
//        println(f)
//      }
//    }
//    beaconService.flushAllData(false, false)
//
//    beaconService.extractMetrics(mb_earlier) match {
//      case Success(result) => {
//        println(result)
//        userKey = UserSiteKey(result.userGuid, result.siteGuid)
//        beaconService.writeMetrics(result, true)
//        beaconService.writeMetrics(result, true)
//        beaconService.writeMetrics(result, true)
//        beaconService.flushAllData(false, true)
//        Thread.sleep(500)
//        beaconService.writeMetrics(result, true)
//        beaconService.writeMetrics(result, true)
//        beaconService.writeMetrics(result, true)
//        beaconService.flushAllData(false, true)
//        Thread.sleep(500)
//      }
//      case Failure(failRes) => {
//        println(failRes)
//      }
//    }
//    beaconService.extractMetrics(mb) match {
//      case Success(result) => {
//        println(result)
//        userKey = UserSiteKey(result.userGuid, result.siteGuid)
//        beaconService.writeMetrics(result, true)
//        beaconService.writeMetrics(result, true)
//        beaconService.writeMetrics(result, true)
//        beaconService.flushAllData(false, false)
//        Thread.sleep(500)
//        beaconService.writeMetrics(result, true)
//        beaconService.writeMetrics(result, true)
//        beaconService.writeMetrics(result, true)
//        beaconService.flushAllData(false, false)
//        Thread.sleep(500)
//      }
//      case Failure(failRes) => {
//        println(failRes)
//      }
//    }
//    beaconService.extractMetrics(mb2) match {
//      case Success(result) => {
//        println(result)
//        userKey = UserSiteKey(result.userGuid, result.siteGuid)
//        beaconService.writeMetrics(result, true)
//        beaconService.writeMetrics(result, true)
//        beaconService.writeMetrics(result, true)
//        beaconService.flushAllData(false, true)
//        Thread.sleep(500)
//        beaconService.writeMetrics(result, true)
//        beaconService.writeMetrics(result, true)
//        beaconService.writeMetrics(result, true)
//        beaconService.flushAllData(false, true)
//        Thread.sleep(500)
//      }
//      case Failure(failRes) => {
//        println(failRes)
//      }
//    }
//
//
//    //make sure site metadata made it
//    val articleMeta = {
//      val dmRange = DateMidnightRange.forSingleDay(mb.timestamp.toGrvDateMidnight)
//      val dhRange = dmRange.toDateHourRange
//
//      Schema.Articles.query2.withKey(publishKey)
//        .withColumns(_.url, _.title)
//        .withFamilies(_.standardMetricsHourlyOld)
//        .filter(
//        _.or(
//          _.betweenColumnKeys(_.standardMetricsHourlyOld, dhRange.fromHour, dhRange.toHour),
//          _.allInFamilies(_.meta)
//        )
//      )
//      .singleOption(skipCache = true)
//    }
//    articleMeta match {
//      case Some(am) => {
//        assert(url == am.column(_.url).getOrElse(""))
//        assert(title == am.column(_.title).getOrElse(""))
//      }
//      case None => {
//        fail("Article metadata not found")
//      }
//    }
//    val articleMeta2 = {
//      val dmRange = DateMidnightRange.forSingleDay(mb.timestamp.toGrvDateMidnight)
//      val dhRange = dmRange.toDateHourRange
//
//      Schema.Articles.query2.withKey(publishKey2)
//        .withColumns(_.url, _.title)
//        .withFamilies(_.standardMetricsHourlyOld)
//        .filter(
//        _.or(
//          _.betweenColumnKeys(_.standardMetricsHourlyOld, dhRange.fromHour, dhRange.toHour),
//          _.allInFamilies(_.meta)
//        )
//      )
//      .singleOption(skipCache = true)
//    }
//    articleMeta2 match {
//      case Some(am) => {
//        assert(url2 ==  am.column(_.url).getOrElse(""))
//        assert(title2 == am.column(_.title).getOrElse(""))
//      }
//      case None => {
//        fail("Article metadata 2 not found")
//      }
//    }
//
//    val article = {
//      val dmRange = DateMidnightRange.forSingleDay(date.toGrvDateMidnight)
//      val dhRange = dmRange.toDateHourRange
//
//      Schema.Articles.query2.withKey(urlKey)
//        .withColumns(_.url, _.title)
//        .withFamilies(_.standardMetricsHourlyOld)
//        .filter(
//        _.or(
//          _.betweenColumnKeys(_.standardMetricsHourlyOld, dhRange.fromHour, dhRange.toHour),
//          _.allInFamilies(_.meta)
//        )
//      )
//      .singleOption(skipCache = true)
//    }
//    val dateMetrics = article.flatMap(_.standardMetricsOld.get(date.toGrvDateMidnight)).getOrElse(StandardMetrics.empty)
//    val hourMetrics = article.flatMap(_.family(_.standardMetricsHourlyOld).get(date.toDateHour)).getOrElse(StandardMetrics.empty)
//    assert(numViewsToday == dateMetrics.views, "date metrics for article don't match")
//    assert(numViewsThisHour == hourMetrics.views, "hour metrics for article don't match")
//
//    val article_earlier = {
//      val dmRange = DateMidnightRange.forSingleDay(earlierDate.toGrvDateMidnight)
//      val dhRange = dmRange.toDateHourRange
//
//      Schema.Articles.query2.withKey(urlKey)
//        .withColumns(_.url, _.title)
//        .withFamilies(_.standardMetricsHourlyOld)
//        .filter(
//        _.or(
//          _.betweenColumnKeys(_.standardMetricsHourlyOld, dhRange.fromHour, dhRange.toHour),
//          _.allInFamilies(_.meta)
//        )
//      )
//      .singleOption(skipCache = true)
//    }
//    val dateMetrics_earlier = article_earlier.flatMap(_.standardMetricsOld.get(earlierDate.toGrvDateMidnight)).getOrElse(StandardMetrics.empty)
//    val hourMetrics_earlier = article_earlier.flatMap(_.family(_.standardMetricsHourlyOld).get(earlierDate.toDateHour)).getOrElse(StandardMetrics.empty)
//    assert(numViewsEarlierIfDifferentDay == dateMetrics_earlier.views, "date metrics for article don't match")
//    assert(numViewsEarlier == hourMetrics_earlier.views, "earlier hour metrics for article don't match")
//    //check user metrics
//
//    Schema.UserSites.query2.withKey(userKey).withFamilies(_.clickStream).singleOption(skipCache = true) match {
//      case Some(av) => {
//        val views = av.clickStream.filterKeys(clickKeys1).values.sum
//        assert(numViews == views)
//      }
//      case None => {
//        fail("user metrics not found")
//      }
//    }
//    Schema.UserSites.query2.withKey(userKey).withFamilies(_.clickStream).singleOption(skipCache = true) match {
//      case Some(av) => {
//        val views = av.clickStream.filterKeys(clickKeys2).values.sum
//        assert(numViewsArticle2 == views)
//      }
//      case None => {
//        fail("user metrics not found")
//      }
//    }
//
//    //check site metrics
//    val site = Schema.Sites.query2.withKey(SiteKey(siteGuid)).withFamilies(_.standardMetricsHourlyOld).singleOption().get
//    assert(numViewsThisHour + numViewsArticle2  == site.family(_.standardMetricsHourlyOld)(date.toDateHour).views, "views for this hour didn't match")
//    assert(numViewsEarlier  == site.family(_.standardMetricsHourlyOld)(earlierDate.toDateHour).views, "views for earlier didn't match")
//    assert(numViewsThisHour + (if (isSameDay) numViewsEarlier else 0) + numViewsArticle2 == site.standardMetricsOld(date.toGrvDateMidnight).views, "views for today didn't match")
//
//    println("avg site write time is: " + BeaconGushedHBasePersistence.averageSiteWriteTime.getTotal)
//    printSite(siteGuid)
//  }
//
//
//}
