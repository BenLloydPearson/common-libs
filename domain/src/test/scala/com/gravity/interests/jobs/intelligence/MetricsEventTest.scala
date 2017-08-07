package com.gravity.interests.jobs.intelligence

import com.gravity.domain.BeaconEvent
import com.gravity.domain.FieldConverters._
import com.gravity.interests.interfaces.userfeedback.{UserFeedbackOption, UserFeedbackPresentation}
import com.gravity.interests.jobs.intelligence.operations.ImpressionPurpose.Type
import com.gravity.interests.jobs.intelligence.operations.{AlgoSettingsData, ClickFields, _}
import com.gravity.test.domainTesting
import com.gravity.utilities.BaseScalaTest
import com.gravity.utilities.eventlogging.FieldValueRegistry
import org.joda.time.DateTime
import org.junit.Assert._

import scalaz.{Failure, Success}

/*
*     __         __
*  /"  "\     /"  "\
* (  (\  )___(  /)  )
*  \               /
*  /               \
* /    () ___ ()    \  erik 4/16/14
* |      (   )      |
*  \      \_/      /
*    \...__!__.../
*/

object MetricsEventTest {

  val geo = 1
  val geoDesc = ""
  val isMobile = true
  val pageViewIdWidgetLoaderWindowUrl = "http://viewid/~tilde"
  val pageViewDateTime: DateTime = new DateTime(27)
  val pageViewIdRand = 27L
  val pageViewUA = "v27"
  val clientTime = 27L
  val ipAddress = "v28"
  val hostname = "v28"
  val isMaintenance = false
  val currentSectionPath: SectionPath = SectionPath(Seq("v29-"))
  val recommendationScope = "v29-"
  val desiredSectionPath: SectionPath = SectionPath(Seq("v29-"))
  val affiliateId = "affiliate"
  val impressionAffiliateId = "impression affiliate"
  val renderType = "widget"
  val sitePlacementId = 5l
  val partnerPlacementId = "thisismyreallylongparamichose" //make a value when grccTypes are bumped
  val contextPath = "/"
  val userFeedbackVariation: Int = UserFeedbackOption.meh.id
  val userFeedbackPresentation: Int = UserFeedbackPresentation.inline.id
  val referrerImpressionHash = "ayylmao"
  val impressionPurpose: Type = ImpressionPurpose.defaultValue
  val pd1: ProbabilityDistribution = ProbabilityDistribution(Seq(ValueProbability(1.0, 50), ValueProbability(2.0, 100)))
  val dd: DistributionData = DistributionData(pd1, 1.0)
  val settingDataString: String = DistributionData.serializeToString(dd)
  val recoBucket = 13
  val referrer = "http://someplace"
  val clickedSitePlacementId = 666L
  val chosenUserFeedbackOption = Some(UserFeedbackOption.amused)
  val toInterstitial = true
  val interstitialUrl = "http://example.com/interstitial"
  val algoSettingsDataList: Seq[AlgoSettingsData] = Seq(
    AlgoSettingsData("setting1", 3, "setting1Value"),
    AlgoSettingsData("setting2", 1, "true"),
    AlgoSettingsData("setting3", 2, "1.5"),
    AlgoSettingsData("setting4", AlgoSettingType.ProbabilityDistribution, settingDataString)
  )
  val article1: ArticleRecoData = ArticleRecoData(ArticleKey(1), "http://www.cats.com/fur/~tilde", new DateTime().minusHours(1), 4, 6, "cause", 32, 44, 16, 12, 980L, "SPONSEEGUID", "SPONSOREGUID", "POOLGUID", "auction", CampaignKey("CAMPAIGNGUID", 1).toString, 1, -1, 0, -1, "", algoSettingsDataList, 1, "EXCHANGEGUID")
  val article2: ArticleRecoData = ArticleRecoData(ArticleKey(2), "http://www.cats.com/claws/", new DateTime().minusHours(1), 4, 6, "cause", 32, 44, 16, 12, 980L, "SPONSEEGUID", "SPONSOREGUID", "POOLGUID", "auction", CampaignKey("CAMPAIGNGUID", 1).toString, 1, -1, 1, -1, "", algoSettingsDataList, 1, "EXCHANGEGUID")
  val article3: ArticleRecoData = ArticleRecoData(ArticleKey(3), "http://www.cats.com/claws2/", new DateTime().minusHours(1), 4, 6, "cause", 32, 44, 16, 12, 980L, "SPONSEEGUID", "SPONSOREGUID", "POOLGUID", "auction", CampaignKey("CAMPAIGNGUID", 1).toString, 1, -1, 2, -1, "", algoSettingsDataList, 2, "EXCHANGEGUID")

  val article4: ArticleRecoData = ArticleRecoData(ArticleKey(4), "http://www.cats.com/claws4/", new DateTime().minusHours(1), 4, 6, "cause", 32, 44, 16, 12, 980L, "SPONSEEGUID", "SPONSOREGUID", "POOLGUID", "auction", CampaignKey("CAMPAIGNGUID", 1).toString, 1, -1, 0, -1, "", List.empty[AlgoSettingsData], 3, "EXCHANGEGUID")
  val article5: ArticleRecoData = ArticleRecoData(ArticleKey(5), "http://www.cats.com/claws5/", new DateTime().minusHours(1), 4, 6, "cause", 32, 44, 16, 12, 980L, "SPONSEEGUID", "SPONSOREGUID", "POOLGUID", "auction", CampaignKey("CAMPAIGNGUID", 1).toString, 1, -1, 3, -1, "", algoSettingsDataList, 6, "EXCHANGEGUID")

  val textEventExtraFields: EventExtraFields = EventExtraFields(renderType, sitePlacementId, partnerPlacementId,
    recoBucket, referrer, contextPath, userFeedbackVariation, referrerImpressionHash, impressionPurpose,
    userFeedbackPresentation, "page view guid", LogMethod.unknown)
  val testEvent: ImpressionEvent = ImpressionEvent(new DateTime, "SITEGUID", "USERGUID", "http://www.cats.com",
    Seq(article1, article2, article3, article4, article5), geo, geoDesc, isMobile, pageViewIdWidgetLoaderWindowUrl, pageViewDateTime, pageViewIdRand, pageViewUA, clientTime, ipAddress, hostname, isMaintenance, currentSectionPath, recommendationScope, desiredSectionPath, impressionAffiliateId, true, textEventExtraFields)
  val testClickEvent: ClickEvent = ClickEvent(System.currentTimeMillis(), "SITEGUID", "ADVSITEGUID", "USERGUID",
    "http://www.cats.com/~click", article3, geo, geoDesc, testEvent.getHashHex, ipAddress, hostname, isMaintenance,
    SectionPath("v30-"), "v30-", SectionPath("v30-"), affiliateId, partnerPlacementId, clickedSitePlacementId,
    chosenUserFeedbackOption, toInterstitial, interstitialUrl, Some(ClickFields(System.currentTimeMillis(), "http://clicky", "useragent", "referrer", "http://rawclicky", Some("CLICKHASH"), Some("TRACKINGPARAMID"))))

  val testEvent1: ImpressionEvent = ImpressionEvent(new DateTime, "SITEGUID", "USERGUID", "http://www.cats.com/~thing", Seq(article1), geo, geoDesc, isMobile, pageViewIdWidgetLoaderWindowUrl, pageViewDateTime, pageViewIdRand, pageViewUA, clientTime, ipAddress, hostname, isMaintenance, currentSectionPath, recommendationScope, desiredSectionPath, impressionAffiliateId, false, textEventExtraFields)
  val testEvent2: ImpressionEvent = ImpressionEvent(new DateTime, "SITEGUID", "USERGUID", "http://www.cats.com/~thing", Seq(article2), geo, geoDesc, isMobile, pageViewIdWidgetLoaderWindowUrl, pageViewDateTime, pageViewIdRand, pageViewUA, clientTime, ipAddress, hostname, isMaintenance, currentSectionPath, recommendationScope, desiredSectionPath, impressionAffiliateId, true, textEventExtraFields)

  val testEvent3: ImpressionEvent = ImpressionEvent(new DateTime, "SITEGUID", "USERGUID2", "http://www.cats.com", Seq(article2), geo, geoDesc, isMobile, pageViewIdWidgetLoaderWindowUrl, pageViewDateTime, pageViewIdRand, pageViewUA, clientTime, ipAddress, hostname, isMaintenance, currentSectionPath, recommendationScope, desiredSectionPath, impressionAffiliateId, false, textEventExtraFields)

  val testEvent4: ImpressionEvent = ImpressionEvent(new DateTime, "SITEGUID", "USERGUID", "http://www.cats.com",
    Seq(article4, article4, article4), geo, geoDesc, isMobile, pageViewIdWidgetLoaderWindowUrl, pageViewDateTime, pageViewIdRand, pageViewUA, clientTime, ipAddress, hostname, isMaintenance, currentSectionPath, recommendationScope, desiredSectionPath, impressionAffiliateId, false, textEventExtraFields)

  val withoutGeo = "ImpressionEvent^0^1363727021505^SITEGUID^USERGUID^493dbb0ac57845101d973d443c6d3243^0^0^0^5^2^6^http://www.cats.com^ArticleRecoData%5E0%5E1%5Ehttp%3A%2F%2Fwww.cats.com%2Ffur%5E1363723421029%5E4%5E6%5Ecause%5E32%5E44%5E16%5E12%5E980%5ESPONSEEGUID%5ESPONSOREGUID%5EPOOLGUID%5Eauction%5EsiteId%3A4325391211507853434_campaignId%3A1%5E1%5E^"

  val testStr = "I am a string with a \n newline and delimiters | ^"
  val testBeaconEvent = new BeaconEvent(42, testStr, testStr, testStr, testStr, testStr, testStr,
    testStr, testStr, testStr, testStr, testStr, testStr, testStr, testStr, testStr, testStr, testStr, testStr,
    testStr, testStr, testStr, testStr, testStr, 23, testStr, testStr, testStr, testStr, testStr, testStr, testStr,
    testStr, isRedirect = true, testStr, testStr, testStr, isOptedOut = true, conversionTrackingParamUuid = testStr
  )
}

class MetricsEventTest extends BaseScalaTest with domainTesting {
  import MetricsEventTest._

  test("testReadOld()") {
    FieldValueRegistry.getInstanceFromString[ImpressionEvent](withoutGeo) match {
      case Success(a: ImpressionEvent) =>
        assertTrue(a.isInstanceOf[ImpressionEvent])
        //assertEquals(fieldString, a.toDelimitedFieldString)
        println(a)
        println(a.toDelimitedFieldString())
      case Failure(fails) => assertTrue(fails.toString(), false)
    }
  }

  test("printFieldString()") {
    println(testEvent1.toDelimitedFieldString())
  }

  test("mergeTest()") {

    val merged = ImpressionEvent.mergeList(Seq(testEvent1, testEvent2, testEvent3))
    println(merged)
    assert(merged.size == 2, "merged list has wrong size")
    val merged1 = merged.head
    val merged2 = merged.tail.head

    assert(merged1.key == testEvent1.key, "keys did not match: " + merged1.key + " != " + testEvent1.key)

    assert(merged1.key == testEvent2.key)
    assert(merged2.key == testEvent3.key)
    assert(merged1.articlesInReco.contains(article1))
    assert(merged1.articlesInReco.contains(article2))
    assert(!merged2.articlesInReco.contains(article1))
    assert(merged2.articlesInReco.contains(article2))
  }

  test("testFieldReadWrite()") {

    val fieldString = testEvent.toDelimitedFieldString()
    println(fieldString)
    FieldValueRegistry.getInstanceFromString[ImpressionEvent](fieldString) match {
      case Success(a: ImpressionEvent) =>
        assertTrue(a.isInstanceOf[ImpressionEvent])
        assertEquals(fieldString, a.toDelimitedFieldString())
        println(a)
      case Failure(fails) => assertTrue(fails.toString(), false)
    }

    val clickFieldString = testClickEvent.toDelimitedFieldString
    println(clickFieldString)
    FieldValueRegistry.getInstanceFromString[ClickEvent](clickFieldString) match {
      case Success(a: ClickEvent) =>
        assertTrue(a.isInstanceOf[ClickEvent])
        assertEquals(clickFieldString, a.toDelimitedFieldString)
        println(a)
      case Failure(fails) => assertTrue(fails.toString(), false)
    }

  }

  test("testFieldReadWriteBytes()") {
    val fieldBytes = testEvent.toFieldBytes()
    println(fieldBytes.mkString(","))
    FieldValueRegistry.getInstanceFromBytes[ImpressionEvent](fieldBytes) match {
      case Success(a: ImpressionEvent) =>
        assertTrue(a.isInstanceOf[ImpressionEvent])
        println(a.toFieldBytes().mkString(","))
        println(testEvent)
        println(a)
        assertArrayEquals(fieldBytes, a.toFieldBytes())
        println(a)
      case Failure(fails) => assertTrue(fails.toString(), false)
    }

    val clickFieldBytes = testClickEvent.toFieldBytes()
//    println(clickFieldBytes)
    FieldValueRegistry.getInstanceFromBytes[ClickEvent](clickFieldBytes) match {
      case Success(a: ClickEvent) =>
        assertTrue(a.isInstanceOf[ClickEvent])
        assertArrayEquals(clickFieldBytes, a.toFieldBytes())
//        println(a)
      case Failure(fails) => assertTrue(fails.toString(), false)
    }

  }

  test("grcc3 test") {
    val grcc3 = testClickEvent.toGrcc3
    ClickEvent.fromGrcc3(grcc3, grcc3IsRaw = true, None) match {
      case Success(fromGrcc3) =>
        assertEquals(testClickEvent.toDelimitedFieldString, fromGrcc3.toDelimitedFieldString)
      case Failure(fails) => fail(fails.toString)
    }
  }

}


//object GrccPerfComparison extends App {
//  val iter = 50000
//
//  println(MetricsEventTest.testClickEvent.toGrcc2.length)
//  println(MetricsEventTest.testClickEvent.toGrcc3.length)
//
//  //readLine("go")
//
//  val start1 = System.currentTimeMillis()
//
//  for { i <- 0 until iter} {
//    MetricsEventTest.testClickEvent.toGrcc2
//  }
//
//  val start2 = System.currentTimeMillis()
//
//  for { i <- 0 until iter } {
//    MetricsEventTest.testClickEvent.toGrcc3
//  }
//
//  val end = System.currentTimeMillis()
//
//  //readLine("done")
//
//  val grcc2Time = start2 - start1
//
//  val grcc3Time = end - start2
//
//  println("grcc2Time " + grcc2Time + " avg " + (grcc2Time.toFloat / iter.toFloat))
//  println("grcc3Time " + grcc3Time + " avg " + (grcc3Time.toFloat / iter.toFloat))
//}