package com.gravity.domain

import org.junit.Assert._
import org.junit.Test
import org.scalatest.Matchers
import org.scalatest.junit.AssertionsForJUnit

/**
 * Created by Jim Plush
 * User: jim
 * Date: 4/19/11
 */

class BeaconTest extends AssertionsForJUnit with Matchers {

//  @Test def testYahooGeneratedBeacon() {
//    val beacon = "2013-07-06 00:43:01^beacon^494d6e560c95708877ba0eff211c02f3^Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 5.1; Trident/4.0; FunWebProducts; GTB7.5; .NET CLR 1.1.4322)^http://uk.news.yahoo.com/prince-harry-qualifies-apache-commander-172621436.html^^89.168.140.147^^d41d8cd98f00b204e9800998ecf8427e^^^^^undefined^^^^^^^^^^^0^^^^http://uk.news.yahoo.com/prince-harry-qualifies-apache-commander-172621436.html^^^^^^^^^^yahoo_news_uk^^1^0^"
//
//    val magellan = BeaconEvent.fromTokenizedString(beacon,rawBeaconFormat = true)
//
//    println(magellan.sectionId)
//    println(magellan.userGuid)
//    println(magellan.siteGuidOpt)
//    println(magellan.userAgentOpt)
//  }

//  @Test def testYahooSectionalBeacon() {
//    val beacon = "magellan<GRV>2013-06-16 16:06:18<GRV>2013-06-16 16:06:18^beacon^494d6e560c95708877ba0eff211c02f3^Mozilla/5.0 (Macintosh; Intel Mac OS X 10_8_4) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/27.0.1453.110 Safari/537.36^http://news.yahoo.com/beyond-nyc-other-places-adapting-climate-too-105538665.html^http://news.yahoo.com/^0:0:0:0:0:0:0:1%0^^add476abe07aabd1f9c6be2af2dd6066^^^^^Beyond NYC: Other places adapting to climate, too - Yahoo! News^^^^^^^^^^^0^^^^http://news.yahoo.com/beyond-nyc-other-places-adapting-climate-too-105538665.html^^^^^^^^^^hithere|people|of|foearth^^0^0^"
//
//    val magellan = BeaconEvent.fromTokenizedString(beacon,rawBeaconFormat = false)
//
//    Assert.assertEquals("hithere|people|of|foearth",magellan.sectionId)
//    println(magellan.sectionId)
//  }

//  @Test def testBeacon() {
//    val beaconTimestamp = "2012-03-13 09:50:43".validateDateTime(BeaconEvent.dateTimeFmt) match {
//      case Success(dateTime) =>
//        println(dateTime.getMillis)
//        dateTime
//      case Failure(fails) => fail("what the hell")
//    }
//    val sampleBeacon = "magellan<GRV>2012-03-13 09:40:43<GRV>2012-03-13 09:50:43^newpost^TEST93f88b0dda2a035be1e9998b8014^googleChrome^http://www.pbnation.com/forumdisplay.php?s=ceb^http://www.pbnation.com/^127.0.0.1^Unregistered^dd497cee28d01bcdcfd6e2e2704c61db^88^^999^9999^Post Title Here^BaseballCategory^FootballTag^^^this is my message^{\"tags_as_tags\":true}^1286435516000^Jim Bonez^M^key,words,tend,to,suck^2^<html>goflyers</html>^test1.jpg^summaryhere^http://www.pbnation.com/forumdisplay.php?grv=rawurl^^^^^^^^^^^^1"
//
//    val b = BeaconEvent.fromTokenizedString(sampleBeacon, rawBeaconFormat = true)
//
//    // check minutes old
//    val now = new DateTime()
//    val minutesOld = Minutes.minutesBetween(b.timestamp, now ).getMinutes
//    grvstrings.pr("MINUTES OLD", minutesOld)
//
//    //assertEquals("2012-03-13 09:50:43", b.dateStr.get)
//    assertEquals("newpost", b.action)
//    assertEquals("TEST93f88b0dda2a035be1e9998b8014", b.siteGuid)
//    assertEquals("googleChrome", b.userAgent)
//    assertEquals("http://www.pbnation.com/forumdisplay.php?s=ceb", b.pageUrl)
//    assertEquals("http://www.pbnation.com/", b.referrer)
//    assertEquals("www.pbnation.com", b.domainOpt.get)
//    assertEquals("127.0.0.1", b.ipAddress)
//    assertEquals("Unregistered", b.userName)
//    assertEquals("dd497cee28d01bcdcfd6e2e2704c61db", b.userGuid)
//    assertEquals("FootballTag", b.articleTags)
//    assertEquals("this is my message", b.payloadField)
//    assertEquals("Jim Bonez", b.authorName)
//    assertEquals("M", b.authorGender)
//    assertEquals(1286435516000l, b.publishTimestampAsLong.get)
//    assertEquals("key,words,tend,to,suck", b.articleKeywords)
//    assertEquals(true, b.doNotTrack())
//    assertEquals("<html>goflyers</html>", b.rawHtml)
//    assertEquals("http://www.pbnation.com/forumdisplay.php?grv=rawurl", b.href)
//
//    b.doRecommendationsStr should not (be ('empty))
//    b.doRecommendations should equal (true)
//
//    println(b.articleTitle)
//  }

//  @Test def testArticleTitleUnEscape() {
//
//    val sampleBeacon = "magellan<GRV>2012-03-13 09:40:43<GRV>2012-03-13 09:50:43^newpost^TEST93f88b0dda2a035be1e9998b8014^googleChrome^http://www.pbnation.com/forumdisplay.php?s=ceb^http://www.pbnation.com/^127.0.0.1^Unregistered^dd497cee28d01bcdcfd6e2e2704c61db^88^^999^9999^Post <em>Title</em> &amp; Stuff&ndash;Junk Here^BaseballCategory^FootballTag^^^this is my message^{\"tags_as_tags\":true}^1286435516000^Jim Bonez^M^key,words,tend,to,suck^1^<html>goflyers</html>^test1.jpg^summaryhere^http://www.pbnation.com/forumdisplay.php?grv=rawurl"
//
//    val b = BeaconEvent.fromTokenizedString(sampleBeacon, rawBeaconFormat = true)
//
//    // note the dash in expected string is a &ndash;
//    assertEquals("Post Title & Stuff–Junk Here", b.articleTitle)
//  }

  /**
  * WSJ will be sending us the user id of a logged in user as the UserName field of the magellan beacon, if it's null then we should
  * get a "NONE" back otherwise we should get the value of that uuid
  */
//  @Test def userIdIsNull() {
//
//    val sampleBeacon = "2011-08-22 02:06:22^beacon^18f3b45caef80572d421f68a979bd6dc^Mozilla/4.0^http://online.wsj.com/mdc/public/page/mdc_international.html?refresh=on^^74.123.148.66^NULL^3567dce4fc494cac4abafe8563681d94^^^^^International Markets Home - Markets Data Center - WSJ.com^^^^^^^^^^^0^^^^http://online.wsj.com/mdc/public/page/mdc_international.html?refresh=on"
//
//    val b = BeaconEvent.fromTokenizedString(sampleBeacon, rawBeaconFormat = false)
//    assert(b.userName.isEmpty)
//
//    val sampleBeaconLoggedInUser = "2011-08-22 02:06:22^beacon^18f3b45caef80572d421f68a979bd6dc^Mozilla/4.0^http://online.wsj.com/mdc/public/page/mdc_international.html?refresh=on^^74.123.148.66^UUIDWOULDGOHEREFORUSERNAME^3567dce4fc494cac4abafe8563681d94^^^^^International Markets Home - Markets Data Center - WSJ.com^^^^^^^^^^^0^^^^http://online.wsj.com/mdc/public/page/mdc_international.html?refresh=on"
//
//    val b2 = BeaconEvent.fromTokenizedString(sampleBeaconLoggedInUser, rawBeaconFormat = false)
//    assertEquals("UUIDWOULDGOHEREFORUSERNAME", b2.userName)
//
//  }

//  @Test def testSuperSiteGuid() {
//    val sampleMeeboBeacon = "magellan<GRV>2012-05-27 14:47:11<GRV>2012-05-27 14:47:11^beacon^meebo:hearstnewspapers:sfgate^Mozilla/5.0 (Windows; U; Windows NT 6.0; en-US; rv:1.9.2.3) Gecko/20100401 Firefox/3.6.3 (.NET CLR 3.5.30729)^http://www.sfgate.com/cgi-bin/article.cgi?f=/c/a/2004/10/01/BAGG6928JA1.DTL^http://www.google.com/url?sa=t&rct=j&q=brooke+silverglide&source=web&cd=4&ved=0CFcQFjAD&url=http%3A%2F%2Fwww.sfgate.com%2Fcgi-bin%2Farticle.cgi%3Ff%3D%2Fc%2Fa%2F2004%2F10%2F01%2FBAGG6928JA1.DTL&ei=baDCT5GrFsfi2QWaiJHzBA&usg=AFQjCNHklqjllGNlARdt7hLRyEElb0Q8rQ^208.81.191.100^^2b1dd868dd2d94ad296b^^^^^OAKLAND / Sausage factory secretary tells of fear / Employee says she was worried her boss would shoot her in rampage that killed 3^^^^^^^^^^^0^^^^http://www.sfgate.com/cgi-bin/article.cgi?f=/c/a/2004/10/01/BAGG6928JA1.DTL^^^^^^^^^bbe72cc3cc7e35c3eac3abc447ebaeea^^"
//    val b = BeaconEvent.fromTokenizedString(sampleMeeboBeacon, rawBeaconFormat = true)
//    Assert.assertEquals("bbe72cc3cc7e35c3eac3abc447ebaeea", b.superSiteGuid)
//
//
//    val sampleVoicesBeacon = "magellan<GRV>2012-05-27 15:07:16<GRV>2012-05-27 15:07:16^beacon^aa08660c4e79c71c48ea63f792a91327^Mozilla/5.0 (Macintosh; Intel Mac OS X 10.6; rv:12.0) Gecko/20100101 Firefox/12.0^http://voices.yahoo.com/natural-solutions-hot-flashes-night-sweats-223153.html^http://www.google.co.nz/url?sa=t&rct=j&q=rolling%20over%20in%20bed%20hot%20sweats&source=web&cd=2&ved=0CI8BEBYwAQ&url=http%3A%2F%2Fvoices.yahoo.com%2Fnatural-solutions-hot-flashes-night-sweats-223153.html&ei=pKPCT6DBEaiaiAeFmrWZCg&usg=AFQjCNH78-XminnJO_E_vMCGltAViwYZgA^125.239.195.27^^62d58138055b31ab5b4a3fded9f1b163^^^^^Natural Solutions for Hot Flashes &amp; Night Sweats - Yahoo! Voices - voices.yahoo.com^^^^^^^^^^^0^^^^http://voices.yahoo.com/natural-solutions-hot-flashes-night-sweats-223153.html?cat=5^^^^^^^^^^^"
//
//    val b2 = BeaconEvent.fromTokenizedString(sampleVoicesBeacon, rawBeaconFormat = true)
//    Assert.assertTrue("", b2.superSiteGuid.isEmpty)
//
//  }

//  @Test def testBeaconFromSourceFile() {
//
//    val sampleBeacon = "2011-02-19 12:28:51^newpost^TEST93f88b0dda2a035be1e9998b8014^googleChrome^http://www.pbnation.com/forumdisplay.php?s=ce6ad130f5ecde8f750f14ce067899ba&f=701^http://www.pbnation.com/^127.0.0.1^Unregistered^dd497cee28d01bcdcfd6e2e2704c61db^88^^999^9999^Post Title Here^BaseballCategory^FootballTag^^^this is my message^"
//
//    val b = BeaconEvent.fromTokenizedString(sampleBeacon)
//    assertEquals("newpost", b.action)
//    assertEquals("TEST93f88b0dda2a035be1e9998b8014", b.siteGuid)
//    assertEquals("dd497cee28d01bcdcfd6e2e2704c61db", b.userGuid)
//  }

//  @Test def testAgeComparisons() {
//
//    val sampleBeaconOlder = "2011-02-19 12:28:51^newpost^TEST93f88b0dda2a035be1e9998b8014^googleChrome^http://www.pbnation.com/forumdisplay.php?s=ce6ad130f5ecde8f750f14ce067899ba&f=701^http://www.pbnation.com/^127.0.0.1^Unregistered^dd497cee28d01bcdcfd6e2e2704c61db^88^^999^9999^Post Title Here^BaseballCategory^FootballTag^^^this is my message^"
//    val sampleBeaconNewer = "2011-02-19 12:30:00^newpost^TEST93f88b0dda2a035be1e9998b8014^googleChrome^http://www.pbnation.com/forumdisplay.php?s=ce6ad130f5ecde8f750f14ce067899ba&f=701^http://www.pbnation.com/^127.0.0.1^Unregistered^dd497cee28d01bcdcfd6e2e2704c61db^88^^999^9999^Post Title Here^BaseballCategory^FootballTag^^^this is my message^"
//
//
//    val newer = BeaconEvent.fromTokenizedString(sampleBeaconNewer)
//    val older = BeaconEvent.fromTokenizedString(sampleBeaconOlder)
//    assertTrue(newer isNewerThan older)
//  }

//  @Test def testAgeComparisonsFalse() {
//
//    val sampleBeaconOlder = "2011-02-19 12:28:51^newpost^TEST93f88b0dda2a035be1e9998b8014^googleChrome^http://www.pbnation.com/forumdisplay.php?s=ce6ad130f5ecde8f750f14ce067899ba&f=701^http://www.pbnation.com/^127.0.0.1^Unregistered^dd497cee28d01bcdcfd6e2e2704c61db^88^^999^9999^Post Title Here^BaseballCategory^FootballTag^^^this is my message^"
//    val sampleBeaconNewer = "2011-02-19 12:30:00^newpost^TEST93f88b0dda2a035be1e9998b8014^googleChrome^http://www.pbnation.com/forumdisplay.php?s=ce6ad130f5ecde8f750f14ce067899ba&f=701^http://www.pbnation.com/^127.0.0.1^Unregistered^dd497cee28d01bcdcfd6e2e2704c61db^88^^999^9999^Post Title Here^BaseballCategory^FootballTag^^^this is my message^"
//
//
//    val newer = BeaconEvent.fromTokenizedString(sampleBeaconNewer)
//    val older = BeaconEvent.fromTokenizedString(sampleBeaconOlder)
//    assertFalse(older isNewerThan newer)
//  }

//  @Test def testIsNewerThanWithPublishDateVariations() {
//    val strOlderNoPublish = "magellan<GRV>2011-02-19 12:28:51<GRV>2011-02-19 12:28:51^newpost^TEST93f88b0dda2a035be1e9998b8014^googleChrome^http://www.pbnation.com/forumdisplay.php?s=ce6ad130f5ecde8f750f14ce067899ba&f=701^http://www.pbnation.com/^127.0.0.1^Unregistered^dd497cee28d01bcdcfd6e2e2704c61db^88^^999^9999^Post Title Here^BaseballCategory^FootballTag^^^this is my message^"
//    val strNewerNoPublish = "magellan<GRV>2011-02-19 12:30:00<GRV>2011-02-19 12:30:00^newpost^TEST93f88b0dda2a035be1e9998b8014^googleChrome^http://www.pbnation.com/forumdisplay.php?s=ce6ad130f5ecde8f750f14ce067899ba&f=701^http://www.pbnation.com/^127.0.0.1^Unregistered^dd497cee28d01bcdcfd6e2e2704c61db^88^^999^9999^Post Title Here^BaseballCategory^FootballTag^^^this is my message^"
//
//    val strOlderWithPublish = "magellan<GRV>2011-02-19 12:28:51<GRV>2011-02-19 12:28:51^newpost^TEST93f88b0dda2a035be1e9998b8014^googleChrome^http://www.pbnation.com/forumdisplay.php?s=ce6ad130f5ecde8f750f14ce067899ba&f=701^http://www.pbnation.com/^127.0.0.1^Unregistered^dd497cee28d01bcdcfd6e2e2704c61db^88^^999^9999^Post Title Here^BaseballCategory^FootballTag^^^this is my message^^1286435516000"
//    val strNewerWithPublish = "2magellan<GRV>2011-02-19 12:28:51<GRV>2011-02-19 12:28:51^newpost^TEST93f88b0dda2a035be1e9998b8014^googleChrome^http://www.pbnation.com/forumdisplay.php?s=ce6ad130f5ecde8f750f14ce067899ba&f=701^http://www.pbnation.com/^127.0.0.1^Unregistered^dd497cee28d01bcdcfd6e2e2704c61db^88^^999^9999^Post Title Here^BaseballCategory^FootballTag^^^this is my message^^1286435916000"
//
//    val beaconOlderNoPublish = BeaconEvent.fromTokenizedString(strOlderNoPublish, rawBeaconFormat = true)
//    val beaconNewerNoPublish = BeaconEvent.fromTokenizedString(strNewerNoPublish, rawBeaconFormat = true)
//
//    val beaconOlderWithPublish = BeaconEvent.fromTokenizedString(strOlderWithPublish, rawBeaconFormat = true)
//    val beaconNewerWithPublish = BeaconEvent.fromTokenizedString(strNewerWithPublish, rawBeaconFormat = true)
//
//    /**
//    * ASSERT ALL PERMUTATIONS
//    * /
//*/
//    // all publish date vs. no publish date
//    assertTrue("With publish date should be newer than no publish date EVEN if the date is not newer! (older w/publish date vs. newer w/o publish date)", beaconOlderWithPublish isNewerThan beaconNewerNoPublish)
//    assertTrue("With publish date should be newer than no publish date EVEN if the date is the same! (older w/publish date vs. older w/o publish date)", beaconOlderWithPublish isNewerThan beaconOlderNoPublish)
//    assertTrue("With publish date should be newer than no publish date ESPECIALLY if the date IS newer! (newer w/publish date vs. older w/o publish date)", beaconNewerWithPublish isNewerThan beaconOlderNoPublish)
//    assertTrue("With publish date should be newer than no publish date EVEN if the date is same! (newer w/publish date vs. newer w/o publish date)", beaconNewerWithPublish isNewerThan beaconNewerNoPublish)
//
//    // assert when both have publish dates
//    assertTrue("Publish date vs. publish date comparisons should be as expected! (newer w/publish date vs. older w/publish date)", beaconNewerWithPublish isNewerThan beaconOlderWithPublish)
//
//    // assert when both do NOT have publish dates
//    assertTrue("Publish date vs. publish date comparisons should be as expected! (newer w/o publish date vs. older w/o publish date)", beaconNewerNoPublish isNewerThan beaconOlderNoPublish)
//  }

  @Test def testEmpty() {
    assertEquals(1, 1)
  }


}

