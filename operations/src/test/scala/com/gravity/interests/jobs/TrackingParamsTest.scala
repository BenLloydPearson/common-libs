package com.gravity.interests.jobs

import com.gravity.interests.jobs.intelligence.ArticleKey
import com.gravity.interests.jobs.intelligence.operations._
import com.gravity.utilities.grvstrings._
import com.gravity.utilities.grvtime._
import org.joda.time.format.ISODateTimeFormat
import org.junit.Assert._
import org.junit.{Before, Test}
import org.mockito.Mockito
import org.mockito.Mockito._

/**
 * Created with IntelliJ IDEA.
 * User: ahiniker
 * Date: 2/20/13
 */
class TrackingParamsTest {

  var clickEventSpy: ClickEvent = _

  val ard = ArticleRecoData(ArticleKey("http://cats.com/page"), "http://cats.com/page", currentTime, 13, 2525, "", 687, 687, 3, 3, 255, "sponseeguid", "sponsorguid", "poolguid", "", "", 50, 1, 7, 123, "", List.empty, -1, "exchangeguid")
  val clickEvent: ClickEvent = ClickEvent.empty.copy(date = System.currentTimeMillis(), pubSiteGuid = "testsiteguid",
    advertiserSiteGuid = "testadvertiserguid", userGuid = "testuserguid", currentUrl = "http://cats.com", article = ard,
    countryCodeId = 0, geoLocationDesc = null, impressionHash = null, ipAddress = null, gravityHost = null,
    isMaintenance = false, affiliateId = "affiliateId", partnerPlacementId = "partnerPlacmentId",
    clickFields = Some(ClickFields(System.currentTimeMillis(), "http://cats.com", "Ua", "referrer", "http://cats.com", Some("CLICKHASH"), Some("TRACKINGPARAMID")))
  )


  @Before
  def setup() {
    clickEventSpy = Mockito.spy(clickEvent)
  }

  @Test
  def testReplaceMacros() {

    assertEquals("http://example.com/referrer?src=gravity&clicktime=" + clickEvent.getDate.toString(ISODateTimeFormat.dateTime()).urlEncode() + "&domain=cats.com&site=testsiteguid",
                 TrackingParams.replaceMacros(clickEvent, "http://example.com/referrer?src=gravity&clicktime=%ClickTime%&domain=%Domain%&site=%SiteGuid%"))


  }

  @Test
  def testIndexMacro() {

    assertEquals("http://example.com/referrer?src=gravity&tid=" + clickEvent.article.displayIndex + "&domain=cats.com&site=testsiteguid",
      TrackingParams.replaceMacros(clickEvent, "http://example.com/referrer?src=gravity&tid=%DisplayIndex%&domain=%Domain%&site=%SiteGuid%"))

  }

  @Test
  def testExchangeGuidMacro() {

    assertEquals("http://example.com/referrer?src=gravity&eg=" + clickEvent.article.exchangeGuid + "&domain=cats.com&site=testsiteguid",
      TrackingParams.replaceMacros(clickEvent, "http://example.com/referrer?src=gravity&eg=%ExchangeGuid%&domain=%Domain%&site=%SiteGuid%"))

  }

  /*
    Verify that we don't try to execute macros if they are not present
   */
  @Test
  def testReplaceNoMacros() {

    assertEquals("http://example.com/referrer?src=gravity",
      TrackingParams.replaceMacros(clickEventSpy, "http://example.com/referrer?src=gravity"))

    verifyZeroInteractions(clickEventSpy)

  }
}
