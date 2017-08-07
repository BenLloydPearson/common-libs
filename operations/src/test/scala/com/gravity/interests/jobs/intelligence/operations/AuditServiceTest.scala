package com.gravity.interests.jobs.intelligence.operations

import com.gravity.utilities.analytics.DateMidnightRange
import org.mockito.Mockito._
import org.mockito.Mockito
import org.mockito.Matchers.{eq => mockEq, _}
import com.gravity.utilities.BaseScalaTest
import com.gravity.interests.jobs.intelligence._
import com.gravity.utilities.components.FailureResult
import com.gravity.interests.jobs.intelligence.hbase.{CanBeScopedKey, ScopedKey, ScopedKeyTypes}
import org.junit.Assert
import com.gravity.interests.jobs.intelligence.operations.audit.{AuditServiceComponents, AuditLogPersistence}
import com.gravity.interests.jobs.intelligence.operations.audit.ChangeDetectors._

import scalaz._
import Scalaz.{when => scalazWhen, _}

/**
 * Created with IntelliJ IDEA.
 * User: Unger
 * Date: 6/12/13
 * Time: 4:14 PM
 */

class AuditServiceTest extends BaseScalaTest {

  val mP = mock[AuditLogPersistence]
  val mAR = mock[AuditRow2]
  when(mP.persist(any[CanBeScopedKey], any[Long], any[AuditEvents.Type], any[String], any[String], any[String], any[Seq[String]], any[Option[String]])) thenReturn mAR.successNel[FailureResult]

  trait FakePersistence extends AuditLogPersistence {
    protected[operations] def persist[T <: CanBeScopedKey](key: T, userId: Long, eventType: AuditEvents.Type, fieldName: String, fromString: String, toString: String, notes: Seq[String], forSiteGuid: Option[String]): ValidationNel[FailureResult, AuditRow2] = {
      mP.persist(key, userId, eventType, fieldName, fromString, toString, Seq.empty, forSiteGuid)
    }

    protected[operations] def persistScopedKey(key: ScopedKey, userId: Long, eventType: AuditEvents.Type, fieldName: String, fromString: String, toString: String, notes: Seq[String], forSiteGuid: Option[String]): ValidationNel[FailureResult, AuditRow2] = {
      mP.persistScopedKey(key, userId, eventType, fieldName, fromString, toString, Seq.empty, forSiteGuid)
    }

    def search[T <: CanBeScopedKey](key: T, eventTypeFilter: Seq[AuditEvents.Type] = Seq.empty, dateRangeFilter: Option[DateMidnightRange] = None, offset: Option[Int] = None, limit: Option[Int] = None, optionalFilter: Option[(AuditRow2) => Boolean] = None): ValidationNel[FailureResult, Iterable[AuditRow2]] = {
      mP.search(key)
    }
  }

  object AuditService extends AuditServiceComponents with FakePersistence with TableOperations[AuditTable2, AuditKey2, AuditRow2] {
    override def table: AuditTable2 = Schema.Audits2
  }

  val campaignKey = CampaignKey(SiteKey("gwidblah"),55l)

//  test("Detect campaign changes") {
//
//    val fromCampaignRow = Mockito.mock(classOf[CampaignRow], Mockito.RETURNS_MOCKS)
//    val toCampaignRow = Mockito.mock(classOf[CampaignRow], Mockito.RETURNS_MOCKS)
//    when(fromCampaignRow.nameOrNotSet) thenReturn "fromName"
//    when(toCampaignRow.nameOrNotSet) thenReturn "toName"
//
////    val feedsMap = Map("http://cats.com/rss" -> RssFeedSettings.default, "http://dogs.com/rss" -> RssFeedSettings.default)
////    when(fromCampaignRow.feeds) thenReturn feedsMap
////    when(toCampaignRow.feeds) thenReturn (feedsMap +
////       ("http://kitties.com/rss" -> RssFeedSettings.default) + //feed added
////       ("http://cats.com/rss" -> RssFeedSettings.default.copy(initialArticleBlacklisted = false)) + //feed settings change
////       () - //feed status change
////       ())
//
//
////    when(fromCampaignRow.column(_.siteAffected))
//    val res = AuditService.logAllCampaignChanges(42, Some(fromCampaignRow), toCampaignRow)
////    println(res)
////    verify(mP, times(1)).persist(any[Long], argThat(mockEq(AuditEvents.campaignNameUpdate)), any[SiteKey], any[String], any[String], any[AuditService.Scopes])
//    verify(mP, times(1)).persist(any[Long], mockEq(AuditEvents.campaignNameUpdate), any[SiteKey], any[String], any[String], any[AuditService.Scopes])  //one of these per expected AuditEvent.Type
//
//  }

//  test("get correct scopes for ") {
//    val fromCampaignRow = Mockito.mock(classOf[CampaignRow])//, Mockito.CALLS_REAL_METHODS)
//    val toCampaignRow = Mockito.mock(classOf[CampaignRow])//, Mockito.CALLS_REAL_METHODS)
//    when(fromCampaignRow.campaignKey) thenReturn CampaignKey("something", 55)
//    when(toCampaignRow.campaignKey) thenReturn CampaignKey("something", 55)
//
//    val feedsMapFrom = Map("http://cats.com/rss" -> RssFeedSettings.default, "http://dogs.com/rss" -> RssFeedSettings.default, "http://fish.com/rss" -> RssFeedSettings.default)
//    val feedsMapTo = feedsMapFrom +
//      ("http://kitties.com/rss" -> RssFeedSettings.default) + //feed added
//      ("http://cats.com/rss" -> RssFeedSettings.default.copy(initialArticleBlacklisted = false)) + //feed settings change
//      ("http://dogs.com/rss" -> RssFeedSettings.default.copy(feedStatus = CampaignArticleStatus.inactive)) - //feed status change
//      ("http://fish.com/rss")  //delete feed
//
//    when(fromCampaignRow.feeds) thenReturn feedsMapFrom
//    when(toCampaignRow.feeds) thenReturn feedsMapTo
//
//    when(fromCampaignRow.feedUrls) thenReturn feedsMapFrom.keySet
//    when(toCampaignRow.feedUrls) thenReturn feedsMapTo.keySet
//
//    AuditService.logAllCampaignChanges(42, Some(fromCampaignRow), toCampaignRow)
//
//    val expectedScopes = AuditService.emptyScope +
//      (ScopedKeyTypes.CAMPAIGN -> Set(CampaignKey("something", 55).toScopedKey)) +
//      (ScopedKeyTypes.RSS_FEED -> Set(RssKey("http://kitties.com/rss").toScopedKey))
//    verify(mP, times(1)).persist(any[Long], any[AuditEvents.Type], any[SiteKey], any[String], any[String], mockEq(expectedScopes))  //Verify scope
//  }

  test("test AdditionDetector") {
    val fromCampaignRow = Mockito.mock(classOf[CampaignRow])//, Mockito.CALLS_REAL_METHODS)
    val toCampaignRow = Mockito.mock(classOf[CampaignRow])//, Mockito.CALLS_REAL_METHODS)
    when(fromCampaignRow.campaignKey) thenReturn CampaignKey("something", 55)
    when(toCampaignRow.campaignKey) thenReturn CampaignKey("something", 55)

    val feedsMapFrom = Map("http://cats.com/rss" -> RssFeedSettings.default, "http://dogs.com/rss" -> RssFeedSettings.default, "http://fish.com/rss" -> RssFeedSettings.default)
    val feedsMapTo = feedsMapFrom +
      ("http://kitties.com/rss" -> RssFeedSettings.default) + //feed added
      ("http://cats.com/rss" -> RssFeedSettings.default.copy(initialArticleBlacklisted = false)) + //feed settings change
      ("http://dogs.com/rss" -> RssFeedSettings.default.copy(feedStatus = CampaignArticleStatus.inactive)) - //feed status change
      ("http://fish.com/rss")  //delete feed

    when(fromCampaignRow.feeds) thenReturn feedsMapFrom
    when(toCampaignRow.feeds) thenReturn feedsMapTo

    when(fromCampaignRow.feedUrls) thenReturn feedsMapFrom.keySet
    when(toCampaignRow.feedUrls) thenReturn feedsMapTo.keySet

    val logSeq = AdditionDetector((_: CampaignRow).feedUrls.toSeq).detectChange(Some(fromCampaignRow), Some(toCampaignRow))
    Assert.assertEquals("one change detected", 1, logSeq.size)

    val log = logSeq.head
    Assert.assertEquals("""null""", log.from)
    Assert.assertEquals(""""http://kitties.com/rss"""", log.to)
  }

  test("test CampaignRemovalDetector") {
    val fromCampaignRow = Mockito.mock(classOf[CampaignRow])
    val toCampaignRow = Mockito.mock(classOf[CampaignRow])
    when(fromCampaignRow.campaignKey) thenReturn CampaignKey("something", 55)
    when(toCampaignRow.campaignKey) thenReturn CampaignKey("something", 55)

    val feedsMapFrom = Map("http://cats.com/rss" -> RssFeedSettings.default, "http://dogs.com/rss" -> RssFeedSettings.default, "http://fish.com/rss" -> RssFeedSettings.default)
    val feedsMapTo = feedsMapFrom +
      ("http://kitties.com/rss" -> RssFeedSettings.default) + //feed added
      ("http://cats.com/rss" -> RssFeedSettings.default.copy(initialArticleBlacklisted = false)) + //feed settings change
      ("http://dogs.com/rss" -> RssFeedSettings.default.copy(feedStatus = CampaignArticleStatus.inactive)) - //feed status change
      ("http://fish.com/rss")  //delete feed

    when(fromCampaignRow.feeds) thenReturn feedsMapFrom
    when(toCampaignRow.feeds) thenReturn feedsMapTo

    when(fromCampaignRow.feedUrls) thenReturn feedsMapFrom.keySet
    when(toCampaignRow.feedUrls) thenReturn feedsMapTo.keySet

    val logSeq = RemovalDetector((_: CampaignRow).feedUrls.toSeq).detectChange(Some(fromCampaignRow), Some(toCampaignRow))
    Assert.assertEquals("one change detected", 1, logSeq.size)

    val log = logSeq.head
    Assert.assertEquals(""""http://fish.com/rss"""", log.from)
    Assert.assertEquals("""null""", log.to)
  }

  test("test CampaignMapChangeDetector - feedSettings") {
    val fromCampaignRow = Mockito.mock(classOf[CampaignRow])//, Mockito.CALLS_REAL_METHODS)
    val toCampaignRow = Mockito.mock(classOf[CampaignRow])//, Mockito.CALLS_REAL_METHODS)
    when(fromCampaignRow.campaignKey) thenReturn CampaignKey("something", 55)
    when(toCampaignRow.campaignKey) thenReturn CampaignKey("something", 55)

    val feedsMapFrom = Map("http://cats.com/rss" -> RssFeedSettings.default, "http://dogs.com/rss" -> RssFeedSettings.default, "http://fish.com/rss" -> RssFeedSettings.default)
    val feedsMapTo = feedsMapFrom +
      ("http://kitties.com/rss" -> RssFeedSettings.default) + //feed added
      ("http://cats.com/rss" -> RssFeedSettings.default.copy(initialArticleBlacklisted = false)) + //feed settings change
      ("http://dogs.com/rss" -> RssFeedSettings.default.copy(feedStatus = CampaignArticleStatus.inactive)) - //feed status change
      ("http://fish.com/rss")  //delete feed

    when(fromCampaignRow.feeds) thenReturn feedsMapFrom
    when(toCampaignRow.feeds) thenReturn feedsMapTo

    val statusChangesSeq = MapChangeDetector[CampaignRow, RssFeedSettings](_.feeds.toMap).detectChange(Some(fromCampaignRow), Some(toCampaignRow))
    Assert.assertEquals("two changes detected", 2, statusChangesSeq.size)
    val logStatus = statusChangesSeq.head
//                           {["http://cats.com/rss":{"skipAlreadyIngested":true,"ignoreMissingImages":false,"initialArticleStatus":"inactive","feedStatus":"active","scrubberEnum":"no-op","initialArticleBlacklisted":true}]}
    Assert.assertEquals("""{"http://cats.com/rss":{"skipAlreadyIngested":true,"ignoreMissingImages":false,"initialArticleStatus":"inactive","feedStatus":"active","scrubberEnum":"no-op","initialArticleBlacklisted":true}}""", logStatus.from)
    Assert.assertEquals("""{"http://cats.com/rss":{"skipAlreadyIngested":true,"ignoreMissingImages":false,"initialArticleStatus":"inactive","feedStatus":"active","scrubberEnum":"no-op","initialArticleBlacklisted":false}}""", logStatus.to)

    val logStatus2 = statusChangesSeq.tail.head
    Assert.assertEquals("""{"http://dogs.com/rss":{"skipAlreadyIngested":true,"ignoreMissingImages":false,"initialArticleStatus":"inactive","feedStatus":"active","scrubberEnum":"no-op","initialArticleBlacklisted":true}}""", logStatus2.from)
    Assert.assertEquals("""{"http://dogs.com/rss":{"skipAlreadyIngested":true,"ignoreMissingImages":false,"initialArticleStatus":"inactive","feedStatus":"inactive","scrubberEnum":"no-op","initialArticleBlacklisted":true}}""", logStatus2.to)
  }

  test("test CampaignMapChangeDetector - feedStatus") {
    val fromCampaignRow = Mockito.mock(classOf[CampaignRow])
    val toCampaignRow = Mockito.mock(classOf[CampaignRow])
    when(fromCampaignRow.campaignKey) thenReturn CampaignKey("something", 55)
    when(toCampaignRow.campaignKey) thenReturn CampaignKey("something", 55)

    val feedsMapFrom = Map("http://cats.com/rss" -> RssFeedSettings.default, "http://dogs.com/rss" -> RssFeedSettings.default, "http://fish.com/rss" -> RssFeedSettings.default)
    val feedsMapTo = feedsMapFrom +
      ("http://kitties.com/rss" -> RssFeedSettings.default) + //feed added
      ("http://cats.com/rss" -> RssFeedSettings.default.copy(initialArticleBlacklisted = false)) + //feed settings change
      ("http://dogs.com/rss" -> RssFeedSettings.default.copy(feedStatus = CampaignArticleStatus.inactive)) - //feed status change
      ("http://fish.com/rss")  //delete feed

    when(fromCampaignRow.feeds) thenReturn feedsMapFrom
    when(toCampaignRow.feeds) thenReturn feedsMapTo

    val statusChangesSeq = MapChangeDetector((_: CampaignRow).feeds.mapValues(_.feedStatus).toMap).detectChange(Some(fromCampaignRow), Some(toCampaignRow))
    Assert.assertEquals("one change detected", 1, statusChangesSeq.size)
    val logStatus = statusChangesSeq.head
    Assert.assertEquals("""{"http://dogs.com/rss":"active"}""", logStatus.from)
    Assert.assertEquals("""{"http://dogs.com/rss":"inactive"}""", logStatus.to)
  }

  test("test Map Detectors on trackingParams") {
    val fromCampaignRow = Mockito.mock(classOf[CampaignRow])
    val toCampaignRow = Mockito.mock(classOf[CampaignRow])
    when(fromCampaignRow.campaignKey) thenReturn CampaignKey("something", 55)
    when(toCampaignRow.campaignKey) thenReturn CampaignKey("something", 55)

    val trackingParamsMapFrom = Map("utm_publisher" -> "%SiteGuid%", "utm_time" -> "%ClickTime%", "setting_to_delete" -> "%DeleteMe%")
    val trackingParamsMapTo = trackingParamsMapFrom +
      ("US" -> "%CountryCode%") + // added
      ("utm_publisher" -> "%ChangedSiteGuid%") - // change
      ("setting_to_delete") //delete

    when(fromCampaignRow.trackingParams) thenReturn trackingParamsMapFrom
    when(toCampaignRow.trackingParams) thenReturn trackingParamsMapTo

    val mapChanges = MapChangeDetector((_: CampaignRow).trackingParams.toMap).detectChange(Some(fromCampaignRow), Some(toCampaignRow))
    val mapAdds = MapAdditionDetector((_: CampaignRow).trackingParams.toMap).detectChange(Some(fromCampaignRow), Some(toCampaignRow))
    val mapDeletes = MapDeletionDetector((_: CampaignRow).trackingParams.toMap).detectChange(Some(fromCampaignRow), Some(toCampaignRow))

    Assert.assertEquals("one change detected", 1, mapChanges.size)
    Assert.assertEquals("""{"utm_publisher":"%SiteGuid%"}""", mapChanges.head.from)
    Assert.assertEquals("""{"utm_publisher":"%ChangedSiteGuid%"}""", mapChanges.head.to)

    Assert.assertEquals("one add detected", 1, mapAdds.size)
    Assert.assertEquals("null", mapAdds.head.from)
    Assert.assertEquals("""{"US":"%CountryCode%"}""", mapAdds.head.to)

    Assert.assertEquals("one delete detected", 1, mapDeletes.size)
    Assert.assertEquals("""{"setting_to_delete":"%DeleteMe%"}""", mapDeletes.head.from)
    Assert.assertEquals("null", mapDeletes.head.to)
  }

}
