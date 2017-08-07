package com.gravity.data.configuration

import com.gravity.data.configuration.Implicits._
import com.gravity.domain.StrictUserGuid
import com.gravity.domain.aol.{AolDynamicLeadChannels, ChannelToPinnedSlot}
import com.gravity.domain.articles.{ContentGroupSourceTypes, ContentGroupStatus, SitePlacementType}
import com.gravity.domain.rtb.gms.ContentGroupIdToPinnedSlot
import com.gravity.interests.interfaces.userfeedback.UserFeedbackVariation
import com.gravity.interests.jobs.intelligence._
import com.gravity.interests.jobs.intelligence.hbase.ScopedKey
import com.gravity.interests.jobs.intelligence.operations.NameMatcher
import com.gravity.interests.jobs.intelligence.operations.recommendations.model.SitePlacementStatus
import com.gravity.utilities.analytics.articles.ArticleWhitelist
import com.gravity.utilities.grvstrings._
import com.gravity.utilities.MurmurHash
import com.gravity.valueclasses.ValueClassesForDomain._
import play.api.libs.json.{JsValue, Json}

import scala.collection._
import scala.slick.driver.JdbcDriver
import scala.slick.jdbc.JdbcBackend._
import scalaz.NonEmptyList

/**
 * Created by IntelliJ IDEA.
 * Author: Robbie Coleman
 * Date: 8/8/14
 * Time: 3:33 PM
 *            _ 
 *          /  \
 *         / ..|\
 *        (_\  |_)
 *        /  \@'
 *       /     \
 *   _  /  \   |
 *  \\/  \  | _\
 *   \   /_ || \\_
 *    \____)|_) \_)
 *
 */
trait ConfigurationQuerySupport
extends DlPlacementSettingQuerySupport
with ContentGroupQuerySupport
with WidgetLoaderHookQuerySupport
with WidgetHookQuerySupport
with WidgetSnapshotQuerySupport
with StaticWidgetSettingQuerySupport
with PlacementConfigQuerySupport
{
 import com.gravity.logging.Logging._

  val counterCategory: String = "Configuration DB Queries"

  def driver: JdbcDriver

  def database: Database

  def readOnlyDatabase: Database

  def permacacherDefaultTTL: Long

  protected def hashSiteUserToBucket(siteGuid: String, userGuid: String): Int = {
    def randomInt: Int = new scala.util.Random().nextInt(100)

    if (siteGuid == ArticleWhitelist.siteGuid(_.HUFFINGTON_POST)) {
      randomInt
    } else if (userGuid.isEmpty || userGuid == StrictUserGuid.emptyUserGuidHash) {
      //The hard coded user guid here is the md5 of an empty string, a common bug from people using the API
      randomInt
    } else {
      math.abs(MurmurHash.hash64(userGuid) % 100).toInt
    }
  }

  /* READ queries */

  // -- ContentGroup - \/ BEGIN \/
  def getContentGroupIdsForSiteWithoutCaching(siteGuid: String): Set[Long]
  def getContentGroupIdsForSite(siteGuid: String): Set[Long] = getContentGroupIdsForSiteWithoutCaching(siteGuid)

  def getContentGroupIdsForSitePlacementWithoutCaching(sitePlacementId: Long): Set[Long]
  def getContentGroupIdsForSitePlacement(sitePlacementId: Long): Set[Long] = getContentGroupIdsForSitePlacementWithoutCaching(sitePlacementId)

  def getContentGroupsForSiteWithoutCaching(siteGuid: String): Map[ContentGroupKey, ContentGroupRow]
  def getContentGroupsForSite(siteGuid: String): Map[ContentGroupKey, ContentGroupRow] = getContentGroupsForSiteWithoutCaching(siteGuid)

  def getContentGroupsForSitePlacementWithoutCaching(sitePlacementId: Long): Map[ContentGroupKey, ContentGroupRow]
  def getContentGroupsForSitePlacement(sitePlacementId: Long): Map[ContentGroupKey, ContentGroupRow]

  def getContentGroupsForSitePlacementsWithoutCaching(sitePlacementIds: Set[Long]): Map[Long, List[ContentGroupRow]]
  def getContentGroupsForSitePlacements(sitePlacementIds: Set[Long]): Map[Long, List[ContentGroupRow]] = {
    sitePlacementIds.toIterable.map(spId => spId -> getContentGroupsForSitePlacement(spId).values.toList).toMap
  }

  def getContentGroupWithoutCaching(groupId: Long): Option[ContentGroupRow]
  def getContentGroup(groupId: Long): Option[ContentGroupRow] = getContentGroupWithoutCaching(groupId)

  def getContentGroupsWithoutCaching(groupIds: Set[Long]): List[ContentGroupRow]
  def getContentGroups(groupIds: Set[Long]): List[ContentGroupRow] = getContentGroupsWithoutCaching(groupIds)

  def getAllContentGroupsWithoutCaching: List[ContentGroupRow]
  def getAllContentGroups: List[ContentGroupRow] = getAllContentGroupsWithoutCaching

  def getGmsContentGroupsWithoutCaching(optSiteGuid: Option[SiteGuid]): List[ContentGroupRow]
  def getGmsContentGroups(optSiteGuid: Option[SiteGuid]): List[ContentGroupRow]

  def getAllContentGroupsForTypeWithoutCaching(sourceType: ContentGroupSourceTypes.Type): List[ContentGroupRow]
  def getAllContentGroupsForType(sourceType: ContentGroupSourceTypes.Type): List[ContentGroupRow] = getAllContentGroupsForTypeWithoutCaching(sourceType)

  def findContentGroupWithoutCaching(name: String, sourceType: ContentGroupSourceTypes.Type, sourceKey: ScopedKey, forSiteGuid: String): Option[ContentGroupRow]
  def findContentGroup(name: String, sourceType: ContentGroupSourceTypes.Type, sourceKey: ScopedKey, forSiteGuid: String): Option[ContentGroupRow] = {
    findContentGroupWithoutCaching(name, sourceType, sourceKey, forSiteGuid)
  }

  def getContentGroupFuzzySearchWithoutCaching(term: NonEmptyString, siteGuids: Seq[String] = Nil): List[ContentGroupRow]
  def getContentGroupFuzzySearch(term: NonEmptyString, siteGuids: Seq[String] = Nil): List[ContentGroupRow] = {
    getContentGroupFuzzySearchWithoutCaching(term, siteGuids)
  }

  def getContentGroupFuzzySearch2WithoutCaching(
                                                 contentGroupIdFilter: Option[Set[Long]] = None,
                                                 nameFilter: Option[Set[NameMatcher]] = None,
                                                 sourceTypeFilter: Option[Set[ContentGroupSourceTypes.Type]] = None,
                                                 sourceKeyFilter: Option[Set[ScopedKey]] = None,
                                                 forSiteGuidFilter: Option[Set[String]] = None,
                                                 statusFilter: Option[Set[ContentGroupStatus.Type]] = None,
                                                 isGmsManagedFilter: Option[Boolean] = None,
                                                 isAthenaFilter: Option[Boolean] = None,
                                                 chubClientIdFilter: Option[Set[String]] = None,
                                                 chubChannelIdFilter: Option[Set[String]] = None,
                                                 chubFeedIdFilter: Option[Set[String]] = None
                                               ): List[ContentGroupRow]
  def getContentGroupFuzzySearch2(
                                   contentGroupIdFilter: Option[Set[Long]] = None,
                                   nameFilter: Option[Set[NameMatcher]] = None,
                                   sourceTypeFilter: Option[Set[ContentGroupSourceTypes.Type]] = None,
                                   sourceKeyFilter: Option[Set[ScopedKey]] = None,
                                   forSiteGuidFilter: Option[Set[String]] = None,
                                   statusFilter: Option[Set[ContentGroupStatus.Type]] = None,
                                   isGmsManagedFilter: Option[Boolean] = None,
                                   isAthenaFilter: Option[Boolean] = None,
                                   chubClientIdFilter: Option[Set[String]] = None,
                                   chubChannelIdFilter: Option[Set[String]] = None,
                                   chubFeedIdFilter: Option[Set[String]] = None
                                 ): List[ContentGroupRow] = {
    getContentGroupFuzzySearch2WithoutCaching(
      contentGroupIdFilter = contentGroupIdFilter,
      nameFilter = nameFilter,
      sourceTypeFilter = sourceTypeFilter,
      sourceKeyFilter = sourceKeyFilter,
      forSiteGuidFilter = forSiteGuidFilter,
      statusFilter = statusFilter,
      isGmsManagedFilter = isGmsManagedFilter,
      isAthenaFilter = isAthenaFilter,
      chubClientIdFilter = chubClientIdFilter,
      chubChannelIdFilter = chubChannelIdFilter,
      chubFeedIdFilter = chubFeedIdFilter
    )
  }

  def getContentGroupsForSlotWithoutCaching(slotId: Long): List[ContentGroupRow]
  def getContentGroupsForSlot(slotId: Long): List[ContentGroupRow] = getContentGroupsForSlotWithoutCaching(slotId)

  def getContentGroupsForSlotsWithoutCaching(slotIds: Set[Long]): Map[Long, List[ContentGroupRow]]
  def getContentGroupsForSlots(slotIds: Set[Long]): Map[Long, List[ContentGroupRow]] = {
    getContentGroupsForSlotsWithoutCaching(slotIds)
  }

  def getContentGroupsForPluginsWithoutCaching(pluginIds: Seq[Long], contentGroupIds: Seq[Long] = Nil,
                                contentGroupSearchTerm: Option[NonEmptyString] = None): Map[Long, Set[ContentGroupRow]]
  def getContentGroupsForPlugins(pluginIds: Seq[Long], contentGroupIds: Seq[Long] = Nil,
                                contentGroupSearchTerm: Option[NonEmptyString] = None): Map[Long, Set[ContentGroupRow]] = {
    getContentGroupsForPluginsWithoutCaching(pluginIds, contentGroupIds, contentGroupSearchTerm)
  }
  // -- ContentGroup - /\ END /\

  // -- SitePlacement - \/ BEGIN \/
  def getSitePlacementWithoutCaching(sitePlacementId: SitePlacementId): Option[SitePlacementRow]
  def getSitePlacement(sitePlacementId: SitePlacementId): Option[SitePlacementRow] = getSitePlacementWithoutCaching(sitePlacementId)

  def getSitePlacementWithoutCaching(siteGuid: String, placementId: Int): Option[SitePlacementRow]
  def getSitePlacement(siteGuid: String, placementId: Int): Option[SitePlacementRow] = {
    getSitePlacementWithoutCaching(siteGuid, placementId)
  }

  def getSitePlacementsWithoutCaching(siteGuid: String, placementOrMultiWidget: PlacementIdOrMultiWidgetId): List[SitePlacementRow]
  def getSitePlacements(siteGuid: String, placementOrMultiWidget: PlacementIdOrMultiWidgetId): List[SitePlacementRow] = {
    getSitePlacementsWithoutCaching(siteGuid, placementOrMultiWidget)
  }

  def getSitePlacementsWithoutCaching(siteGuid: String, placementIds: NonEmptyList[Int]): List[SitePlacementRow]
  def getSitePlacements(siteGuid: String, placementIds: NonEmptyList[Int]): List[SitePlacementRow] = {
    getSitePlacementsWithoutCaching(siteGuid, placementIds)
  }

  def getSitePlacementsWithoutCaching(sitePlacementIds: List[Long]): List[SitePlacementRow]
  def getSitePlacements(sitePlacementIds: List[Long]): List[SitePlacementRow] = {
    getSitePlacementsWithoutCaching(sitePlacementIds)
  }

  def getSitePlacementsWithoutCaching(naturalKeys: NonEmptyList[SitePlacementNaturalKey]): List[SitePlacementRow]
  def getSitePlacements(naturalKeys: NonEmptyList[SitePlacementNaturalKey]): List[SitePlacementRow] = {
    getSitePlacementsWithoutCaching(naturalKeys)
  }

  def allServeableSegmentsWithoutCaching: Map[SitePlacementRow, Seq[SegmentRow]]
  def allServeableSegments: Map[SitePlacementRow, Seq[SegmentRow]]

  def allSegmentsWithoutCachingInStatuses(siteGuid: SiteGuid, statuses: Set[SitePlacementStatus.Type] = Set.empty): Map[SitePlacementRow, Seq[SegmentRow]]
  def allSegmentsInStatuses(siteGuid: SiteGuid, statuses: Set[SitePlacementStatus.Type] = Set.empty): Map[SitePlacementRow, Seq[SegmentRow]]

  def getSitePlacementsForSiteWithoutCaching(siteGuid: String): List[SitePlacementRow]
  def getSitePlacementsForSite(siteGuid: String): List[SitePlacementRow] = {
    getSitePlacementsForSiteWithoutCaching(siteGuid)
  }

  def getAllSitePlacementsWithoutCaching(includeInactive: Boolean,
                                         statuses: Set[SitePlacementStatus.Type] = Set.empty): List[SitePlacementRow]
  def getAllSitePlacements(includeInactive: Boolean,
                           statuses: Set[SitePlacementStatus.Type] = Set.empty): List[SitePlacementRow]

  def activeSitePlacementKeysWithoutCaching(siteGuid: String): Set[SitePlacementIdKey]

  def getSitePlacementsForMultiWidgetWithoutCaching(multiWidgetId: Int): List[SitePlacementRow]
  def getSitePlacementsForMultiWidget(multiWidgetId: Int): List[SitePlacementRow] = {
    getSitePlacementsForMultiWidgetWithoutCaching(multiWidgetId)
  }

  def maxPlacementIdForSiteWithoutCaching(siteGuid: String): Option[Int]
  // -- SitePlacement - /\ END /\

  // -- WidgetConf - \/ BEGIN \/
  def getWidgetConfWithoutCaching(confId: Long): Option[WidgetConfRow]
  def getWidgetConf(confId: Long): Option[WidgetConfRow] = getWidgetConfWithoutCaching(confId)

  def getAllWidgetConfRowsWithoutCaching: List[WidgetConfRow]
  def getAllWidgetConfRows: List[WidgetConfRow]
  def widgetConfRowsForSitePlacementIdsWithoutCaching(spIds: List[Long]): Set[WidgetConfRow]
  // -- WidgetConf - /\ END /\

  // -- MultiWidget - \/ BEGIN \/
  def getMultiWidgetWithoutCaching(multiWidgetId: Int): Option[MultiWidgetRow]
  def getMultiWidget(multiWidgetId: Int): Option[MultiWidgetRow] = getMultiWidgetWithoutCaching(multiWidgetId)

  def getMultiWidgetsWithoutCaching(siteGuid: String): List[MultiWidgetRow]
  def getMultiWidgets(siteGuid: String): List[MultiWidgetRow] = getMultiWidgetsWithoutCaching(siteGuid)

  def getAllMultiWidgetsWithoutCaching: List[MultiWidgetRow]
  def getAllMultiWidgets: List[MultiWidgetRow] = getAllMultiWidgetsWithoutCaching

  def getAllMultiWidgetsToSitePlacementsWithoutCaching: List[MultiWidgetToSitePlacementRow]
  def getAllMultiWidgetsToSitePlacements: List[MultiWidgetToSitePlacementRow] = getAllMultiWidgetsToSitePlacementsWithoutCaching
  // -- MultiWidget - /\ END /\

  // -- Segment - \/ BEGIN \/
  def doesSiteHaveControlSegmentsWithoutCaching(siteGuid: String): Boolean
  def doesSiteHaveControlSegments(siteGuid: String): Boolean = {
    doesSiteHaveControlSegmentsWithoutCaching(siteGuid)
  }

  def getSegmentForDatabaseSegmentCompositeKeyWithoutCaching(key: DatabaseSegmentCompositeKey): Option[SitePlacementRowWithAssociatedRows]
  def getSegmentForDatabaseSegmentCompositeKey(key: DatabaseSegmentCompositeKey): Option[SitePlacementRowWithAssociatedRows] = {
    getSegmentForDatabaseSegmentCompositeKeyWithoutCaching(key)
  }

  def getSegmentsForCompositeKeysWithoutCaching(keys: Set[DatabaseSegmentCompositeKey]): List[SitePlacementRowWithAssociatedRows]
  def getSegmentsForCompositeKeys(keys: Set[DatabaseSegmentCompositeKey]): List[SitePlacementRowWithAssociatedRows] = {
    getSegmentsForCompositeKeysWithoutCaching(keys)
  }

  def getSegmentsBySegmentAndSitePlacementIdWithoutCaching(sitePlacementId: SitePlacementId, segmentId: BucketId, includeContentGroups: Boolean, includeWidgetConf: Boolean): Option[SitePlacementRowWithAssociatedRows]
  def getSegmentsBySegmentIdAndSitePlacementId(sitePlacementId: SitePlacementId, segmentId: BucketId, includeContentGroups: Boolean, includeWidgetConf: Boolean): Option[SitePlacementRowWithAssociatedRows] = {
    getSegmentsBySegmentAndSitePlacementIdWithoutCaching(sitePlacementId, segmentId, includeContentGroups, includeWidgetConf)
  }

  def getSegmentsBySitePlacementIdWithoutCaching(sitePlacementId: SitePlacementId, includeContentGroups: Boolean, includeWidgetConf: Boolean): Option[SitePlacementRowWithAssociatedRows]
  def getSegmentsBySitePlacementId(sitePlacementId: SitePlacementId, includeContentGroups: Boolean, includeWidgetConf: Boolean): Option[SitePlacementRowWithAssociatedRows] = {
    getSegmentsBySitePlacementIdWithoutCaching(sitePlacementId, includeContentGroups, includeWidgetConf)
  }

  def doAllPluginsHaveControlSegmentsForSiteWithoutCaching(siteGuid: String): Boolean
  def doAllPluginsHaveControlSegmentsForSite(siteGuid: String): Boolean = {
    doAllPluginsHaveControlSegmentsForSiteWithoutCaching(siteGuid)
  }

  def doAnyOfThesePluginsHaveControlSegmentsWithoutCaching(pluginIds: List[Long]): Boolean
  def doAnyOfThesePluginsHaveControlSegments(pluginIds: List[Long]): Boolean = {
    doAnyOfThesePluginsHaveControlSegmentsWithoutCaching(pluginIds)
  }

  def doAllOfThesePluginsHaveControlSegmentsWithoutCaching(pluginIds: List[Long]): Boolean
  def doAllOfThesePluginsHaveControlSegments(pluginIds: List[Long]): Boolean = {
    doAllOfThesePluginsHaveControlSegmentsWithoutCaching(pluginIds)
  }

  def getSegmentIsControlWithoutCaching(sitePlacementId: Long, bucketId: Int): Option[Boolean]
  def getSegmentIsControl(sitePlacementId: Long, bucketId: Int): Option[Boolean] = {
    getSegmentIsControlWithoutCaching(sitePlacementId, bucketId)
  }

  def getSegmentIsControlWithoutCaching(siteGuid: String, placementId: Int, bucketId: Int): Option[Boolean]
  def getSegmentIsControl(siteGuid: String, placementId: Int, bucketId: Int): Option[Boolean] = {
    getSegmentIsControlWithoutCaching(siteGuid, placementId, bucketId)
  }

  def getLiveSegmentWithoutCaching(siteGuid: String, placementId: Int, bucketId: Int): Option[SitePlacementRowWithAssociatedRows]
  def getLiveSegment(siteGuid: String, placementId: Int, bucketId: Int): Option[SitePlacementRowWithAssociatedRows] = {
    getLiveSegmentWithoutCaching(siteGuid, placementId, bucketId)
  }

  def getSegmentForSitePlacementAndBucketWithoutCaching(sitePlacementId: Long, bucketId: Int, liveOnly: Boolean): Option[SitePlacementRowWithAssociatedRows]
  def getSegmentForSitePlacementAndBucket(sitePlacementId: Long, bucketId: Int, liveOnly: Boolean): Option[SitePlacementRowWithAssociatedRows] = {
    getSegmentForSitePlacementAndBucketWithoutCaching(sitePlacementId, bucketId, liveOnly)
  }

  def getLiveSegmentsForRecommenderIdsWithoutCaching(recommenderIds: Set[Long], includeContentGroups: Boolean, includeWidgetConf: Boolean): List[SitePlacementRowWithAssociatedRows]
  def getLiveSegmentsForRecommenderIds(recommenderIds: Set[Long], includeContentGroups: Boolean, includeWidgetConf: Boolean): List[SitePlacementRowWithAssociatedRows] = {
    getLiveSegmentsForRecommenderIdsWithoutCaching(recommenderIds, includeContentGroups, includeWidgetConf)
  }

  def getSegmentsForSiteWithoutCaching(siteGuid: String, liveOnly: Boolean, includeContentGroups: Boolean, includeWidgetConf: Boolean): List[SitePlacementRowWithAssociatedRows]
  def getSegmentsForSite(siteGuid: String, liveOnly: Boolean, includeContentGroups: Boolean, includeWidgetConf: Boolean): List[SitePlacementRowWithAssociatedRows] = {
    getSegmentsForSiteWithoutCaching(siteGuid, liveOnly, includeContentGroups, includeWidgetConf)
  }

  def getSegmentsForSitePlacementWithoutCaching(sitePlacementId: SitePlacementId, withWidgetConf: Boolean = true): Option[SitePlacementRowWithAssociatedRows]
  def getSegmentsForSitePlacement(sitePlacementId: SitePlacementId, withWidgetConf: Boolean = true): Option[SitePlacementRowWithAssociatedRows] = {
    getSegmentsForSitePlacementWithoutCaching(sitePlacementId, withWidgetConf)
  }

  def getSegmentsForSitePlacementsWithoutCaching(sitePlacementIds: List[SitePlacementId], includeDisabled: Boolean,
                                                 withWidgetConf: Boolean): List[SitePlacementRowWithAssociatedRows]
  def getSegmentsForSitePlacements(sitePlacementIds: List[SitePlacementId], includeDisabled: Boolean,
                                   withWidgetConf: Boolean = true): List[SitePlacementRowWithAssociatedRows] = {
    getSegmentsForSitePlacementsWithoutCaching(sitePlacementIds, includeDisabled, withWidgetConf)
  }

  def getSegmentsForExchangeWithoutCaching(exchangeKey: ExchangeKey, includeDisabled: Boolean, withWidgetConf: Boolean): Iterable[SitePlacementRowWithAssociatedRows]
  def getSegmentsForExchange(exchangeKey: ExchangeKey, includeDisabled: Boolean, withWidgetConf: Boolean): Iterable[SitePlacementRowWithAssociatedRows] = {
    getSegmentsForExchangeWithoutCaching(exchangeKey, includeDisabled, withWidgetConf)
  }

  def getAllSegmentsWithoutCaching(includeContentGroups: Boolean, includeWidgetConf: Boolean): List[SitePlacementRowWithAssociatedRows]
  def getAllSegments(includeContentGroups: Boolean, includeWidgetConf: Boolean): List[SitePlacementRowWithAssociatedRows] = {
    getAllSegmentsWithoutCaching(includeContentGroups, includeWidgetConf)
  }

  def getMaxBucketIdForSitePlacement(sitePlacementId: SitePlacementId): BucketId

  /**
    * Updates a segment's user feedback variation "in place", i.e. without going through the normal immutable segment
    * update process which involves disabling the old segment and creating a new one.
    */
  def updateSegmentUserFeedbackVariationInPlace(segmentId: DatabaseSegmentCompositeKey,
                                                userFeedbackVariation: UserFeedbackVariation.Type): Int
  // -- Segment - /\ END /\

  // -- AolDlPinnedArticleTable - \/ BEGIN \/
  def getArticleKeyForChannelAndSlot(channel: AolDynamicLeadChannels.Type, slot: Int): Option[ArticleKey]

  def getPinnedRowsForChannelsToSlots(channelToSlot: List[ChannelToPinnedSlot], exceptForArticleKey: ArticleKey): List[AolDlPinnedArticleRow]

  def getPinnedSlotForChannelAndArticle(channel: AolDynamicLeadChannels.Type, articleKey: ArticleKey): Option[Int]

  def getPinnedRowsForChannel(channel: AolDynamicLeadChannels.Type): List[AolDlPinnedArticleRow]

  def getAllPinnedArticles: List[AolDlPinnedArticleRow]

  def setAllChannelPins(channel: AolDynamicLeadChannels.Type, pins: scala.collection.Map[Int, ArticleKey]): SetAllChannelPinsResult
  case class SetAllChannelPinsResult(unpinnedArticleKeys: Set[ArticleKey], addedPinnedArticleKeys: Map[ArticleKey, Int]) {
    def allModifiedKeys: Set[ArticleKey] = unpinnedArticleKeys.toSet ++ addedPinnedArticleKeys.keySet
  }
  
  def deletePinningsForArticleKey(articleKey: ArticleKey): Int
  // -- AolDlPinnedArticleTable - /\ END /\

  // -- GmsPinnedArticleTable - \/ BEGIN \/
  def getArticleKeyForContentGroupAndSlot(contentGroupId: Long, slot: Int): Option[ArticleKey]

  def getPinnedRowsForContentGroupsToSlots(groupToSlot: List[ContentGroupIdToPinnedSlot], exceptForArticleKey: ArticleKey): List[GmsPinnedArticleRow]

  def getPinnedRowsForContentGroup(contentGroupId: Long): List[GmsPinnedArticleRow]

  def getPinnedRowsForContentGroups(contentGroupIds: Set[Long]): List[GmsPinnedArticleRow]

  def setAllContentGroupPins(contentGroupId: Long, pins: scala.collection.Map[Int, ArticleKey]): SetAllContentGroupPinsResult

  def deleteGmsPinningsForArticleKey(siteGuid: SiteGuid, articleKey: ArticleKey): Int
  // -- GmsPinnedArticleTable - /\  END  /\


  /* CREATE queries */
  def createAll(): Unit

  def insertSitePlacement(sitePlacementInsert: SitePlacementInsert): SitePlacementRow

  def insertSegment(segmentInsert: SegmentInsert): SegmentRow

  def insertArticleSlots(articleSlotsInsert: ArticleSlotsInsert): ArticleSlotsRow

  def insertContentGroup(contentGroupInsert: ContentGroupInsert): ContentGroupRow

  def insertWidgetConf(wc: WidgetConfRow): WidgetConfRow

  def associateContentGroupToSlots(contentGroupId: Long, articleSlotsId: Long): Long

  /* UPDATE queries */
  def updateContentGroup(group: ContentGroupRow): ContentGroupRow

  def updateSegmentsToDisabledForSite(siteGuid: SiteGuid)(implicit session: Session): Int

  def updateSegmentsToDisabledForSitePlacement(sitePlacementId: SitePlacementId)(implicit session: Session): Int

  def updateSitePlacementConfigVersion(sitePlacementId: SitePlacementId, configVersion: Long)(implicit session: Session): Int

  def updateSitePlacementFallbacksVersion(sitePlacementId: SitePlacementId, fallbacksVersion: Long)(implicit session: Session): Int

  def updateSegmentFallbackDetails(sitePlacementId: SitePlacementId, bucketId: BucketId, sitePlacementConfigVersion: Long, fallbackDetails: FallbackDetails)(implicit session: Session): Int

  def updateSitePlacementType(sitePlacementId: SitePlacementId, sitePlacementType: SitePlacementType.Type)(implicit session: Session): Int

  def persistUpserts(upserts: SegmentInsertsAndUpdates, configVersion: Long): (List[SitePlacementRowWithAssociatedRows], SegmentIdToPassedSegment)
  type SegmentIdToPassedSegment = Map[SitePlacementBucketKeyAlt, SegmentInsertWithAssociatedData]
  
  /* DELETE queries */
  def deleteContentGroup(contentGroupId: Long): Boolean

  def deleteSitePlacement(sitePlacementId: SitePlacementId): Boolean

  /* Database and schema support */
  private[data] val configDb = new ConfigurationDatabase(driver)

  private[data] def ddl = configDb.ddl


  protected val contentGroupTable = configDb.ContentGroupTable

  protected val sitePlacementTable = configDb.SitePlacementTable

  protected val dlPlacementSettingTable = configDb.DlPlacementSettingTable

  protected val staticWidgetSettingTable = configDb.staticWidgetSettingTable

  protected val aolDlPinnedArticleTable = configDb.AolDlPinnedArticleTable

  protected val articleSlotsToContentGroupsAssocTable = configDb.ArticleSlotsToContentGroupsAssocTable

  protected val articleSlotsTable = configDb.ArticleSlotsTable

  protected val widgetConfQuery = configDb.widgetConfQuery

  protected val widgetLoaderHookTable = configDb.WidgetLoaderHookTable

  protected val widgetHookQuery = configDb.widgetHookQuery

  protected val widgetSnapshotQuery = configDb.widgetSnapshotQuery

  protected val segmentTable = configDb.SegmentTable

  protected val multiWidgetTable = configDb.MultiWidgetTable

  protected val multiWidgetToSitePlacementTable = configDb.MultiWidgetToSitePlacementTable

  protected val gmsPinnedArticleTable = configDb.GmsPinnedArticleTable

  protected val algoTypeTable = configDb.AlgoTypeTable
  protected val recommenderTypeTable = configDb.RecommenderTypeTable
  protected val algoTable = configDb.AlgoTable
  protected val strategyTable = configDb.StrategyTable
  protected val recommenderTable = configDb.RecommenderTable
  protected val recommenderAlgoPriorityTable = configDb.RecommenderAlgoPriorityTable

  def printSitePlacementTable(): Unit

  def printSegmentTable(): Unit

  def printArticleSlotsTable(): Unit

  def printContentGroupsTable(): Unit

  def printWidgetConfTable(): Unit

  def printArticleSlotsToContentGroupsAssociationTable(): Unit
}

class ConfigurationDatabase(val driver: JdbcDriver)
  extends ContentGroupTable
  with SitePlacementTable
  with DlPlacementSettingTable
  with AolDlPinnedArticleTable
  with ArticleSlotsToContentGroupsAssocTable
  with ArticleSlotsTable
  with WidgetConfTable
  with SegmentTable
  with MultiWidgetTable
  with MultiWidgetToSitePlacementTable
  with WidgetLoaderHookTable
  with WidgetHookTable
  with WidgetSnapshotTable
  with StaticWidgetSettingTable
  with GmsPinnedArticleTable
  with AlgoTypeTable
  with RecommenderTypeTable
  with AlgoTable
  with StrategyTable
  with RecommenderTable
  with RecommenderAlgoPriorityTable {

  import driver.simple._
  
  def ddl = {
      ContentGroupTable.ddl ++
      SitePlacementTable.ddl ++
      DlPlacementSettingTable.ddl ++
      staticWidgetSettingTable.ddl ++
      AolDlPinnedArticleTable.ddl ++
      ArticleSlotsToContentGroupsAssocTable.ddl ++
      ArticleSlotsTable.ddl ++
      widgetConfQuery.ddl ++
      SegmentTable.ddl ++
      MultiWidgetTable.ddl ++
      MultiWidgetToSitePlacementTable.ddl ++
      WidgetLoaderHookTable.ddl ++
      widgetHookQuery.ddl ++
      widgetSnapshotQuery.ddl ++
      GmsPinnedArticleTable.ddl ++
      AlgoTypeTable.ddl ++
      RecommenderTypeTable.ddl ++
      AlgoTable.ddl ++
      StrategyTable.ddl ++
      RecommenderTable.ddl ++
      RecommenderAlgoPriorityTable.ddl
  }

  //these are here because some magic is not carried along to get the create method on ddl or delete method on tablequery in ConfigurationQueries, and i'm sick of trying to figure out how to make it happen

  def createAll(implicit session : scala.slick.jdbc.JdbcBackend#SessionDef) = {

    try {
      // println(ddl.createStatements.mkString("\n"))
      ddl.create
    } catch {
      case e: Exception if e.getMessage != null && e.getMessage.contains("already exists") => // no-op
      case e: Exception => e.printStackTrace()
    }
  }

  def dropAll(implicit session : scala.slick.jdbc.JdbcBackend#SessionDef) = ddl.drop

  def findContentGroupWithoutCachingBS(name: String, sourceType: ContentGroupSourceTypes.Type, sourceKey: ScopedKey, forSiteGuid: String)
                                      (implicit session : scala.slick.jdbc.JdbcBackend#SessionDef) : Option[ContentGroupRow] = {
    findContentGroupWithoutCachingImpl(name, sourceType, sourceKey, forSiteGuid)
  }

  def getGmsContentGroups(optSiteGuid: Option[SiteGuid])
                         (implicit session : scala.slick.jdbc.JdbcBackend#SessionDef): List[ContentGroupRow] = {
    getGmsContentGroupsImpl(optSiteGuid)
  }

  def getContentGroupsForSourceType(sourceType: ContentGroupSourceTypes.Type)
                                   (implicit session : scala.slick.jdbc.JdbcBackend#SessionDef): List[ContentGroupRow] = {
    getContentGroupsForSourceTypeImpl(sourceType)
  }

  def deleteFromContentGroup(contentGroupId : Long) (implicit session : scala.slick.jdbc.JdbcBackend#SessionDef) = {
    ContentGroupTable.filter(_.id === contentGroupId).delete
  }

  def deleteFromSitePlacement(sitePlacementId: SitePlacementId)(implicit session : scala.slick.jdbc.JdbcBackend#SessionDef) = {
    SitePlacementTable.filter(_.id === sitePlacementId.raw).delete
  }

  def deleteFromArticleSlotsToContentGroups(articleSlotId : Long)(implicit session : scala.slick.jdbc.JdbcBackend#SessionDef) = {
    ArticleSlotsToContentGroupsAssocTable.filter(_.articleSlotId === articleSlotId).delete
  }

  def deleteFromAolDlPinnedArticle(articleKey: ArticleKey)(implicit session : scala.slick.jdbc.JdbcBackend#SessionDef): Int = {
    AolDlPinnedArticleTable.filter(_.articleId === articleKey.articleId).delete
  }

  def deleteFromAolDlPinnedArticle(channel: AolDynamicLeadChannels.Type)(implicit session : scala.slick.jdbc.JdbcBackend#SessionDef): Int = {
    AolDlPinnedArticleTable.filter(_.channel === channel).delete
  }

  /** @return The pinned articles that were deleted. */
  def findAndDeleteFromAolDlPinnedArticle(channel: AolDynamicLeadChannels.Type)(implicit session : scala.slick.jdbc.JdbcBackend#SessionDef): List[AolDlPinnedArticleRow] = {
    session.withTransaction {
      val baseQ = AolDlPinnedArticleTable.filter(_.channel === channel)
      val removedArticles = baseQ.list
      baseQ.delete
      removedArticles
    }
  }

  def deleteFromGmsPinnedArticle(siteGuid: SiteGuid, articleKey: ArticleKey)(implicit session : scala.slick.jdbc.JdbcBackend#SessionDef): Int = {
    session.withTransaction {
      // Do an inner join to get the list of ContentGroupIds that pin the this articleKey for this siteGuid.
      val baseQ = for {
        pinnedRow <- GmsPinnedArticleTable
        if pinnedRow.articleId === articleKey.articleId
        (contentGroup, _) <- ContentGroupTable innerJoin GmsPinnedArticleTable on (_.id === _.contentGroupId)
        if contentGroup.forSiteGuid === siteGuid.raw
      } yield {
        pinnedRow.contentGroupId
      }

      // Delete rows in GmsPinnedArticleTable that have matching (articleId, contentGroupId).
      val contentGroupIds = baseQ.list

      val rowsToDeleteQuery = GmsPinnedArticleTable
        .filter(_.contentGroupId inSetBind(contentGroupIds))
        .filter(_.articleId === articleKey.articleId)

      rowsToDeleteQuery.delete
    }
  }

  /** @return The pinned articles that were deleted. */
  def findAndDeleteFromGmsPinnedArticle(contentGroupId: Long)(implicit session : scala.slick.jdbc.JdbcBackend#SessionDef): List[GmsPinnedArticleRow] = {
    session.withTransaction {
      val baseQ = GmsPinnedArticleTable.filter(_.contentGroupId === contentGroupId)
      val removedArticles = baseQ.list
      baseQ.delete
      removedArticles
    }
  }

  implicit val jsValueColumnType = MappedColumnType.base[JsValue, String](js => Json.stringify(js), s => Json.parse(s))
}
