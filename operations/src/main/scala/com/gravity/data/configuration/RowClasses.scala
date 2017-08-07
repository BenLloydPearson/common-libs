package com.gravity.data.configuration

import com.gravity.domain.CssDimension
import com.gravity.domain.articles.{ContentGroupSourceTypes, ContentGroupStatus, SitePlacementType}
import com.gravity.domain.recommendations.{ContentGroup, ContentGroupFields}
import com.gravity.interests.interfaces.userfeedback.UserFeedbackSpec
import com.gravity.interests.jobs.intelligence._
import com.gravity.interests.jobs.intelligence.hbase.ScopedKey
import com.gravity.utilities.components.FailureResult
import com.gravity.utilities.grvjson
import com.gravity.utilities.grvstrings._
import com.gravity.utilities.thumby.ThumbyMode
import com.gravity.valueclasses.ValueClassesForDomain._
import org.joda.time.DateTime
import play.api.libs.json._
import com.gravity.grvlogging._

import scala.collection.mutable
import scalaz.{Failure, NonEmptyList, Success}

/**
 * Created by IntelliJ IDEA.
 * Author: Robbie Coleman
 * Date: 8/8/14
 * Time: 3:19 PM
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
trait RowClasses {
  /* A marker trait just to allow searching */
}

case class ColumnParseFailureResult(id: Long, columnName: String, reason: String = "Column Value was NULL") extends FailureResult(s"Failed to parse column `$columnName` for rowID: $id, reason: $reason!", None)

case class ContentGroupRow(id: Long, name: String, sourceType: ContentGroupSourceTypes.Type, sourceKey: ScopedKey, forSiteGuid: String, status: ContentGroupStatus.Type, isGmsManaged: Boolean, isAthena: Boolean, chubClientId: String, chubChannelId: String, chubFeedId: String) extends ContentGroupFields {
  def asContentGroup: ContentGroup = withId(id)
  def key: ContentGroupKey = ContentGroupKey(id)
}

case class ContentGroupInsert(name: String, sourceType: ContentGroupSourceTypes.Type, sourceKey: ScopedKey, forSiteGuid: String, status: ContentGroupStatus.Type, isGmsManaged: Boolean, isAthena: Boolean, chubClientId: String, chubChannelId: String, chubFeedId: String) extends ContentGroupFields {
  def toRow(id: Long) = ContentGroupRow(id, name, sourceType, sourceKey, forSiteGuid, status, isGmsManaged, isAthena, chubClientId, chubChannelId, chubFeedId)
  def toRow() : ContentGroupRow = toRow(0)
}

object ContentGroupRow extends ((Long, String, ContentGroupSourceTypes.Type, ScopedKey, String, ContentGroupStatus.Type, Boolean, Boolean, String, String, String) => ContentGroupRow) {
  val default = ContentGroupRow(-1, "Default Group", ContentGroupSourceTypes.notUsed, EverythingKey.toScopedKey, "NO_SITE_GUID", ContentGroupStatus.active, false, false, "", "", "")
  val defaultList = List(default)

  def fromDomain(group: ContentGroup): ContentGroupRow = {
    ContentGroupRow(group.id, group.name, group.sourceType, group.sourceKey, group.forSiteGuid, group.status, group.isGmsManaged, group.isAthena, group.chubClientId, group.chubChannelId, group.chubFeedId)
  }
}

case class ArticleSlotsRow(id: Long, segmentId: Long, minSlotInclusive: Int, maxSlotExclusive: Int, recommenderId: Long,
                           failureStrategyId: Int) {
  var maxDaysOld: Option[Int] = None
}

//object ArticleSlotsRow
//  extends((Long, Long, Int, Int, Long, Int) => ArticleSlotsRow) {
//
//  def apply(id: Long, segmentId: Long, minSlotInclusive: Int, maxSlotExclusive: Int, recommenderId: Long, failureStrategyId: Int): ArticleSlotsRow =
//    ArticleSlotsRow(id, segmentId, minSlotInclusive, maxSlotExclusive, recommenderId, failureStrategyId)
//}

case class ArticleSlotsInsert(segmentId: Long, minSlotInclusive: Int, maxSlotExclusive: Int, recommenderId: Long,
                              failureStrategyId: Int, maxDaysOld: Option[Int]) {
  def toRow(id: Long) = ArticleSlotsRow(id, segmentId, minSlotInclusive, maxSlotExclusive, recommenderId,
    failureStrategyId)
  def toRow() :ArticleSlotsRow = toRow(0)
}

/** @param maxDaysOld This is actually saved as an AlgoSetting in Hbase -- i.e. not a property in the SQL DB. */
case class ArticleSlotsPreSegmentInsert(minSlotInclusive: Int, maxSlotExclusive: Int, recommenderId: Long, failureStrategyId: Int,
                                        maxDaysOld: Option[Int], groupIds: List[Long] = Nil) {
  def toInsert(segmentId: Long): ArticleSlotsInsert =
    ArticleSlotsInsert(segmentId, minSlotInclusive, maxSlotExclusive, recommenderId, failureStrategyId, maxDaysOld)
}

case class SegmentRow(id: Long, bucketId: Int, isControl: Boolean, sitePlacementId: Long, isLive: Boolean,
                      minUserInclusive: Int, maxUserExclusive: Int, widgetConfId: Option[Long], insertedTime: DateTime,
                      insertedByUserId: Long, updatedTime: DateTime, updatedByUserId: Long,
                      sitePlacementConfigVersion: Long, fallbacksLastAttemptTime: Option[DateTime],
                      fallbacksLastSuccessTime: Option[DateTime], fallbacksErrorMessage: Option[String],
                      displayName: Option[String], index: Int, criteria: Option[JsValue],
                      userFeedbackVariation: String, userFeedbackPresentation: String) {
  def b = bucketId.asBucketId

  def altKey: SitePlacementBucketKeyAlt = SitePlacementBucketKeyAlt(bucketId, sitePlacementId)

  def userFeedbackSpec: UserFeedbackSpec = UserFeedbackSpec(userFeedbackVariation, userFeedbackPresentation)
}

object SegmentRowFilters {

  implicit class SegmentRowIterableExtensions[T](segments: List[T]) {

    implicit def pruneSegments(getSegmentRow: T => SegmentRow, getSitePlacementRow: T => SitePlacementRow): List[T] = {
      val bucketAndVersions = new mutable.HashSet[(Int, Long)]

      val segmentData = segments.map(t => (t, getSegmentRow(t)))

      segmentData.groupBy(s => (getSitePlacementRow(s._1), s._2.bucketId)).map{ case (_, segs) => {
        val configVersion = getSitePlacementRow(segs.head._1).configVersion
        val fallbacksVersion = getSitePlacementRow(segs.head._1).fallbacksVersion

        val configSegments = segs.configVersionOnly(t => getSegmentRow(t._1), t => getSitePlacementRow(t._1))
        val fallbackSegments = segs.fallbacksVersionOnly(t => getSegmentRow(t._1), t => getSitePlacementRow(t._1))

        bucketAndVersions ++= configSegments.map(tup => tup._2.bucketId -> tup._2.sitePlacementConfigVersion)
        if (configVersion != fallbacksVersion){
          bucketAndVersions ++= fallbackSegments.map(tup => tup._2.bucketId -> tup._2.sitePlacementConfigVersion)
        }

        if (configSegments.isEmpty && fallbackSegments.isEmpty) {
          bucketAndVersions += (segs.head._2.bucketId -> segs.head._2.sitePlacementConfigVersion)
        }
      }}

      segments.filter(t => {
        val sr = getSegmentRow(t)
        bucketAndVersions.contains((sr.bucketId, sr.sitePlacementConfigVersion))
      })
    }

    implicit def configVersionOnly(segmentRow: T => SegmentRow, sitePlacementRow: T => SitePlacementRow): List[T] = {
      segments.filter(t => segmentRow(t).sitePlacementConfigVersion == sitePlacementRow(t).configVersion)
    }

    implicit def fallbacksVersionOnly(segmentRow: T => SegmentRow, sitePlacementRow: T => SitePlacementRow): List[T] = {
      segments.filter(t => segmentRow(t).sitePlacementConfigVersion == sitePlacementRow(t).fallbacksVersion)
    }

    implicit def configOrFallbacksVersionOnly(segmentRow: T => SegmentRow, sitePlacementRow: T => SitePlacementRow): List[T] = {
      segments.filter(t => segmentRow(t).sitePlacementConfigVersion == sitePlacementRow(t).fallbacksVersion ||
        segmentRow(t).sitePlacementConfigVersion == sitePlacementRow(t).configVersion)
    }

  }

}

/**
  * @param userFeedbackVariation Maintained as a String and not a UserFeedbackVariation.Type to allow downstream
  *                              systems to pass through new user feedback variations without needing the most
  *                              up-to-date code.
  */
case class SegmentInsert(bucketId: Int, isControl: Boolean, sitePlacementId: Long, isLive: Boolean,
                         minUserInclusive: Int, maxUserExclusive: Int, widgetConfId: Option[Long],
                         insertedTime: DateTime, insertedByUserId: Long, updatedTime: DateTime, updatedByUserId: Long,
                         sitePlacementConfigVersion: Long, displayName: Option[String], index: Int,
                         criteria: Option[JsValue], userFeedbackVariation: String, userFeedbackPresentation: String) {
  def toRow(id: Long) = SegmentRow(id, bucketId, isControl, sitePlacementId, isLive,
    minUserInclusive, maxUserExclusive, widgetConfId, insertedTime,
    insertedByUserId, updatedTime, updatedByUserId, sitePlacementConfigVersion, None, None, None, displayName, index,
    criteria, userFeedbackVariation, userFeedbackPresentation)
  def toRow() : SegmentRow = toRow(0)
}

object SegmentInsert {
  def apply(bucketId: Int, isControl: Boolean, sitePlacementId: Long, isLive: Boolean,
            minUserInclusive: Int, maxUserExclusive: Int, widgetConfId: Option[Long], time: DateTime,
            byUserId: Long, sitePlacementConfigVersion: Long, displayName: Option[String], index: Int,
            criteria: JsValue, userFeedbackVariation: String, userFeedbackPresentation: String): SegmentInsert =
    SegmentInsert(bucketId, isControl, sitePlacementId, isLive, minUserInclusive, maxUserExclusive, widgetConfId,
      time, byUserId, time, byUserId, sitePlacementConfigVersion, displayName, index, Some(criteria),
      userFeedbackVariation, userFeedbackPresentation)
}

case class SegmentInsertWithAssociatedData(segmentInsert: SegmentInsert, slotsList: List[ArticleSlotsPreSegmentInsert])

case class SegmentInsertsAndUpdates(inserts: Seq[SegmentInsertWithAssociatedData]) {
  def isEmpty: Boolean = inserts.isEmpty
  def nonEmpty: Boolean = !isEmpty
}

case class FallbackDetails(lastAttemptTime: Option[DateTime], lastSuccessTime: Option[DateTime], errorMessage: Option[String])
object FallbackDetails {
  val empty = FallbackDetails(None, None, None)
}

case class SitePlacementRow(id: Long, siteGuid: String, placementId: Int, displayName: String,
                            expectsAolPromoIds: Boolean, doAolOmniture: Boolean,
                            clickThroughOnRightClick: Boolean, exposeWidgetErrorHook: Boolean, mlid: String,
                            cid: String, mnid: String, cmsSrc: String, statusId: Int, paginateSubsequent: Boolean,
                            configVersion: Long, fallbacksVersion: Long, externalPlacementId: Option[String],
                            placementType: SitePlacementType.Type, forceBucket: Option[Int], generateStaticJson: Boolean,
                            contextualRecosUrlOverride: String) {
  val sg = siteGuid.asSiteGuid
  val sp = id.asSitePlacementId
  def siteKey: SiteKey = SiteKey(siteGuid)
}

/** A companion of [[SitePlacementRow]], but so named because case class tupled method needed for Slick. */
object SitePlacementRowCompanion {
  implicit val jsonWrites = Writes[SitePlacementRow](row => Json.obj(
    "id" -> row.id,
    "siteGuid" -> row.siteGuid,
    "placementId" -> row.placementId,
    "displayName" -> row.displayName,
    "expectsAolPromoIds" -> row.expectsAolPromoIds,
    "doAolOmniture" -> row.doAolOmniture,
    "clickThroughOnRightClick" -> row.clickThroughOnRightClick,
    "exposeWidgetErrorHook" -> row.exposeWidgetErrorHook,
    "mlid" -> row.mlid,
    "cid" -> row.cid,
    "mnid" -> row.mnid,
    "cmsSrc" -> row.cmsSrc,
    "statusId" -> row.statusId,
    "paginateSubsequent" -> row.paginateSubsequent,
    "configVersion" -> row.configVersion,
    "fallbacksVersion" -> row.fallbacksVersion,
    "externalPlacementId" -> row.externalPlacementId,
    "placementType" -> row.placementType,
    "forceBucket" -> row.forceBucket,
    "generateStaticJson" -> row.generateStaticJson,
    "contextualRecosUrlOverride" -> row.contextualRecosUrlOverride
  ))
}

case class SitePlacementInsert(siteGuid: String, placementId: Int, displayName: String,
                               expectsAolPromoIds: Boolean, doAolOmniture: Boolean,
                               clickThroughOnRightClick: Boolean, exposeWidgetErrorHook: Boolean, mlid: String,
                               cid: String, mnid: String, cmsSrc: String, statusId: Int, paginateSubsequent: Boolean,
                               configVersion: Long, fallbacksVersion: Long, externalPlacementId: Option[String],
                               placementType: SitePlacementType.Type, forceBucket: Option[Int],
                               generateStaticJson: Boolean, contextualRecosUrlOverride: String) {
  def toRow(id: Long) = SitePlacementRow(id, siteGuid, placementId, displayName,
    expectsAolPromoIds, doAolOmniture,
    clickThroughOnRightClick, exposeWidgetErrorHook, mlid,
    cid, mnid, cmsSrc, statusId, paginateSubsequent, configVersion,
    fallbacksVersion, externalPlacementId, placementType, forceBucket, generateStaticJson, contextualRecosUrlOverride)
  def toRow() : SitePlacementRow = toRow(0)
}

object SitePlacementInsert {
  def apply(siteGuid: String, placementId: Int, displayName: String, placementType: SitePlacementType.Type = SitePlacementType.defaultValue): SitePlacementInsert = {
    SitePlacementInsert(siteGuid, placementId, displayName, expectsAolPromoIds = false, doAolOmniture = false, clickThroughOnRightClick = false,
      exposeWidgetErrorHook = false, mlid = emptyString, cid = emptyString, mnid = emptyString, cmsSrc = "Gravity", statusId = 0,
      paginateSubsequent = false, configVersion = 1, fallbacksVersion = 0, externalPlacementId = None, placementType = placementType,
      forceBucket = None, generateStaticJson = false, contextualRecosUrlOverride = emptyString)
  }
}

case class MultiWidgetRow(id: Int, siteGuid: String, displayName: String)

case class MultiWidgetToSitePlacementRow(multiWidgetId: Int, sitePlacementId: Long, tabDisplayName: String,
                                      tabDisplayOrder: Short)

case class WidgetConfRow(
  id: Long
  ,articleLimit: Int
  ,maxTitleLength: Int
  ,maxContentLength: Int
  ,widgetDefaultTitle: String
  ,cssOverride: String
  ,width: String
  ,height: String
  ,useDynamicHeight: Boolean
  ,footerTemplate: String
  ,attributionTemplate: String
  ,defaultImageUrl: String
  ,thumbyMode: ThumbyMode.Type
  ,numMarkupLists: Int
  ,description: String
  ,truncateArticleTitles: Boolean
  ,truncateArticleContent: Boolean
  ,organicsInNewTab: Boolean
  ,beaconInWidgetLoader: Boolean
  ,suppressImpressionViewed: Boolean
  ,enableImageTooltip: Boolean
  ,doImageMagicBgBlur: Boolean
) {
  import WidgetConfRow._

  override def toString: String = grvjson.jsonStr(this)

  def cssWidth: Option[CssDimension] = getCssDimensionSuccessOrLog(width)

  def cssHeight: Option[CssDimension] = getCssDimensionSuccessOrLog(height)

  private def getCssDimensionSuccessOrLog(valueString: String): Option[CssDimension] = CssDimension(valueString) match {
    case Success(value) => value
    case Failure(err) => {
      warn(err, "Skipping invalid WidgetConf property")
      None
    }
  }

  /** Fail-safe is that dynamic height will be used if CSS height in DB is invalid. */
  def useDynamicHeightWithFailSafe: Boolean = useDynamicHeight || height.isEmpty

  def key: WidgetConfKey = WidgetConfKey(id)
}

object WidgetConfRow {
  implicit val thumbyJsonFormat = ThumbyMode.jsonFormat
  implicit val jsonWrites: Writes[WidgetConfRow] = Writes[WidgetConfRow](row => Json.obj(
    "id" -> row.id
    ,"articleLimit" -> row.articleLimit
    ,"maxTitleLength" -> row.maxTitleLength
    ,"maxContentLength" -> row.maxContentLength
    ,"widgetDefaultTitle" -> row.widgetDefaultTitle
    ,"cssOverride" -> row.cssOverride
    ,"width" -> row.width
    ,"height" -> row.height
    ,"useDynamicHeight" -> row.useDynamicHeight
    ,"footerTemplate" -> row.footerTemplate
    ,"attributionTemplate" -> row.attributionTemplate
    ,"defaultImageUrl" -> row.defaultImageUrl
    ,"thumbyMode" -> row.thumbyMode
    ,"numMarkupLists" -> row.numMarkupLists
    ,"description" -> row.description
    ,"truncateArticleTitles" -> row.truncateArticleTitles
    ,"truncateArticleContent" -> row.truncateArticleContent
    ,"organicsInNewTab" -> row.organicsInNewTab
    ,"beaconInWidgetLoader" -> row.beaconInWidgetLoader
    ,"suppressImpressionViewed" -> row.suppressImpressionViewed
    ,"enableImageTooltip" -> row.enableImageTooltip
    ,"doImageMagicBgBlur" -> row.doImageMagicBgBlur
  ))
}

object DefaultWidgetConfRow extends WidgetConfRow(
  id = -1L,
  articleLimit = 10,
  maxTitleLength = 0,
  maxContentLength = -1,
  widgetDefaultTitle = "Most Popular Stories",
  cssOverride = emptyString,
  width = emptyString,
  height = emptyString,
  useDynamicHeight = false,
  footerTemplate = emptyString,
  attributionTemplate = "/personalization/attribution.scaml",
  defaultImageUrl = emptyString,
  thumbyMode = ThumbyMode.off,
  numMarkupLists = 1,
  description = emptyString,
  truncateArticleTitles = true,
  truncateArticleContent = false,
  organicsInNewTab = false,
  beaconInWidgetLoader = false,
  suppressImpressionViewed = false,
  enableImageTooltip = true,
  doImageMagicBgBlur = false
)

case class ArticleSlotsRowWithContentGroups(slots: ArticleSlotsRow, contentGroups: List[ContentGroupRow])

case class SegmentRowWithAssociatedRows(segment: SegmentRow, sitePlacement: SitePlacementRow, slots: List[ArticleSlotsRowWithContentGroups], widgetConf: Option[WidgetConfRow])

case class ArticleSlotsRowWithAssociatedContentGroupRows(slots: ArticleSlotsRow, groups: List[ContentGroupRow])

case class SegmentRowWithAssociatedArticleSlots(segment: SegmentRow, slots: NonEmptyList[ArticleSlotsRowWithAssociatedContentGroupRows], widgetConf: Option[WidgetConfRow])

case class SitePlacementRowWithAssociatedRows(sitePlacement: SitePlacementRow, segments: List[SegmentRowWithAssociatedArticleSlots])


/**
 * New placement config
 */
case class AlgoTypeRow(id: Long, name: String)

object AlgoTypeRow {

  // Personalized
  val SEMANTIC_CLUSTERED = "Semantic Clustered"
  val BEHAVIORAL_CLUSTERED = "Behavioral Clustered"
  val CONTEXTUAL_CLUSTERED = "Contextual Clustered"

  // Fallbacks/Popularity
  val POPULARITY = "Popularity"
  val RECENCY = "Recency"

  // Live was for go90
  val LIVE = "Live"
}


case class RecommenderTypeRow(id: Long, name: String) {

  def isLive = RecommenderTypeRow.LIVE.equals(name)

  def isDlug = RecommenderTypeRow.DLUG.equals(name)
}

object RecommenderTypeRow {

  val LIVE = "Live"
  val DLUG = "DLUG"
}

case class AlgoRow(id: Long, name: String, algoTypeId: Long)

case class Algo(id: Long, name: String, algoType: AlgoTypeRow) {

  def algoId: Int = id.toInt
}

case class AlgoPriority(priority: Long, algo: Algo)

case class StrategyRow(id: Long, name: String)

case class Strategy(id: Long, name: String) {

  def strategyId:Int = id.toInt
}

case class StrategyAlgoRow(strategyId: Long, algoId: Long)

case class RecommenderRow(id: Long, name: String, isDefault: Boolean, typeId: Long, strategyId: Long)

case class RecommenderAlgoPriorityRow(recommenderId: Long, algoId: Long, priority: Long)

case class RecommenderConfig(id: Long, name: String, isDefault: Boolean, recommenderType: RecommenderTypeRow, strategy: Strategy, algos:Seq[AlgoPriority] = Seq.empty[AlgoPriority]) {

  /**
   * Either overridden or set in the strategy. Assumes min 1 max 1
   *
   * Convert to Int to maintain the existing code and logging infrastructure that expects int
   *
   * @return Seq[Long]
   */
  def algosWithOverrides(algoTypeName:String): Seq[Int] = {

    algos.filter(_.algo.algoType.name == algoTypeName).sortBy(_.priority).map(_.algo.algoId)
  }

  def fallbackAlgos: Seq[Int] = {
    algos.filter(algo=>{algo.algo.algoType.name == AlgoTypeRow.POPULARITY || algo.algo.algoType.name == AlgoTypeRow.RECENCY}).sortBy(_.priority).map(_.algo.algoId)
  }
  def semanticClusteredAlgos: Seq[Int] = algosWithOverrides(AlgoTypeRow.SEMANTIC_CLUSTERED)
  def behavioralClusteredAlgos: Seq[Int] = algosWithOverrides(AlgoTypeRow.BEHAVIORAL_CLUSTERED)
  def contextualAlgos: Seq[Int] = algosWithOverrides(AlgoTypeRow.CONTEXTUAL_CLUSTERED)
  def liveAlgos: Seq[Int] = algosWithOverrides(AlgoTypeRow.LIVE)
  // remove these two below
  def itemSimilarityAlgos: Seq[Int] = fallbackAlgos
  def yoAlgos: Seq[Int] = fallbackAlgos

}

object RecommenderConfig {

  val DEFAULT_RECOMMENDER_ID = 1
}

