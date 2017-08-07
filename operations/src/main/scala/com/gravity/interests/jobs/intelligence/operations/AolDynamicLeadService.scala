package com.gravity.interests.jobs.intelligence.operations

import java.util.concurrent.atomic.AtomicInteger

import com.fasterxml.jackson.core.JsonProcessingException
import com.gravity.data.configuration.{AolDlPinnedArticleRow, ConfigurationQueryService}
import com.gravity.domain.GrvDuration
import com.gravity.domain.aol.{AolDynamicLeadArticle, AolDynamicLeadMetrics, _}
import com.gravity.domain.gms.{GmsAlgoSettings, UniArticleId, GmsRoute, GmsArticleStatus}
import com.gravity.domain.recommendations.ContentGroup
import com.gravity.hbase.schema.{ByteConverter, ComplexByteConverter, OpsResult}
import com.gravity.interests.jobs.hbase.HBaseConfProvider
import com.gravity.interests.jobs.intelligence.ArtGrvMap._
import com.gravity.interests.jobs.intelligence.SchemaTypes.ArticleKeyConverter
import com.gravity.interests.jobs.intelligence._
import com.gravity.interests.jobs.intelligence.hbase._
import com.gravity.interests.jobs.intelligence.operations.ServiceFailures.RowNotFound
import com.gravity.interests.jobs.intelligence.operations.audit.AolUniArticleAuditLoggingEvents._
import com.gravity.interests.jobs.intelligence.operations.audit.AuditService
import com.gravity.service.grvroles
import com.gravity.utilities._
import com.gravity.utilities.analytics.DateMidnightRange
import com.gravity.utilities.analytics.articles.AolMisc
import com.gravity.utilities.cache.{CacheFactory, PermaCacher, SingletonCache}
import com.gravity.utilities.components.FailureResult
import com.gravity.utilities.grvannotation.BasicNoteTypes.{MsgNote, Note, SimpleMessageMetaNote}
import com.gravity.utilities.grvannotation._
import com.gravity.utilities.grvcoll._
import com.gravity.utilities.grvenum.GrvEnum
import com.gravity.utilities.grvenum.GrvEnum._
import com.gravity.utilities.grvjson._
import com.gravity.utilities.grvstage.StageWithMeta
import com.gravity.utilities.grvstrings._
import com.gravity.utilities.grvtime._
import com.gravity.utilities.grvz._
import com.gravity.utilities.time.DateMinute
import com.gravity.utilities.web.NegatableEnum
import com.gravity.valueclasses.ValueClassesForDomain.SitePlacementId
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp
import org.apache.hadoop.hbase.filter.{BinaryComparator, ValueFilter}
import org.joda.time.DateTime
import play.api.libs.json._

import scala.collection._
import scalaz.std.option._
import scalaz.syntax.apply._
import scalaz.syntax.foldable._
import scalaz.syntax.monoid._
import scalaz.syntax.std.option._
import scalaz.syntax.validation._
import scalaz.{Failure, NonEmptyList, Success, ValidationNel}

/**
 * Created by IntelliJ IDEA.
 * Author: Robbie Coleman
 * Date: 5/6/14
 * Time: 2:07 PM
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

// Provides unified services for DLUG and GMS articles.
object AolUniService extends DlugOrGmsService {
  val counterCategory: String = "AolUniService"
}

case class AolUniArticleInfo(aolArticleFieldsOption: Option[AolUniArticle]) {
  lazy val isEmpty = aolArticleFieldsOption.isEmpty

  //
  // AOL Specific Fields to support their Dynamic Lead Unit (DL):
  //

  // GMS-USAGE: The following fields are used only by ArticleMeta (or to construct fields used only by ArticleMeta).
  lazy val aolCategoryTextOption: Option[String] = aolArticleFieldsOption.map(_.categoryText)
  lazy val aolCategoryLinkOption:Option[String] = aolArticleFieldsOption.map(_.categoryUrl)
  lazy val aolTitleOption:Option[String] = aolArticleFieldsOption.map(_.title)
  lazy val aolHeadlineOption:Option[String] = aolArticleFieldsOption.map(_.headline)
  lazy val aolImageOption:Option[String] = aolArticleFieldsOption.map(_.imageUrl)
  lazy val aolPromotionIsActiveOption: Option[Boolean] = aolArticleFieldsOption.map(_.isActive)
  lazy val aolSecondaryHeaderOption: Option[String] = aolArticleFieldsOption.map(_.secondaryHeader)
  lazy val aolSourceTextOption:Option[String] = aolArticleFieldsOption.map(_.sourceText)
  lazy val aolSourceLinkOption:Option[String] = aolArticleFieldsOption.map(_.sourceUrl)
  lazy val aolSecondaryLinks:Seq[AolLink] = aolArticleFieldsOption.flatMap(_.secondaryLinks.noneForEmpty).getOrElse(Nil)
  lazy val aolCategoryOption: Option[AolLink] = (aolCategoryTextOption tuple aolCategoryLinkOption).map({case (text: String, url: String) => AolLink(url, text)})
  lazy val aolSourceOption: Option[AolLink] = (aolSourceTextOption tuple aolSourceLinkOption).map({case (text: String, url: String) => AolLink(url, text)})
  lazy val aolShowVideoButton: Boolean = aolArticleFieldsOption.exists(_.showVideoIcon)

  // GMS-USAGE: This field is also used by ArticleCandidateReference.
  lazy val aolSummaryOption:Option[String] = aolArticleFieldsOption.map(_.summary)

  // GMS-USAGE: These fields are also used by AolUniService.checkIfRecommendableAndUpdateIfNeeded, and ChezRobbie.
  lazy val aolStartDateOption: Option[DateTime] = aolArticleFieldsOption.flatMap(_.startDate)
  lazy val aolEndDateOption: Option[DateTime] = aolArticleFieldsOption.flatMap(_.endDate)
  lazy val aolDurationOption: Option[GrvDuration] = aolArticleFieldsOption.flatMap(_.duration)

  // GMS-USAGE: This field is used in about 8 places.
  lazy val aolGmsStatusOption: Option[GmsArticleStatus.Type] = aolArticleFieldsOption.flatMap(dlArticle => GmsArticleStatus.get(dlArticle.dlArticleStatus))

  // GMS-USAGE: Used in ArticleCandidateReference (for use by AOLIngestRecencyScores) and had been used in FilterScripts.
  lazy val aolIngestTime: DateTime = aolArticleFieldsOption.fold(grvtime.epochDateTime)(_.updatedDateTime)

  // GMS-USAGE: Used in ArticleCandidateReference (for use by AolIsActiveMostRecentBundle and AolImpressionViewedScoreBundle).
  lazy val aolPromotionIsActive: Boolean = aolPromotionIsActiveOption.getOrElse(true)
}

case class NoGrvMapFailureResult(articleKey: ArticleKey, urlOption: Option[String], gmsRoute: GmsRoute)
  extends FailureResult(s"No GrvMap found for article: articleId: ${articleKey.articleId}, gmsRoute: $gmsRoute, URL: ${urlOption.getOrElse("NO_URL")}")

object GrvMapAolUniCacher extends SingletonCache[AolUniArticle] {
  override def cacheName = "gen-grvmap-aoluni"
}

object AllArtGrvMapSource extends GrvEnum[Byte] {
  case class Type(i: Byte, n: String) extends ValueTypeBase(i, n)
  override def mkValue(id: Byte, name: String): Type = Type(id, name)

  val unknown: Type = Value(0, "unknown")
  val articleRow: Type = Value(1, "AR")
  val articleRecommendationData: Type = Value(2, "ARD")
  val articleCandidateReference: Type = Value(3, "ACR")

  val defaultValue: Type = unknown

  @transient implicit val byteConverter: ComplexByteConverter[Type] = byteEnumByteConverter(this)

  implicit val enumJsonSerializer: EnumNameSerializer[AllArtGrvMapSource.type] = new EnumNameSerializer(this)

  implicit val jsonFormat: Format[Type] = makeJsonFormat[Type]
}

trait DlugOrGmsService extends MaintenanceModeProtection {
  import com.gravity.logging.Logging._

  val successfulEmptyOpsResult: ValidationNel[FailureResult, OpsResult] = OpsResult(0, 0, 0).successNel
  val successfulSingleOpsResult: ValidationNel[FailureResult, OpsResult] = OpsResult(0, 1, 0).successNel
  val isNotRecommendable: ValidationNel[FailureResult, Boolean] = false.successNel
  val isRecommendable: ValidationNel[FailureResult, Boolean] = true.successNel
  val defaultCampaignArticleSettings = CampaignArticleSettings.getDefault(isActive = true)
  implicit val conf = HBaseConfProvider.getConf.defaultConf

  // See also ArticleCandidate.isManagedArticleCandidate and SlotContext.isManagedSlotContext
  def isManagedSitePlacement(spId: SitePlacementId, skipCache: Boolean = false) =
    GmsService.isGmsManagedSitePlacement(spId, skipCache)

  def getSelectedUniArticlesFromHbase(articleKeys: Seq[ArticleKey], gmsRoute: GmsRoute, withMetrics: Boolean = true, pullMetricsFromCacheOnly: Boolean = false): ValidationNel[FailureResult, List[AolUniArticle]] = {
    if (articleKeys.isEmpty)
      return Nil.successNel

    if (gmsRoute.isDlugScopeKey)
      AolDynamicLeadService.getSelectedArticlesFromHbase(articleKeys, withMetrics = withMetrics, pullMetricsFromCacheOnly = pullMetricsFromCacheOnly)
    else
      GmsService.getSelectedGmsArticlesFromHbase(articleKeys, gmsRoute, withMetrics = withMetrics, pullMetricsFromCacheOnly = pullMetricsFromCacheOnly)
  }

  // Each ArticleKey may have DLUG/GMS articles for more than one site, some of which may be invalid.  Try to build them all.
  def getAllUniArticlesForArticleKey(articleKey: ArticleKey): ValidationNel[FailureResult, Map[UniArticleId, ValidationNel[FailureResult, AolUniArticle]]] = {
    for {
      artRow <- ArticleService.fetch(articleKey)(_.withFamilies(_.meta, _.allArtGrvMap))

      // Find all available GmsRoutes in the grv:map OneScopeKeys
      allRoutes = GmsRoute.allRoutes(artRow.allArtGrvMap)   // In getAllUniArticlesForArticleKey
    } yield {
      (for {
        gmsRoute <- allRoutes
      } yield {
        if (gmsRoute.isDlugScopeKey)
          UniArticleId(articleKey, gmsRoute) -> AolDynamicLeadService.tryBuildDynamicLeadArticleFromArtRowWithoutMetrics(artRow, gmsRoute)
        else
          UniArticleId(articleKey, gmsRoute) -> GmsService.tryBuildGmsArticleFromArtRowWithoutMetrics(artRow, gmsRoute)
      }).toMap
    }
  }

  def updateDlArticleStatus(uniArticleId: UniArticleId, status: GmsArticleStatus.Type, userId: Long, updateCampaignSettingsOnly: Boolean): ValidationNel[FailureResult, OpsResult] = {
    if (uniArticleId.forGms) {
      SiteService.sg(uniArticleId.siteKey).toValidationNel(FailureResult(s"Could not get SiteGUid for ${uniArticleId.siteKey}")).flatMap { siteGuid =>
        GmsService.updateDlArticleStatus(siteGuid, uniArticleId.articleKey, status, userId, updateCampaignSettingsOnly)
      }
    } else {
      OpsResult(0, 0, 0).successNel
//      AolDynamicLeadService.updateDlArticleStatus(uniArticleId.articleKey, status, userId, updateCampaignSettingsOnly)
    }
  }

  def checkIfRecommendableAndUpdateIfNeeded(uniArticleId: UniArticleId, url: String, dlStatus: GmsArticleStatus.Type,
                                            startDateOpt: Option[DateTime], endDateOpt: Option[DateTime], durationOpt: Option[GrvDuration],
                                            executeUpdate: Boolean): ValidationNel[FailureResult, Boolean] = {
    if (uniArticleId.forGms) {
      SiteService.sg(uniArticleId.siteKey).toValidationNel(FailureResult(s"Could not get SiteGUid for ${uniArticleId.siteKey}")).flatMap { siteGuid =>
        GmsService.checkIfRecommendableAndUpdateIfNeeded(siteGuid, uniArticleId.articleKey, url, dlStatus, startDateOpt, endDateOpt, durationOpt, executeUpdate)
      }
    } else {
      false.successNel
//      AolDynamicLeadService.checkIfRecommendableAndUpdateIfNeeded(uniArticleId.articleKey, url, dlStatus, startDateOpt, endDateOpt, durationOpt, executeUpdate)
    }
  }

  /**
    * Uses all DL/GMS Specific Business Logic to determine if the specified `article` is recommendable
    *
    * @param article The DL/GMS field populated [[com.gravity.interests.jobs.intelligence.ArticleRow]] we are checking
    * @param executeUpdate Whether or not to actually update an article's status. Most cases should be `true`, otherwise
    *                      pass `false` (for instance in performance intensive areas).
    * @return `true` if this article passes all DL Specific Business Logic
    */
  def checkIfRecommendableAndUpdateIfNeeded(executeUpdate: Boolean)(article: ArticleRow, ck: CampaignKey): Boolean = {
    val dlInfo = article.getAolUniArticleInfo(ck)

    dlInfo.aolGmsStatusOption.fold(false) { case dlStatus: GmsArticleStatus.Type =>
      val gmsRoute = GmsRoute(ck)  // GmsRoute(ck) is safe here.

      checkIfRecommendableAndUpdateIfNeeded(
        UniArticleId(article.articleKey, gmsRoute), article.url, dlStatus,
        dlInfo.aolStartDateOption, dlInfo.aolEndDateOption,
        dlInfo.aolDurationOption, executeUpdate).valueOr(_ => false)
    }
  }

  // Given the ContentGroups, such as from a SlotDef, return a Seq of found non-empty AolUniArticleInfo for the article.
  def getAolUniArticleInfos(articleKey: ArticleKey, urlOption: Option[String], allArtGrvMapSrc: AllArtGrvMapSource.Type, allArtGrvMap: ArtGrvMap.AllScopesMap, contentGroups: Seq[ContentGroup]): Seq[AolUniArticleInfo] = {
    if (allArtGrvMap.isEmpty)
      Seq()
    else
      GmsRoute.fromContentGroups(contentGroups).map(gmsRoute => getAolUniArticleInfo(articleKey, urlOption, allArtGrvMapSrc, allArtGrvMap, gmsRoute.some)).filterNot(_.isEmpty)
  }

  def bestAolUniArticleInfo(articleKey: ArticleKey, urlOption: Option[String], allArtGrvMapSrc: AllArtGrvMapSource.Type, allArtGrvMap: ArtGrvMap.AllScopesMap, contentGroups: Seq[ContentGroup]): AolUniArticleInfo =
    getAolUniArticleInfos(articleKey, urlOption, allArtGrvMapSrc, allArtGrvMap, contentGroups).headOption getOrElse AolUniArticleInfo(None)

  // Caches the result of building an AolDynamicLeadArticle or AolGmsArticle from the serialized GrvMap
  def getAolUniArticleInfo(articleKey: ArticleKey, urlOption: Option[String], allArtGrvMapSrc: AllArtGrvMapSource.Type, allArtGrvMap: ArtGrvMap.AllScopesMap, optGmsRoute: Option[GmsRoute]): AolUniArticleInfo = {
    // Only cache successes; there's a much bigger difference between has-an-article and no-article than between article versions.
    val optDlArticle: Option[AolUniArticle] = optGmsRoute.flatMap { gmsRoute =>
      val cacheKey = s"${allArtGrvMapSrc}_${gmsRoute.oneScopeKey}_${articleKey.articleId}"

      GrvMapAolUniCacher.cache.getItem(cacheKey) match {
        case Some(aolUniArticle) => aolUniArticle.some

        case None =>
          val dlArticleV = if (gmsRoute.isDlugScopeKey)
            AolDynamicLeadService.tryBuildDynamicLeadArticleFromGrvMap(articleKey, urlOption, allArtGrvMap, gmsRoute, AolDynamicLeadMetricsWithMinutely.emptyMetrics)
          else
            GmsService.tryBuildGmsArticleFromGrvMap(articleKey, urlOption, allArtGrvMap, gmsRoute, AolDynamicLeadMetricsWithMinutely.emptyMetrics)

          dlArticleV match {
            case Success(dlArticle) =>
              GrvMapAolUniCacher.cache.putItem(cacheKey, dlArticle, 60)
              dlArticle.some

            case Failure(fails) =>
              if (fails.len > 1 && !fails.head.message.startsWith("No GrvMap found for article: ")) {
                // GrvMap was present but we failed somewhere in the building process
                warn(fails, "Failed to build DL Article from article row with id: " + articleKey.articleId)
              }
              None
          }
      }
    }

    AolUniArticleInfo(optDlArticle)
  }

  def scopedMetricsCacheQuery: ScopedMetricsService.QuerySpec = {
    val oldestMinute = ScopedMetricsKey.partialByStartDate(grvtime.currentMinute.minusMinutes(30))
    val oldestMonthHour = ScopedMetricsKey.partialByStartDate(grvtime.currentMonth.minusYears(2).toDateHour)

    _.withFamilies(_.scopedMetricsByMinute, _.scopedMetricsByMonth).filter(
      _.or(
        _.lessThanColumnKey(_.scopedMetricsByMinute, oldestMinute),
        _.lessThanColumnKey(_.scopedMetricsByMonth, oldestMonthHour)
      )
    )
  }

  def validateSecondaryLinksFromGrvMap(oneScopeMap: OneScopeMap): ValidationNel[FailureResult, List[AolLink]] = {
    implicit val reads = AolLink.internalJsonReads

    for {
      jsonString <- oneScopeMap.validateString(AolUniFieldNames.SecondaryLinks, isOptional = true)
      _ = if (jsonString.isEmpty) return List.empty[AolLink].successNel
      links <- {
        try
          Json.fromJson[List[AolLink]](Json.parse(jsonString)).toValidation.leftMap(err => nel(FailureResult(err.toString)))
        catch {
          case e: JsonProcessingException => return FailureResult(e).failureNel
        }
      }
    } yield links
  }

  /**
    * Based on the specified values, find any cases that would result in a change of status
    *
    * @param currentStatus The `currentStatus` of an Aol Dynamic Lead Article
    * @param startDateOption The `startDateOption` of an Aol Dynamic Lead Article
    * @param endDateOption The `endDateOption` of an Aol Dynamic Lead Article
    * @return `Some(newStatus)` if a required change has been found, otherwise `None`
    */
  def findNewStatus(currentStatus: GmsArticleStatus.Type, startDateOption: Option[DateTime],
                    endDateOption: Option[DateTime], durationOpt: Option[GrvDuration]): Option[GmsArticleStatus.Type] = {
    currentStatus match {
      case GmsArticleStatus.Approved =>
        // approved status is used only to stage units until its start time comes
        val now = grvtime.currentTime

        endDateOption.foreach(endDate => {
          if (now.isAfter(endDate) || now.equals(endDate)) {
            // This article has now expired since we are now on or after the end date.
            trace("Found new status of `expired` for previous status: {0}", currentStatus)
            return GmsArticleStatus.Expired.some
          }
        })

        val effectiveStartDate = startDateOption.getOrElse(now)
        val effectiveEndDate = durationOpt.map(_.fromTime(effectiveStartDate)).getOrElse {
          endDateOption.getOrElse(now.plusDays(1))
        }

        if (effectiveStartDate.isOnOrBefore(now) && effectiveEndDate.isAfter(now)) {
          // we should now be LIVE
          trace("Found new status of `live` for previous status: {0}", currentStatus)
          GmsArticleStatus.Live.some
        }
        else {
          None
        }

      case GmsArticleStatus.Live =>
        val now = grvtime.currentTime
        startDateOption.foreach(startDate => {
          if (now.isBefore(startDate)) {
            // Should never happen, but in this case we must assume it was at one time approved, and since
            // it is still before its time, move it back to approved as it will transition on its own on time.
            trace("Found new status of `approved` for previous status: {0}", currentStatus)
            return GmsArticleStatus.Approved.some
          }
        })

        durationOpt.flatMap(duration => startDateOption.map(duration.fromTime)).orElse(endDateOption).foreach(endDate => {
          if (now.isAfter(endDate) || now.equals(endDate)) {
            // This article has now expired since we are now on or after the end date.
            trace("Found new status of `expired` for previous status: {0}", currentStatus)
            return GmsArticleStatus.Expired.some
          }
        })

        None

      case _ => None
    }
  }

  def buildStatusUpdateGrvMap(status: GmsArticleStatus.Type, webUserId: Int, isNewlySubmitted: Boolean,
                              includeStatusInMap: Boolean, startDate: Option[DateTime] = None): ArtGrvMap.OneScopeMap = {

    val now = grvtime.currentTime
    val nowInEpochSeconds = now.getSeconds
    val nowMetaVal = ArtGrvMapPublicMetaVal(nowInEpochSeconds)
    object F extends AolUniFieldNames
    val relatedFieldsMap = status match {
      case GmsArticleStatus.Live =>
        val baseMap = Map(F.LastGoLiveTime -> nowMetaVal, F.ApprovedRejectedUserId -> ArtGrvMapPublicMetaVal(webUserId))
        if (startDate.isEmpty) {
          baseMap ++ Map(F.StartDate -> nowMetaVal)
        }
        else {
          baseMap
        }

      case GmsArticleStatus.Pending => if (isNewlySubmitted) {
        Map(F.SubmittedTime -> nowMetaVal, F.SubmittedUserId -> ArtGrvMapPublicMetaVal(webUserId))
      } else {
        Map(F.SubmittedTime -> nowMetaVal)
      }
      case GmsArticleStatus.Rejected => Map(F.ApprovedRejectedUserId -> ArtGrvMapPublicMetaVal(webUserId))
      case _ => ArtGrvMap.emptyOneScopeMap
    }

    if (includeStatusInMap) {
      relatedFieldsMap ++ Map(F.Status -> ArtGrvMapPublicMetaVal(status.toString))
    }
    else {
      relatedFieldsMap
    }
  }
}

object AolDynamicLeadService extends DlugOrGmsService {
  import com.gravity.logging.Logging._
  import com.gravity.interests.jobs.intelligence.operations.AolDynamicLeadStage._

  val counterCategory: String = "AolDynamicLeadService"

  private val batchSize = 10000 //if (Settings2.isDevelopmentEnvironment) 1000 else 10000

  private val dlugGmsRoute = new GmsRoute(AolMisc.aolSiteKey, AolMisc.dlugOneScopeKey)

//  private val atLeastOneChannelFailureMessage = "Every DL Article MUST have at least one channel!"
//  val noChannelsFailureResult = FailureResult(atLeastOneChannelFailureMessage)
//  val noChannelsFailNel: ValidationNel[FailureResult, Unit] = noChannelsFailureResult.failureNel[Unit]
//  private val unitSuccessNel: ValidationNel[FailureResult, Unit] = {}.successNel[FailureResult]
//
//  private val sitePlacementScopedKeyToChannelMap = AolDynamicLeadChannels.dlPlacementToChannelMap.map(kv => kv._1.sitePlacementIdKey.toScopedKey -> kv._2)
//
//  private val channelCampaignsToChannel = AolDynamicLeadChannels.values.filter(_.id > 0).map(ch => ch.campaignKey -> ch).toMap
//
//  private val channelCampaignKeys = channelCampaignsToChannel.keySet
//
//  def getPinnedDlArticleLitesForChannel(channel: AolDynamicLeadChannels.Type): ValidationNel[FailureResult, Map[Int, DlArticleLite]] = {
//    for {
//      pinnedRows <- tryToSuccessNEL(ConfigurationQueryService.queryRunner.getPinnedRowsForChannel(channel), ex => FailureResult("Failed to retrieve rows for channel: " + channel, ex))
//      articleKeys = pinnedRows.map(_.articleKey).toSet
//      _ = if (articleKeys.isEmpty) {
//        return Map.empty[Int, DlArticleLite].successNel
//      } else {
//        {}
//      }
//      articleRows <- ArticleService.fetchMulti(articleKeys)(_.withColumn(_.url).withFamilies(_.allArtGrvMap))
//    } yield {
//      (for {
//        pr <- pinnedRows
//        ar <- articleRows.get(pr.articleKey)
//        dl <- tryBuildDynamicLeadArticleFromArtRowWithoutMetrics(ar, dlugGmsRoute).toOption
//      } yield {
//        pr.slot -> DlArticleLite(pr.articleId, dl.title, dl.url, grvtime.fromEpochSeconds(dl.publishOnDate), dl.sourceText)
//      }).toMap
//    }
//  }
//
//  private val pinnedArticleCacheReloadSeconds = if (grvroles.isInRole(grvroles.STATIC_WIDGETS)) 5L else 60L
//
//  def pinnedArticlesBySitePlacementIdMap: Map[SitePlacementId, Map[Int, ArticleRow]] = {
//    def factoryFun(): Map[SitePlacementId, Map[Int, ArticleRow]] = {
//      if (GmsAlgoSettings.aolComDlugUsesMultisiteGms) {
//        Map.empty
//      } else {
//        val pinnedArticleRows = ConfigurationQueryService.queryRunner.getAllPinnedArticles
//        val channelToPinByArticle = pinnedArticleRows.groupBy(_.articleKey).mapValues {
//          case rows: List[AolDlPinnedArticleRow] => rows.groupBy(_.channel).map {
//            case (channel: AolDynamicLeadChannels.Type, rows: List[AolDlPinnedArticleRow]) => rows.map(r => channel -> r.slot).toMap
//          }.flatten.toMap
//        }
//        val pinnedArticleKeys = pinnedArticleRows.map(_.articleKey).toSet
//
//        val articleMap = try {
//          val articleMapFromHbase = ArticleRecommendations.articleDataLiteQuerySpec(Schema.Articles.query2.withKeys(pinnedArticleKeys)).multiMap(skipCache = true, returnEmptyRows = true)
//          articleMapFromHbase.map {
//            case (ak: ArticleKey, origRow: ArticleRow) =>
//              val updatedGrvMap = origRow.allArtGrvMap.get(AolMisc.dlugOneScopeKey).map(oneScopeMap => {
//                val channelToPin = channelToPinByArticle.getOrElse(ak, Map.empty[AolDynamicLeadChannels.Type, Int])
//                val jsonString = Json.stringify(Json.toJson(channelToPin))
//                val mutatedScopedMap = oneScopeMap ++ Map(AolDynamicLeadFieldNames.ChannelsToPin -> ArtGrvMapPublicMetaVal(jsonString))
//                Map(AolMisc.dlugOneScopeKey -> mutatedScopedMap)
//              }).getOrElse(origRow.allArtGrvMap)
//
//              ak -> origRow.mutateGrvMap(updatedGrvMap)
//          }
//        }
//        catch {
//          case ex: Exception =>
//            warn(ex, "Failed to load articleDataLite for AolDynamicLeadService.getPinnedArticlesBySitePlacementId")
//            Map.empty[ArticleKey, ArticleRow]
//        }
//
//        for {
//          (channel, rows) <- pinnedArticleRows.groupBy(_.channel)
//          pinnedRows = rows.flatMap(r => {
//            articleMap.get(r.articleKey).map(ar => (r.slot - 1) -> ar)
//          }).toMap
//          spId <- channel.allBoundSitePlacementIds
//        } yield {
//          spId -> pinnedRows
//        }
//      }
//    }
//
//    PermaCacher.getOrRegister("AolDynamicLeadService.getPinnedArticlesBySitePlacementId",
//      PermaCacher.retryUntilNoThrow(factoryFun), reloadInSeconds = pinnedArticleCacheReloadSeconds, mayBeEvicted = false)
//  }
//
//  def getPinnedArticlesBySitePlacementId(spId: SitePlacementId): Map[Int, ArticleRow] = {
//    val useMe = if (AolMisc.usesSameContentAsDlMain(spId)) {  // in getPinnedArticlesBySitePlacementId/usesSameContentAsDlMain
//      AolMisc.aolDlMainPlacementId                            // in getPinnedArticlesBySitePlacementId/usesSameContentAsDlMain
//    }
//    else {
//      spId
//    }
//
//    pinnedArticlesBySitePlacementIdMap.getOrElse(useMe, Map.empty)
//  }
//
//  private def getChannelToPins(ak: ArticleKey): List[ChannelToPinnedSlot] = {
//    val results = for {
//      (spid, pinMap) <- pinnedArticlesBySitePlacementIdMap.toList
//      channel <- AolDynamicLeadChannels.sitePlacementIdToChannel.get(spid).toIterable
//      (slotIndex, ar) <- pinMap
//      if ar.articleKey == ak
//    } yield ChannelToPinnedSlot(channel, slotIndex + 1)
//
//    results.toSet.toList
//  }
//
//  def updateChannelPins(channel: AolDynamicLeadChannels.Type, pins: Map[Int, ArticleKey]): ValidationNel[FailureResult, Unit] = {
//    // First we must verify that no ArticleKey repeats
//    val uniqueArticleKeys = mutable.Set[ArticleKey]()
//    val failedToBeUniqueArticleKeys = for {
//      ak <- pins.values
//      if !uniqueArticleKeys.add(ak)
//    } yield {
//      FailureResult(s"An article may only be pinned to a single slot per channel. Channel `$channel` had articleId: ${ak.articleId} more than once.")
//    }
//
//    failedToBeUniqueArticleKeys.toNel.foreach {
//      fails => return fails.failure[Unit]
//    }
//
//    for {
//      setChannelPinsResult <- tryToSuccessNEL(
//        ConfigurationQueryService.queryRunner.setAllChannelPins(channel, pins),
//        ex => FailureResult("Failed to set pins in MySql!", ex)
//      )
//
//      _ <- GmsArticleIndexer.updateIndex(uniqueArticleKeys.toSeq)
//
//      // All good; done with work
//      _ = if (setChannelPinsResult.allModifiedKeys.isEmpty)
//        return ().successNel[FailureResult]
//
//      modifiedArticles <- ArticleService.fetchMulti(setChannelPinsResult.allModifiedKeys)(_.withFamilies(_.allArtGrvMap))
//
//      _ <- (for {
//        modifiedAk <- setChannelPinsResult.allModifiedKeys
//        pinnedSlotNumberForChannel = setChannelPinsResult.addedPinnedArticleKeys.get(modifiedAk)
//        articleGrvMap = modifiedArticles(modifiedAk).allArtGrvMap
//        oneScopedKey = AolMisc.dlugOneScopeKey
//        dlugArticleGrvMap <- articleGrvMap.get(oneScopedKey)
//        updateChannelToPinResult = for {
//          channelToPin <- validateChannelToPinFromGrvMap(dlugArticleGrvMap)
//          updateResult <- ArticleService.modifyPut(modifiedAk)(
//            _.valueMap(
//              _.allArtGrvMap, {
//                articleGrvMap ++ Map(oneScopedKey -> (dlugArticleGrvMap + (
//                  AolDynamicLeadFieldNames.ChannelsToPin -> ArtGrvMapPublicMetaVal(
//                    Json.toJson(pinnedSlotNumberForChannel.fold(channelToPin - channel)(pinNum => channelToPin + (channel -> pinNum)))
//                  ))
//                  ))
//              }
//            )
//          )
//        } yield ()
//      } yield updateChannelToPinResult).extrude
//    } yield ()
//  }
//
//  def clearChannels(articleKey: ArticleKey, userId: Long): ValidationNel[FailureResult, ArticleKey] = withMaintenance {
//    import com.gravity.interests.jobs.intelligence.operations.TableOperations.OpsResultMonoid
//
//    val akc = implicitly[ByteConverter[ArticleKey]]
//
//    try {
//      Schema.Articles.delete(articleKey).values(_.campaigns, channelCampaignKeys).values(_.campaignSettings, channelCampaignKeys).execute()
//    } catch {
//      case ex: Exception => return FailureResult("Failed to remove ALL channels campaigns from article: " + articleKey, ex).failureNel
//    }
//
//    for ((channelKey, channel) <- channelCampaignsToChannel) {
//
//      val result = for {
//      // fetch all existing PublishDateAndArticleKey's for the channel
//        row <- CampaignService.fetch(channelKey)(_.withFamilies(_.recentArticles)
//          .filter(_.and(_ => new ValueFilter(CompareOp.EQUAL, new BinaryComparator(akc.toBytes(articleKey))).some)))
//        delete <- {
//          if (row.recentArticles.keySet.nonEmpty)
//            CampaignService.doModifyDelete(channelKey)(_.values(_.recentArticles, row.recentArticles.keySet))
//          else mzero[OpsResult].successNel
//        }
//      } yield delete
//
//      result match {
//        case Success(_) => // continue
//        case Failure(NonEmptyList(RowNotFound(_, _))) => // nothing to delete
//        case Failure(fails) => return FailureResult(s"Failed to remove $articleKey from channel campaign: $channelKey (channel: $channel). failure(s): " + fails.list.mkString(", ")).failureNel
//      }
//    }
//
//    articleKey.successNel
//  }
//
//  private def articleKeyToScopedFromToKeys(articleKey: ArticleKey): Set[ScopedFromToKey] = {
//    val articleScopedKey = articleKey.toScopedKey
//    sitePlacementScopedKeyToChannelMap.keySet.map(spScopedKey => articleScopedKey.fromToKey(spScopedKey))
//  }
//
//  private def scopedFromArticleToSitePlacementIdKeys(article: ArticleRow): Set[ScopedFromToKey] = {
//    articleKeyToScopedFromToKeys(article.articleKey)
//  }

  def articleQuery: ArticleService.QuerySpec = {
    _.withFamilies(_.meta).withColumn(_.allArtGrvMap, AolMisc.dlugOneScopeKey)
  }

//  /**
//    * @return Articles with metrics for articles having scoped metrics (i.e. articles without scoped metrics will not be
//    *         in the resulting list).
//    */
//  private def getArticleRowsWithScopedDlugMetrics(articleRows: List[ArticleRow], fromCacheOnly: Boolean = false): ValidationNel[FailureResult, List[ArticleRowWithScopedDlugMetrics]] = {
//    val allScopedKeys = mutable.HashSet[ScopedFromToKey]()
//    val keyMap = articleRows.map(a => {
//      val articleScopedKeys = scopedFromArticleToSitePlacementIdKeys(a)
//      allScopedKeys ++= articleScopedKeys
//      a.articleKey -> articleScopedKeys
//    }).toMap
//
//    CacheFactory.getMetricsCache.get(allScopedKeys.toSet, fromCacheOnly)(scopedMetricsCacheQuery).map {
//      case keyToBuckets: Map[ScopedFromToKey, ScopedMetricsBuckets] =>
//        for {
//          article <- articleRows
//          scopedFromToKeys <- keyMap.get(article.articleKey)
//        } yield {
//          val channelToBuckets = mutable.Map[AolDynamicLeadChannels.Type, ScopedMetricsBuckets]()
//
//          for {
//            fromTo <- scopedFromToKeys
//            buckets <- keyToBuckets.get(fromTo)
//            channel <- sitePlacementScopedKeyToChannelMap.get(fromTo.to)
//          }  {
//            channelToBuckets += {
//              val oldMetrics = channelToBuckets.getOrElse(channel, ScopedMetricsBuckets.empty)
//              val newMetrics = oldMetrics + buckets
//
//              channel -> newMetrics
//            }
//          }
//
//          ArticleRowWithScopedDlugMetrics(article, channelToBuckets.toMap)
//        }
//    }
//  }

  def getArticle(articleId: Long): ValidationNel[FailureResult, AolDynamicLeadArticle] = {
    for {
      article <- ArticleService.fetch(ArticleKey(articleId))(articleQuery)
      dlArticle <- tryBuildDynamicLeadArticleFromArtRowWithoutMetrics(article, dlugGmsRoute)
    } yield dlArticle
  }

  def getArticles(articleKeys: Seq[ArticleKey]): ValidationNel[FailureResult, Seq[AolDynamicLeadArticle]] = {
    if (articleKeys.isEmpty)
      return Seq.empty.successNel

    for {
      articles <- ArticleService.fetchMultiOrdered(articleKeys)(articleQuery)
      dlArticles <- tryBuildDynamicLeadArticlesFromArtRowWithoutMetrics(articles, dlugGmsRoute)
    } yield dlArticles
  }

  def getAllArticleKeys: ValidationNel[FailureResult, Set[ArticleKey]] = {
    for {
      camp <- CampaignService.fetch(AolMisc.dynamicLeadDLUGCampaignKey)(_.withColumns(_.siteGuid).withFamilies(_.recentArticles))
    } yield camp.recentArticleKeys
  }

//  def getAllArticlesFromHbase(withMetrics: Boolean = true, pullMetricsFromCacheOnly: Boolean = false): ValidationNel[FailureResult, List[AolDynamicLeadArticle]] = {
//    for {
//      articleKeys <- getAllArticleKeys
//      allArticles <- getSelectedArticlesFromHbase(articleKeys.toSeq, withMetrics = withMetrics, pullMetricsFromCacheOnly = pullMetricsFromCacheOnly)
//    } yield {
//      allArticles
//    }
//  }

  def getSelectedArticlesFromHbase(articleKeys: Seq[ArticleKey], withMetrics: Boolean = true, pullMetricsFromCacheOnly: Boolean = false): ValidationNel[FailureResult, List[AolDynamicLeadArticle]] = {
    val articleKeysToSelect = {
      if (articleKeys.isEmpty)
        getAllArticleKeys.valueOr(fails => return fails.failure)
      else
        articleKeys.toSet
    }

    ifTrace(trace("Attempting to fetchMulti with {0} total article keys...", articleKeysToSelect.size))

    for {
      articleMap <- ArticleService.batchFetchMulti(articleKeysToSelect, batchSize = batchSize)(articleQuery)
      articleList = articleMap.values.toList
      artRowsWithMetrics <-
//        if (withMetrics) {
//          getArticleRowsWithScopedDlugMetrics(articleList, pullMetricsFromCacheOnly)
//        } else {
          articleList.map(ArticleRowWithScopedDlugMetrics.withEmptyMetrics).successNel
//        }
    } yield {
      artRowsWithMetrics.flatMap(ar => tryBuildDynamicLeadArticleFromArtRow(ar, dlugGmsRoute, forChannel = None).toOption)
    }
  }

//  def queryArticles(pager: Option[Pager] = None, channel: Option[AolDynamicLeadChannels.Type] = None, searchTerm: Option[String] = None,
//                    status: Option[Set[NegatableEnum[GmsArticleStatus.Type]]] = None, submittedUserId: Option[Int] = None,
//                    submittedDate: Option[DateMidnightRange] = None, deliveryMethod: Option[AolDeliveryMethod.Type] = None,
//                    aolCategory: Option[String] = None, aolSource: Option[String] = None): ValidationNel[FailureResult, AolDynamicLeadQueryResult] = {
//    val filteredSearchTerm = searchTerm.filter(_.nonEmpty).map(_.toLowerCase)
//    val searchTerms = filteredSearchTerm.toSet.flatMap(_.splitBetter(" ")).map(_.trim).filter(_.nonEmpty)
//    val filteredChannel = channel.filterNot(_ == AolDynamicLeadChannels.NotSet)
//
//    for {
//      keyQueryResult <- GmsArticleIndexer.query(pager, AolMisc.aolSiteGuid.some, filteredChannel, searchTerms,
//        status.getOrElse(Set.empty), submittedUserId, submittedDate, deliveryMethod, aolCategory, aolSource)
//      AolDynamicLeadKeyQueryResult(pageCount, articleKeys, totalItems, countsByStatus) = AolDynamicLeadKeyQueryResult.fromAolGmsKeyQueryResult(keyQueryResult)
//      articles <- ArticleService.fetchMultiOrdered(articleKeys)(articleQuery)
//      articlesWithMetricsResult <- getArticleRowsWithScopedDlugMetrics(articles.toList, fromCacheOnly = false)
//    } yield {
//      var totalMetrics = MetricsBase.empty
//      val finalArticles = articlesWithMetricsResult.flatMap(ar => {
//        tryBuildDynamicLeadArticleFromArtRow(ar, dlugGmsRoute, forChannel = filteredChannel).toOption.map(aolArt => {
//          aolArt.metrics.get("total").foreach(m => totalMetrics += m.lifetime)
//          aolArt
//        })
//      })
//      val finalArticlesByKey = finalArticles.mapBy(_.articleKey)
//      val finalSortedArticles = articleKeys.collect(finalArticlesByKey)
//
//      AolDynamicLeadQueryResult(pageCount, finalSortedArticles, totalItems, countsByStatus, totalMetrics)
//    }
//  }

  //private def getOptionalSeconds(value: Option[DateTime]): Int = value.fold(0)(_.getSeconds)

  def validateChannelsFromGrvMap(oneScopeMap: OneScopeMap): ValidationNel[FailureResult, Set[AolDynamicLeadChannels.Type]] = {
    implicit val fmt = AolDynamicLeadChannels.jsonFormat

    for {
      jsonString <- oneScopeMap.validateString(AolDynamicLeadFieldNames.Channels, isOptional = true)
      _ = if (jsonString.isEmpty) return Set.empty[AolDynamicLeadChannels.Type].successNel
      channels <- {
        try
          Json.fromJson[List[AolDynamicLeadChannels.Type]](Json.parse(jsonString)).toValidation.leftMap(err => nel(FailureResult(err.toString)))
        catch {
          case e: JsonProcessingException => return FailureResult(e).failureNel
        }
      }
    } yield channels.toSet

  }

  def validateChannelToPinFromGrvMap(oneScopeMap: OneScopeMap): ValidationNel[FailureResult, Map[AolDynamicLeadChannels.Type, Int]] = for {
    jsValue <- oneScopeMap.validateJson("channelToPin", isOptional = true, default = Json.obj())
    channelToPinMap <- jsValue.validate[Map[AolDynamicLeadChannels.Type, Int]].toFailureResultValidationNel
  } yield channelToPinMap

  def tryBuildDynamicLeadArticleFromArtRowWithoutMetrics(article: ArticleRow,
                                                         gmsRoute: GmsRoute,
                                                         notes: mutable.Buffer[DynamicLeadNote] = mutable.Buffer.empty[DynamicLeadNote]
                                                        ): ValidationNel[FailureResult, AolDynamicLeadArticle] = {
    tryBuildDynamicLeadArticleFromArtRow(ArticleRowWithScopedDlugMetrics(article, Map.empty[AolDynamicLeadChannels.Type, ScopedMetricsBuckets]), gmsRoute, notes, None)
  }

  private def tryBuildDynamicLeadArticlesFromArtRowWithoutMetrics(articles: Seq[ArticleRow], gmsRoute: GmsRoute): ValidationNel[FailureResult, Seq[AolDynamicLeadArticle]] = {
    articles.map(a => tryBuildDynamicLeadArticleFromArtRowWithoutMetrics(a, gmsRoute)).extrude
  }

  private def tryBuildDynamicLeadArticleFromArtRow(
                                            artWithMetrics: ArticleRowWithScopedDlugMetrics,
                                            gmsRoute: GmsRoute,
                                            notes: mutable.Buffer[DynamicLeadNote] = mutable.Buffer.empty[DynamicLeadNote],
                                            forChannel: Option[AolDynamicLeadChannels.Type] = None): ValidationNel[FailureResult, AolDynamicLeadArticle] = {
    // The scoped metrics will be more detailed for the 'forChannel' channel.
    val metricsWithMinutely = buildMetrics(artWithMetrics, forChannel)

    val ar = artWithMetrics.row
    tryBuildDynamicLeadArticleFromGrvMap(ar.articleKey, ar.urlOption, ar.allArtGrvMap, gmsRoute, metricsWithMinutely, notes)
  }

  def tryBuildDynamicLeadArticleFromGrvMap(
                                            articleKey: ArticleKey,
                                            urlOption: Option[String],
                                            allArtGrvMap: ArtGrvMap.AllScopesMap,
                                            gmsRoute: GmsRoute,
                                            metricsWithMinutely: AolDynamicLeadMetricsWithMinutely,
                                            notes: mutable.Buffer[DynamicLeadNote] = mutable.Buffer.empty[DynamicLeadNote]
                                          ): ValidationNel[FailureResult, AolDynamicLeadArticle] = {
    // If we can't get a GrvMap from one of the desired scopes, that's a fail.
    val grvMap = GmsService.gmsOrDlugGrvMap(allArtGrvMap, gmsRoute)
      .toValidationNel(NoGrvMapFailureResult(articleKey, urlOption, gmsRoute))
      .valueOr(fails => return fails.failure)

    val F = AolDynamicLeadFieldNames

    val failBuff = mutable.Buffer[FailureResult]()

    def messageNote(msg: String): Unit = {
      notes += makeNote(msg)
    }

    def handleFailedToGet(fieldName: String, fails: NonEmptyList[FailureResult]): Unit = {
      handleFails("Failed to get `" + fieldName + "`", fails)
    }

    def handleFails(msg: String, fails: NonEmptyList[FailureResult]): Unit = {
      failBuff ++= fails.list
      notes += makeNote(msg)
      notes += makeFailsNote(fails)
    }

    val articleId = articleKey.articleId.toString

    val url = urlOption match {
      case Some(u) => u
      case None =>
        val msg = "No `url` found in article row (articleId: " + articleId + ")!"
        notes += makeNote(msg)
        failBuff += FailureResult(msg)
        emptyString
    }

    val title = grvMap.getStringFromGrvMap(F.Title, failureHandler = handleFails("Failed to get `title`", _))

    val channelPageTitleOpt = grvMap.getValueFromGrvMap(F.ChannelPageTitle, None: Option[String], isOptional = true, failureHandler = handleFailedToGet(F.ChannelPageTitle, _))(_.getString.map(_.some))

    val statusString = grvMap.getStringFromGrvMap(F.Status, failureHandler = handleFails("Failed to get `status`", _))
    val status = GmsArticleStatus.get(statusString) match {
      case Some(st) => st
      case None =>
        val msg = "Invalid GmsArticleStatus: " + statusString
        failBuff += FailureResult(msg)
        messageNote(msg)
        GmsArticleStatus.defaultValue
    }

    val lastGoLiveTime = grvMap.getIntFromGrvMap(F.LastGoLiveTime, isOptional = true, failureHandler = handleFailedToGet(F.LastGoLiveTime, _))

    val submittedTime = grvMap.getIntFromGrvMap(F.SubmittedTime, isOptional = true, failureHandler = handleFailedToGet(F.SubmittedTime, _))

    val submittedUserId = grvMap.getIntFromGrvMap(F.SubmittedUserId, isOptional = true, default = -2525, failureHandler = handleFailedToGet(F.SubmittedUserId, _))

    val category = grvMap.getStringFromGrvMap(F.CategoryText, failureHandler = handleFailedToGet(F.CategoryText, _))

    val categoryLink = grvMap.getStringFromGrvMap(F.CategoryLink)

    val categoryIdOpt = grvMap.getValueFromGrvMap(F.CategoryId, None: Option[Int], isOptional = true, failureHandler = handleFailedToGet(F.CategoryId, _))(_.getInt.map(_.some))

    val categorySlugOpt = grvMap.getValueFromGrvMap(F.CategorySlug, None: Option[String], isOptional = true, failureHandler = handleFailedToGet(F.CategorySlug, _))(_.getString.map(_.some))

    val source = grvMap.getStringFromGrvMap(F.SourceText, failureHandler = handleFailedToGet(F.SourceText, _))

    val sourceLink = grvMap.getStringFromGrvMap(F.SourceLink, failureHandler = handleFailedToGet(F.SourceLink, _))

    val summary = grvMap.getStringFromGrvMap(F.Summary, failureHandler = handleFailedToGet(F.Summary, _))

    val headline = grvMap.getStringFromGrvMap(F.Headline, failureHandler = handleFailedToGet(F.Headline, _))

    val secondaryHeader = grvMap.getStringFromGrvMap(F.SecondaryHeader, failureHandler = handleFailedToGet(F.SecondaryHeader, _))

    val secondaryLinks = validateSecondaryLinksFromGrvMap(grvMap).valueOr {
      case fails: NonEmptyList[FailureResult] =>
        handleFails("Failed to validateSecondaryLinksFromGrvMap", fails)
        List.empty[AolLink]
    }

    val channels = validateChannelsFromGrvMap(grvMap).valueOr {
      case fails: NonEmptyList[FailureResult] =>
        handleFails("Failed to validateChannelsFromGrvMap", fails)
        Set.empty[AolDynamicLeadChannels.Type]
    }

    val image = grvMap.getStringFromGrvMap(F.Image, failureHandler = handleFailedToGet(F.Image, _))

    val showVideoIcon = grvMap.getBoolFromGrvMap(F.ShowVideoIcon, failureHandler = handleFailedToGet(F.ShowVideoIcon, _))

    val updatedTime = grvMap.getIntFromGrvMap(F.UpdatedTime, failureHandler = handleFailedToGet(F.UpdatedTime, _))

    val dlCampaignOpt = grvMap.getValueFromGrvMap(F.AolDynamicLeadCampaign, None: Option[AolDynamicLeadCampaign], isOptional = true, failureHandler = handleFailedToGet(F.AolDynamicLeadCampaign, _)) {
      case metaVal: ArtGrvMapMetaVal =>
        for {
          jsonString <- metaVal.getString
          dlCamp <- Json.fromJson[AolDynamicLeadCampaign](Json.parse(jsonString)).toValidation.leftMap(e => nel(FailureResult(e.toString)))
        } yield dlCamp.some
    }

    val startDateOpt = grvMap.getValueFromGrvMap(F.StartDate, None: Option[DateTime], isOptional = true, failureHandler = handleFailedToGet(F.StartDate, _))(_.getInt.map(_.secondsFromEpoch.some))

    val endDateOpt = grvMap.getValueFromGrvMap(F.EndDate, None: Option[DateTime], isOptional = true, failureHandler = handleFailedToGet(F.EndDate, _))(_.getInt.map(_.secondsFromEpoch.some))

    val durationOpt = grvMap.getValueFromGrvMap(F.Duration, None: Option[GrvDuration], isOptional = true, failureHandler = handleFailedToGet(F.Duration, _))(_.getString.flatMap(GrvDuration.parse).map(_.some))

    val approvedRejectedUserIdOpt = grvMap.getValueFromGrvMap(F.ApprovedRejectedUserId, None: Option[Int], isOptional = true, failureHandler = handleFailedToGet(F.ApprovedRejectedUserId, _))(_.getInt.map(_.some))

    val aolRibbonImageUrl = grvMap.getValueFromGrvMap(F.RibbonImageUrl, None: Option[String], isOptional = true, failureHandler = handleFailedToGet(F.RibbonImageUrl, _))(_.getString.map(_.some))

    val aolImageSource = grvMap.getValueFromGrvMap(F.ImageCredit, None: Option[String], isOptional = true, failureHandler = handleFailedToGet(F.ImageCredit, _))(_.getString.map(_.some))

    val channelImage = grvMap.getValueFromGrvMap(F.ChannelImage, None: Option[String], isOptional = true, failureHandler = handleFailedToGet(F.ChannelImage, _))(_.getString.map(_.some))

    val channelImageSource = grvMap.getValueFromGrvMap(F.ChannelImageSource, None: Option[String], isOptional = true, failureHandler = handleFailedToGet(F.ChannelImageSource, _))(_.getString.map(_.some))

    val narrowBandImage = grvMap.getValueFromGrvMap(F.NarrowBandImage, None: Option[String], isOptional = true, failureHandler = handleFailedToGet(F.NarrowBandImage, _))(_.getString.map(_.some))

    val highResImage = grvMap.getValueFromGrvMap(F.HighResImage, None: Option[String], isOptional = true,
      failureHandler = handleFailedToGet(F.HighResImage, _))(_.getString.map(_.some))

    val channelToPin = List[ChannelToPinnedSlot]() // Was getChannelToPins(articleKey), but this service is decomnissioned.

    val subChannelRibbon = grvMap.getValueFromGrvMap(F.ChannelLabel, None: Option[AolChannelRibbon], isOptional = true, failureHandler = handleFailedToGet(F.ChannelLabel, _)) {
      case metaVal: ArtGrvMapMetaVal =>
        for {
          jsonString <- metaVal.getString
          subRib <- Json.fromJson[AolChannelRibbon](Json.parse(jsonString)).toValidation.leftMap(e => nel(FailureResult(e.toString)))
        } yield subRib.some
    }

    failBuff.toNel match {
      case Some(fails) => fails.failure
      case None =>
        val dlArticle = AolDynamicLeadArticle(
          url,
          articleId,
          title,
          metricsWithMinutely.metrics,
          status.toString,
          lastGoLiveTime,
          submittedTime,
          submittedUserId,
          category,
          categoryLink,
          categoryIdOpt,
          categorySlugOpt,
          source,
          sourceLink,
          summary,
          headline,
          secondaryHeader,
          secondaryLinks,
          image,
          showVideoIcon,
          updatedTime,
          dlCampaignOpt,
          startDateOpt,
          endDateOpt,
          durationOpt,
          approvedRejectedUserIdOpt,
          aolRibbonImageUrl,
          aolImageSource,
          channelImage,
          channels,
          channelImageSource,
          narrowBandImage,
          channelToPin,
          subChannelRibbon,
          metricsWithMinutely.minutely,
          channelPageTitleOpt,
          highResImage
        )

        messageNote("Successfully built DL Article from GrvMap: " + dlArticle)

        dlArticle.successNel
    }
  }

//  private def buildTotalMetrics(metricsMap: Map[AolDynamicLeadChannels.Type, ScopedMetricsBuckets]): Map[String, AolDynamicLeadMetrics] = {
//    val minutelyToBeGreaterThan = grvtime.currentMinute.minusMinutes(31)
//
//    def isLastThirtyMinutes(minute: DateMinute): Boolean = minute.isAfter(minutelyToBeGreaterThan)
//
//    def buildForChannel(buckets: ScopedMetricsBuckets, channel: AolDynamicLeadChannels.Type): AolDynamicLeadMetrics = {
//      var (lifetimeClicks, thirtyMinuteClicks, lifetimeImps, thirtyMinuteImps) = (0L, 0L, 0L, 0L)
//
//      for {
//        (mk, value) <- buckets.minutely.map
//        keyDateMinute = mk.dateMinute
//        if isLastThirtyMinutes(keyDateMinute)
//      } {
//        mk.countBy match {
//          case RecommendationMetricCountBy.click =>
//            thirtyMinuteClicks += value
//
//          case RecommendationMetricCountBy.unitImpressionViewed =>
//            thirtyMinuteImps += value
//
//          case _ => // ignore
//        }
//      }
//
//      for ((mk, value) <- buckets.monthly.map) {
//        mk.countBy match {
//          case RecommendationMetricCountBy.click =>
//            lifetimeClicks += value
//
//          case RecommendationMetricCountBy.unitImpressionViewed =>
//            lifetimeImps += value
//
//          case _ => // ignore
//        }
//      }
//
//      AolDynamicLeadMetrics(MetricsBase(lifetimeClicks, lifetimeImps), MetricsBase(thirtyMinuteClicks, thirtyMinuteImps))
//    }
//
//    var totalMetrics = AolDynamicLeadMetrics.empty
//
//    val channelMetrics = AolDynamicLeadChannels.values.map(channel => {
//      val metrics = metricsMap.get(channel).map(buildForChannel(_, channel)) match {
//        case Some(m) => totalMetrics = totalMetrics + m; m
//        case None => AolDynamicLeadMetrics.empty
//      }
//      channel.name -> metrics
//    }).toMap
//
//    channelMetrics ++ Map("total" -> totalMetrics)
//  }

  // The scoped metrics will be more detailed for the 'forChannel' channel (or would be, if this method wasn't now a stump).
  private def buildMetrics(article: ArticleRowWithScopedDlugMetrics, forChannel: Option[AolDynamicLeadChannels.Type] = None): AolDynamicLeadMetricsWithMinutely =
    AolDynamicLeadMetricsWithMinutely.emptyMetrics

//  was: {
//    val minutelyToBeGreaterThan = grvtime.currentMinute.minusMinutes(31)
//    val minutelyMap = mutable.Map[DateMinute, ClicksImps]()
//
//    def isLastThirtyMinutes(minute: DateMinute): Boolean = minute.isAfter(minutelyToBeGreaterThan)
//
//    def shouldWeMinutely(channel: AolDynamicLeadChannels.Type): Boolean = forChannel match {
//      case Some(onlyThisChannel) if onlyThisChannel == channel => true
//      case _ => false
//    }
//
//    def buildForChannel(buckets: ScopedMetricsBuckets, channel: AolDynamicLeadChannels.Type): AolDynamicLeadMetrics = {
//      var (lifetimeClicks, thirtyMinuteClicks, lifetimeImps, thirtyMinuteImps) = (0L, 0L, 0L, 0L)
//
//      val areWeUpdatingMinutely = shouldWeMinutely(channel)
//
//      for {
//        (mk, value) <- buckets.minutely.map
//        keyDateMinute = mk.dateMinute
//        if isLastThirtyMinutes(keyDateMinute)
//      } {
//        mk.countBy match {
//          case RecommendationMetricCountBy.click =>
//            thirtyMinuteClicks += value
//            if (areWeUpdatingMinutely) {
//              val minutely = minutelyMap.getOrElseUpdate(keyDateMinute, ClicksImps.empty)
//              minutelyMap.update(keyDateMinute, minutely.plusClicks(value))
//            }
//
//          case RecommendationMetricCountBy.unitImpressionViewed =>
//            thirtyMinuteImps += value
//            if (areWeUpdatingMinutely) {
//              val minutely = minutelyMap.getOrElseUpdate(keyDateMinute, ClicksImps.empty)
//              minutelyMap.update(keyDateMinute, minutely.plusImps(value))
//            }
//
//          case _ => // ignore
//        }
//      }
//
//      for ((mk, value) <- buckets.monthly.map) {
//        mk.countBy match {
//          case RecommendationMetricCountBy.click =>
//            lifetimeClicks += value
//
//          case RecommendationMetricCountBy.unitImpressionViewed =>
//            lifetimeImps += value
//
//          case _ => // ignore
//        }
//      }
//
//      AolDynamicLeadMetrics(MetricsBase(lifetimeClicks, lifetimeImps), MetricsBase(thirtyMinuteClicks, thirtyMinuteImps))
//    }
//
//    var totalMetrics = AolDynamicLeadMetrics.empty
//
//    val channelMetrics = AolDynamicLeadChannels.values.map(channel => {
//      val metrics = article.metrics.get(channel).map(buildForChannel(_, channel)) match {
//        case Some(m) => totalMetrics = totalMetrics + m; m
//        case None => AolDynamicLeadMetrics.empty
//      }
//      channel.name -> metrics
//    }).toMap
//
//    AolDynamicLeadMetricsWithMinutely(channelMetrics ++ Map("total" -> totalMetrics), minutelyMap.toMap)
//  }
//
//  private val emptyArticleSuccess: ValidationNel[FailureResult, AolDynamicLeadArticle] = AolDynamicLeadArticle.empty.successNel
//
//  private val canBeNoteFRSingle = new CanBeNote[NonEmptyList[FailureResult]] {
//    def toNote(t: NonEmptyList[FailureResult]): Note = {
//      MsgNote(t.list.map(_.messageWithExceptionInfo).mkString(" AND "))
//    }
//  }
//
//  private def saveChannels(channels: NonEmptyList[AolDynamicLeadChannels.Type], articleKey: ArticleKey, userId: Long, status: GmsArticleStatus.Type): ValidationNel[FailureResult, Boolean] = {
//    clearChannels(articleKey, userId).leftMap {
//      case fails: NonEmptyList[FailureResult] => return fails.failure
//    }
//
//    val cas = status.campaignArticleSettings
//    val successBuf = mutable.Buffer[AolDynamicLeadChannels.Type]()
//    val failBuff = mutable.Buffer[AolDynamicLeadChannels.Type]()
//
//    channels.foreach(channel => CampaignService.addArticleKey(channel.campaignKey, articleKey, userId, cas) match {
//      case Success(_) => successBuf += channel
//      case Failure(_) => failBuff += channel
//    })
//
//    GmsArticleIndexer.updateIndex(articleKey)
//
//    successBuf.toList.toNel -> failBuff.toList.toNel match {
//      case (Some(succeeded), Some(failed)) => PartialChannelSaveFailureResult(succeeded, failed).failureNel
//      case (None, Some(failed)) => FailureResult("Failed to add article to all requested channels: " + failed.list.mkString(", ") + "!").failureNel
//      case (None, None) => false.successNel
//      case (Some(_), None) => true.successNel
//    }
//  }
//
//  def saveDynamicLeadArticle(url: String, fields: AolDynamicLeadModifyFields, userId: Long, isNewlySubmitted: Boolean = false): Annotated[ValidationNel[FailureResult, AolDynamicLeadArticle]] = {
//    val now = grvtime.currentTime
//    val publishDateOption = fields.startDate orElse now.some
//    val webUserId = userId.toInt
//    val articleKey = ArticleKey(url)
//
//    val notes = mutable.Buffer[DynamicLeadNote]()
//
//    notes += makeMetaNote("saveDynamicLeadArticle called with", DynamicLeadStageMeta(initialParams = SaveDynamicLeadArticleParams(url, fields, userId, isNewlySubmitted, AolMisc.dynamicLeadDLUGCampaignKey).some))
//
//    def noteIt(note: DynamicLeadNote): Unit = {
//      notes += note
//    }
//
//    def noteMessage(msg: String): Unit = {
//      noteIt(makeNote(msg))
//    }
//
//    def noteFails(fails: NonEmptyList[FailureResult]): Unit = {
//      noteIt(makeFailsNote(fails))
//    }
//
//    val result = for {
//      _ <- if (fields.channels.isEmpty) {
//        noteMessage(atLeastOneChannelFailureMessage)
//        noChannelsFailNel
//      } else {
//        unitSuccessNel
//      }
//      campaign <- CampaignService.fetchOrEmptyRow(AolMisc.dynamicLeadDLUGCampaignKey)(_.withColumns(_.siteGuid, _.recentArticlesMaxAge).withFamilies(_.recentArticles)) match {
//        case campSuccess @ Success(_) =>
//          noteMessage("Successfully retrieved campaign for article TTL check.")
//          campSuccess
//        case campFailed @ Failure(fails) =>
//          noteMessage("Failed to get campaign: `" + AolMisc.dynamicLeadDLUGCampaignKey + "` for article TTL check.")
//          noteFails(fails)
//          campFailed
//      }
//      previousDlArticle <- if (isNewlySubmitted) {
//        noteMessage("No need to fetch previous article since this is newly created.")
//        val articleKey = ArticleKey(url)
//        if (campaign.recentArticleKeys.contains(articleKey)) {
//          noteMessage(s"Duplicate article already exists for URL: `$url` (articleId: ${articleKey.articleId})!")
//          return DuplicateArticleKeyFailureResult(articleKey, url).failNotes(notes)
//        }
//
//        emptyArticleSuccess
//      } else getArticle(articleKey.articleId) match {
//        case dlArticleSuccess @ Success(_) =>
//          noteMessage("Successfully retrieved article's previous state.")
//          dlArticleSuccess
//        case failed @ Failure(fails) =>
//          noteMessage("Failed to get the previous state for article URL: " + url + " (articleId: " + articleKey.articleId + ")")
//          noteFails(fails)
//          failed
//      }
//      previousGrvMap = previousDlArticle.toOneScopeGrvMap
//      previousStatus = GmsArticleStatus.getOrDefault(previousDlArticle.dlArticleStatus)
//      // If a status was sent in the modify fields, use it, otherwise, use the current (soon-to-be previous) DL Article Status
//      requestedOrCurrentStatus = fields.statusOption.getOrElse(previousStatus)
//      // If a new status is identified (some rule was tripped), use it as it should override previous or requested, otherwise, use the requested/current
//      effectiveStatus = findNewStatus(requestedOrCurrentStatus, fields.startDate, fields.endDate, fields.duration).getOrElse(requestedOrCurrentStatus)
//      // check that the publish date is within the ttl range
//      maxDays = campaign.recentArticlesMaxAge.getOrElse(Int.MaxValue)
//      _ <- if (maxDays < publishDateOption.map(_.daysAgo).getOrElse(0)) {
//        val msg = "Publish date is too old for campaign article TTL: " + maxDays + ". URL: " + url
//        noteMessage(msg)
//        FailureResult(msg).failureNel
//      } else {
//        noteMessage("Article publish date IS within campaign article TTL, so we're all good here.")
//        Unit.successNel
//      }
//      grvMapToStore = fields.toGrvMap(AolMisc.dlugOneScopeKey, webUserId, isNewlySubmitted, previousGrvMap, effectiveStatus, previousStatus, articleKey)
//      article <- ArticleService.saveArticle(url, AolMisc.aolSiteGuid, IngestionTypes.fromAolPromotion, now, "Created by DLUG user: " + userId, publishDateOption = publishDateOption)(
//        spec => {
//          spec
//            .value(_.title, fields.title)
//            .value(_.summary, fields.summary)
//            .valueMap(_.allArtGrvMap, grvMapToStore)
//        },
//        articleQuery
//      ) match {
//        case articleSuccess @ Success(_) =>
//          noteMessage("ArticleService.saveArticle call succeeded! YAY!")
//          articleSuccess
//        case failed @ Failure(fails) =>
//          noteMessage("ArticleService.saveArticle FAILED!")
//          noteFails(fails)
//          failed
//      }
//      _ <- CampaignService.addArticleKey(AolMisc.dynamicLeadDLUGCampaignKey, article.articleKey, userId, effectiveStatus.campaignArticleSettings) match {
//        case succeeded @ Success(_) =>
//          noteMessage("Successfully added/updated campaign article settings.")
//          succeeded
//        case failed @ Failure(fails) =>
//          noteMessage("FAILED to add/update campaign article settings!")
//          noteFails(fails)
//          failed
//      }
//      _ <- fields.channels.toList.filter(_.id > 0).toNel match {
//        case None =>
//          noteMessage("Article is being saved without any channels. We will now attempt to clear any previously set.")
//          clearChannels(article.articleKey, userId) match {
//            case Success(_) =>
//              noteMessage("Successfully cleared all channels from article")
//              true.successNel
//            case Failure(fails) =>
//              noteMessage("FAILED to clear all channels for article!")
//              noteFails(fails)
//              fails.failure
//          }
//
//        case Some(channels) =>
//          noteMessage("Article is bound to " + channels.len + " channels.")
//          saveChannels(channels, article.articleKey, userId, effectiveStatus) match {
//            case succeeded @ Success(_) =>
//              noteMessage("Successfully bound article to all " + channels.len + " channels.")
//              succeeded
//            case failed @ Failure(fails) =>
//              noteMessage("FAILED to bound article to all " + channels.len + " channels.")
//              noteFails(fails)
//              fails.head match {
//                case partial: PartialChannelSaveFailureResult =>
//                  noteMessage("Since some but not all channels were successfully bound, we'll now update the `channels` field in its GrvMap to only be those that bound.")
//                  val oneMap = grvMapToStore(AolMisc.dlugOneScopeKey)
//                  val correctedGrvMap = for ((key, value) <- oneMap) yield {
//                    val correctValue = if (key == AolDynamicLeadFieldNames.Channels) {
//                      ArtGrvMapPublicMetaVal(partial.successfullySaved.list.mkString("[\"", "\",\"", "\"]"))
//                    } else {
//                      value
//                    }
//                    key -> correctValue
//                  }
//                  val correctedAllScopesMap: ArtGrvMap.AllScopesMap = Map(AolMisc.dlugOneScopeKey -> correctedGrvMap)
//                  ArticleService.modifyPut(article.articleKey)(_.valueMap(_.allArtGrvMap, correctedAllScopesMap)) match {
//                    case Success(_) =>
//                      noteMessage("Successfully corrected the channels we actually bound to.")
//                    case Failure(moreFails) =>
//                      noteMessage("FAILED to correct the channels previously saved to only those successfully bound.")
//                      noteFails(moreFails)
//                  }
//
//                case _ => // nothing to see here...
//              }
//              failed
//          }
//      }
//      dlArticle <- tryBuildDynamicLeadArticleFromArtRowWithoutMetrics(article, dlugGmsRoute, notes = notes)
//    } yield {
//      countPerSecond(counterCategory, "DL Articles saved")
//      val fromRow: Option[AolDynamicLeadArticle] = if (isNewlySubmitted) None else previousDlArticle.some
//      AuditService.logAllChangesWithForSiteGuid(article.articleKey, userId, fromRow, dlArticle, Seq("AolDynamicLeadService.saveDynamicLeadArticle"), AolMisc.aolSiteGuid).valueOr {
//        case fails: NonEmptyList[FailureResult] =>
//          val msg = "The AolDynamicLeadService.saveDynamicLeadArticle completed successfully, but we failed to log changes to the Audit Log! URL: " + url
//          noteMessage(msg)
//          noteFails(fails)
//          warn(fails, msg)
//      }
//      noteMessage("Successfully completed saveDynamicLeadArticle! YAYAYAYAYAY!")
//      dlArticle
//    }
//
//    GmsArticleIndexer.updateIndex(articleKey)
//
//    result.annotate(notes)
//  }
//
//  private val articleStatusUpdateQuery: ArticleService.QuerySpec = _.withFamilies(_.campaignSettings).withColumns(_.url).withColumn(_.allArtGrvMap, AolMisc.dlugOneScopeKey)
//  private val articleStatusUpdateQueryCampaignOnly: ArticleService.QuerySpec = _.withFamilies(_.campaignSettings).withColumns(_.url)
//
//  def updateDlArticleStatus(ak: ArticleKey, status: GmsArticleStatus.Type, userId: Long, updateCampaignSettingsOnly: Boolean = false): ValidationNel[FailureResult, OpsResult] = {
//
//    // In the only case where we will actually update the status, we will set this var to the existing GrvMap
//    val previousDlArticleOption = if (updateCampaignSettingsOnly) None else getArticle(ak.articleId).toOption
//    val query: ArticleService.QuerySpec = if (updateCampaignSettingsOnly) articleStatusUpdateQueryCampaignOnly else articleStatusUpdateQuery
//    var previousGrvMap: Option[ArtGrvMap.OneScopeMap] = None
//
//    // We first need to check if this is actually a change and:
//    //   * If it IS, we need to call CampaignService.addArticleKey to update the CampaignArticleSettings for both the campaign and article
//    //   * If it is NOT, then return a successful, empty, OpsResult
//    val opsResultForCampaignSettings = ArticleService.fetch(ak)(query) match {
//      case Success(article) =>
//        var updates = 0
//        status.campaignArticleSettings.foreach(newSettings => {
//
//          for {
//            (ck, existingSettings) <- article.campaignSettings
//            if AolDynamicLeadChannels.isDlugCampaign(ck)
//            if !newSettings.equateActiveAndBlacklisted(existingSettings)
//          } {
//            trace("Attempting to update :: ak = {0} :: ck = {1} :: settings = {2}", ak, ck, newSettings)
//            CampaignService.addArticleKey(ck, ak, userId, existingSettings.copy(isBlacklisted = newSettings.isBlacklisted, status = newSettings.status).some) match {
//              case Success(_) =>
//                trace("Successfully updated :: ak = {0} :: ck = {1} :: settings = {2}", ak, ck, newSettings)
//                updates += 1
//
//              case Failure(fails) =>
//                trace("**FAILED TO UPDATE** :: ak = {0} :: ck = {1} :: settings = {2}", ak, ck, newSettings)
//                return fails.failure
//            }
//          }
//
//        })
//
//        if (!updateCampaignSettingsOnly) {
//          previousGrvMap = article.columnFromFamily(_.allArtGrvMap, AolMisc.dlugOneScopeKey)
//        }
//
//        trace("Successfully updated {0} campaigns for articleKey: {1}", updates, ak)
//
//        OpsResult(0, updates, 0).successNel[FailureResult]
//
//      case Failure(fails) => return fails.failure
//    }
//
//    val werePinsCleared = if (!updateCampaignSettingsOnly &&
//      (status == GmsArticleStatus.Deleted || status == GmsArticleStatus.Expired ||
//      status == GmsArticleStatus.Rejected || status == GmsArticleStatus.Invalid)) {
//
//      countPerSecond(counterCategory, "updateDlArticleStatus.deletePinningsForArticleKey.called")
//
//      ScalaMagic.retryOnException(3, 50) {
//        ConfigurationQueryService.queryRunner.deletePinningsForArticleKey(ak)
//      } {
//        case ex: Exception =>
//          countPerSecond(counterCategory, "updateDlArticleStatus.deletePinningsForArticleKey.exceptions")
//          FailureResult(s"Failed to unpin article (articleId: ${ak.articleId} ) that has been set to ${status.toString}", ex)
//      } match {
//        case Success(_) =>
//          countPerSecond(counterCategory, "updateDlArticleStatus.deletePinningsForArticleKey.successes")
//          true
//
//        case Failure(fails) =>
//          countPerSecond(counterCategory, "updateDlArticleStatus.deletePinningsForArticleKey.failures")
//          val attempts = fails.len
//          warn(fails, s"Tried (the maximum of) $attempts times, to unpin article (articleId: ${ak.articleId}) that had an updated status of: ${status.toString}.")
//          false
//      }
//    }
//    else {
//      false
//    }
//
//    // now that campaign article settings are all wired up properly, check if DL fields also need updating and do so if required
//    if (updateCampaignSettingsOnly || previousDlArticleOption.exists(_.articleStatus == status)) {
//      // previous status matches new status, therefore, nothing more to update
//      trace("No change required to update in GrvMap")
//      return opsResultForCampaignSettings
//    }
//
//    // If we've reached this point, then we've already successfully updated the campaign, and we do indeed have a new DL status.
//    // -- so... let's now attempt to update the article itself
//    val updateResult = ArticleService.modifyPut(ak)(spec => {
//      val webUserId = userId.toInt
//      val statusUpdateMap = buildStatusUpdateGrvMap(status, webUserId, isNewlySubmitted = false, includeStatusInMap = true, startDate = previousDlArticleOption.flatMap(_.startDate))
//
//      // If there was an existing GrvMap, we will overlay the status-only-changed GrvMap onto it so as not to lose previous data
//      val mergedGrvMap = previousGrvMap.fold(statusUpdateMap)(_ ++ statusUpdateMap)
//
//      // If channel pins were cleared, we need to exclude that data from the updatedMap
//      val updatedMap = if (werePinsCleared) {
//        mergedGrvMap.filterKeys(key => key != AolDynamicLeadFieldNames.ChannelsToPin)
//      }
//      else {
//        mergedGrvMap
//      }
//
//      spec.valueMap(_.allArtGrvMap, Map(AolMisc.dlugOneScopeKey -> updatedMap))
//    })
//
//    // Now that all updates are complete, we need to add an audit log for our changes
//    getArticle(ak.articleId) match {
//      case Success(updatedDlArticle) =>
//        AuditService.logAllChangesWithForSiteGuid(ak, userId, previousDlArticleOption, updatedDlArticle, Seq("AolDynamicLeadService.updateDlArticleStatus"), AolMisc.aolSiteGuid).valueOr {
//          case fails: NonEmptyList[FailureResult] => warn(fails, "Failed to log all changes from AolDynamicLeadService.updateDlArticleStatus!")
//        }
//      case Failure(fails) => warn(fails, "Failed to retrieve updated DL Article for audit logging. No audits can be logged!")
//    }
//
//    GmsArticleIndexer.updateIndex(ak)
//
//    // if the article update was successful, increment the numPuts to account for the campaign update
//    updateResult.map(opsRes => opsRes.copy(numPuts = opsRes.numPuts + opsResultForCampaignSettings.toOption.fold(0)(_.numPuts)))
//  }
//
//  def checkIfRecommendableAndUpdateIfNeeded(articleKey: ArticleKey, url: String, dlStatus: GmsArticleStatus.Type,
//                                            startDateOpt: Option[DateTime], endDateOpt: Option[DateTime], durationOpt: Option[GrvDuration],
//                                            executeUpdate: Boolean): ValidationNel[FailureResult, Boolean] = {
//    def updateStatus(newStatus: GmsArticleStatus.Type): ValidationNel[FailureResult, Unit] = {
//      if (executeUpdate) {
//        updateDlArticleStatus(articleKey, newStatus, Settings2.INTEREST_SERVICE_USER_ID) match {
//          case Success(_) =>
//            // YAY
//            ifTrace(trace("Successfully transitioned DL Article Status (url: `" + url + "`) to `" + newStatus + "` from `" + dlStatus + "`!")).successNel
//          case Failure(fails) =>
//            warn(fails, "Failed to transition DL Article Status (url: `" + url + "`) to `" + newStatus + "` from `" + dlStatus + "`!")
//            fails.failure
//        }
//      } else {}.successNel
//    }
//
//    val statusToCheckAgainst = findNewStatus(dlStatus, startDateOpt, endDateOpt, durationOpt) match {
//      case Some(newStatus) =>
//        updateStatus(newStatus).leftMap(fails => return fails.failure)
//        newStatus
//
//      case None => dlStatus
//    }
//
//    if (statusToCheckAgainst.isRecommendable) isRecommendable else isNotRecommendable
//  }
//
//  def synchronizeCampaignArticleSettingsToDlStatus(article: ArticleRow, ck: CampaignKey): ValidationNel[FailureResult, Boolean] = {
//    for {
//      status <- {
//        val dlInfo = article.getAolUniArticleInfo(ck)
//
//        dlInfo.aolGmsStatusOption.toValidationNel(FailureResult("No DL status present in articleRow for articleId: " + article.articleKey.articleId))
//      }
//
//      opsResult <- updateDlArticleStatus(article.articleKey, status, Settings2.INTEREST_SERVICE_USER_ID, updateCampaignSettingsOnly = true)
//    } yield {
//      opsResult.numPuts > 0
//    }
//  }
//
//  def synchronizeDlugArticleSettings(maxArticlesToProcess: Int = -1): String = {
//
//    val articleIdsUpdated = new GrvConcurrentMap[Long, String]()
//
//    val counterMap = new GrvConcurrentMap[String, AtomicInteger]()
//
//    def myCounter2(label: String, amount: Int): Unit = {
//      counterMap.getOrElseUpdate(label, new AtomicInteger(0)).getAndAdd(amount)
//    }
//
//    def myCounter(label: String): Unit = myCounter2(label, 1)
//
//    getAllArticleKeys match {
//      case Success(dlArticleKeys) =>
//        val articleKeyList = dlArticleKeys.toList
//        myCounter2("01. Articles Retrieved", articleKeyList.size)
//        info("Retrieved {0} DL Article Keys To Process...", articleKeyList.size)
//        val articleKeysToProcess = if (maxArticlesToProcess > 0) articleKeyList.take(maxArticlesToProcess) else articleKeyList
//        val currentArticleIndex = new AtomicInteger(0)
//        val totalToProcess = articleKeysToProcess.size
//        info("Will begin processing of {0} total DL Article Keys...", totalToProcess)
//
//        articleKeysToProcess.par.foreach { ak =>
//          val currentIndex = currentArticleIndex.incrementAndGet()
//          myCounter("02. Articles Attempted")
//          AolDynamicLeadService.getArticle(ak.articleId) match {
//            case Success(dl) =>
//              myCounter("03. Articles Retrieved")
//
//              val statusNumber = dl.articleStatus.id + 6 match {
//                case lessThan10 if lessThan10 < 10 => "0" + lessThan10
//                case other => other.toString
//              }
//              myCounter(s"$statusNumber. Articles with status = ${dl.articleStatus.name}")
//              updateDlArticleStatus(dl.articleKey, dl.articleStatus, 307L, updateCampaignSettingsOnly = true) match {
//                case Success(opsResult) =>
//                  val pf = if (opsResult.numPuts > 0) "*" else "."
//                  info(s"$pf Successfully processed article ($currentIndex of $totalToProcess): ${dl.articleId} `${dl.title}` with ${opsResult.numPuts} campaigns updated based on status: ${dl.dlArticleStatus}")
//                  myCounter("04. Articles Processed")
//                  myCounter2("05. Campaigns Updated", opsResult.numPuts)
//                  if (opsResult.numPuts > 0) articleIdsUpdated += dl.articleKey.articleId -> dl.dlArticleStatus
//
//                case Failure(fails) =>
//                  myCounter("!! Articles Failed !!")
//                  warn(fails, s"FAILED TO PROCESS ARTICLE ($currentIndex of $totalToProcess): ${dl.articleId} `${dl.title}`!")
//              }
//
//            case Failure(fails) =>
//              myCounter("!! getArticle FAILURES !!")
//              warn(fails, s"FAILED TO GET ARTICLE ($currentIndex of $totalToProcess): ${ak.articleId}!")
//          }
//        }
//
//      case Failure(fails) =>
//        myCounter("AolDynamicLeadService.getAllArticlesFromHbase :: Failed")
//        warn(fails, "Failed to pull DLUG Articles for `CheckDlug`")
//    }
//
//    val resultBuilder = new StringBuilder
//
//    val completedMap = counterMap.toMap.mapValues(_.get)
//
//    resultBuilder.append(s"Process completed with the following ${completedMap.size} counters:\n")
//    completedMap.toSeq.sortBy(_._1).foreach {
//      case (label: String, amount: Int) => resultBuilder.append(s"\t$label:\n\t\t=> $amount\n")
//    }
//    resultBuilder.append("\n")
//    resultBuilder.append(s"There were a total of ${articleIdsUpdated.size} articles that were updated:\n")
//    articleIdsUpdated.toSeq.sortBy(_._1).foreach{case (aid, status) => resultBuilder.append(s"\t=> $aid (status: $status)\n")}
//
//    resultBuilder.toString()
//  }
}

case class ImageWithCredit(image: String, credit: Option[String])

class AolDynamicLeadModifyFields(
               val title: String,
               val category: String,
               val categoryLink: String,
               val categoryId: Int,
               val categorySlug: String,
               val source: String,
               val sourceLink: String,
               val summary: String,
               val headline: String,
               val secondaryHeader: String,
               val secondaryLinks: List[AolLink],
               val image: String,
               val showVideoIcon: Boolean,
               val aolDynamicLeadCampaign: Option[AolDynamicLeadCampaign],
               val startDate: Option[DateTime],
               val endDate: Option[DateTime],
               val duration: Option[GrvDuration],
               val statusOption: Option[GmsArticleStatus.Type],
               val aolRibbonImageUrl: Option[String],
               val aolImageSource: Option[String],
               val channelImage: Option[ImageWithCredit],
               val channels: Set[AolDynamicLeadChannels.Type],
               val narrowBandImage: Option[String],
               val channelsToPinned: List[ChannelToPinnedSlot],
               val fieldsToClear: Set[String],
               val subChannelRibbon: Option[AolChannelRibbon],
               val channelPageTitle: Option[String],
               val highResImage: Option[String]) {


  override def toString: String = {
    val sb = new StringBuilder
    val nl = '\n'
    val tb = '\t'
    val dq = '"'

    sb.append("AolDynamicLeadModifyFields==>").append(nl).append(tb)

    def appendStr(label: String, obj: Any, wrapQuotes: Boolean = true): Unit = {
      val str = obj.toString
      sb.append(label).append(": ")
      if (wrapQuotes) sb.append(dq)
      sb.append(str)
      if (wrapQuotes) sb.append(dq)
      sb.append(nl).append(tb)
    }

    appendStr("title", title)
    appendStr("category", category)
    appendStr("categoryLink", categoryLink)
    appendStr("categoryId", categoryId)
    appendStr("categorySlug", categorySlug)
    appendStr("source", source)
    appendStr("sourceLink", sourceLink)
    appendStr("summary", summary)
    appendStr("headline", headline)
    appendStr("secondaryHeader", secondaryHeader)
    appendStr("secondaryLinks", secondaryLinks.mkString("\n\t\t", "\n\t\t", emptyString), wrapQuotes = false)
    appendStr("image", image)
    appendStr("showVideoIcon", showVideoIcon)
    appendStr("aolDynamicLeadCampaign", aolDynamicLeadCampaign.fold(emptyString)(_.toString))
    appendStr("startDate", startDate.fold(emptyString)(_.toString))
    appendStr("endDate", endDate.fold(emptyString)(_.toString))
    appendStr("duration", duration.fold(emptyString)(_.toString))
    appendStr("statusOption", statusOption.fold(emptyString)(_.toString))
    appendStr("aolRibbonImageUrl", aolRibbonImageUrl.fold(emptyString)(_.toString))
    appendStr("aolImageSource", aolImageSource.fold(emptyString)(_.toString))
    appendStr("channelImage", channelImage.fold(emptyString)(_.toString))
    appendStr("channels", channels.mkString("\n\t\t", "\n\t\t", emptyString), wrapQuotes = false)
    appendStr("narrowBandImage", narrowBandImage.fold(emptyString)(_.toString))
    appendStr("channelsToPinned", channelsToPinned.mkString("\n\t\t", "\n\t\t", emptyString), wrapQuotes = false)
    appendStr("fieldsToClear", fieldsToClear.mkString("\n\t\t", "\n\t\t", emptyString), wrapQuotes = false)
    appendStr("subChannelRibbon", subChannelRibbon.fold(emptyString)(_.toString))
    appendStr("channelPageTitle", channelPageTitle.fold(emptyString)(_.toString))
    appendStr("highResImage", highResImage.fold(emptyString)(_.toString))
    sb.append("===>").append(nl).toString()
  }

  def toOneScopeMap(webUserId: Int, isNewlySubmitted: Boolean, previousGrvMap: ArtGrvMap.OneScopeMap,
                    effectiveStatus: GmsArticleStatus.Type, previousStatus: GmsArticleStatus.Type,
                    articleKey: ArticleKey): ArtGrvMap.OneScopeMap = {
    val nowInEpochSeconds = grvtime.currentTime.getSeconds

    val F = AolDynamicLeadFieldNames

    val nonOptionalValueMap: ArtGrvMap.OneScopeMap = Map(
      F.Title -> ArtGrvMapPublicMetaVal(title)
      , F.CategoryText -> ArtGrvMapPublicMetaVal(category)
      , F.CategoryLink -> ArtGrvMapPublicMetaVal(categoryLink)
      , F.CategoryId -> ArtGrvMapPublicMetaVal(categoryId)
      , F.CategorySlug -> ArtGrvMapPublicMetaVal(categorySlug)
      , F.SourceText -> ArtGrvMapPublicMetaVal(source)
      , F.SourceLink -> ArtGrvMapPublicMetaVal(sourceLink)
      , F.Summary -> ArtGrvMapPublicMetaVal(summary)
      , F.Headline -> ArtGrvMapPublicMetaVal(headline)
      , F.SecondaryHeader -> ArtGrvMapPublicMetaVal(secondaryHeader)
      , F.Image -> ArtGrvMapPublicMetaVal(image)
      , F.ShowVideoIcon -> ArtGrvMapPublicMetaVal(showVideoIcon)
      , F.UpdatedTime -> ArtGrvMapPublicMetaVal(nowInEpochSeconds)
      , F.GravityCalculatedPlid -> ArtGrvMapPublicMetaVal(articleKey.intId)
      , F.ChannelPageTitle -> ArtGrvMapPublicMetaVal(channelPageTitle.getOrElse(title))
    )

    val statusBasedValueMap = if (isNewlySubmitted || effectiveStatus != previousStatus) {
      AolDynamicLeadService.buildStatusUpdateGrvMap(effectiveStatus, webUserId, isNewlySubmitted, includeStatusInMap = isNewlySubmitted, startDate = startDate)
    } else {
      ArtGrvMap.emptyOneScopeMap
    }

    val optionalFields = List(
      F.SecondaryLinks -> secondaryLinks.toNel
      , F.AolDynamicLeadCampaign -> aolDynamicLeadCampaign
      , F.StartDate -> startDate
      , F.EndDate -> endDate
      , F.Duration -> duration
      , F.Status -> statusOption
      , F.RibbonImageUrl -> aolRibbonImageUrl
      , F.ImageCredit -> aolImageSource
      , F.ChannelImage -> channelImage.map(_.image)
      , F.ChannelImageSource -> channelImage.flatMap(_.credit)
      , F.Channels -> channels.toList.toNel
      , F.NarrowBandImage -> narrowBandImage
      , F.ChannelLabel -> subChannelRibbon
      , F.HighResImage -> highResImage
    )

    val optionalValueMap: ArtGrvMap.OneScopeMap = (for {
      (k1, value) <- optionalFields
      if !fieldsToClear.contains(k1)
      (key, metaValue) <- AolDynamicLeadArticle.toGrvMapKeyVal(k1, value)
    } yield {
      key -> metaValue
    }).toMap

    val combinedMap = previousGrvMap ++ nonOptionalValueMap ++ statusBasedValueMap ++ optionalValueMap

    if (fieldsToClear.isEmpty) {
      combinedMap
    }
    else {
      combinedMap.filterKeys(k => !fieldsToClear.contains(k))
    }
  }

  def toGrvMap(oneScopeKey: ArtGrvMap.OneScopeKey, webUserId: Int, isNewlySubmitted: Boolean,
               previousGrvMap: ArtGrvMap.OneScopeMap, effectiveStatus: GmsArticleStatus.Type,
               previousStatus: GmsArticleStatus.Type, articleKey: ArticleKey): ArtGrvMap.AllScopesMap = {
    val allValueMap = toOneScopeMap(webUserId, isNewlySubmitted, previousGrvMap, effectiveStatus, previousStatus, articleKey)
    Map(oneScopeKey -> allValueMap)
  }
}

object AolDynamicLeadModifyFields {
  def apply(
             title: String,
             category: String,
             categoryLink: String,
             categoryId: Int,
             categorySlug: String,
             source: String,
             sourceLink: String,
             summary: String,
             headline: String,
             secondaryHeader: String,
             secondaryLinks: List[AolLink],
             image: String,
             showVideoIcon: Boolean,
             aolDynamicLeadCampaign: Option[AolDynamicLeadCampaign],
             startDate: Option[DateTime],
             endDate: Option[DateTime],
             duration: Option[GrvDuration],
             statusOption: Option[GmsArticleStatus.Type] = None,
             aolRibbonImageUrl: Option[String] = None,
             aolImageSource: Option[String] = None,
             channelImage: Option[ImageWithCredit] = None,
             channels: Set[AolDynamicLeadChannels.Type] = Set.empty[AolDynamicLeadChannels.Type],
             narrowBandImage: Option[String] = None,
             channelsToPinned: List[ChannelToPinnedSlot] = Nil,
             fieldsToClear: Set[String] = Set.empty[String],
             subChannelRibbon: Option[AolChannelRibbon] = None,
             channelPageTitle: Option[String] = None,
             highResImage: Option[String] = None): AolDynamicLeadModifyFields = {
    new AolDynamicLeadModifyFields(title, category, categoryLink, categoryId, categorySlug, source, sourceLink, summary,
      headline, secondaryHeader, secondaryLinks, image, showVideoIcon, aolDynamicLeadCampaign, startDate, endDate, duration,
      statusOption, aolRibbonImageUrl, aolImageSource, channelImage, channels, narrowBandImage, channelsToPinned,
      fieldsToClear, subChannelRibbon, channelPageTitle, highResImage)
  }

  def fromDlArticleWithStatusAndEnd(article: AolDynamicLeadArticle, status: GmsArticleStatus.Type, endDate: Option[DateTime]): AolDynamicLeadModifyFields = {
    AolDynamicLeadModifyFields(article.title, article.categoryText, article.categoryUrl,
      article.aolCategoryId.getOrElse(0), article.aolCategorySlug.getOrElse(emptyString), article.sourceText,
      article.sourceUrl, article.summary, article.headline, article.secondaryHeader, article.secondaryLinks,
      article.imageUrl, article.showVideoIcon, article.aolCampaign, article.startDate, endDate, None, status.some,
      article.aolRibbonImageUrl, article.aolImageSource,
      article.channelImage.map(i => ImageWithCredit(i, article.channelImageSource)), article.channels,
      article.narrowBandImage, article.channelsToPinned, subChannelRibbon = article.subChannelRibbon,
      channelPageTitle = article.channelPageTitle, highResImage = article.highResImage)
  }
}

class AolGmsModifyFields(val siteKey: SiteKey,
                         val title: String,
                         val category: String,
                         val categoryLink: String,
                         val categoryId: Int,
                         val categorySlug: String,
                         val source: String,
                         val sourceLink: String,
                         val summary: String,
                         val headline: String,
                         val secondaryHeader: String,
                         val secondaryLinks: List[AolLink],
                         val image: String,
                         val showVideoIcon: Boolean,
                         val startDate: Option[DateTime],
                         val endDate: Option[DateTime],
                         val duration: Option[GrvDuration],
                         val statusOption: Option[GmsArticleStatus.Type],
                         val aolRibbonImageUrl: Option[String],
                         val aolImageSource: Option[String],
                         val channelImage: Option[ImageWithCredit],
                         val narrowBandImage: Option[String],
                         /* val contentGroupsToPinned: List[ContentGroupIdToPinnedSlot], */
                         val channelPageTitle: Option[String],
                         val highResImage: Option[String],
                         val contentGroupIds: Set[Long],
                         val fieldsToClear: Set[String],
                         val altTitleOption: Option[String],
                         val altImageOption: Option[String],
                         val altImageSourceOption: Option[String],
                         val trackingParamsOption: Option[immutable.Map[String, String]]
                        ) {


  override def toString: String = {
    val sb = new StringBuilder
    val nl = '\n'
    val tb = '\t'
    val dq = '"'

    // GMS-FIELD-UPDATE location
    val F = AolGmsFieldNames

    sb.append("AolGmsModifyFields==>").append(nl).append(tb)

    def appendStr(label: String, obj: Any, wrapQuotes: Boolean = true): Unit = {
      val str = obj.toString
      sb.append(label).append(": ")
      if (wrapQuotes) sb.append(dq)
      sb.append(str)
      if (wrapQuotes) sb.append(dq)
      sb.append(nl).append(tb)
    }

    // GMS-FIELD-UPDATE location
    appendStr("siteKey", siteKey.stringConverterSerialized)
    appendStr("contentGroupIds", contentGroupIds.toString)
    appendStr("title", title)
    appendStr("category", category)
    appendStr("categoryLink", categoryLink)
    appendStr("categoryId", categoryId)
    appendStr("categorySlug", categorySlug)
    appendStr("source", source)
    appendStr("sourceLink", sourceLink)
    appendStr("summary", summary)
    appendStr("headline", headline)
    appendStr("secondaryHeader", secondaryHeader)
    appendStr("secondaryLinks", secondaryLinks.mkString("\n\t\t", "\n\t\t", emptyString), wrapQuotes = false)
    appendStr("image", image)
    appendStr("showVideoIcon", showVideoIcon)
//    appendStr("aolGmsCampaign", aolGmsCampaign.fold(emptyString)(_.toString))
    appendStr("startDate", startDate.fold(emptyString)(_.toString))
    appendStr("endDate", endDate.fold(emptyString)(_.toString))
    appendStr("duration", duration.fold(emptyString)(_.toString))
    appendStr("statusOption", statusOption.fold(emptyString)(_.toString))
    appendStr("aolRibbonImageUrl", aolRibbonImageUrl.fold(emptyString)(_.toString))
    appendStr("aolImageSource", aolImageSource.fold(emptyString)(_.toString))
    appendStr("channelImage", channelImage.fold(emptyString)(_.toString))
//    appendStr("channels", channels.mkString("\n\t\t", "\n\t\t", emptyString), wrapQuotes = false)
    appendStr("narrowBandImage", narrowBandImage.fold(emptyString)(_.toString))
//    appendStr("contentGroupsToPinned", contentGroupsToPinned.mkString("\n\t\t", "\n\t\t", emptyString), wrapQuotes = false)
    appendStr("fieldsToClear", fieldsToClear.mkString("\n\t\t", "\n\t\t", emptyString), wrapQuotes = false)
//    appendStr("subChannelRibbon", subChannelRibbon.fold(emptyString)(_.toString))
    appendStr("channelPageTitle", channelPageTitle.fold(emptyString)(_.toString))
    appendStr("highResImage", highResImage.fold(emptyString)(_.toString))
    appendStr(F.AltTitle, altTitleOption.getOrElse(emptyString))
    appendStr(F.AltImage, altImageOption.getOrElse(emptyString))
    appendStr(F.AltImageSource, altImageSourceOption.getOrElse(emptyString))
    sb.append("===>").append(nl).toString()
  }

  def toOneScopeMap(webUserId: Int, isNewlySubmitted: Boolean, previousGrvMap: ArtGrvMap.OneScopeMap,
                    effectiveStatus: GmsArticleStatus.Type, previousStatus: GmsArticleStatus.Type,
                    siteKey: SiteKey, articleKey: ArticleKey): ArtGrvMap.OneScopeMap = {
    val nowInEpochSeconds = grvtime.currentTime.getSeconds

    // GMS-FIELD-UPDATE location
    val F = AolGmsFieldNames

    val nonOptionalValueMap: ArtGrvMap.OneScopeMap = Map(
      F.SiteId -> ArtGrvMapPublicMetaVal(siteKey.siteId.toString)
      , F.Title -> ArtGrvMapPublicMetaVal(title)
      , F.CategoryText -> ArtGrvMapPublicMetaVal(category)
      , F.CategoryLink -> ArtGrvMapPublicMetaVal(categoryLink)
      , F.CategoryId -> ArtGrvMapPublicMetaVal(categoryId)
      , F.CategorySlug -> ArtGrvMapPublicMetaVal(categorySlug)
      , F.SourceText -> ArtGrvMapPublicMetaVal(source)
      , F.SourceLink -> ArtGrvMapPublicMetaVal(sourceLink)
      , F.Summary -> ArtGrvMapPublicMetaVal(summary)
      , F.Headline -> ArtGrvMapPublicMetaVal(headline)
      , F.SecondaryHeader -> ArtGrvMapPublicMetaVal(secondaryHeader)
      , F.Image -> ArtGrvMapPublicMetaVal(image)
      , F.ShowVideoIcon -> ArtGrvMapPublicMetaVal(showVideoIcon)
      , F.UpdatedTime -> ArtGrvMapPublicMetaVal(nowInEpochSeconds)
      , F.GravityCalculatedPlid -> ArtGrvMapPublicMetaVal(articleKey.intId)
      , F.ChannelPageTitle -> ArtGrvMapPublicMetaVal(channelPageTitle.getOrElse(title))
    )

    val statusBasedValueMap = if (isNewlySubmitted || effectiveStatus != previousStatus) {
      GmsService.buildStatusUpdateGrvMap(effectiveStatus, webUserId, isNewlySubmitted, includeStatusInMap = isNewlySubmitted, startDate = startDate)
    } else {
      ArtGrvMap.emptyOneScopeMap
    }

    val optionalFields = List(
      F.ContentGroupIds -> contentGroupIds.toList.toNel
      , F.SecondaryLinks -> secondaryLinks.toNel
      , F.StartDate -> startDate
      , F.EndDate -> endDate
      , F.Duration -> duration
      , F.Status -> statusOption
      , F.RibbonImageUrl -> aolRibbonImageUrl
      , F.ImageCredit -> aolImageSource
      , F.ChannelImage -> channelImage.map(_.image)
      , F.ChannelImageSource -> channelImage.flatMap(_.credit)
      , F.NarrowBandImage -> narrowBandImage
      , F.HighResImage -> highResImage
      , F.AltTitle -> altTitleOption
      , F.AltImage -> altImageOption
      , F.AltImageSource -> altImageSourceOption
      , F.TrackingParams -> trackingParamsOption
    )

    val optionalValueMap: ArtGrvMap.OneScopeMap = (for {
      (k1, value) <- optionalFields
      if !fieldsToClear.contains(k1)
      (key, metaValue) <- AolGmsArticle.toGrvMapKeyVal(k1, value)
    } yield {
      key -> metaValue
    }).toMap

    val combinedMap = previousGrvMap ++ nonOptionalValueMap ++ statusBasedValueMap ++ optionalValueMap

    if (fieldsToClear.isEmpty) {
      combinedMap
    }
    else {
      combinedMap.filterKeys(k => !fieldsToClear.contains(k))
    }
  }

  def toGrvMap(gmsRoute: GmsRoute, webUserId: Int, isNewlySubmitted: Boolean,
               previousGrvMap: ArtGrvMap.OneScopeMap, effectiveStatus: GmsArticleStatus.Type,
               previousStatus: GmsArticleStatus.Type, articleKey: ArticleKey): ArtGrvMap.AllScopesMap = {
    val allValueMap = toOneScopeMap(webUserId, isNewlySubmitted, previousGrvMap, effectiveStatus, previousStatus, gmsRoute.siteKey, articleKey)

    Map(gmsRoute.oneScopeKey -> allValueMap)
  }
}

object AolGmsModifyFields {
  def apply(siteKey: SiteKey,
            title: String,
            category: String,
            categoryLink: String,
            categoryId: Int,
            categorySlug: String,
            source: String,
            sourceLink: String,
            summary: String,
            headline: String,
            secondaryHeader: String,
            secondaryLinks: List[AolLink],
            image: String,
            showVideoIcon: Boolean,
            startDate: Option[DateTime],
            endDate: Option[DateTime],
            duration: Option[GrvDuration],
            statusOption: Option[GmsArticleStatus.Type] = None,
            aolRibbonImageUrl: Option[String] = None,
            aolImageSource: Option[String] = None,
            channelImage: Option[ImageWithCredit] = None,
            narrowBandImage: Option[String] = None,
            channelPageTitle: Option[String] = None,
            highResImage: Option[String] = None,
            contentGroupIds: Set[Long] = Set.empty[Long],
            altTitleOption: Option[String] = None,
            altImageOption: Option[String] = None,
            altImageSourceOption: Option[String] = None,
            trackingParamsOption: Option[immutable.Map[String, String]] = None,
            fieldsToClear: Set[String] = Set.empty[String]
           ): AolGmsModifyFields = {
    // GMS-FIELD-UPDATE location
    new AolGmsModifyFields(siteKey, title, category, categoryLink, categoryId, categorySlug, source,
      sourceLink, summary, headline, secondaryHeader, secondaryLinks, image, showVideoIcon, startDate,
      endDate, duration, statusOption, aolRibbonImageUrl, aolImageSource, channelImage, narrowBandImage,
      /* contentGroupsToPinned, */
      channelPageTitle, highResImage, contentGroupIds, fieldsToClear = fieldsToClear, altTitleOption = altTitleOption,
      altImageOption = altImageOption, altImageSourceOption = altImageSourceOption, trackingParamsOption = trackingParamsOption)
  }

}

case class ArticleRowWithScopedDlugMetrics(row: ArticleRow, metrics: immutable.Map[AolDynamicLeadChannels.Type, ScopedMetricsBuckets])

object ArticleRowWithScopedDlugMetrics {
  def withEmptyMetrics(row: ArticleRow): ArticleRowWithScopedDlugMetrics = ArticleRowWithScopedDlugMetrics(row, Map.empty[AolDynamicLeadChannels.Type, ScopedMetricsBuckets])
}

case class ArticleRowWithScopedGmsMetrics(row: ArticleRow, metrics: immutable.Map[Long, ScopedMetricsBuckets])

object ArticleRowWithScopedGmsMetrics {
  def withEmptyMetrics(row: ArticleRow): ArticleRowWithScopedGmsMetrics = ArticleRowWithScopedGmsMetrics(row, Map.empty[Long, ScopedMetricsBuckets])
}

case class SaveDynamicLeadArticleParams(url: String, fields: AolDynamicLeadModifyFields, userId: Long, isNewlySubmitted: Boolean, campaignKey: CampaignKey)

case object AolDynamicLeadStage extends StageWithMeta {
  val label = "Aol DL stage"
  trait DynamicLeadNote extends Note
  type StageNote = DynamicLeadNote
  type MetaType = DynamicLeadStageMeta

  private val c = ','
  private val ob = '{'
  private val cb = '}'
  private val q = '"'
  private val l = ':'

  case class DynamicLeadStageMeta(initialParams: Option[SaveDynamicLeadArticleParams] = None) {
    def appendToString(sb: StringBuilder): StringBuilder = {
      initialParams match {
        case Some(p) =>
          sb.append(ob) // {
            .append(q).append("url").append(q).append(l) // "url":
            .append(q).append(p.url).append(q) // "..."
            .append(c) // ,
            .append(q).append("fields").append(q).append(l) // "fields":
            .append(ob) // {
            .append(q).append("title").append(q).append(l) // "title":
            .append(q).append(p.fields.title).append(q).append(c) // "...",
            .append(q).append("category").append(q).append(l) // "category":
            .append(q).append(p.fields.category).append(q).append(c) // "...",
            .append(q).append("categoryLink").append(q).append(l) // "categoryLink":
            .append(q).append(p.fields.categoryLink).append(q).append(c) // "...",
            .append(q).append("categoryId").append(q).append(l) // "categoryId":
            .append(p.fields.categoryId).append(c) // 0,
            .append(q).append("categorySlug").append(q).append(l) // "categorySlug":
            .append(q).append(p.fields.categorySlug).append(q).append(c) // "...",
            .append(q).append("source").append(q).append(l) // "source":
            .append(q).append(p.fields.source).append(q).append(c) // "...",
            .append(q).append("sourceLink").append(q).append(l) // "sourceLink":
            .append(q).append(p.fields.sourceLink).append(q).append(c) // "...",
            .append(q).append("summary").append(q).append(l) // "summary":
            .append(q).append(p.fields.summary).append(q).append(c) // "...",
            .append(q).append("headline").append(q).append(l) // "headline":
            .append(q).append(p.fields.headline).append(q).append(c) // "...",
            .append(q).append("secondaryHeader").append(q).append(l) // "secondaryHeader":
            .append(q).append(p.fields.secondaryHeader).append(q).append(c) // "...",
            .append(q).append("secondaryLinks").append(q).append(l)
          p.fields.secondaryLinks.addString(sb, "[", ",", "]")
          sb.append(q).append("image").append(q).append(l)
            .append(q).append(p.fields.image).append(q).append(c)
            .append(q).append("showVideoIcon").append(q).append(l) // "showVideoIcon":
            .append(p.fields.showVideoIcon) // false
          p.fields.aolDynamicLeadCampaign.foreach(camp => {
            sb.append(c).append(q).append("aolDynamicLeadCampaign").append(q).append(l).append(camp.toString)
          })
          p.fields.startDate.foreach(sd => {
            sb.append(c).append(q).append("startDate").append(q).append(l).append(sd.getSeconds)
          })
          p.fields.endDate.foreach(ed => {
            sb.append(c).append(q).append("endDate").append(q).append(l).append(ed.getSeconds)
          })
          p.fields.statusOption.foreach(status => {
            sb.append(c).append(q).append("status").append(q).append(l).append(q).append(status.toString).append(q)
          })
          p.fields.aolRibbonImageUrl.foreach(ribbon => {
            sb.append(c).append(q).append("aolRibbonImageUrl").append(q).append(l).append(q).append(ribbon).append(q)
          })
          p.fields.aolImageSource.foreach(image => {
            sb.append(c).append(q).append("aolImageSource").append(q).append(l).append(q).append(image).append(q)
          })
          sb.append(cb)

          sb.append(c).append(q).append("userId").append(q).append(l).append(p.userId)

          sb.append(c).append(q).append("isNewlySubmitted").append(q).append(l).append(p.isNewlySubmitted)

          sb.append(c).append(q).append("campaignKey").append(q).append(l).append(q).append(p.campaignKey.toString).append(q)

          sb.append(cb)

        case None => sb.append("Empty DynamicLeadStageMeta")
      }
    }
  }

  def noteCT = classTag[StageNote]
  def makeNote(message: String) = new MsgNote(message) with StageNote
  def makeFailsNote(fails: NonEmptyList[FailureResult]) = new MsgNote(fails.map(_.messageWithExceptionInfo).mkString(" AND ")) with StageNote
  def makeMetaNote(message: String, res: MetaType, continuable: Boolean = true) = new SimpleMessageMetaNote(message, res, continuable) with StageNote
}

case class SaveGmsArticleParams(url: String, fields: AolGmsModifyFields, userId: Long, isNewlySubmitted: Boolean)

case object AolGmsStage extends StageWithMeta {
  val label = "Aol DL stage"
  trait GmsNote extends Note
  type StageNote = GmsNote
  type MetaType = GmsStageMeta

  private val c = ','
  private val ob = '{'
  private val cb = '}'
  private val q = '"'
  private val l = ':'

  case class GmsStageMeta(initialParams: Option[SaveGmsArticleParams] = None) {
    def appendToString(sb: StringBuilder): StringBuilder = {
      initialParams match {
        case Some(p) =>
          sb.append(ob) // {
            .append(q).append("url").append(q).append(l) // "url":
            .append(q).append(p.url).append(q) // "..."
            .append(c) // ,
            .append(q).append("fields").append(q).append(l) // "fields":
            .append(ob) // {
            .append(q).append("title").append(q).append(l) // "title":
            .append(q).append(p.fields.title).append(q).append(c) // "...",
            .append(q).append("category").append(q).append(l) // "category":
            .append(q).append(p.fields.category).append(q).append(c) // "...",
            .append(q).append("categoryLink").append(q).append(l) // "categoryLink":
            .append(q).append(p.fields.categoryLink).append(q).append(c) // "...",
            .append(q).append("categoryId").append(q).append(l) // "categoryId":
            .append(p.fields.categoryId).append(c) // 0,
            .append(q).append("categorySlug").append(q).append(l) // "categorySlug":
            .append(q).append(p.fields.categorySlug).append(q).append(c) // "...",
            .append(q).append("source").append(q).append(l) // "source":
            .append(q).append(p.fields.source).append(q).append(c) // "...",
            .append(q).append("sourceLink").append(q).append(l) // "sourceLink":
            .append(q).append(p.fields.sourceLink).append(q).append(c) // "...",
            .append(q).append("summary").append(q).append(l) // "summary":
            .append(q).append(p.fields.summary).append(q).append(c) // "...",
            .append(q).append("headline").append(q).append(l) // "headline":
            .append(q).append(p.fields.headline).append(q).append(c) // "...",
            .append(q).append("secondaryHeader").append(q).append(l) // "secondaryHeader":
            .append(q).append(p.fields.secondaryHeader).append(q).append(c) // "...",
            .append(q).append("secondaryLinks").append(q).append(l)
          p.fields.secondaryLinks.addString(sb, "[", ",", "]")
          sb.append(q).append("image").append(q).append(l)
            .append(q).append(p.fields.image).append(q).append(c)
            .append(q).append("showVideoIcon").append(q).append(l) // "showVideoIcon":
            .append(p.fields.showVideoIcon) // false
//          p.fields.aolGmsCampaign.foreach(camp => {
//            sb.append(c).append(q).append("aolGmsCampaign").append(q).append(l).append(camp.toString)
//          })
          p.fields.startDate.foreach(sd => {
            sb.append(c).append(q).append("startDate").append(q).append(l).append(sd.getSeconds)
          })
          p.fields.endDate.foreach(ed => {
            sb.append(c).append(q).append("endDate").append(q).append(l).append(ed.getSeconds)
          })
          p.fields.statusOption.foreach(status => {
            sb.append(c).append(q).append("status").append(q).append(l).append(q).append(status.toString).append(q)
          })
          p.fields.aolRibbonImageUrl.foreach(ribbon => {
            sb.append(c).append(q).append("aolRibbonImageUrl").append(q).append(l).append(q).append(ribbon).append(q)
          })
          p.fields.aolImageSource.foreach(image => {
            sb.append(c).append(q).append("aolImageSource").append(q).append(l).append(q).append(image).append(q)
          })
          sb.append(cb)

          sb.append(c).append(q).append("userId").append(q).append(l).append(p.userId)

          sb.append(c).append(q).append("isNewlySubmitted").append(q).append(l).append(p.isNewlySubmitted)

//          sb.append(c).append(q).append("campaignKey").append(q).append(l).append(q).append(p.campaignKey.toString).append(q)

          sb.append(cb)

        case None => sb.append("Empty GmsStageMeta")
      }
    }
  }

  def noteCT = classTag[StageNote]
  def makeNote(message: String) = new MsgNote(message) with StageNote
  def makeFailsNote(fails: NonEmptyList[FailureResult]) = new MsgNote(fails.map(_.messageWithExceptionInfo).mkString(" AND ")) with StageNote
  def makeMetaNote(message: String, res: MetaType, continuable: Boolean = true) = new SimpleMessageMetaNote(message, res, continuable) with StageNote
}

case class PartialChannelSaveFailureResult(successfullySaved: NonEmptyList[AolDynamicLeadChannels.Type], failedToSave: NonEmptyList[AolDynamicLeadChannels.Type])
  extends FailureResult(s"Only partially saved channels!. Succeeded to save: ${successfullySaved.list.mkString(" & ")}. Failed to save: ${failedToSave.list.mkString(" & ")}.", None)

case class PartialContentGroupSaveFailureResult(successfullySaved: NonEmptyList[Long], failedToSave: NonEmptyList[Long])
  extends FailureResult(s"Only partially saved content groups!. Succeeded to save: ${successfullySaved.list.mkString(" & ")}. Failed to save: ${failedToSave.list.mkString(" & ")}.", None)

case class DuplicateArticleKeyFailureResult(dupeKey: ArticleKey, url: String)
  extends FailureResult(s"Duplicate DL Unit already exists for articleId: ${dupeKey.articleId} (url: `$url`)!", None)

case class ArticlesWithMetricsResult(articlesWithMetrics: List[ArticleRowWithScopedDlugMetrics], splits: ScopedMetricsDateKeySplits)

object ArticlesWithMetricsResult {
  def withEmptyMetrics(rows: List[ArticleRow]): ArticlesWithMetricsResult = ArticlesWithMetricsResult(rows.map(ArticleRowWithScopedDlugMetrics.withEmptyMetrics), ScopedMetricsDateKeySplits.forNow)
}
