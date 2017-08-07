package com.gravity.interests.jobs.intelligence.operations

import java.util.concurrent.atomic.AtomicInteger

import com.fasterxml.jackson.core.JsonProcessingException
import com.gravity.data.configuration.{ConfigurationQueryService, ContentGroupRow, GmsPinnedArticleRow, SetAllContentGroupPinsResult, SitePlacementRow}
import com.gravity.domain.GrvDuration
import com.gravity.domain.aol._
import com.gravity.domain.gms.{GmsArticleStatus, GmsRoute, UniArticleId}
import com.gravity.domain.recommendations.ContentGroup
import com.gravity.domain.rtb.gms.ContentGroupIdToPinnedSlot
import com.gravity.hbase.schema.{ByteConverter, OpsResult}
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
import com.gravity.utilities.cache.{CacheFactory, LruRefreshedCache, PermaCacher}
import com.gravity.utilities.components.FailureResult
import com.gravity.utilities.grvannotation._
import com.gravity.utilities.grvcoll.{Pager, _}
import com.gravity.utilities.grvjson._
import com.gravity.utilities.grvstrings._
import com.gravity.utilities.grvtime._
import com.gravity.utilities.grvz._
import com.gravity.utilities.time.DateMinute
import com.gravity.utilities.web.NegatableEnum
import com.gravity.valueclasses.ValueClassesForDomain.{SiteGuid, SitePlacementId}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp
import org.apache.hadoop.hbase.filter.{BinaryComparator, ValueFilter}
import org.joda.time.DateTime
import play.api.libs.json.Json

import scala.collection.mutable
import scala.util.Try
import scalaz.Scalaz._
import scalaz._

/**
 * Created by robbie on 08/04/2015.
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
 * Placeholder for what's coming in INTERESTS-7686
 */
object GmsService extends DlugOrGmsService with MaintenanceModeProtection {
  import com.gravity.utilities.Counters._

  import com.gravity.interests.jobs.intelligence.operations.AolGmsStage._
  import com.gravity.logging.Logging._

  val counterCategory: String = "GmsService"

  private val batchSize = 10000 //if (Settings2.isDevelopmentEnvironment) 1000 else 10000

  private val atLeastOneContentGroupFailureMessage = "Every GMS Article MUST belong to at least one content group!"
  val noContentGroupsFailureResult = FailureResult(atLeastOneContentGroupFailureMessage)

  def articleQuery(optGmsRoute: Option[GmsRoute]): ArticleService.QuerySpec = optGmsRoute match {
    case Some(gmsRoute) => _.withFamilies(_.meta).withColumn(_.allArtGrvMap, gmsRoute.oneScopeKey)
    case None => _.withFamilies(_.meta, _.allArtGrvMap)
  }

  /**
    * @return Articles with metrics for articles having scoped metrics (i.e. articles without scoped metrics will not be
    *         in the resulting list).
    */
  def getArticleRowsWithScopedGmsMetrics(siteGuid: SiteGuid, articleRows: List[ArticleRow], fromCacheOnly: Boolean = false): ValidationNel[FailureResult, List[ArticleRowWithScopedGmsMetrics]] = {
    val articleKeys = articleRows.map(_.articleKey).toSeq

    for {
      spInfoMap <- getManagedSitePlacementsWithContentGroups(siteGuid, skipCache = true)

      sitePlacementIds = spInfoMap.keySet

      sitePlacementScopedKeys = sitePlacementIds.map(spId => SitePlacementIdKey(spId).toScopedKey)

      keyMap = (for {
        articleKey <- articleKeys
        articleScopedKey = articleKey.toScopedKey
        fromToKeys = sitePlacementScopedKeys.map(spScopedKey => articleScopedKey.fromToKey(spScopedKey))
      } yield {
        articleKey -> fromToKeys
      }).toMap

      allScopedKeys = keyMap.values.flatten.toSet

      sitePlacementScopedKeyToContentGroupMap = spInfoMap.map { case (sitePlacementId, (spRow, contentGroups)) =>
        SitePlacementIdKey(sitePlacementId).toScopedKey -> contentGroups
      }

      result <- allScopedKeys.grouped(10000).toIterable.map { chunk =>
        CacheFactory.getMetricsCache.get(chunk, fromCacheOnly)(scopedMetricsCacheQuery)
      }.extrude.map { seq =>
        seq.fold(Map.empty[ScopedFromToKey, ScopedMetricsBuckets])(_ ++ _)
      }.map { keyToBuckets =>
        for {
          article <- articleRows
          scopedFromToKeys <- keyMap.get(article.articleKey)
        } yield {
          val channelToBuckets = mutable.Map[Long, ScopedMetricsBuckets]()

          for {
            fromTo <- scopedFromToKeys.toSeq
            buckets <- keyToBuckets.get(fromTo).toSeq
            contentGroups = sitePlacementScopedKeyToContentGroupMap.getOrElse(fromTo.to, Nil)
            contentGroup <- contentGroups
          } {
            val cgId = contentGroup.id

            channelToBuckets += {
              val oldMetrics = channelToBuckets.getOrElse(cgId, ScopedMetricsBuckets.empty)
              val newMetrics = oldMetrics + buckets

              cgId -> newMetrics
            }
          }

          ArticleRowWithScopedGmsMetrics(article, channelToBuckets.toMap)
        }
      }
    } yield {
      // DEBUG
      //      def onlyImportant(map: Map[ScopedMetricsKey, Long]) =
      //        map.filterKeys(met => met.countBy != RecommendationMetricCountBy.unitImpression)
      //
      //      def printIfImportant(cgId: Long, scopedMetricsBuckets: ScopedMetricsBuckets) = {
      //        val seq = Seq(
      //          "monthly"  -> scopedMetricsBuckets.monthly.map.toMap,
      //          "daily"    -> scopedMetricsBuckets.daily.map.toMap,
      //          "hourly"   -> scopedMetricsBuckets.hourly.map.toMap,
      //          "minutely" -> scopedMetricsBuckets.minutely.map.toMap
      //        )
      //
      //        for {
      //          (str, allMap) <- seq
      //          impMap = onlyImportant(allMap)
      //          if impMap.nonEmpty
      //        } {
      //          println(s"*** metrics cg=$cgId, $str.map=${impMap}")
      //        }
      //      }
      //
      //      result.foreach { rowWithMets =>
      //        rowWithMets.metrics.foreach { case (cg, scopedMets) =>
      //          printIfImportant(cg, scopedMets)
      //        }
      //      }

      result
    }
  }

  /**
    * Reads a GMS article for the given gmsRoute/articleKey combination from HBase (without metrics).
    */
  def getGmsArticleFromHbaseWithoutMetrics(gmsRoute: GmsRoute, articleKey: ArticleKey): ValidationNel[FailureResult, AolGmsArticle] = {
    for {
      article <- ArticleService.fetch(articleKey)(articleQuery(gmsRoute.some))
      dlArticle <- tryBuildGmsArticleFromArtRowWithoutMetrics(article, gmsRoute)
    } yield dlArticle
  }

  /**
    * Reads GMS articles for the given gmsRoute/articleKeys combination from HBase (possibly with metrics).
    *
    * IMPROVE: If this were changed to take arbitrary key/route combinations, and if it returned ordered results, we wouldn't need next two functions.
    */
  def getSelectedGmsArticlesFromHbase(articleKeys: Seq[ArticleKey], gmsRoute: GmsRoute, withMetrics: Boolean = true, pullMetricsFromCacheOnly: Boolean = false, forContentGroup: Option[Long] = None)(implicit conf: Configuration): ValidationNel[FailureResult, List[AolGmsArticle]] = {
    if (articleKeys.isEmpty)
      return Nil.successNel

    for {
      articles <- ArticleService.batchFetchMulti(articleKeys.toSet, batchSize = batchSize)(articleQuery(gmsRoute.some))

      siteGuid <- SiteService.sg(gmsRoute.siteKey).toValidationNel {
        FailureResult(s"Failed to get SiteGuid for siteKey: ${gmsRoute.siteKey}")
      }

      artRowsWithMetrics <- perhapsWithMetrics(siteGuid, articles.values.toList, withMetrics = withMetrics, pullMetricsFromCacheOnly = pullMetricsFromCacheOnly, forContentGroup = forContentGroup)
    } yield artRowsWithMetrics.flatMap(ar => {
      tryBuildGmsArticleFromArtRow(ar, gmsRoute, forContentGroup = forContentGroup) match {
        case Success(gmsArt) =>
          gmsArt.some

        case Failure(fails) =>
          warn(fails, s"Failed to build GMS Article from grvmap for: articleId = ${ar.row.articleKey.articleId}, gmsRoute = $gmsRoute!")
          None
      }
    })
  }

  def getUnorderedGmsArticlesFromHbase(gmsArticleIds: Seq[UniArticleId], withMetrics: Boolean = true, pullMetricsFromCacheOnly: Boolean = false): ValidationNel[FailureResult, Seq[AolGmsArticle]] = {
    for {
      seqOfSeq <- (for {
        (gmsRoute, artKeys) <- gmsArticleIds.groupBy(gmsArtId => GmsRoute(gmsArtId.siteKey)).mapValues(_.map(_.articleKey)).toList
      } yield {
        getSelectedGmsArticlesFromHbase(artKeys, gmsRoute, withMetrics = withMetrics, pullMetricsFromCacheOnly = pullMetricsFromCacheOnly)
      }).extrude
    } yield {
      seqOfSeq.flatten
    }
  }

  def getOrderedGmsArticlesFromHbase(gmsArticleIds: Seq[UniArticleId], withMetrics: Boolean = true, pullMetricsFromCacheOnly: Boolean = false): ValidationNel[FailureResult, Seq[AolGmsArticle]] = {
    for {
      articles <- getUnorderedGmsArticlesFromHbase(gmsArticleIds, withMetrics = withMetrics, pullMetricsFromCacheOnly = pullMetricsFromCacheOnly)

      haveMap = articles.toList.mapBy(_.uniArticleId)
    } yield {
      gmsArticleIds.collect(haveMap)
    }
  }

  // The scoped metrics will be more detailed for the 'forContentGroup' content group.
  def perhapsWithMetrics(siteGuid: SiteGuid, articleList: List[ArticleRow], withMetrics: Boolean = true, pullMetricsFromCacheOnly: Boolean = false, forContentGroup: Option[Long] = None): ValidationNel[FailureResult, List[ArticleRowWithScopedGmsMetrics]] = {
    val gmsRoute = GmsRoute(siteGuid.siteKey)

    for {
      articlesWithMetrics <- if (withMetrics) {
        getArticleRowsWithScopedGmsMetrics(siteGuid, articleList, fromCacheOnly = pullMetricsFromCacheOnly)
      } else {
        articleList.map(ArticleRowWithScopedGmsMetrics.withEmptyMetrics).successNel
      }
    } yield {
      articlesWithMetrics
    }
  }

  def queryGmsArticles(siteGuid: String, pager: Option[Pager] = None, contentGroupId: Option[Long] = None, searchTerm: Option[String] = None,
                       status: Option[Set[NegatableEnum[GmsArticleStatus.Type]]] = None, submittedUserId: Option[Int] = None,
                       submittedDate: Option[DateMidnightRange] = None, deliveryMethod: Option[AolDeliveryMethod.Type] = None,
                       aolCategory: Option[String] = None, aolSource: Option[String] = None): ValidationNel[FailureResult, AolGmsQueryResult] = {
    val filteredSearchTerm = searchTerm.filter(_.nonEmpty).map(_.toLowerCase)
    val searchTerms = filteredSearchTerm.toSet.flatMap(_.splitBetter(" ")).map(_.trim).filter(_.nonEmpty)
    val filteredContentGroupId = contentGroupId.filterNot(_ == -1L)
    val gmsRoute = GmsRoute(SiteKey(siteGuid))

    for {
      keyQueryResult <- GmsArticleIndexer.query(pager, siteGuid.some, None, searchTerms,
        status.getOrElse(Set.empty), submittedUserId, submittedDate, deliveryMethod, aolCategory, aolSource, "GMS".some, filteredContentGroupId)
      AolGmsKeyQueryResult(pageCount, articleKeys, totalItems, countsByStatus) = keyQueryResult
      articles <- ArticleService.fetchMultiOrdered(articleKeys)(articleQuery(gmsRoute.some))
      articlesWithMetricsResult <- getArticleRowsWithScopedGmsMetrics(SiteGuid(siteGuid), articles.toList, fromCacheOnly = false)
    } yield {
      var totalMetrics = MetricsBase.empty
      val finalArticles = articlesWithMetricsResult.flatMap(ar => {
        tryBuildGmsArticleFromArtRow(ar, gmsRoute, forContentGroup = filteredContentGroupId).toOption.map(aolArt => {
          aolArt.metrics.get("total").foreach(m => totalMetrics += m.lifetime)
          aolArt
        })
      })
      val finalArticlesByKey = finalArticles.mapBy(_.articleKey)
      val finalSortedArticles = articleKeys.collect(finalArticlesByKey)

      AolGmsQueryResult(pageCount, finalSortedArticles, totalItems, countsByStatus, totalMetrics)
    }
  }

  // Gets the Gms-Managed CampaignKeys for a site, or for all sites. Actually makes database calls,
  // so see also getPermaCachedGmsManagedCampaignKeys and isGmsManaged, which are permacached and much faster.
  def getGmsManagedCampaignKeys(optSiteGuid: Option[SiteGuid], skipCache: Boolean = false): ValidationNel[FailureResult, Set[CampaignKey]] = {
    for {
      groups <- getGmsContentGroups(None, skipCache = skipCache)
    } yield {
      toSiteGuidCampaignKeys(groups).map(_._2).toSet
    }
  }

  // Gets the Gms-Managed CampaignKeys for all sites. Permacached, and so quite lightweight.
  def getPermaCachedGmsManagedCampaignKeys: Set[CampaignKey] = {
    def factoryFun(): Set[CampaignKey] = {
      getGmsManagedCampaignKeys(None, skipCache = true) match {
        case Success(cks) => cks

        case Failure(fails) =>
          warn(fails, "Failure in GmsService.getPermaCachedGmsManagedCampaignKeys.")
          throw new Exception(fails.list.mkString(", "))
      }
    }

    PermaCacher.getOrRegister("GmsService.getPermaCachedGmsManagedCampaignKeys",
      PermaCacher.retryUntilNoThrow(factoryFun, retryOnFailSecs = 5), reloadInSeconds = 60, mayBeEvicted = false)
  }

  // Returns true if the campaign is for a GMS-managed content group.  Permacached, and so quite lightweight.
  def isGmsManaged(ck: CampaignKey) =
    getPermaCachedGmsManagedCampaignKeys contains ck

  def getAllGmsArticleKeys(query: CampaignService.QuerySpec = _.withColumns(_.siteGuid).withFamilies(_.recentArticles)): ValidationNel[FailureResult, Set[(SiteGuid, ArticleKey)]] = {
    for {
      groups <- getGmsContentGroups(None, skipCache = false)

      sgAndCks = toSiteGuidCampaignKeys(groups)

      sgAndArtsSeq = sgAndCks.map { case (sg, ck) =>
        CampaignService.fetch(ck)(query).map(_.recentArticles.values.map(sg -> _))
      }

      sgAkPairs <- sgAndArtsSeq.extrude.map(_.flatten.toSet)
    } yield {
      sgAkPairs
    }
  }

  def getOptionalSeconds(value: Option[DateTime]): Int = value.fold(0)(_.getSeconds)

  def validateContentGroupIdsFromGrvMap(oneScopeMap: OneScopeMap): ValidationNel[FailureResult, Set[Long]] = {
    for {
      jsonString <- oneScopeMap.validateString(AolGmsFieldNames.ContentGroupIds, isOptional = true)
      _ = if (jsonString.isEmpty) return Set.empty[Long].successNel

      cgIdStrs <- {
        try
          Json.fromJson[List[String]](Json.parse(jsonString)).toValidation.leftMap(err => nel(FailureResult(err.toString)))
        catch {
          case e: JsonProcessingException => return FailureResult(e).failureNel
        }
      }

      cgIds <- cgIdStrs.map(s => s.tryToLong.toValidationNel(FailureResult(s"`$s` is not a Content Group ID (must be a Long)"))).extrude.map(_.toSet)
    } yield cgIds.toSet
  }

  def tryBuildGmsArticleFromArtRowWithoutMetrics(article: ArticleRow,
                                                 gmsRoute: GmsRoute,
                                                 notes: mutable.Buffer[GmsNote] = mutable.Buffer.empty[GmsNote]
                                                ): ValidationNel[FailureResult, AolGmsArticle] = {
    tryBuildGmsArticleFromArtRow(ArticleRowWithScopedGmsMetrics(article, Map.empty[Long, ScopedMetricsBuckets]), gmsRoute, notes, forContentGroup = None)
  }

  def tryBuildGmsArticlesFromArtRowWithoutMetrics(articles: Seq[ArticleRow], gmsRoute: GmsRoute): ValidationNel[FailureResult, Seq[AolGmsArticle]] = {
    articles.map(a => tryBuildGmsArticleFromArtRowWithoutMetrics(a, gmsRoute)).extrude
  }

  def siteContentGroupIdsForGmsRoute(gmsRoute: GmsRoute): ValidationNel[FailureResult, Seq[Long]] = {
    tryToSuccessNEL(
      ConfigurationQueryService.queryRunner.getGmsContentGroups(None).filter(cg => SiteKey(cg.forSiteGuid) == gmsRoute.siteKey).map(_.id).sorted,
      ex => FailureResult("Failed to retrieve GmsPinnedArticleRows from configurationDb!", ex)
    )
  }

  def getPinnedArtSlotsForContentGroups(optSiteGuid: Option[SiteGuid]): ValidationNel[FailureResult, Map[Long, List[GmsPinnedArticleRow]]] = {
    for {
      contentGroupIds <- getGmsContentGroups(optSiteGuid, skipCache = false).map(_.map(_.id).toSet)

      pinnedArtSlotsForContentGroups <- getPinnedArticleSlotsForContentGroups(contentGroupIds)
    } yield {
      pinnedArtSlotsForContentGroups
    }
  }

  def getContentGroupsToPinned(siteGuid: SiteGuid, articleKey: ArticleKey): ValidationNel[FailureResult, List[ContentGroupIdToPinnedSlot]] = {
    for {
      pinnedArtSlotsForContentGroups <- getPinnedArtSlotsForContentGroups(siteGuid.some)

      gmsPinnedArticleRows = pinnedArtSlotsForContentGroups.values.flatten.filter(_.articleKey == articleKey).toSet
    } yield {
      gmsPinnedArticleRows.toSeq.map { gmsPinnnedArtRow => ContentGroupIdToPinnedSlot(gmsPinnnedArtRow.contentGroupId, gmsPinnnedArtRow.slotPosition) }.toList
    }
  }

  def tryBuildGmsArticleFromArtRow(artWithMetrics: ArticleRowWithScopedGmsMetrics,
                                   gmsRoute: GmsRoute,
                                   notes: mutable.Buffer[GmsNote] = mutable.Buffer.empty[GmsNote],
                                   forContentGroup: Option[Long] = None)
  : ValidationNel[FailureResult, AolGmsArticle] = {

    for {
      siteContentGroupIds <- siteContentGroupIdsForGmsRoute(gmsRoute)

      // The scoped metrics will be more detailed for the 'forContentGroup' content group.
      metricsWithMinutely = buildMetrics(artWithMetrics, siteContentGroupIds, forContentGroup)

      ar = artWithMetrics.row
      article <- tryBuildGmsArticleFromGrvMap(ar.articleKey, ar.urlOption, ar.allArtGrvMap, gmsRoute, metricsWithMinutely, notes, ar.getAuthorName)
    } yield {
      article
    }
  }

  def tryBuildGmsArticleFromGrvMap(
                                    articleKey: ArticleKey,
                                    urlOption: Option[String],
                                    allArtGrvMap: ArtGrvMap.AllScopesMap,
                                    gmsRoute: GmsRoute,
                                    metricsWithMinutely: AolDynamicLeadMetricsWithMinutely,
                                    notes: mutable.Buffer[GmsNote] = mutable.Buffer.empty[GmsNote],
                                    author: Option[String] = None
                                  ): ValidationNel[FailureResult, AolGmsArticle] = {
    val articleId = articleKey.articleId.toString

    // If we can't get a GrvMap from one of the desired scopes, that's a fail.
    val grvMap = GmsService.gmsOrDlugGrvMap(allArtGrvMap, gmsRoute)
      .toValidationNel(NoGrvMapFailureResult(articleKey, urlOption, gmsRoute))
      .valueOr(fails => return fails.failure)

    val F = AolGmsFieldNames

    val failBuff = mutable.Buffer[FailureResult]()

    def messageNote(msg: String): Unit = {
      notes += makeNote(msg)
    }

    def handleFailedToGet(fieldName: String, fails: NonEmptyList[FailureResult]): Unit = {
      handleFails(s"Failed to get `$fieldName` for articleId: `$articleId` and `$gmsRoute`", fails)
    }

    def handleFails(msg: String, fails: NonEmptyList[FailureResult]): Unit = {
      failBuff ++= fails.list
      notes += makeNote(msg)
      notes += makeFailsNote(fails)
    }

    val url = urlOption match {
      case Some(u) => u
      case None =>
        val msg = "No `url` found in article row (articleId: " + articleId + ")!"
        notes += makeNote(msg)
        failBuff += FailureResult(msg)
        emptyString
    }

    def optLongExtractor(gmVal: ArtGrvMapMetaVal): ValidationNel[FailureResult, Option[Long]] = {
      gmVal.getString.flatMap(_.tryToLong.map(_.some).toValidationNel(FailureResult("The value must contain a string that can be parsed as a Long.")))
    }

    val siteId = grvMap.getValueFromGrvMap(F.SiteId, None: Option[Long], isOptional = false, failureHandler = handleFailedToGet(F.SiteId, _))(optLongExtractor) match {
      case Some(sid) =>
        sid
      case None =>
        handleFailedToGet(F.SiteId, FailureResult("No siteId found in GrvMap!").wrapNel)
        -1
    }
    val siteKey = SiteKey(siteId)

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

    val contentGroupIds = validateContentGroupIdsFromGrvMap(grvMap).valueOr {
      case fails: NonEmptyList[FailureResult] =>
        handleFails("Failed to validateContentGroupIdsFromGrvMap", fails)
        Set.empty[Long]
    }

    val image = grvMap.getStringFromGrvMap(F.Image, failureHandler = handleFailedToGet(F.Image, _))

    val showVideoIcon = grvMap.getBoolFromGrvMap(F.ShowVideoIcon, failureHandler = handleFailedToGet(F.ShowVideoIcon, _))

    val updatedTime = grvMap.getIntFromGrvMap(F.UpdatedTime, failureHandler = handleFailedToGet(F.UpdatedTime, _))

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

    val altTitleOption = grvMap.getValueFromGrvMap(F.AltTitle, None: Option[String], isOptional = true, failureHandler = handleFailedToGet(F.AltTitle, _))(_.getString.map(_.some))
    val altImageOption = grvMap.getValueFromGrvMap(F.AltImage, None: Option[String], isOptional = true, failureHandler = handleFailedToGet(F.AltImage, _))(_.getString.map(_.some))
    val altImageSourceOption = grvMap.getValueFromGrvMap(F.AltImageSource, None: Option[String], isOptional = true, failureHandler = handleFailedToGet(F.AltImageSource, _))(_.getString.map(_.some))

    val trackingParams = grvMap.getValueFromGrvMap(F.TrackingParams, Map.empty[String, String], isOptional = true, failureHandler = handleFailedToGet(F.TrackingParams, _))(_.getString.map(jsonMapString => {
      Json.parse(jsonMapString).asOpt[Map[String, String]].getOrElse(Map.empty[String, String])
    }))

    for {
      _ <- failBuff.toNel match {
        case Some(fails) => fails.failure
        case None => ().successNel
      }

      siteGuid <- SiteService.sg(siteKey).toValidationNel {
        val fail = FailureResult(s"Failed to get SiteGuid for siteKey: $siteKey")
        handleFails(fail.message, fail.wrapNel)
        fail
      }

      contentGroupsToPinned <- getContentGroupsToPinned(siteGuid, articleKey)
    } yield {
      val dlArticle = AolGmsArticle(
        siteKey, contentGroupIds, contentGroupsToPinned, url, articleId, title, metricsWithMinutely.metrics, status.toString,
        lastGoLiveTime, submittedTime, submittedUserId, category, categoryLink, categoryIdOpt, categorySlugOpt, source,
        sourceLink, summary, headline, secondaryHeader, secondaryLinks, image, showVideoIcon, updatedTime, startDateOpt,
        endDateOpt, durationOpt, approvedRejectedUserIdOpt, aolRibbonImageUrl, aolImageSource, channelImage, channelImageSource,
        narrowBandImage, metricsWithMinutely.minutely, channelPageTitleOpt, highResImage, altTitleOption, altImageOption,
        altImageSourceOption, trackingParams, author
      )

      messageNote("Successfully built GMS Article from GrvMap: " + dlArticle)

      dlArticle
    }
  }

  // Clear all of siteGuid's GMS-managed campaigns from the given article, except for those associated with the cgIdStopList.
  def clearContentGroups(siteGuid: SiteGuid, articleKey: ArticleKey, userId: Long, cgIdStopList: Set[Long] = Set.empty): ValidationNel[FailureResult, ArticleKey] = withMaintenance {
    try {
      clearContentGroupsWithoutIndexUpdate(siteGuid, articleKey, userId, cgIdStopList)
    } finally {
      val sk = siteGuid.siteKey
      GmsArticleIndexer.updateGmsIndex(UniArticleId.forGms(sk, articleKey), sk) // In clearContentGroups
    }
  }

  def clearContentGroupsWithoutIndexUpdate(siteGuid: SiteGuid, articleKey: ArticleKey, userId: Long, cgIdStopList: Set[Long] = Set.empty): ValidationNel[FailureResult, ArticleKey] = withMaintenance {
    import com.gravity.interests.jobs.intelligence.operations.TableOperations.OpsResultMonoid

    val akc = implicitly[ByteConverter[ArticleKey]]

    for {
    // Get the (ContentGroup, CampaignKey) tuples for the campaigns from which the article should be removed.
      groupAndCkTuples <- getGmsContentGroups(siteGuid.some, skipCache = true).map(toContentGroupMapWithCampaignKeys(_).values.toList).map { list =>
        list.filterNot(tup => cgIdStopList.contains(tup._1.id))
      }

      cksToDelete = groupAndCkTuples.map(_._2).toSet

      // Delete all known GMS-managed campaigns from the ArticleRow's campaign tracking.
      artOpsResults <- try {
        // Make sure that we don't call delete with an empty campaignKeys set (would delete the whole article)
        if (cksToDelete.nonEmpty)
          Schema.Articles.delete(articleKey)
            .values(_.campaigns, cksToDelete)
            .values(_.campaignSettings, cksToDelete)
            .execute().successNel
        else
          mzero[OpsResult].successNel
      } catch {
        case ex: Exception => FailureResult("Failed to remove ALL contentGroups campaigns from article: " + articleKey, ex).failureNel
      }
    } yield {
      for ((contentGroup, campaignKey) <- groupAndCkTuples) {
        val result = for {
        // Get the CampaignRow, and return only the subject ArticleKey in CampaignRow.recentArticles, if present.
          row <- CampaignService.fetch(campaignKey)(_.withFamilies(_.recentArticles)
            .filter(_.and(_ => new ValueFilter(CompareOp.EQUAL, new BinaryComparator(akc.toBytes(articleKey))).some)))

          // If the ArticleKey exists in the campaign, then delete it.
          delete <- {
            val artKeySet = row.recentArticles.keySet

            // Make sure that we don't call delete with an empty artKeySet (would delete the whole article)
            if (artKeySet.nonEmpty)
              CampaignService.doModifyDelete(campaignKey)(_.values(_.recentArticles, artKeySet))
            else
              mzero[OpsResult].successNel
          }
        } yield delete

        result match {
          case Success(_) => // continue
          case Failure(NonEmptyList(RowNotFound(_, _))) => // nothing to delete
          case Failure(fails) => return FailureResult(s"Failed to remove $articleKey from GMS campaign: $campaignKey (contentGroup: $contentGroup). failure(s): " + fails.list.mkString(", ")).failureNel
        }
      }

      articleKey
    }
  }

  // The scoped metrics will be more detailed for the 'forContentGroup' content group.
  def buildMetrics(article: ArticleRowWithScopedGmsMetrics, siteContentGroupIds: Seq[Long], forContentGroup: Option[Long] = None): AolDynamicLeadMetricsWithMinutely = {
    val minutelyToBeGreaterThan = grvtime.currentMinute.minusMinutes(31)
    val minutelyMap = mutable.Map[DateMinute, ClicksImps]()

    def isLastThirtyMinutes(minute: DateMinute): Boolean = minute.isAfter(minutelyToBeGreaterThan)

    def shouldWeMinutely(contentGroup: Long): Boolean = forContentGroup match {
      case Some(onlyThisContentGroup) if onlyThisContentGroup == contentGroup => true
      case _ => false
    }

    def buildForContentGroup(buckets: ScopedMetricsBuckets, contentGroup: Long): AolDynamicLeadMetrics = {
      var (lifetimeClicks, thirtyMinuteClicks, lifetimeImps, thirtyMinuteImps) = (0L, 0L, 0L, 0L)

      val areWeUpdatingMinutely = shouldWeMinutely(contentGroup)

      for {
        (mk, value) <- buckets.minutely.map
        keyDateMinute = mk.dateMinute
        if isLastThirtyMinutes(keyDateMinute)
      } {
        mk.countBy match {
          case RecommendationMetricCountBy.click =>
            thirtyMinuteClicks += value
            if (areWeUpdatingMinutely) {
              val minutely = minutelyMap.getOrElseUpdate(keyDateMinute, ClicksImps.empty)
              minutelyMap.update(keyDateMinute, minutely.plusClicks(value))
            }

          case RecommendationMetricCountBy.unitImpressionViewed =>
            thirtyMinuteImps += value
            if (areWeUpdatingMinutely) {
              val minutely = minutelyMap.getOrElseUpdate(keyDateMinute, ClicksImps.empty)
              minutelyMap.update(keyDateMinute, minutely.plusImps(value))
            }

          //          case RecommendationMetricCountBy.unitImpression =>    // FAKE
          //            thirtyMinuteClicks += value / 2
          //            if (areWeUpdatingMinutely) {
          //              val minutely = minutelyMap.getOrElseUpdate(keyDateMinute, ClicksImps.empty)
          //              minutelyMap.update(keyDateMinute, minutely.plusClicks(value / 2))
          //            }
          //            thirtyMinuteImps += value
          //            if (areWeUpdatingMinutely) {
          //              val minutely = minutelyMap.getOrElseUpdate(keyDateMinute, ClicksImps.empty)
          //              minutelyMap.update(keyDateMinute, minutely.plusImps(value))
          //            }

          case _ => // ignore
        }
      }

      for ((mk, value) <- buckets.monthly.map) {
        mk.countBy match {
          case RecommendationMetricCountBy.click =>
            lifetimeClicks += value

          case RecommendationMetricCountBy.unitImpressionViewed =>
            lifetimeImps += value

          //          case RecommendationMetricCountBy.unitImpression =>    // FAKE
          //            lifetimeClicks += value / 2
          //            lifetimeImps += value

          case _ => // ignore
        }
      }

      AolDynamicLeadMetrics(MetricsBase(lifetimeClicks, lifetimeImps), MetricsBase(thirtyMinuteClicks, thirtyMinuteImps))
    }

    var totalMetrics = AolDynamicLeadMetrics.empty

    val contentGroupMetrics = siteContentGroupIds.map(contentGroup => {
      val metrics = article.metrics.get(contentGroup).map(buildForContentGroup(_, contentGroup)) match {
        case Some(m) => totalMetrics = totalMetrics + m; m
        case None => AolDynamicLeadMetrics.empty
      }
      contentGroup.toString -> metrics
    }).toMap

    AolDynamicLeadMetricsWithMinutely(contentGroupMetrics ++ Map("total" -> totalMetrics), minutelyMap.toMap)
  }

  def saveContentGroups(siteGuid: SiteGuid, contentGroupIds: NonEmptyList[Long], articleKey: ArticleKey, userId: Long, status: GmsArticleStatus.Type, trackingParamsOption: Option[Map[String, String]] = None): ValidationNel[FailureResult, Boolean] = {
    try {
      saveContentGroupsWithoutIndexUpdate(siteGuid, contentGroupIds, articleKey, userId, status, trackingParamsOption)
    } finally {
      val sk = siteGuid.siteKey
      GmsArticleIndexer.updateGmsIndex(UniArticleId.forGms(sk, articleKey), sk) // In saveContentGroups
    }
  }

  def saveContentGroupsWithoutIndexUpdate(siteGuid: SiteGuid, contentGroupIds: NonEmptyList[Long], articleKey: ArticleKey, userId: Long, status: GmsArticleStatus.Type, trackingParamsOption: Option[Map[String, String]] = None): ValidationNel[FailureResult, Boolean] = {
    val contentGroupIdSet = contentGroupIds.toSet
    val cas = trackingParamsOption match {
      case Some(trackingParams) => status.campaignArticleSettings.map(_.copy(trackingParams = trackingParams))
      case None => status.campaignArticleSettings
    }

    val successBuf = mutable.Buffer[Long]()
    val failBuff = mutable.Buffer[Long]()

    for {
      siteGmsGroups <- getGmsContentGroups(siteGuid.some, skipCache = true)

      siteGmsGroupsToSave = siteGmsGroups.filter(cg => contentGroupIdSet.contains(cg.id))

      groupAndCkTuplesToSave = toContentGroupMapWithCampaignKeys(siteGmsGroupsToSave).values.toList

      _ = groupAndCkTuplesToSave.foreach { case (contentGroup, campaignKey) =>
        CampaignService.addArticleKey(campaignKey, articleKey, userId, cas) match {
          case Success(_) => successBuf += contentGroup.id
          case Failure(_) => failBuff += contentGroup.id
        }
      }

      // Remove the article from any content groups that it should no longer be a member of.
      _ <- clearContentGroupsWithoutIndexUpdate(siteGuid, articleKey, userId, cgIdStopList = contentGroupIds.list.toSet)

      result <- successBuf.toList.toNel -> failBuff.toList.toNel match {
        case (Some(succeeded), Some(failed)) => PartialContentGroupSaveFailureResult(succeeded, failed).failureNel
        case (None, Some(failed)) => FailureResult("Failed to add article to all requested contentGroups: " + failed.list.mkString(", ") + "!").failureNel
        case (None, None) => false.successNel
        case (Some(_), None) => true.successNel
      }
    } yield {
      result
    }
  }

  def saveGmsArticle(url: String, fields: AolGmsModifyFields, userId: Long, isNewlySubmitted: Boolean = false, logElapsed: Boolean = false): Annotated[ValidationNel[FailureResult, AolGmsArticle]] = {
    try {
      val beginMs = System.currentTimeMillis
      val result  = saveGmsArticleWithoutIndexUpdate(url, fields, userId, isNewlySubmitted)
      val elapMs  = System.currentTimeMillis - beginMs

      if (logElapsed)
        info(s"Took ${elapMs}ms in saveGmsArticle(`$url`, userId=$userId)")

      result
    } finally {
      val sk = fields.siteKey
      GmsArticleIndexer.updateGmsIndex(UniArticleId.forGms(sk, ArticleKey(url)), sk) // In saveGmsArticle
    }
  }

  def saveGmsArticleWithoutIndexUpdate(url: String, fields: AolGmsModifyFields, userId: Long, isNewlySubmitted: Boolean = false): Annotated[ValidationNel[FailureResult, AolGmsArticle]] = {
    val now = grvtime.currentTime
    val publishDateOption = now.some
    val webUserId = userId.toInt

    val articleKey = ArticleKey(url)
    val siteKey = fields.siteKey
    val gmsRoute = GmsRoute(siteKey)

    val notes = mutable.Buffer[GmsNote]()

    notes += makeMetaNote("saveGmsArticle called with", GmsStageMeta(initialParams = SaveGmsArticleParams(url, fields, userId, isNewlySubmitted).some))

    def noteIt(note: GmsNote): Unit = {
      notes += note
    }

    def noteMessage(msg: String): Unit = {
      noteIt(makeNote(msg))
    }

    def noteFails(fails: NonEmptyList[FailureResult]): Unit = {
      noteIt(makeFailsNote(fails))
    }

    // GMS-FIELD-UPDATE location

    val result = for {
      _ <- if (fields.contentGroupIds.isEmpty) {
        val fail = noContentGroupsFailureResult
        noteFails(fail.wrapNel)
        fail.failureNel
      } else {
        unitSuccessNel
      }

      siteGuid <- SiteService.sg(siteKey).toValidationNel {
        val fail = FailureResult(s"Failed to get SiteGuid for siteKey: $siteKey")
        noteFails(fail.wrapNel)
        fail
      }

      previousDlArticle <- {
        getGmsArticleFromHbaseWithoutMetrics(gmsRoute, articleKey) match {
          case dlArticleSuccess@Success(_) =>
            if (isNewlySubmitted) {
              noteMessage(s"Duplicate article already exists for URL: `$url` (articleId: ${articleKey.articleId})!")
              return DuplicateArticleKeyFailureResult(articleKey, url).failNotes(notes)
            } else {
              noteMessage("Successfully retrieved article's previous state.")
            }
            dlArticleSuccess

          case failed@Failure(fails@NonEmptyList(RowNotFound(_, _))) =>
            if (isNewlySubmitted) {
              emptyArticleSuccess(siteKey)
            } else {
              noteMessage("Failed to get the previous state for article URL: " + url + " (articleId: " + articleKey.articleId + ")")
              noteFails(fails)
              failed
            }

          case failed@Failure(fails@NonEmptyList(NoGrvMapFailureResult(_, _, _))) =>
            if (isNewlySubmitted) {
              emptyArticleSuccess(siteKey)
            } else {
              noteMessage("Failed to get the previous state for article URL: " + url + " (articleId: " + articleKey.articleId + ")")
              noteFails(fails)
              failed
            }

          case failed@Failure(fails) =>
            noteMessage("Failed to get the previous state for article URL: " + url + " (articleId: " + articleKey.articleId + ")")
            noteFails(fails)
            failed
        }
      }

      previousGrvMap = previousDlArticle.toOneScopeGrvMap
      previousStatus = GmsArticleStatus.getOrDefault(previousDlArticle.dlArticleStatus)
      // If a status was sent in the modify fields, use it, otherwise, use the current (soon-to-be previous) GMS Article Status
      requestedOrCurrentStatus = fields.statusOption.getOrElse(previousStatus)
      // If a new status is identified (some rule was tripped), use it as it should override previous or requested, otherwise, use the requested/current
      effectiveStatus = findNewStatus(requestedOrCurrentStatus, fields.startDate, fields.endDate, fields.duration).getOrElse(requestedOrCurrentStatus)
      grvMapToStore = fields.toGrvMap(gmsRoute, webUserId, isNewlySubmitted, previousGrvMap, effectiveStatus, previousStatus, articleKey)
      article <- ArticleService.saveArticle(url, siteGuid.raw, IngestionTypes.fromAolPromotion, now, "Created by GMS user: " + userId, publishDateOption = publishDateOption)(
        spec => {
          spec
            .value(_.title, fields.title)
            .value(_.summary, fields.summary)
            .valueMap(_.allArtGrvMap, grvMapToStore)
        },
        articleQuery(gmsRoute.some)
      ) match {
        case articleSuccess@Success(_) =>
          noteMessage("ArticleService.saveArticle call succeeded! YAY!")
          articleSuccess
        case failed@Failure(fails) =>
          noteMessage("ArticleService.saveArticle FAILED!")
          noteFails(fails)
          failed
      }

      // Add the article to the campaign for any content groups that it is a member of.
      _ <- fields.contentGroupIds.toList.filter(_ > 0).toNel match {
        case None =>
          // Uh oh, an attempt to save an article without any content groups. Not gonna happen.
          val fails = FailureResult("Attempted to save article without any content groups.").wrapNel
          noteFails(fails)
          fails.failure

        case Some(contentGroups) =>
          noteMessage("Article is bound to " + contentGroups.len + " contentGroups.")
          saveContentGroupsWithoutIndexUpdate(siteGuid, contentGroups, article.articleKey, userId, effectiveStatus, fields.trackingParamsOption) match {
            case succeeded@Success(_) =>
              noteMessage("Successfully bound article to all " + contentGroups.len + " content groups.")
              succeeded
            case failed@Failure(fails) =>
              noteMessage("FAILED to bind article to all " + contentGroups.len + " content groups.")
              noteFails(fails)
              fails.head match {
                case partial: PartialContentGroupSaveFailureResult =>
                  noteMessage("Since some but not all content groups were successfully bound, we'll now update the `contentGroupIds` field in its GrvMap to only be those that bound.")
                  val oneMap = grvMapToStore(gmsRoute.oneScopeKey)
                  val correctedGrvMap = for ((key, value) <- oneMap) yield {
                    val correctValue = if (key == AolGmsFieldNames.ContentGroupIds) {
                      ArtGrvMapPublicMetaVal(partial.successfullySaved.list.mkString("[\"", "\",\"", "\"]"))
                    } else {
                      value
                    }
                    key -> correctValue
                  }
                  val correctedAllScopesMap: ArtGrvMap.AllScopesMap = Map(gmsRoute.oneScopeKey -> correctedGrvMap)
                  ArticleService.modifyPut(article.articleKey)(_.valueMap(_.allArtGrvMap, correctedAllScopesMap)) match {
                    case Success(_) =>
                      noteMessage("Successfully corrected the content groups we actually bound to.")
                    case Failure(moreFails) =>
                      noteMessage("FAILED to correct the content groups previously saved to only those successfully bound.")
                      noteFails(moreFails)
                  }

                case _ => // nothing to see here...
              }
              failed
          }
      }
      dlArticle <- tryBuildGmsArticleFromArtRowWithoutMetrics(article, gmsRoute, notes = notes)
    } yield {
      countPerSecond(counterCategory, "GMS Articles saved")
      val fromRow: Option[AolGmsArticle] = if (isNewlySubmitted) None else previousDlArticle.some
      AuditService.logAllChangesWithForSiteGuid(article.articleKey, userId, fromRow, dlArticle, Seq("AolGmsService.saveGmsArticle"), siteGuid.raw).valueOr {
        case fails: NonEmptyList[FailureResult] =>
          val msg = "The AolGmsService.saveGmsArticle completed successfully, but we failed to log changes to the Audit Log! URL: " + url
          noteMessage(msg)
          noteFails(fails)
          warn(fails, msg)
      }
      noteMessage("Successfully completed saveGmsArticle! YAYAYAYAYAY!")
      dlArticle
    }

    result.annotate(notes)
  }

  def emptyArticleSuccess(siteKey: SiteKey): ValidationNel[FailureResult, AolGmsArticle] = AolGmsArticle.empty(siteKey).successNel

  def updateDlArticleStatus(siteGuid: SiteGuid, ak: ArticleKey, status: GmsArticleStatus.Type, userId: Long, updateCampaignSettingsOnly: Boolean, updateGmsIndex: Boolean = true): ValidationNel[FailureResult, OpsResult] = {
    def articleStatusUpdateQueryCampaignOnly: ArticleService.QuerySpec                         = _.withFamilies(_.campaignSettings).withColumns(_.url)
    def articleStatusUpdateQuery(oneScopeKey: ArtGrvMap.OneScopeKey): ArticleService.QuerySpec = _.withFamilies(_.campaignSettings).withColumns(_.url).withColumn(_.allArtGrvMap, oneScopeKey)

    val gmsRoute = GmsRoute(siteGuid.siteKey)

    val previousDlArticleOption = if (updateCampaignSettingsOnly) None else getGmsArticleFromHbaseWithoutMetrics(gmsRoute, ak).toOption

    val query: ArticleService.QuerySpec = if (updateCampaignSettingsOnly)
      articleStatusUpdateQueryCampaignOnly
    else
      articleStatusUpdateQuery(gmsRoute.oneScopeKey)

    // In the only case where we will actually update the status, we will set this var to the existing GrvMap
    var previousGrvMap: Option[ArtGrvMap.OneScopeMap] = None

    // What are the GMS-managed campaigns for this Site?
    val siteGmsCks = getGmsContentGroups(siteGuid.some, skipCache = true).map(_.flatMap(toOptCampaignKey).toSet).valueOr { fails =>
      trace(s"**FAILED TO UPDATE** :: sg = ${siteGuid.raw} :: ak = ${ak}")
      return fails.failure
    }

    // We first need to check if this is actually a CampaignArticleSettings-visible change and:
    //   * If it IS, we need to call CampaignService.addArticleKey to update the CampaignArticleSettings for both the campaign and article
    //   * If it is NOT, then return a successful, empty, OpsResult
    val opsResultForCampaignSettings = ArticleService.fetch(ak)(query) match {
      case Success(article) =>
        var updates = 0
        status.campaignArticleSettings.foreach(newSettings => {

          for {
            (ck, existingSettings) <- article.campaignSettings // An existing campaign...
            if siteGmsCks contains ck // ...that is for one of the GMS-managed Content Groups...
            if !newSettings.equateActiveAndBlacklisted(existingSettings) // ...and which would be a CAS-visible change...
          } {
            trace(s"Attempting to update :: sg = ${siteGuid.raw} :: ak = ${ak} :: ck = ${ck} :: settings = ${newSettings}")
            CampaignService.addArticleKey(ck, ak, userId, existingSettings.copy(isBlacklisted = newSettings.isBlacklisted, status = newSettings.status).some) match {
              case Success(_) =>
                trace(s"Successfully updated :: sg = ${siteGuid.raw} :: ak = ${ak} :: ck = ${ck} :: settings = ${newSettings}")
                updates += 1

              case Failure(fails) =>
                trace(s"**FAILED TO UPDATE** :: sg = ${siteGuid.raw} :: ak = ${ak} :: ck = ${ck} :: settings = ${newSettings}")
                return fails.failure
            }
          }

        })

        if (!updateCampaignSettingsOnly)
          previousGrvMap = article.columnFromFamily(_.allArtGrvMap, gmsRoute.oneScopeKey)

        trace(s"Successfully updated ${updates} campaigns for sg = ${siteGuid.raw} :: ak = ${ak}")

        OpsResult(0, updates, 0).successNel[FailureResult]

      case Failure(fails) => return fails.failure
    }

    val werePinsCleared = if (!updateCampaignSettingsOnly &&
      (status == GmsArticleStatus.Deleted || status == GmsArticleStatus.Expired ||
        status == GmsArticleStatus.Rejected || status == GmsArticleStatus.Invalid)) {

      countPerSecond(counterCategory, "updateDlArticleStatus.deletePinningsForArticleKey.called")

      ScalaMagic.retryOnException(3, 50) {
        ConfigurationQueryService.queryRunner.deleteGmsPinningsForArticleKey(siteGuid, ak)
      } {
        case ex: Exception =>
          countPerSecond(counterCategory, "updateDlArticleStatus.deletePinningsForArticleKey.exceptions")
          FailureResult(s"Failed to unpin article (articleId: ${ak.articleId} ) that has been set to ${status.toString}", ex)
      } match {
        case Success(_) =>
          countPerSecond(counterCategory, "updateDlArticleStatus.deletePinningsForArticleKey.successes")
          true

        case Failure(fails) =>
          countPerSecond(counterCategory, "updateDlArticleStatus.deletePinningsForArticleKey.failures")
          val attempts = fails.len
          warn(fails, s"Tried (the maximum of) $attempts times, to unpin article (articleId: ${ak.articleId}) that had an updated status of: ${status.toString}.")
          false
      }
    }
    else {
      false
    }

    // now that campaign article settings are all wired up properly, check if GMS fields also need updating and do so if required
    if (updateCampaignSettingsOnly || previousDlArticleOption.exists(_.articleStatus == status)) {
      // previous status matches new status, therefore, nothing more to update
      trace("No change required to update in GrvMap")
      return opsResultForCampaignSettings
    }

    // If we've reached this point, then we've already successfully updated the campaign, and we do indeed have a new GMS status.
    // -- so... let's now attempt to update the article itself
    val updateResult = ArticleService.modifyPut(ak)(spec => {
      val webUserId = userId.toInt
      val statusUpdateMap = buildStatusUpdateGrvMap(status, webUserId, isNewlySubmitted = false, includeStatusInMap = true, startDate = previousDlArticleOption.flatMap(_.startDate))

      // If there was an existing GrvMap, we will overlay the status-only-changed GrvMap onto it so as not to lose previous data
      val mergedGrvMap = previousGrvMap.fold(statusUpdateMap)(_ ++ statusUpdateMap)

      // If channel pins were cleared, we need to exclude that data from the updatedMap
      val updatedMap = if (werePinsCleared) {
        mergedGrvMap.filterKeys(key => key != AolGmsFieldNames.ContentGroupsToPinned)
      }
      else {
        mergedGrvMap
      }

      spec.valueMap(_.allArtGrvMap, Map(gmsRoute.oneScopeKey -> updatedMap))
    })

    // Now that all updates are complete, we need to add an audit log for our changes
    // NOTE: There is a race condition here where a competing change may be attributed to this thread (because we re-read the "current" article for logging).
    getGmsArticleFromHbaseWithoutMetrics(gmsRoute, ak) match {
      case Success(updatedDlArticle) =>
        AuditService.logAllChangesWithForSiteGuid(ak, userId, previousDlArticleOption, updatedDlArticle, Seq("AolGmsService.updateDlArticleStatus"), siteGuid.raw).valueOr {
          case fails: NonEmptyList[FailureResult] => warn(fails, "Failed to log all changes from AolGmsService.updateDlArticleStatus!")
        }
      case Failure(fails) => warn(fails, "Failed to retrieve updated GMS Article for audit logging. No audits can be logged!")
    }
    val sk = siteGuid.siteKey
    GmsArticleIndexer.updateGmsIndex(UniArticleId.forGms(sk, ak), sk) // In updateDlArticleStatus

    // if the article update was successful, increment the numPuts to account for the campaign update
    updateResult.map(opsRes => opsRes.copy(numPuts = opsRes.numPuts + opsResultForCampaignSettings.toOption.fold(0)(_.numPuts)))
  }

  def checkIfRecommendableAndUpdateIfNeeded(siteGuid: SiteGuid, articleKey: ArticleKey, url: String, dlStatus: GmsArticleStatus.Type,
                                            startDateOpt: Option[DateTime], endDateOpt: Option[DateTime], durationOpt: Option[GrvDuration],
                                            executeUpdate: Boolean): ValidationNel[FailureResult, Boolean] = {
    def updateStatus(newStatus: GmsArticleStatus.Type): ValidationNel[FailureResult, Unit] = {
      if (executeUpdate) {
        updateDlArticleStatus(siteGuid, articleKey, newStatus, Settings2.INTEREST_SERVICE_USER_ID, updateCampaignSettingsOnly = false) match {
          case Success(_) =>
            // YAY
            trace("Successfully transitioned GMS Article Status (url: `" + url + "`) to `" + newStatus + "` from `" + dlStatus + "`!").successNel
          case Failure(fails) =>
            warn(fails, "Failed to transition GMS Article Status (url: `" + url + "`) to `" + newStatus + "` from `" + dlStatus + "`!")
            fails.failure
        }
      } else {}.successNel
    }

    val statusToCheckAgainst = findNewStatus(dlStatus, startDateOpt, endDateOpt, durationOpt) match {
      case Some(newStatus) =>
        updateStatus(newStatus).leftMap(fails => return fails.failure)
        newStatus

      case None => dlStatus
    }

    if (statusToCheckAgainst.isRecommendable) isRecommendable else isNotRecommendable
  }

  def synchronizeGmsArticleSettingsSafe(maxArticlesToProcess: Int = -1): Validation[FailureResult, String] = {
    tryToSuccess(
      synchronizeGmsArticleSettings(maxArticlesToProcess),
      ex => FailureResult("Exception thrown/caught during 'synchronizeGmsArticleSettings'!", ex)
    )
  }

  def synchronizeGmsArticleSettings(maxArticlesToProcess: Int = -1): String = {
    getAllGmsArticleKeys() match {
      case Success(sgAkPairs) =>
        val sgToAks = sgAkPairs.groupBy(_._1).mapValues(_.map(_._2))
        val sgNames = sgToAks.keys.map(sg => sg -> SiteService.siteNameByGuid(sg.raw)).toMap
        val numSites = sgNames.size

        info("Will now process articles for {0} total sites.", numSites)

        // Process site-by-site, sorted by site name
        val output = sgToAks.toSeq.sortBy(tup => sgNames(tup._1)).map { case (sg, aks) =>
          synchronizeGmsArticleSettings(sg, aks, maxArticlesToProcess)
        }.mkString("\n------------------------------------------------------------------------------------------\n\n")

        info("Completed processing articles for {0} total sites.", numSites)

        output

      case Failure(fails) =>
        val failMsg = "GMS Process failed to pull Article Keys for `synchronizeGmsArticleSettings`: " + fails.list.map(_.message).mkString(", ")
        warn(fails, failMsg)
        failMsg
    }
  }

  def synchronizeGmsArticleSettings(sg: SiteGuid, aks: Set[ArticleKey], maxArticlesToProcess: Int): String = {
    val articleIdsUpdated = new GrvConcurrentMap[Long, String]()

    val counterMap = new GrvConcurrentMap[String, AtomicInteger]()

    def myCounter2(label: String, amount: Int): Unit = {
      counterMap.getOrElseUpdate(label, new AtomicInteger(0)).getAndAdd(amount)
    }

    def myCounter(label: String): Unit = myCounter2(label, 1)

    val akList = aks.toList
    myCounter2("01. Article Keys Retrieved", akList.size)
    info("Retrieved {0} GMS Article Keys To Process for siteGuid: {1}", akList.size, sg.raw)
    val articleKeysToProcess = if (maxArticlesToProcess > 0) akList.take(maxArticlesToProcess) else akList

    val currentArticleIndex = new AtomicInteger(0)
    val totalArticlesProcessed = new AtomicInteger(0)
    val totalToProcess = articleKeysToProcess.size
    info("Will begin processing of {0} total GMS Article Keys...", totalToProcess)

    articleKeysToProcess.par.foreach { ak =>
      val gmsRoute = GmsRoute(sg.siteKey)

      val currentIndex = currentArticleIndex.incrementAndGet()
      myCounter("02. Articles Attempted")
      getGmsArticleFromHbaseWithoutMetrics(gmsRoute, ak) match {
        case Success(dl) =>
          myCounter("03. Articles Retrieved")

          val statusNumber = dl.articleStatus.id + 6 match {
            case lessThan10 if lessThan10 < 10 => "0" + lessThan10
            case other => other.toString
          }
          myCounter(s"$statusNumber. Articles with status = ${dl.articleStatus.name}")
          updateDlArticleStatus(sg, dl.articleKey, dl.articleStatus, 307L, updateCampaignSettingsOnly = true) match {
            case Success(opsResult) =>
              val pf = if (opsResult.numPuts > 0) "*" else "."
              trace(s"$pf Successfully processed article ($currentIndex of $totalToProcess): ${dl.articleId} `${dl.title}` with ${opsResult.numPuts} campaigns updated based on status: ${dl.dlArticleStatus}")
              myCounter("04. Articles Processed")
              myCounter2("05. Campaigns Updated", opsResult.numPuts)
              if (opsResult.numPuts > 0) articleIdsUpdated += dl.articleKey.articleId -> dl.dlArticleStatus
              totalArticlesProcessed.incrementAndGet()

            case Failure(fails) =>
              myCounter("!! Articles Failed !!")
              warn(fails, s"FAILED TO PROCESS GMS ARTICLE ($currentIndex of $totalToProcess): ${dl.articleId} `${dl.title}`!")
          }

        case Failure(fails) =>
          myCounter("!! getArticle FAILURES !!")
          warn(fails, s"FAILED TO GET GMS ARTICLE ($currentIndex of $totalToProcess): ${ak.articleId}!")
      }
    }

    info("Successfully processed {0} articles for siteGuid: {1}.", totalArticlesProcessed.get(), sg.raw)

    val resultBuilder = new StringBuilder

    val completedMap = counterMap.toMap.mapValues(_.get)

    val sgName = SiteService.siteNameByGuid(sg.raw)
    resultBuilder.append(s"Processed Site $sgName (${sg.raw}) with the following ${completedMap.size} counters:\n")
    completedMap.toSeq.sortBy(_._1).foreach {
      case (label: String, amount: Int) => resultBuilder.append(s"\t$label:\n\t\t=> $amount\n")
    }
    resultBuilder.append("\n")
    resultBuilder.append(s"There were a total of ${articleIdsUpdated.size} articles that were updated:\n")
    articleIdsUpdated.toSeq.sortBy(_._1).foreach{case (aid, status) => resultBuilder.append(s"\t=> $aid (status: $status)\n")}

    resultBuilder.toString()
  }

  def pinArticles(siteKey: SiteKey, contentGroupId: Long, pinnedArticleKeys: Map[Int, ArticleKey]): ValidationNel[FailureResult, SetAllContentGroupPinsResult] = {
    // First we must verify that no ArticleKey repeats
    val uniqueArticleKeys = mutable.Set[ArticleKey]()
    val failedToBeUniqueArticleKeys = for {
      ak <- pinnedArticleKeys.values
      if !uniqueArticleKeys.add(ak)
    } yield {
      FailureResult(s"An article may only be pinned to a single slot per content group. Content Group ID: $contentGroupId had articleId: ${ak.articleId} more than once.")
    }

    failedToBeUniqueArticleKeys.toNel.foreach {
      fails => return fails.failure
    }

    val result = try {
      ConfigurationQueryService.queryRunner.setAllContentGroupPins(contentGroupId, pinnedArticleKeys).successNel
    }
    catch {
      case ex: Exception => FailureResult("Failed to set pins for contentGroupId: " + contentGroupId, ex).failureNel
    }

    // Update the changed articles in the index.
    val uniArticleIds = uniqueArticleKeys.toSeq.map { ak => UniArticleId.forGms(siteKey, ak) }

    GmsArticleIndexer.updateGmsIndex(uniArticleIds, siteKey)

    result
  }

  def getPinnedArticleSlotsForContentGroups(contentGroupIds: Set[Long]): ValidationNel[FailureResult, Map[Long, List[GmsPinnedArticleRow]]] = {
    if (contentGroupIds.isEmpty) return Map.empty[Long, List[GmsPinnedArticleRow]].successNel

    // Filter out results for content groups that don't have any pins.
    GmsPinnedDataCache.getMulti(contentGroupIds).map(answers => answers.filter { case (keey, vaal) => vaal.nonEmpty })
  }

  def getPinnedArticleSlotsForContentGroupsFromDb(contentGroupIds: Set[Long]): ValidationNel[FailureResult, Map[Long, List[GmsPinnedArticleRow]]] = {
    if (contentGroupIds.isEmpty) return Map.empty[Long, List[GmsPinnedArticleRow]].successNel

    tryToSuccessNEL(
      ConfigurationQueryService.queryRunner.getPinnedRowsForContentGroups(contentGroupIds).groupBy(_.contentGroupId),
      ex => FailureResult("Failed to retrieve GmsPinnedArticleRows from configurationDb!", ex)
    )
  }

  def getPinnedDlArticleLitesForContentGroupId(siteGuid: SiteGuid, contentGroupId: Long): ValidationNel[FailureResult, Map[Int, DlArticleLite]] = {
    val gmsRoute = GmsRoute(siteGuid.siteKey)

    for {
      pinnedMap <- GmsService.getPinnedArticleSlotsForContentGroupsFromDb(Set(contentGroupId).toSet)
      pinnedRows = pinnedMap.values.flatten.toList
      articleKeys = pinnedRows.map(_.articleKey).toSet
      _ = if (articleKeys.isEmpty) {
        return Map.empty[Int, DlArticleLite].successNel
      } else {
        {}
      }
      articleRows <- ArticleService.fetchMulti(articleKeys)(_.withColumn(_.url).withColumn(_.allArtGrvMap, gmsRoute.oneScopeKey))
    } yield {
      (for {
        pr <- pinnedRows
        ar <- articleRows.get(pr.articleKey)
        dl <- tryBuildGmsArticleFromArtRowWithoutMetrics(ar, gmsRoute).toOption
      } yield {
        pr.slotPosition -> DlArticleLite(pr.articleId, dl.title, dl.url, grvtime.fromEpochSeconds(dl.publishOnDate), dl.sourceText)
      }).toMap
    }
  }

  // NOTE: This call takes about 60ms (1/17 second) if skipCache = true. Use skipCache=false where you can!
  def getGmsContentGroups(forSite: Option[SiteGuid], skipCache: Boolean): ValidationNel[FailureResult, Seq[ContentGroup]] = {
    tryToSuccessNEL(
      if (skipCache)
        ConfigurationQueryService.queryRunner.getGmsContentGroupsWithoutCaching(forSite).map(_.asContentGroup)
      else
        ConfigurationQueryService.queryRunner.getGmsContentGroups(forSite).map(_.asContentGroup),
      ex => FailureResult(s"Failed to getGmsContentGroups($forSite).", ex)
    )
  }

  def toOptCampaignKey(cg: ContentGroup): Option[CampaignKey] = {
    cg.sourceKey.tryToTypedKey[CampaignKey] match {
      case Success(ck) => Some(ck)

      case Failure(failed) =>
        warn("Failed to convert sourceKey to a CampaignKey for ContentGroup: id = {0} :: name = {1} :: sourceType = {2} :: sourceKey = {3} :: forSiteGuid = {4} ::: FailureResult: {5}",
          cg.id, cg.name, cg.sourceType.name, cg.sourceKey.keyString, cg.forSiteGuid, failed.messageWithExceptionInfo)
        None
    }
  }

  def toSiteGuidCampaignKeys(groups: Seq[ContentGroup]): Seq[(SiteGuid, CampaignKey)] = {
    groups.flatMap(cg => toOptCampaignKey(cg).map(ck => SiteGuid(cg.forSiteGuid) -> ck))
  }

  def toContentGroupMapWithCampaignKeys(groups: Seq[ContentGroup]): Map[Long, (ContentGroup, CampaignKey)] = {
    groups.flatMap(cg => toOptCampaignKey(cg).map(ck => cg.id -> (cg, ck))).toMap
  }

  def toContentGroupMapWithCampaigns(groups: Seq[ContentGroup])(query: CampaignService.QuerySpec = _.withColumns(_.siteGuid).withFamilies(_.recentArticles)) = {
    for {
      cgMapWithCampKeys <- toContentGroupMapWithCampaignKeys(groups).successNel

      campaignKeys = cgMapWithCampKeys.values.map(_._2).toSet

      ckToCampaign <- CampaignService.fetchMulti(campaignKeys)(query)
    } yield {
      for {
        (contentGroupId, (contentGroup, campKey)) <- cgMapWithCampKeys

        if ckToCampaign.contains(campKey)
        campRow = ckToCampaign(campKey)
      } yield {
        (contentGroupId, (contentGroup, campRow))
      }
    }
  }

  def uniqueSiteGuids(contentGroups: Seq[ContentGroup]): Set[SiteGuid] =
    contentGroups.map(cg => SiteGuid(cg.forSiteGuid)).toSet

  def getAllGmsArticlesFromHbase(withMetrics: Boolean = true, pullMetricsFromCacheOnly: Boolean = false): ValidationNel[FailureResult, Seq[(SiteGuid, AolGmsArticle)]] = {
    for {
      cgMap <- getAllGmsArticlesFromHbaseAsCgMap(withMetrics, pullMetricsFromCacheOnly)

      sgTuples = (for {
        (cg, arts) <- cgMap.toList
        art <- arts
      } yield {
        SiteGuid(cg.forSiteGuid) -> art
      }).distinctBy(tup => (tup._1, tup._2.articleId))
    } yield {
      sgTuples
    }
  }

  // Improvement: Review the choice of returned type here; maybe should be ValNel[FailureResult, Map[ContengtGroupId, (ContentGroup, CampRow, List[AolGmsArticles)]],
  // ..and a case class would probably be easier to deal with than a tuple.
  def getAllGmsArticlesForSiteFromHbase(siteGuid: SiteGuid, withMetrics: Boolean = true, pullMetricsFromCacheOnly: Boolean = false)(implicit conf: Configuration): ValidationNel[FailureResult, Map[ContentGroup, List[AolGmsArticle]]] = {
    val gmsRoute = GmsRoute(siteGuid.siteKey)

    for {
      groups <- getGmsContentGroups(siteGuid.some, skipCache = false)

      cgMapWithCampaigns <- toContentGroupMapWithCampaigns(groups)()

      allArticleKeys = cgMapWithCampaigns.values.map(_._2.recentArticleKeys).reduceOption(_ ++ _).getOrElse(Set.empty)

      // Written in a long chain like this so that we're not holding on to large old intermediate results throughout.
      resultMap <- ArticleService.batchFetchMulti(allArticleKeys, batchSize = batchSize) (
        _.withFamilies(_.meta).withColumn(_.allArtGrvMap, gmsRoute.oneScopeKey)).flatMap { artRowsByArtKey =>

        perhapsWithMetrics(siteGuid, artRowsByArtKey.values.toList, withMetrics = withMetrics, pullMetricsFromCacheOnly = pullMetricsFromCacheOnly, forContentGroup = None)
      }.map { artRowsWithMetrics =>
        artRowsWithMetrics.flatMap(arWithMetrics => tryBuildGmsArticleFromArtRow(arWithMetrics, gmsRoute, forContentGroup = None).toOption)
      }.map { dlArticles =>
        dlArticles.map(dlArticle => dlArticle.articleKey -> dlArticle).toMap
      }.map { dlArticlesByArtKey =>
        cgMapWithCampaigns.values.map { case (cg, campRow) => cg -> campRow.recentArticleKeys.toList.collect(dlArticlesByArtKey) }.toMap
      }
    } yield resultMap
  }

  def getAllGmsArticlesFromHbaseAsCgMap(withMetrics: Boolean = true, pullMetricsFromCacheOnly: Boolean = false): ValidationNel[FailureResult, Map[ContentGroup, List[AolGmsArticle]]] = {
    for {
      contentGroups <- getGmsContentGroups(None, skipCache = false)

      siteGuids = uniqueSiteGuids(contentGroups)

      _ = if (siteGuids.isEmpty) {
        return Map.empty[ContentGroup, List[AolGmsArticle]].successNel
      }

      // IMPROVE: Could make this fail-fast.
      cgArts <- siteGuids.toSeq.map { siteGuid =>
        // There are (probably) too many articles in Aol.com to use live metrics.  The other sites are still tiny.
        val usePullMetricsFromCacheOnly = pullMetricsFromCacheOnly || siteGuid == SiteGuid(AolMisc.aolSiteGuid)

        getAllGmsArticlesForSiteFromHbase(siteGuid, withMetrics = withMetrics, pullMetricsFromCacheOnly = usePullMetricsFromCacheOnly)
      }.extrude.map(_.reduce(_ |+| _))
    } yield {
      cgArts
    }
  }

  def getAllGmsArticlesFromHbaseAsSgMap(withMetrics: Boolean = true, pullMetricsFromCacheOnly: Boolean = false): ValidationNel[FailureResult, Map[SiteGuid, Seq[AolGmsArticle]]] = {
    for {
      sgTuples <- getAllGmsArticlesFromHbase(withMetrics, pullMetricsFromCacheOnly)

      sgMap = (for {
        (sg, art) <- sgTuples
      } yield {
        Map(sg -> Seq(art))
      }).reduceOption(_ |+| _).getOrElse(Map())
    } yield {
      sgMap
    }
  }

  def getManagedContentGroupsForSitePlacement(spId: SitePlacementId, skipCache: Boolean = true): ValidationNel[FailureResult, List[ContentGroupRow]] = {
    Try {
      // TODO-FIXGMS-1-HANDLE-DISABLED-SITE-PLACEMENTS
      // I'd use GravityStructure here, but this class, and some of its callers, are in [operations] project.
      val mapContentGroups = if (skipCache)
        ConfigurationQueryService.queryRunner.getContentGroupsForSitePlacementWithoutCaching(spId.raw)
      else
        ConfigurationQueryService.queryRunner.getContentGroupsForSitePlacement(spId.raw)

      mapContentGroups.values.filter(_.isGmsManaged).toList
    }.toValidationNel
  }

  def getManagedSitePlacementIds(siteGuid: SiteGuid,
                                 rowFilter: (SitePlacementRow => Boolean) = (_ => true),
                                 skipCache: Boolean = false
                                ): ValidationNel[FailureResult, Set[SitePlacementId]] = {
    for {
      map <- getManagedSitePlacementsWithContentGroups(siteGuid, skipCache = skipCache)
    } yield {
      (for {
        (spId, (spRow, cg)) <- map.toSeq
        if rowFilter(spRow)
      } yield {
        spId
      }).toSet
    }
  }

  def getManagedSitePlacementsWithContentGroups(siteGuid: SiteGuid, skipCache: Boolean = true): ValidationNel[FailureResult, Map[SitePlacementId, (SitePlacementRow, List[ContentGroup])]] = {
    val result = Try {
      // TODO-FIXGMS-1-HANDLE-DISABLED-SITE-PLACEMENTS
      val sitePlacementRows = if (skipCache)
        ConfigurationQueryService.queryRunner.getSitePlacementsForSiteWithoutCaching(siteGuid.raw)
      else
        ConfigurationQueryService.queryRunner.getSitePlacementsForSite(siteGuid.raw)

      val sitePlacementRowsById = sitePlacementRows.mapBy(spRow => spRow.id)

      val spIds = sitePlacementRowsById.keySet

      // TODO-FIXGMS-1-HANDLE-DISABLED-SITE-PLACEMENTS
      // Map[SitePlacementId, List[ContentGroupRow]]
      val contentGroupsBySpId = if (skipCache)
        ConfigurationQueryService.queryRunner.getContentGroupsForSitePlacementsWithoutCaching(spIds)
      else
        ConfigurationQueryService.queryRunner.getContentGroupsForSitePlacements(spIds)

      (for {
        (spId, cgRows) <- contentGroupsBySpId

        sitePlacement = sitePlacementRowsById(spId)
        contentGroups = cgRows.filter(_.isGmsManaged).map(_.asContentGroup).sortBy(_.id).toList

        if contentGroups.nonEmpty
      } yield {
        SitePlacementId(spId) -> (sitePlacement, contentGroups)
      }).toMap
    }.toValidationNel

    result
  }

  def isGmsManagedSitePlacement(spId: SitePlacementId, skipCache: Boolean = false): Boolean = {
    // TODO-FIXGMS-1-HANDLE-DISABLED-SITE-PLACEMENTS
    if(skipCache)
      ConfigurationQueryService.queryRunner.getContentGroupsForSitePlacementWithoutCaching(spId.raw)
    else
      ConfigurationQueryService.queryRunner.getContentGroupsForSitePlacement(spId.raw)
  } exists(_._2.isGmsManaged)

  def gmsOrDlugGrvMap(allGrvMap: ArtGrvMap.AllScopesMap, gmsRoute: GmsRoute): Option[ArtGrvMap.OneScopeMap] =
    allGrvMap.get(gmsRoute.oneScopeKey)
}

object GmsPinnedDataCache extends LruRefreshedCache[Long, List[GmsPinnedArticleRow]] {
  val name: String = "GMS Pinned Data"
  import com.gravity.utilities.Counters._

  val maxItems = if (grvroles.isInRole(grvroles.STATIC_WIDGETS)) 1000 else 100

  setAverageCount(counterCategory, "Capacity", maxItems)

  val refreshIntervalSeconds = if (grvroles.isInRole(grvroles.STATIC_WIDGETS)) 5 else 60
  val refreshBatchSize = 50

  def getFromOrigin(keys: Set[Long]): ValidationNel[FailureResult, Map[Long, List[GmsPinnedArticleRow]]] = {
    GmsService.getPinnedArticleSlotsForContentGroupsFromDb(keys).map { answers =>
      // getPinnedArticleSlotsForContentGroupsFromDb only returns key-value pairs of the value is non-empty,
      // so fill in the missing key-value pairs with empties.
      keys.toSeq.map { key =>
        key -> answers.getOrElse(key, Nil)
      }.toMap
    }
  }
}