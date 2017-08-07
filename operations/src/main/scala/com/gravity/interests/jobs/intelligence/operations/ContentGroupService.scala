package com.gravity.interests.jobs.intelligence.operations

import com.gravity.domain.articles.ContentGroupSourceTypes
import com.gravity.domain.recommendations.ContentGroup
import com.gravity.interests.jobs.intelligence._
import com.gravity.interests.jobs.intelligence.algorithms.CandidateKey
import com.gravity.interests.jobs.intelligence.hbase.ScopedKey
import com.gravity.utilities.cache.PermaCacher
import com.gravity.utilities.components.FailureResult
import com.gravity.utilities.grvtime._
import com.gravity.utilities.grvz._
import com.gravity.utilities.time.DateHour
import com.gravity.utilities.{NotContentGroupAwareException, grvmath}
import com.gravity.valueclasses.ValueClassesForDomain._

import scala.collection.immutable
import scalaz.Scalaz._
import scalaz._

/** Created by IntelliJ IDEA.
  * Author: Robbie Coleman
  * Date: 10/9/13
  * Time: 11:58 AM
  */
object ContentGroupService {
 import com.gravity.logging.Logging._
  import com.gravity.utilities.Counters._
  val counterCategory = "Content Group Service"
  def permaCached[T <: AnyRef](itemName: String, maxArticles: MaxCandidateSize, maxDaysOld: MaxDaysOld, requestedSection: Option[SectionPath], cacheCandidatesForMinutes: Int)(work: => Option[T]): ValidationNel[FailureResult, T] = {
    // skip PermaCacher if minutes == 0
    if (cacheCandidatesForMinutes <= 0) work.toValidationNel(FailureResult(s"Unable to execute work (item: $itemName)"))
    else {
      trace("About to permacache\nItemName:{0}", itemName)
      val cacheName = itemName + "_" + maxArticles + "_" + maxDaysOld + "_" + requestedSection
      PermaCacher.getOrRegisterFactoryWithOption(cacheName, cacheCandidatesForMinutes * 60)(work).toValidationNel(FailureResult("Unable to retrieve cached item " + cacheName))
    }
  }

  /**
    * Find the lite version of the metrics for Campaign content groups only. No more sponsored content groups, they
    * will be filtered out.
    *
    * @param contentGroups A list of content groups, should be organic/campaign type. All others will be filtered out.
    * @return Map of Content Group ID to metrics
    */
  def metricsLite(contentGroups: Seq[ContentGroup]): ValidationNel[FailureResult, Map[Long, ContentGroupMetricsLite]] = {
    val campaignKeys = contentGroups.filter(_.sourceType == ContentGroupSourceTypes.campaign).map(
      cg => cg.sourceType match {
        case ContentGroupSourceTypes.campaign =>
          Seq(cg.sourceKey.typedKey[CampaignKey]).successNel

        case _ =>
          FailureResult("Only campaign content group source types are supported.").failureNel
      }
    ).extrude.map(_.flatten).valueOr(f => return f.failure).toSet

    // Fetch campaigns
    val campaignsByKey = CampaignService.fetchMulti(campaignKeys, skipCache = false)(
                             _.withFamilies(_.trackingParams)
                               .withColumns(_.useCachedImages, _.thumbyMode, _.status)
                           ).valueOr(f => return f.failure)

    // Aggregate the campaign fields by content group
    (for {
      cg <- contentGroups

      metricsLite = cg.sourceType match {
        case ContentGroupSourceTypes.campaign =>
          val ck = cg.sourceKey.typedKey[CampaignKey]
          campaignsByKey.get(ck).fold(ContentGroupMetricsLite.empty)(cr => ContentGroupMetricsLite.build(cr))

        case _ => ContentGroupMetricsLite.empty
      }
    } yield cg.id -> metricsLite).toMap.successNel
  }

  /** @return Success map is content group ID => metrics. */
  def metricsForContentGroups(contentGroups: Set[ContentGroup], includeOrganic: Boolean = true,
                              includeSponsored: Boolean = true): ValidationNel[FailureResult, immutable.Map[Long, ContentGroupWithMetrics]] = {
    if(!includeOrganic && !includeSponsored)
      return Map.empty[Long, ContentGroupWithMetrics].successNel

    // Separate content groups into "by campaign" and "by advertiser"
    val (campaignCgs, advertiserCgs) = {
      val cgsBySourceType = contentGroups.groupBy(_.sourceType)

      // Contains content groups with source types other than campaign or advertiser
      if(cgsBySourceType.keySet.exists(sourceType => sourceType != ContentGroupSourceTypes.campaign && sourceType != ContentGroupSourceTypes.advertiser))
        return FailureResult("Only campaign and advertiser content group source types are supported.").failureNel

      (
        cgsBySourceType.getOrElse(ContentGroupSourceTypes.campaign, Set()),

        // If sponsored isn't being included, we can simply ignore advertiser type content group, which are always sponsored
        if(includeSponsored)
          cgsBySourceType.getOrElse(ContentGroupSourceTypes.advertiser, Set())
        else
          Set.empty[ContentGroup]
      )
    }

    // Collect site keys from advertisers
    val advertiserSks = for {
      advertiserCg <- advertiserCgs
      siteKey = advertiserCg.sourceKey.tryToTypedKey[SiteKey].valueOr(f => return f.failureNel)
    } yield siteKey

    // Collect campaign keys by advertiser site key
    val advertiserSkToAllCks = {
      if(advertiserCgs.isEmpty)
        Map.empty[ScopedKey, Set[CampaignKey]]
      else
        (for {
          (sk, sr) <- SiteService.fetchMulti(advertiserSks, skipCache = false)(_.withFamilies(_.campaigns)).valueOr(f => return f.failure)
        } yield sk.toScopedKey -> sr.campaignKeys).toMap
    }

    // Collect campaign keys from campaign content groups
    val campaignCgCks = for {
      campaignCg <- campaignCgs
      campaignKey = campaignCg.sourceKey.tryToTypedKey[CampaignKey].valueOr(f => return f.failureNel)
    } yield campaignKey

    // Fetch campaigns with data we need for content group metrics
    val advertiserCks = advertiserSkToAllCks.values.flatten.toSet
    val allCks = advertiserCks ++ campaignCgCks
    val sevenDaysAgoKey = SponsoredMetricsKey.partialByStartDate(DateHour.currentHour.minusDays(7))
    val (ckToCgMetrics, organicCks) = (for {
      campaignsWithMetrics <- CampaignService.fetchMulti(allCks, skipCache = false)(
                                _.withColumns(_.status, _.campaignType)
                                  .withFamilies(_.feeds, _.meta, _.sponsoredDailyMetrics, _.recentArticles)
                                  .filter(_.or(
                                    _.greaterThanColumnKey(_.sponsoredDailyMetrics, sevenDaysAgoKey),
                                    _.allInFamilies(_.meta, _.feeds, _.recentArticles),
                                    _.withPaginationForFamily(_.recentArticles, 1, 0)
                                  )))

      organicCks = campaignsWithMetrics.values.filter(_.isOrganic).map(_.campaignKey).toSet

      // Filter to organic or sponsored if needed
      filteredCampaignsWithMetrics =
        if(includeOrganic && !includeSponsored)
          campaignsWithMetrics.filterKeys(organicCks.contains)
        else if(!includeOrganic && includeSponsored)
          campaignsWithMetrics.filterKeys(ck => !organicCks.contains(ck))
        else
          campaignsWithMetrics

      ckToCgMetrics = filteredCampaignsWithMetrics.map({ case (ck, cr) => (ck.toScopedKey, ContentGroupMetrics.toMetrics(cr)) })
    } yield (ckToCgMetrics, organicCks)).valueOr(f => return f.failure)

    // Now that we know which campaigns are organic, we need to ensure that advertiser content groups only reference
    // sponsored campaigns
    val advertiserSkToCks = advertiserSkToAllCks.mapValues(_.filterNot(organicCks.contains))

    // Collect metrics by content group ID from both campaign content groups and advertiser content groups
    val cgIdToMetrics = {
      implicit val campaignIsOrganicChecker = organicCks.contains _

      campaignCgs.map(
        cg => (
          cg.id,
          ContentGroupWithMetrics(cg.id, cg.name, cg.isOrganic, ckToCgMetrics.getOrElse(cg.sourceKey, ContentGroupMetrics.empty))
        )
      ) ++ advertiserCgs.map(
        cg => {
          val cksForAdvertiser = advertiserSkToCks.getOrElse(cg.sourceKey, Set.empty[CampaignKey]).map(_.toScopedKey)

          (
            cg.id,
            ContentGroupWithMetrics(cg.id, cg.name, cg.isOrganic, cksForAdvertiser.toSeq.collect(ckToCgMetrics).fold(ContentGroupMetrics.empty)(_ + _))
          )
        }
      )
    }.toMap

    // Finally, filter as needed
    (
      if(includeOrganic && !includeSponsored)
        cgIdToMetrics.filter(_._2.isOrganic)
      else if(!includeOrganic && includeSponsored)
        cgIdToMetrics.filter(!_._2.isOrganic)
      else
        cgIdToMetrics
    ).successNel
  }

  def siteKeyForContentGroup(group: ContentGroup): Option[SiteKey] = {

    group.sourceType match {

      case ContentGroupSourceTypes.notUsed => None

      // single campaign articles
      case ContentGroupSourceTypes.campaign => group.sourceKey.tryToTypedKey[CampaignKey].toOption.map(_.siteKey)

      // single site recent articles
      case ContentGroupSourceTypes.siteRecent => group.sourceKey.tryToTypedKey[SiteKey].toOption

      case ContentGroupSourceTypes.singlePoolSite => group.sourceKey.tryToTypedKey[SiteKey].toOption

      // single advertiser site's active campaigns' active articles
      case ContentGroupSourceTypes.advertiser => group.sourceKey.tryToTypedKey[SiteKey].toOption

      // single exchanges' content groups' active articles
      case ContentGroupSourceTypes.exchange => None
    }
  }

  def fetchArticleKeys(siteGuid: SiteGuid, groups: NonEmptyList[ContentGroup], maxCandidateSize: MaxCandidateSize, maxDaysOld: MaxDaysOld, requestedSection: Option[SectionPath], cacheCandidatesForMinutes: Int): ValidationNel[FailureResult, Seq[CandidateKey]] = {

    val dateCutoff = currentTime.withTimeAtStartOfDay().minusDays(maxDaysOld.raw)

    if (groups.len == 1 && groups.head.sourceType == ContentGroupSourceTypes.notUsed) {
      trace("Detected 'No Content Groups Used' case")
      return NotContentGroupsUsedFailureResult.asNel
    }

    trace("Detected Content Groups are in use")

    val results = for (group <- groups.list) yield {
      val groupKeyString = group.sourceKey.keyString
      val permaCacheKey = "contentGroup:" + group.sourceType + ":" + groupKeyString

      val groupResults = group.sourceType match {
        // just do what we used to
        case ContentGroupSourceTypes.notUsed => NotContentGroupsUsedFailureResult.asNel

        // single campaign articles
        case ContentGroupSourceTypes.campaign => {
          for {
            campaignKey <- group.sourceKey.tryToTypedKey[CampaignKey].liftFailNel
            campaign <- permaCached(permaCacheKey, maxCandidateSize, maxDaysOld, requestedSection, cacheCandidatesForMinutes + grvmath.rand.nextInt(5)) {
              CampaignService.fetch(campaignKey, skipCache = false)(_.withFamilies(_.meta, _.recentArticles).filter(
                _.or(
                  _.withPaginationForFamily(_.recentArticles, maxCandidateSize.raw, 0),
                  _.allInFamilies(_.meta)
                ),
                _.or(
                  _.lessThanColumnKey(_.recentArticles, PublishDateAndArticleKey.partialByEndDate(dateCutoff)),
                  _.allInFamilies(_.meta)
                )
              )).toOption
            }
            _ <- if (campaign.isActive) true.successNel else InactiveCampaignFailureResult("Content Group: `" + group.name + "` is not active.").failureNel
            articleKeySet <- campaign.activeRecentArticles().successNel
          } yield articleKeySet.map(ak => CandidateKey(ak, campaign.campaignKey, Some(group)))
        }

        // single site recent articles
        case ContentGroupSourceTypes.siteRecent => {
//
//          for {
//            siteKey <- group.sourceKey.tryToTypedKey[SiteKey].liftFailNel
//            site <- permaCached(permaCacheKey, maxArticles, maxDaysOld, requestedSection, cacheCandidatesForMinutes) {
//              SiteService.fetch(siteKey, skipCache = false)(_.withFamilies(_.meta, _.recentArticles).filter(
//                _.or(
//                  _.allInFamilies(_.meta),
//                  _.lessThanColumnKey(_.recentArticles,
//                    PublishDateAndArticleKey.partialByStartDate(oldestAllowed(maxDaysOld).minusMinutes(1)))
//                ),
//                _.or(
//                  _.withPaginationForFamily(_.recentArticles, maxArticles.raw, 0),
//                  _.allInFamilies(_.meta)
//                )
//              )).toOption
//            }
//            articleKeys <- Option(site.family(_.recentArticles).values) filter (_.nonEmpty) toValidationNel FailureResult("Content Group: `" + group.name + "` has no recent articles")
//          } yield articleKeys.toSet[ArticleKey].map(ak => CandidateKey(ak, campaign.campaignKey, Some(group)))
          throw new NotContentGroupAwareException("ContentGroupService.fetchCandidates")
        }

        // single advertiser site's active campaigns' active articles
        case ContentGroupSourceTypes.advertiser => {
          for {
            siteKey <- group.sourceKey.tryToTypedKey[SiteKey].liftFailNel
            site <- SiteService.fetch(siteKey, skipCache = false)(_.withFamilies(_.meta, _.campaigns))
            activeCampaignKeys <- permaCached(permaCacheKey, maxCandidateSize, maxDaysOld, requestedSection, cacheCandidatesForMinutes) {
              for {
                campaignKeys <- {
                  val acks = (for {
                    (ck, cs) <- site.campaigns
                    if cs == CampaignStatus.active
                  } yield ck).toSet

                  if (acks.isEmpty) None else acks.some
                }
              } yield campaignKeys
            }
            _ <- if (activeCampaignKeys.nonEmpty) true.successNel else FailureResult("Content Group: `" + group.name + "` has no active campaigns for advertiser: `" + site.nameOrNoName + "`").failureNel
            campaigns <- {
              countPerSecond(counterCategory, "SponsoredProvider hit multi fetch")
              CampaignService.fetchMulti(activeCampaignKeys, skipCache = false, ttl = 12 * 60)(_.withFamilies(_.recentArticles, _.meta).filter(
                _.or(
                  _.withPaginationForFamily(_.recentArticles, maxCandidateSize.raw, 0),
                  _.allInFamilies(_.meta)
                ),
                _.or(
                  _.lessThanColumnKey(_.recentArticles, PublishDateAndArticleKey.partialByEndDate(dateCutoff)),
                  _.allInFamilies(_.meta)
                )
              ))
            }
            result <- {
              val aktcm = for {
                (ck, cr) <- campaigns
                if cr.isActive
                if cr.isSponsored // since this sourceType is 'advertiser', we only want sponsored
                ak <- cr.activeRecentArticles()
              } yield CandidateKey(ak, cr.campaignKey, Some(group))

              aktcm.successNel[FailureResult]
            }
          } yield result
        }

        // single exchanges' content groups' active articles
        case ContentGroupSourceTypes.exchange => {
          for {
            exchangeKey <- group.sourceKey.tryToTypedKey[ExchangeKey].liftFailNel
            exchange <- ExchangeService.exchangeMeta(exchangeKey).toValidationNel(FailureResult("Content Group: `" + group.name + "` not found"))
            _ <- if (exchange.exchangeStatus == ExchangeStatus.active) true.successNel else InactiveExchangeFailureResult("Content Group: `" + group.name + "` is not active.").failureNel
            //TODO-JP: eventually, we want to add an ExchangeSite table and probably just grab the accepted content groups from there...
            contentGroups <- exchange.permaCachedContentGroups("Content Group: `" + group.name + "` ")
            contentGroupsNotForThisSite = contentGroups.list.filter(ContentGroupService.siteKeyForContentGroup(_).exists(_ != siteGuid.siteKey))
            _ <- if(contentGroupsNotForThisSite.nonEmpty) true.successNel else FailureResult("Content Group: `" + group.name + "` has no active content sources for site: `" + siteGuid.raw + "`").failureNel
            candidateKeys <- fetchArticleKeys(siteGuid, toNonEmptyList(contentGroupsNotForThisSite), maxCandidateSize, maxDaysOld, requestedSection, cacheCandidatesForMinutes)
          } yield candidateKeys.map(_.copy(exchangeGuid = Option(exchange.exchangeGuid))) //leave contentGroup as the source contentGroup
        }

        // NO CANDIDATES FOR YOU
        case notSupported => {
          warn("Unsupported source type: {0} requested for scopedKey: {1}", notSupported, groupKeyString)
          FailureResult(notSupported.toString + " is not supported!").failureNel
        }
      }

      setAverageCount(counterCategory, s"Group size/$maxCandidateSize - $groupKeyString", groupResults.map(_.size.toLong).getOrElse(0))

      groupResults
    }

    // split up the good from the bad
    val (successes, failures) = results.partitionValidation
    val succeededResults = successes.flatten

    if (succeededResults.isEmpty && failures.nonEmpty) {
      // Nothing good and all bad... FAIL
      nel(failures.head, failures.tail: _*).failure
    } else {
      succeededResults.successNel
    }
  }
}

case class InactiveCampaignFailureResult(msg: String) extends FailureResult("Inactive campaign. " + msg, None)

case class InactiveExchangeFailureResult(msg: String) extends FailureResult("Inactive exchange. " + msg, None)

case object NotContentGroupsUsedFailureResult extends FailureResult("The specified `groups` will not return results.", None) {
  val asNel = this.failureNel
}
