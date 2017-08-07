package com.gravity.interests.jobs.intelligence.operations.sponsored

import com.gravity.interests.jobs.intelligence._
import com.gravity.interests.jobs.intelligence.hbase.CanBeScopedKey
import com.gravity.interests.jobs.intelligence.operations._
import com.gravity.interests.jobs.intelligence.schemas.DollarValue
import com.gravity.utilities.cache.PermaCacher
import com.gravity.utilities.components.FailureResult
import com.gravity.utilities.grvstrings._
import com.gravity.utilities.grvz._
import com.gravity.utilities.{grvmath, HashUtils, ScalaMagic}
import com.gravity.valueclasses.ValueClassesForDomain.SitePlacementId

import scala.collection._
import scalaz.Scalaz._
import scalaz._

/*             )\._.,--....,'``.
.b--.        /;   _.. \   _\  (`._ ,.
`=,-,-'~~~   `----(,_..'--(,_..'`-.;.'  */

case class SponsoredArticle(key: SponsoredArticleKey, data: SponsoredArticleData, article: ArticleRow)

case class SponsoredCampaignArticle(articleKey: ArticleKey, campaignKey: CampaignKey, cpc: DollarValue, articleRow: ArticleRow)

trait SponsoredStoryManager {
 import com.gravity.logging.Logging._

  val poolKeysCacheKey = "SponsoredStoryManager.fetchPoolKeys"

  def fetchPoolKeys: Option[Set[SiteKey]] = PermaCacher.getOrRegisterFactoryWithOption(poolKeysCacheKey, 5 * 60) {
    SiteService.filteredScan()(_.withColumns(_.isSponsoredArticlesPool).filter(
      _.and(
        _.columnValueMustEqual(_.isSponsoredArticlesPool, true)
      )
    )) match {
      case Success(poolSitesMap) => poolSitesMap.keySet.some
      case Failure(fails) => {
        println("Failed to get PermaCached pool keys due to the failure(s) output below!")
        fails.println
        None
      }
    }
  }


  def fetchPools(skipCache: Boolean = true)(selectColumnsAndFamilies: SiteService.QuerySpec): ValidationNel[FailureResult, Map[SiteKey, SiteRow]] = {
    fetchPoolKeys match {
      case Some(poolKeys) => SiteService.fetchMulti(poolKeys, skipCache = skipCache)(selectColumnsAndFamilies)
      case None => {
        println("Failed to get PermaCached pool keys, so we're falling back to a filtered scan!")
        SiteService.filteredScan(skipCache, maxRows = 0) {
          query => {
            selectColumnsAndFamilies(query).withColumns(_.isSponsoredArticlesPool).filter(
              _.and(
                _.columnValueMustEqual(_.isSponsoredArticlesPool, true)
              )
            )
          }
        }
      }
    }

  }

  def fetchActiveSponsoredCampaignsForPool(pool: SiteKey): ValidationNel[FailureResult, Map[CampaignKey, CampaignRow]] = {
    PermaCacher.getOrRegister("fetchActiveSponsoredCampaignsForPool_" + pool, 5 * 60, mayBeEvicted = false) {
      val campaigns = fetchCampaignsForPools(Set(pool))(_.withFamilies(_.meta, _.recentArticles))
        .fold(fail => Map.empty[CampaignKey, CampaignRow], result => result.filter{
        case (_, campRow) => campRow.isActive && campRow.isSponsored
      }) // Currently return empty result on fail.  We may need to change this

      trace(s"fetched ${campaigns.size} campaigns for pool: $pool")

      campaigns.successNel[FailureResult]
    }
  }

  def fetchCampaignsForPools(pools: Set[SiteKey], skipCache:Boolean=false)(query: CampaignService.QuerySpec) = {
    for { sponsors <- SiteService.fetchMulti(pools, skipCache = skipCache, returnEmptyResults = true)(_.withColumn(_.sponsorsInPool)).map(_.values.map(_.sponsorsInPool).flatten)
          campaignKeys <- {
            // for all sites in pool, grab the campaigns for each site...
            val siteCampaigns = SiteService.fetchMulti(sponsors.collect { case sk: SiteKey => sk }.toSet, skipCache = skipCache, returnEmptyResults = true)(_.withFamilies(_.campaigns))
              .fold(fails => Set.empty, _.map(_._2.campaigns.keySet).flatten.toSet)  // on fail return empty set

            // for all campaigns in pool, just return the campaigns
            val campaigns = sponsors.collect{ case ck: CampaignKey => ck}.toSet

            // append the pool's site campaigns and campaigns
            (siteCampaigns ++ campaigns).successNel[FailureResult]
          }
          // fetch the campaigns using the specified query
          campaigns <- CampaignService.fetchMulti(campaignKeys, skipCache=skipCache, returnEmptyResults = true)(query)
    } yield campaigns
  }

  def fetchCampaignsForSponsee(key: CanBeScopedKey, skipCache:Boolean=true)(query: CampaignService.QuerySpec): ValidationNel[FailureResult, Map[CampaignKey, CampaignRow]] = {
    val accessor = (k: CanBeScopedKey) => {
      k match {
        case sk:SiteKey => SiteService.fetchOrEmptyRow(sk, skipCache = skipCache)(_.withColumn(_.poolsSponseeIn)).map(_.poolsSponseeIn)
        case sp:SitePlacementKey => SitePlacementService.fetchOrEmptyRow(sp, skipCache = skipCache)(_.withColumn(_.poolsSponseeIn)).map(_.poolsSponseeIn)
        case _ => FailureResult(s"Unrecognized sponsee key: $key").failureNel
      }
    }

    for {
      pools <- accessor(key)
      sponsors <- SiteService.fetchMulti(pools, skipCache = skipCache, returnEmptyResults = true)(_.withColumn(_.sponsorsInPool)).map(_.values.map(_.sponsorsInPool).flatten)
      campaignKeys <- {
        // for all sites in pool, grab the campaigns for each site...
        val siteCampaigns = SiteService.fetchMulti(sponsors.collect { case sk: SiteKey => sk }.toSet, skipCache = skipCache, returnEmptyResults = true)(_.withFamilies(_.campaigns))
          .fold(fails => Set.empty, _.map(_._2.campaigns.keySet).flatten.toSet)  // on fail return empty set

        // for all campaigns in pool, just return the campaigns
        val campaigns = sponsors.collect{ case ck: CampaignKey => ck}.toSet

        // append the pool's site campaigns and campaigns
        (siteCampaigns ++ campaigns).successNel[FailureResult]
      }
      // fetch the campaigns using the specified query
      campaigns <- CampaignService.fetchMulti(campaignKeys, skipCache=skipCache, returnEmptyResults = true)(query)
    } yield campaigns
  }

  def fetchActiveSponsoredCampaignsForSiteAndPlacement(siteKey: SiteKey, sitePlacementId: Option[SitePlacementId]) = {
    for {
      campaignsForSite <- fetchActiveSponsoredCampaignsForSponsee(siteKey) //Get all campaigns for this site
      campaignsForSitePlacement <- sitePlacementId.map(sp => fetchActiveSponsoredCampaignsForSponsee(SitePlacementKey(sp.raw, siteKey.siteId))).getOrElse(Map.empty.successNel) //Get all campaigns for this site placement
    } yield campaignsForSite ++ campaignsForSitePlacement

  }

  def fetchActiveSponsoredCampaignsForSponsee(sponseeKey: CanBeScopedKey): ValidationNel[FailureResult, Map[CampaignKey, CampaignRow]] = {
    PermaCacher.getOrRegister("fetchActiveSponsoredCampaignsForSponsee_" + sponseeKey, (12 + grvmath.rand.nextInt(5)) * 60, mayBeEvicted = false) {
      val campaigns = fetchCampaignsForSponsee(sponseeKey,skipCache=false)(_.withFamilies(_.meta, _.recentArticles).filter(
        _.and(
          _.columnValueMustEqual(_.status, CampaignStatus.active),
          _.columnValueMustEqual(_.campaignType, CampaignType.sponsored)
        ),
        _.or(
          _.allInFamilies(_.meta, _.recentArticles)
        )))
        .fold(fail => Map.empty[CampaignKey, CampaignRow], result => {

        //Do this so that the activeRecentArticles are fetched inside of the lock
        result.foreach{case (_: CampaignKey, row: CampaignRow) =>
          val _ = row.activeRecentArticles()
        }
        result

      }) // Currently return empty result on fail.  We may need to change this

      trace(s"fetched ${campaigns.size} campaigns for sponseeKey: $sponseeKey")

      campaigns.successNel[FailureResult]
    }
  }

  def fetchSponsoredArticleKeysForSponseeFromCampaigns(siteKey: SiteKey): ValidationNel[FailureResult, Map[ArticleKey, CampaignRow]] = {
    fetchActiveSponsoredCampaignsForSponsee(siteKey).map(campMap => {
      for {
        (ck, cr) <- campMap
        (ak, settings) <- cr.recentArticleSettings(skipCache = false)
        if settings.isActive
      } yield {
        ak -> cr
      }
    })
  }

  def createSponsoredStoryPool(poolName: String): ValidationNel[FailureResult, SiteRow] = {
    for {
      poolNameValidated <- validatePoolName(poolName)

      // Ensure name is unique
      poolGuid = HashUtils.md5(poolNameValidated)
      _ <- SiteService.validateNameUnique(poolNameValidated)

      poolUrl = "http://nourl.com/" + poolGuid
      site <- SiteService.makeSite(poolGuid, poolNameValidated, poolUrl) //Make the Pool (which is a Site)
      poolSite <- SponsoredStoryService.setIsSponsoredStoryPool(site.rowid, isSponsoredStoryPool = true) //Set the Pool to be a Sponsored Stories Pool
    } yield {
      PermaCacher.clearResultFromCache(poolKeysCacheKey)
      poolSite
    }
  }

  /**
   * @param poolGuid The pool (site) GUID to update.
   * @param poolName New pool name.
   */
  def updateSponsoredStoryPool(poolGuid: String, poolName: String): ValidationNel[FailureResult, SiteRow] = {
    for {
      poolNameValidated <- validatePoolName(poolName)
      poolSite <- fetchPoolByGuidLite(poolGuid)

      // Ensure new name is unique
      _ <- SiteService.validateNameUnique(poolNameValidated, poolGuid.some)

      poolKey = SiteKey(poolGuid)
      modifiedSite <- SiteService.modifySite(poolKey)(_.value(_.name, poolNameValidated))
    } yield modifiedSite
  }

  protected def validatePoolName(poolName: String): ValidationNel[FailureResult, String] = {
    poolName.trim.noneForEmpty.toValidationNel(FailureResult("Pool name MUST be non-null & non-empty!"))
  }

  def setIsSponsoredStoryPool(siteKey: SiteKey, isSponsoredStoryPool: Boolean): ValidationNel[FailureResult, SiteRow] = {
    SiteService.modifySite(siteKey) {_.value(_.isSponsoredArticlesPool, isSponsoredStoryPool)}
  }

  def addArticleToSponsoredLists(sites: Set[SiteRow], article: ArticleRow) : ValidationNel[FailureResult, Seq[SiteRow]]= {
    sites.map{site=>
      SiteService.modifySite(site.rowid)(_.valueMap(_.sponsoredArticles, Map(SponsoredArticleKey(article.rowid) -> SponsoredArticleData(SiteKey(article.siteGuid)))))
    }.extrude
  }


  def deleteArticleFromSponsoredLists(sites: Set[SiteRow], article: ArticleRow) = {
    sites.map{site=>
      SiteService.deleteFromSite(site.rowid)(_.values(_.sponsoredArticles, Set(SponsoredArticleKey(article.rowid))))
    }.extrude
  }

  def fetchPoolByKey(poolKey: SiteKey): ValidationNel[FailureResult, SiteRow] = {
    for {
      pool <- SiteService.fetchSiteForManagement(poolKey)
      _ <- if (pool.isSponsoredArticlesPool) {
        true.successNel
      } else {
        FailureResult("This is not a sponsored articles pool siteId: " + poolKey.siteId).failureNel
      }
    } yield pool
  }

  def fetchPoolByGuidLite(poolGuid: String): ValidationNel[FailureResult, SiteRow] = {
    val poolKey = SiteKey(poolGuid)
    for {
      pool <- SiteService.fetch(poolKey)(_.withColumns(_.guid, _.name, _.isSponsoredArticlesPool))
      _ <- if (pool.isSponsoredArticlesPool)
        true.successNel
      else
        FailureResult(s"Site $poolGuid doesn't exist or is not a sponsored pool.").failureNel
    } yield pool
  }

  def fetchPoolByPoolName(poolName: String): ValidationNel[FailureResult, SiteRow] = {
    if (ScalaMagic.isNullOrEmpty(poolName)) return FailureResult("Pool name MUST be non-null & non-empty!").failureNel
    val poolGuid = HashUtils.md5(poolName)
    val poolKey = SiteKey(poolGuid)
    fetchPoolByKey(poolKey)
  }
}

case class SiteAlreadyExistsFailureResult(existingSiteGuid: String, siteName: String) extends FailureResult("Site Already Exists :: `" + siteName + "` with guid: `" + existingSiteGuid + "` already exists!", None)
