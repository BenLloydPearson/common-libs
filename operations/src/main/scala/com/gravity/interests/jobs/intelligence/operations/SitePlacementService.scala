package com.gravity.interests.jobs.intelligence.operations

import com.gravity.interests.jobs.intelligence._
import com.gravity.interests.jobs.intelligence.operations.FieldConverters.{SitePlacementMetaRequestConverter, SitePlacementMetaResponseConverter}
import com.gravity.service.grvroles
import com.gravity.service.remoteoperations.{ProductionRemoteOperationsClient, RemoteOperationsHelper}
import com.gravity.utilities.cache.PermaCacher
import com.gravity.utilities.{Settings, Settings2}

import scala.concurrent.duration._
import scalaz.{Failure, Success}

/**
 * Created by IntelliJ IDEA.
 * Author: Robbie Coleman
 * Date: 5/7/13
 * Time: 2:00 PM
 *
 */
object SitePlacementService extends SitePlacementService

trait SitePlacementService extends TableOperations[SitePlacementsTable, SitePlacementKey, SitePlacementRow]
with SponsoredPoolSponseeOperations[SitePlacementsTable, SitePlacementKey, SitePlacementRow]
{
  import com.gravity.logging.Logging._

  private val shouldGetFromRemote: Boolean = Settings2.getBooleanOrDefault("GetFromRemote.AllSitePlacementMeta", default = false) && !grvroles.isInRole(grvroles.REMOTE_RECOS)  && !RemoteOperationsHelper.isUnitTest
  if(shouldGetFromRemote) RemoteOperationsHelper.registerReplyConverter(SitePlacementMetaResponseConverter)
  val table: SitePlacementsTable = Schema.SitePlacements

  def allSitePlacementMeta = {

    def factoryFun() : Map[SitePlacementKey, SitePlacementRow] = {
      if (shouldGetFromRemote) {
        ProductionRemoteOperationsClient.requestResponse[SitePlacementMetaRequest, SitePlacementMetaResponse](SitePlacementMetaRequest(), 15.seconds, Some(SitePlacementMetaRequestConverter)) match {
          case Success(response) => response.rowMap
          case Failure(fails) =>
            warn("Failed to get site placement meta from remote: " + fails + ". Falling back to scan.")
            Schema.SitePlacements.query2.withFamilies(_.meta, _.blacklistedSiteSettings, _.blacklistedCampaignSettings,
              _.blacklistedArticleSettings, _.blacklistedUrlSettings, _.blacklistedKeywordSettings).scanToIterable {
              row => (row.rowid, row)
            }.toMap
        }
      }
      else
        Schema.SitePlacements.query2.withFamilies(_.meta, _.blacklistedSiteSettings, _.blacklistedCampaignSettings,
          _.blacklistedArticleSettings, _.blacklistedUrlSettings, _.blacklistedKeywordSettings).scanToIterable {
          row => (row.rowid, row)
        }.toMap

    }

    PermaCacher.getOrRegister("SitePlacementService.sitePlacements", PermaCacher.retryUntilNoThrow(factoryFun), 30 * 60)
  }

  def allSitePlacementMetaSerialized = {

    def factoryFun() : Array[Byte]  = {
      SitePlacementMetaResponseConverter.toBytes(SitePlacementMetaResponse(allSitePlacementMeta))
    }

    PermaCacher.getOrRegister("SitePlacementService.sitePlacementsSerialized", PermaCacher.retryUntilNoThrow(factoryFun), 30 * 60)
  }

  def sitePlacementMeta(key: SitePlacementKey) = {
    if (Settings.ENABLE_SITESERVICE_META_CACHE) {
      // In a production environment, cache the entire site placement meta as a map lookup
      allSitePlacementMeta.get(key)
    } else {
      // In other environments, cache single values at a time.
      Schema.SitePlacements.query2.withKey(key).withFamilies(_.meta, _.blacklistedSiteSettings,
        _.blacklistedCampaignSettings, _.blacklistedArticleSettings, _.blacklistedUrlSettings,
        _.blacklistedKeywordSettings).singleOption(skipCache = false)
    }
  }

}

case class SitePlacementMetaRequest()
case class SitePlacementMetaResponse(rowMap: Map[SitePlacementKey, SitePlacementRow])
