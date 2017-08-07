package com.gravity.interests.jobs.intelligence.operations.sites

import com.gravity.utilities.components.FailureResult
import scalaz._
import Scalaz._
import com.gravity.interests.jobs.intelligence._
import com.gravity.hbase.schema.{DeleteOp, PutOp}
import com.gravity.interests.jobs.intelligence.operations.SiteService
import com.gravity.utilities.grvz._
import com.gravity.interests.jobs.intelligence.operations.sponsored.SiteAlreadyExistsFailureResult

/*             )\._.,--....,'``.
 .b--.        /;   _.. \   _\  (`._ ,.
`=,-,-'~~~   `----(,_..'--(,_..'`-.;.'  */


trait SiteManager {
  this : SiteService =>


  def addSiteToSuperSite(siteGuid:String, superSiteGuid:String) : ValidationNel[FailureResult, SiteRow] = {
    val siteKey = SiteKey(siteGuid)
    val superSiteKey = SiteKey(superSiteGuid)
    for {
      site <- fetchModifyPutRefetch(siteKey)(_.withFamilies(_.meta))((site,put)=>put.value(_.superSites, site.superSites + superSiteKey))
      superSite <- fetchModifyPutRefetch(superSiteKey)(_.withFamilies(_.meta))((site,put)=>put.value(_.subSites,site.subSites + siteKey))
    } yield site
  }

  def makeSite(siteGuid: String, siteName: String, baseUrl: String, isEnabled: Boolean = false ): ValidationNel[FailureResult, SiteRow] = {
    val sk = SiteKey(siteGuid)
    fetchSiteForManagement(sk).fold(fail => {
      Schema.Sites
              .put(sk)
              .value(_.name, siteName)
              .value(_.guid, siteGuid)
              .value(_.isEnabled, isEnabled)
              .value(_.supportsLiveRecommendationMetrics, isEnabled)
              .value(_.url, baseUrl)
              .execute().successNel
    }, success => {SiteAlreadyExistsFailureResult(siteGuid, success.name.getOrElse(siteName)).failureNel}).flatMap(siteRow => {
      fetchSiteForManagement(sk)
    })
  }


  def modifySite(key: SiteKey)(work: PutOp[SitesTable, SiteKey] => PutOp[SitesTable, SiteKey]) = {
    fetchSiteForManagement(key).flatMap(siteRow => {
      work(Schema.Sites.put(key)).execute().successNel
    }).flatMap(result => fetchSiteForManagement(key))
  }

  def deleteFromSite(key: SiteKey)(work: DeleteOp[SitesTable, SiteKey] => DeleteOp[SitesTable, SiteKey]) = {
    fetchSiteForManagement(key).flatMap(siteRow => {
      work(Schema.Sites.delete(key)).execute().successNel
    }).flatMap(result => fetchSiteForManagement(key))
  }


  def fetchSitesForManagement(siteKeys: Set[SiteKey], maximumSponsoredArticles:Int = 5000): ValidationNel[FailureResult, Seq[SiteRow]] = {
    siteKeys.map {
      siteKey =>
        SiteService.fetchSiteForManagement(siteKey, maximumSponsoredArticles)
    }.extrude
  }


  def fetchSiteForManagement(siteKey: SiteKey, maximumSponsoredArticles:Int = 5000, skipCache:Boolean=true): ValidationNel[FailureResult, SiteRow] = {
    Schema.Sites.query2.withKey(siteKey).withFamilies(_.meta, _.sponsoredPoolSettingsFamily, _.campaigns, _.campaignsByName, _.sponsoredArticles)
      .filter(
        _.or(
          _.withPaginationForFamily(_.sponsoredArticles,maximumSponsoredArticles,0),
          _.allInFamilies(_.meta,_.sponsoredPoolSettingsFamily)
        )
      )
      .singleOption(skipCache=skipCache) match {
        case Some(site) =>  withValidation(site)(validateSite)
        case None => FailureResult("Did not fetch site " + siteKey).failureNel
      }

  }

  /**
   * This is a list of validations that should be performed when a site is fetched for management
   */
  def validateSite(site : SiteRow) = {
    Seq(
      if (site.column(_.url).isDefined) site.success else FailureResult("no url").failure,
      if (site.column(_.name).isDefined) site.success else FailureResult("no name").failure,
      if (site.column(_.guid).isDefined) site.success else FailureResult("no guid").failure
    )
  }
}

