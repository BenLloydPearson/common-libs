package com.gravity.interests.jobs.intelligence.operations.sites

import com.gravity.utilities.{HashUtils, SplitHost}
import com.gravity.utilities.analytics.articles.ArticleWhitelist
import com.gravity.interests.jobs.intelligence.{SectionKey, SiteKey, Schema}
import scalaz._
import Scalaz._
import com.gravity.utilities.components.FailureResult
import com.gravity.utilities.grvz._
import com.gravity.utilities.cache.PermaCacher
import com.gravity.interests.jobs.intelligence.operations.{SiteDomainLookupOperations, SiteService}
import com.gravity.interests.jobs.intelligence.operations.sections.SectionNameExtractor
import com.gravity.utilities.web.{CategorizedFailureResult, ParamValidationFailure}

object SiteGuidService extends SiteGuidManager

/*             )\._.,--....,'``.
.b--.        /;   _.. \   _\  (`._ ,.
`=,-,-'~~~   `----(,_..'--(,_..'`-.;.'  */
trait SiteGuidManager extends SiteDomainLookupOperations {
  import com.gravity.utilities.Counters._
  override val counterCategory = "Site Guid Manager"
  private val sectionExtractor = new SectionNameExtractor{}

  /** @return Success is the site GUID. */
  def findSiteGuidInSitesTableByUrl(url: String): ValidationNel[CategorizedFailureResult, String] = {
    val urlLower = url.toLowerCase
    PermaCacher.getOrRegisterFactoryWithOption("site-domain-lookup", 60 * 10) {
      val sites = Schema.Sites.query2.withColumns(_.guid, _.url).scanToIterable(itm => itm)
      sites.flatMap(site => {
        site.url tuple site.siteGuid match {
          case Some((siteUrl, siteGuid)) => (siteUrl.toLowerCase, siteGuid).some
          case None => None
        }
      }).toMap.some
    } match {
      case Some(siteUrlLookup) => {
        (siteUrlLookup.find {
          case (siteurl, siteguid) =>
            if (urlLower contains siteurl) true else false
        } match {
          case Some((siteurl, siteguid)) => siteguid.some
          case None => None
        }).toValidationNel(new CategorizedFailureResult(_.NotFound, "Unable to find domain in lookup"))
      }
      case None => new CategorizedFailureResult(_.NotFound, "No domain lookup exists").failureNel
    }
  }

  /**
   * Given a siteKey instance, discover whether or not it exists in the sites table.  If it does not, query the DomainSiteLookup table.
   * @param siteKey
   * @return
   */
  def fetchSiteGuidBySiteKey(siteKey:SiteKey) : ValidationNel[FailureResult,String] = {
    (SiteService.siteMeta(siteKey) match {
      case Some(siteRow) => siteRow.siteGuid.toValidationNel(FailureResult("Found a site but it didn't have a guid"))
      case None => FailureResult("Didn't find a site").failureNel
    }) findSuccess (Schema.DomainSiteLookup.query2.withKey(siteKey.siteId.toString).withAllColumns.singleOption(skipCache=false) match {
      case Some(domainLookupRow) => domainLookupRow.column(_.siteGuid).toValidationNel(FailureResult("Found the domain lookup row but missing the siteGuid column"))
      case None => FailureResult("Did not find a domain lookup row for the sitekey").failureNel
    })
  }

  def fetchSiteDomainBySiteKey(siteKey:SiteKey) : ValidationNel[FailureResult,String] = {
    (SiteService.siteMeta(siteKey) match {
      case Some(siteRow) => siteRow.siteGuid.toValidationNel(FailureResult("Found a site but it didn't have a guid"))
      case None => FailureResult("Didn't find a site").failureNel
    }) findSuccess (Schema.DomainSiteLookup.query2.withKey(siteKey.siteId.toString).withAllColumns.singleOption(skipCache=false) match {
      case Some(domainLookupRow) => domainLookupRow.column(_.urlBase).toValidationNel(FailureResult("Found the domain lookup row but missing the urlBase column"))
      case None => FailureResult("Did not find a domain lookup row for the sitekey " + siteKey.siteId.toString).failureNel
    })
  }

  def extractSectionKeyFromUrl(url:String, siteGuid:String) : ValidationNel[FailureResult,SectionKey] = {
    sectionExtractor.extractSectionKey(url,siteGuid)
  }
  /**
   * Will return the siteGuid of the domain, and, if it can, the sectionGuid of the domain as well
   * @param url
   * @return
   */
  def extractSectionNameFromUrl(url:String, siteGuid:String) : ValidationNel[FailureResult,String] = {
    sectionExtractor.extractSectionName(url,siteGuid)
  }

  /**
   * This will generate a site guid from the domain of a URL.
   * @param url
   * @return
   */
  def generateSiteGuidFromUrl(url:String) : ValidationNel[FailureResult, String] = {
    for {
     registeredDomain <- SplitHost.registeredDomainFromUrl(url).toValidationNel(FailureResult("Could not extract top level host from url"))
    } yield HashUtils.md5(registeredDomain)
  }

  def fetchSiteGuidFromUrl(url: String): ValidationNel[FailureResult, String] = {
    for {
      registeredDomain <- SplitHost.registeredDomainFromUrl(url).toValidationNel(FailureResult("Could not extract top level host from url"))
      foundGuid <- Schema.DomainSiteLookup.query2.withKey(registeredDomain).withAllColumns.singleOption(skipCache = false) match {
        case Some(lookupRow) => {
          countPerSecond(counterCategory, "Site Guid Manager : Sites Looked Up")
          lookupRow.column(_.siteGuid).toValidationNel(FailureResult("No site guid in the row"))
        }
        case None => FailureResult("No record for url `" + url + "` within the DomainSiteLookup table!").failureNel
      }
    } yield foundGuid
  }

  /**
   * Given a url, will attempt to fetch its siteGuid from the DomainSiteLookup table.  If the url is not present, will generate a new guid and put it into the table,
   * along with lookups by the registered domain and by the sitekey hash.
   * @param url
   * @return
   */
  def fetchOrGenerateSiteGuidFromUrl(url: String): ValidationNel[CategorizedFailureResult, String] = {
    for {
      registeredDomain <- SplitHost.registeredDomainFromUrl(url).toValidationNel(new CategorizedFailureResult(_.ParseOther, "Could not extract top level host from url"))
      newGuid <- Schema.DomainSiteLookup.query2.withKey(registeredDomain).withAllColumns.singleOption(skipCache = false) match {
        case Some(lookupRow) => {
          countPerSecond(counterCategory, "Site Guid Manager : Sites Looked Up")
          lookupRow.column(_.siteGuid).toValidationNel(new CategorizedFailureResult(_.ServerError, "No site guid in the row"))
        }
        case None => {
          val newGuid = HashUtils.md5(registeredDomain)
          countPerSecond(counterCategory, "Site Guid Manager : Sites Created")
          Schema.DomainSiteLookup
            .put(registeredDomain).value(_.siteGuid,newGuid).value(_.urlBase,registeredDomain)
            .put(newGuid).value(_.siteGuid, newGuid).value(_.urlBase, registeredDomain)
            .put(SiteKey(newGuid).siteId.toString).value(_.siteGuid, newGuid).value(_.urlBase, registeredDomain)
            .execute()
          newGuid.successNel
        }
      }
    } yield newGuid

  }

  /**
   * Given a url, will try to
   * A) Find the siteGuid of that url from the ArticleWhitelist
   * B) Find the siteGuid by scanning the Sites Table for its registered domain
   * C) Look up the url in the DomainSiteLookup table.  If it doesn't exist, generate a new entry.  If it does exist, return it.
   *            
   * @return Success is the found or newly generated site GUID.
   *         
   * @see [[findSiteGuidByUrl()]]
   */
  def findOrGenerateSiteGuidByUrl(url: String): ValidationNel[CategorizedFailureResult, String] = {
    if (url == null || url.isEmpty)
      new CategorizedFailureResult(_.Required, "Looked Up Site for Empty Url").failureNel
    else
      findSiteGuidByUrl(url) findSuccess fetchOrGenerateSiteGuidFromUrl(url)
  }

  /** @see [[findOrGenerateSiteGuidByUrl()]] */
  def findSiteGuidByUrl(url: String): ValidationNel[CategorizedFailureResult, String] = {
    ArticleWhitelist.getSiteGuidByPartnerArticleUrl(url).toValidationNel(
      new CategorizedFailureResult(_.NotFound, "Could not get siteguid from article url")
    ) findSuccess findSiteGuidInSitesTableByUrl(url)
  }

  def isPartnerUrl(url: String): Boolean = findSiteGuidByUrl(url).isSuccess

}

object FindOrGenerateSiteGuidByUrl extends App {
  print("Enter Site URL with scheme: ")
  val url = scala.io.StdIn.readLine().trim

  println(SiteGuidService.findOrGenerateSiteGuidByUrl(url))
}