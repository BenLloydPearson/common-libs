package com.gravity.interests.jobs.intelligence.operations

import akka.actor.ActorSystem
import com.gravity.hbase.schema.{Column, ColumnFamily, OpsResult}
import com.gravity.interests.jobs.hbase.HBaseConfProvider
import com.gravity.interests.jobs.intelligence.hbase.ScopedKey
import com.gravity.interests.jobs.intelligence.operations.FieldConverters.{SiteMetaRequestConverter, SiteMetaResponseConverter}
import com.gravity.interests.jobs.intelligence.operations.sites.SiteManager
import com.gravity.interests.jobs.intelligence.operations.sponsored.SiteAlreadyExistsFailureResult
import com.gravity.interests.jobs.intelligence.{ArticleAndMetrics, ArticleKey, SiteKey, _}
import com.gravity.service.grvroles
import com.gravity.service.remoteoperations.{ProductionRemoteOperationsClient, RemoteOperationsHelper}
import com.gravity.utilities._
import com.gravity.utilities.analytics.DateMidnightRange
import com.gravity.utilities.analytics.articles.ArticleWhitelist
import com.gravity.utilities.cache.{PermaCacher, TempCacher}
import com.gravity.utilities.components.FailureResult
import com.gravity.utilities.grvakka.Configuration.defaultConf
import com.gravity.utilities.grvtime._
import com.gravity.utilities.time.DateHour
import com.gravity.valueclasses.ValueClassesForDomain.{SiteGuid, _}
import org.apache.hadoop.conf.Configuration
import org.joda.time.{DateTime, ReadableInstant}

import scala.collection._
import scala.collection.immutable.IndexedSeq
import scala.concurrent.duration._
import scalaz.Scalaz._
import scalaz._

/*             )\._.,--....,'``.
.b--.        /;   _.. \   _\  (`._ ,.
`=,-,-'~~~   `----(,_..'--(,_..'`-.;.'  */

object SiteService extends SiteService with SiteOperations {
  val IGW_SUPERSITE: String = "ROLL4dc0a6f860e9f7d03fbfe29124cd"
  val FB_SUPERSITE: String = "ROLL5dc1b4c320b9a7f03fcfe12536ab"

  // Our AWESOME plugin
  val HIGHLIGHTER: String = "SPEC6ef2c5a210b9a7f03fcfe12537cd"

  // partners
  val AOLGUID: String = ArticleWhitelist.siteGuid(_.AOL)
  val TECHCRUNCH: String = ArticleWhitelist.siteGuid(_.TECHCRUNCH)
  val YAHOONEWSGUID: String = ArticleWhitelist.siteGuid(_.YAHOO_NEWS)

  val allPartnerGuids: Set[String] = ArticleWhitelist.Partners.siteGuidToPartner.keySet

  val defaultArticleFamilies: Seq[((ArticlesTable) => ColumnFamily[ArticlesTable, ArticleKey, String, _, _])] =
    Seq((a: ArticlesTable) => Schema.Articles.meta, (a: ArticlesTable) => Schema.Articles.standardMetricsHourlyOld)

}

case class HourMetricsWithAverage(hour: HourTime, metrics: StandardMetrics, average: StandardMetrics)

case class HourTime(dateTime: Long, dateTimeDebug: String)

object HourTime {
  def apply(time: ReadableInstant): HourTime = HourTime(time.getMillis, time.toString)
}

object AllSiteMeta {
 import com.gravity.logging.Logging._
  implicit val conf: Configuration = HBaseConfProvider.getConf.defaultConf
  val shouldGetFromRemote: Boolean = Settings2.getBooleanOrDefault("GetFromRemote.AllSiteMeta", false) && !grvroles.isInRole(grvroles.REMOTE_RECOS) && !RemoteOperationsHelper.isUnitTest
  if(shouldGetFromRemote) RemoteOperationsHelper.registerReplyConverter(SiteMetaResponseConverter)

  implicit val system: ActorSystem = ActorSystem("SiteService", defaultConf)

  private def siteMetaQuery = Schema.Sites.query2.withFamilies(_.meta, _.sponsoredPoolSettingsFamily, _.blacklistedSiteSettings,
    _.blacklistedCampaignSettings, _.blacklistedArticleSettings, _.blacklistedUrlSettings, _.blacklistedKeywordSettings, _.partnerTagging)

  private val getAllSiteMetaCtr = new java.util.concurrent.atomic.AtomicLong(0)

  @volatile var allSiteMeta: Map[SiteKey, SiteRow] = null
  @volatile var allSiteMetaSerialized: Array[Byte] = null
  PermaCacher.getOrRegister("all-site-meta", PermaCacher.retryUntilNoThrow(allSiteMetaFactory), reloadInSeconds = 60 * 5)

  def allSiteMetaFactory(): Boolean =
    try {
      allSiteMeta = getAllSiteMeta
      allSiteMetaSerialized = SiteMetaResponseConverter.toBytes(SiteMetaResponse(allSiteMeta))
      true // This rather-odd code is just using Permacacher as a reload mechanism -- the actual value being permacached is just "true".
    }
    catch {
      case th: Throwable =>
        warn(ScalaMagic.formatException(th))
        throw new Exception("Exception during (re)-loading of AllSiteMeta, will try again in a few minutes", th)
    }

  private def getAllSiteMeta: Map[SiteKey, SiteRow] = {
   //Remove logging.  This is now tracked via PermaCacher so you can look at the admin page for PermaCacher to see when it was reloaded, when it will be reloaded, etc.
    getAllSiteMetaCtr.incrementAndGet()
    val meta = if(shouldGetFromRemote) {
      ProductionRemoteOperationsClient.requestResponse[SiteMetaRequest, SiteMetaResponse](SiteMetaRequest(Seq.empty), 15.seconds, Some(SiteMetaRequestConverter)) match {
        case Success(response) => response.rowMap
        case Failure(fails) =>
          warn("Failed to get site meta from remote: " + fails + ". Falling back to scan.")
          siteMetaQuery.scanToIterable(row => (row.siteKey, row)).toMap
      }
    }
    else {
      siteMetaQuery.scanToIterable(row => (row.siteKey, row)).toMap
    }
    meta
  }
}

case class SiteMetaRequest(siteKeyFilter: Seq[SiteKey] = Seq.empty) //empty seq requests all

case class SiteMetaResponse(rowMap: Map[SiteKey, SiteRow])

trait SiteService extends SiteManager with SiteOperations with UserVisitation {

  val refreshSecondsMinutely: Int = if (Settings.CANONICAL_HOST_NAME.contains(".local")) 1200 else 60
  val refreshSecondsFiveMinutes: Int = if (Settings.CANONICAL_HOST_NAME.contains(".local")) 1200 else 300

  def publisherSiteKeys: Set[SiteKey] = AllSiteMeta.allSiteMeta.filter(_._2.poolsSponseeIn.nonEmpty).keySet
  def advertiserSiteKeys: Set[SiteKey] = AllSiteMeta.allSiteMeta.filter(_._2.poolsSponsorIn.nonEmpty).keySet

  def siteMetaForSiteGuid(sg: SiteGuid): Option[SiteRow] = siteMeta(sg.siteKey)

  def siteMeta(siteGuid: String): Option[SiteRow] = siteMeta(SiteKey(siteGuid))

  /**
   * Given a site key, retrieve the siteguid from the site meta cache.
   * This is friendly, in other words, to bulk lookups
   * @param siteKey The key you want
   * @return
   */
  def siteGuid(siteKey:SiteKey): Option[String] = siteMeta(siteKey).flatMap(_.siteGuid)

  def sg(siteKey: SiteKey): Option[SiteGuid] = siteMeta(siteKey).flatMap(_.siteGuid.map(_.asSiteGuid))

  def siteMeta(siteKey: SiteKey): Option[SiteRow] = {
    if (Settings.ENABLE_SITESERVICE_META_CACHE) {
      // In a production environment, cache the entire site meta as a map lookup
      AllSiteMeta.allSiteMeta.get(siteKey)
    } else {
      // In other environments, cache single values at a time.  (Changed this from no cache because there are tight-loop lookups)
      Schema.Sites.query2.withFamilies(_.meta, _.sponsoredPoolSettingsFamily, _.blacklistedSiteSettings,
        _.blacklistedCampaignSettings, _.blacklistedArticleSettings, _.blacklistedUrlSettings,
        _.blacklistedKeywordSettings, _.partnerTagging).withKey(siteKey).singleOption(skipCache=false)
    }
  }

 // def getRevenueConfig(siteGuid: String): RevenueConfig = getRevenueConfig(SiteKey(siteGuid))

//  def getRevenueConfig(siteKey: SiteKey): RevenueConfig = siteMeta(siteKey) match {
//    case Some(site) => site.revenueConfig
//    case None => RevenueConfig.default
//  }

  def addToRecentArticles(site: SiteKey, articles: Seq[(ArticleKey, DateTime)]): Validation[NonEmptyList[FailureResult], OpsResult] = {
    require(articles.nonEmpty, "No articles passed to add to recent articles")

    val map = articles.map {
      case (articleKey, publishDate) =>
        PublishDateAndArticleKey(publishDate, articleKey) -> articleKey
    }.toMap

    for {
      result <- SiteService.put(Schema.Sites.put(site).valueMap(_.recentArticles, map))
    } yield result
  }

  /**
   * Does the specified `siteGuid` support live recommendations?
   * @param siteGuid the guid of the site you are inquiring about
   * @return `true` if the site supports live recommendations and `false` if it does not or if the site is not found
   */
  def supportsLiveRecommendations(siteGuid: String): Boolean = supportsLiveRecommendations(SiteKey(siteGuid))

  /**
   * Does the specified `siteKey` support live recommendations?
   * @param siteKey the key of the site you are inquiring about
   * @return `true` if the site supports live recommendations and `false` if it does not or if the site is not found
   */
  def supportsLiveRecommendations(siteKey: SiteKey): Boolean = siteMeta(siteKey) match {
    case Some(s) => s.supportsLiveRecommendations
    case None => false
  }

  def supportsLiveRecommendationsIterable(querySpec: QuerySpec): Iterable[SiteRow] = {
    Schema.Sites.query2.withFamilies(_.meta).filter(_.and(_.columnValueMustEqual(_.supportsLiveRecommendations, true))).scanToIterable(row => row)
  }

  /**
   * Does the specified `siteKey` require that articles have images in order to be recommended?
   * @param siteKey
   * @return
   */
  def requiresArticleImagesForRecommendations(siteKey: SiteKey): Boolean = siteMeta(siteKey) match {
    case Some(s) => s.recommendationsRequireImages
    case None => false
  }

  def isRssIngested(siteGuid: String): Boolean = isRssIngested(SiteKey(siteGuid))

  def isRssIngested(siteKey: SiteKey): Boolean = siteMeta(siteKey) match {
    case Some(s) => s.isRssIngested
    case None => false
  }


  /**
   * Is the specified `siteGuid` configured to send all recommendation links through the redirect server?
   * @param siteGuid the guid of the site you are inquiring about
   * @return `true` if the site redirects all recommendation links and `false` if it does not or if the site is not found
   */
  def redirectsOrganics(siteGuid: String): Boolean = redirectsOrganics(SiteKey(siteGuid))

  /**
   * Is the specified `siteKey` configured to send all recommendation links through the redirect server?
   * @param siteKey key of the site you are inquiring about
   * @return `true` if the site redirects all recommendation links and `false` if it does not or if the site is not found
   */
  def redirectsOrganics(siteKey: SiteKey): Boolean = siteMeta(siteKey) match {
    case Some(s) => s.redirectOrganics
    case None => false
  }

  def requiresOutboundTrackingParams(siteGuid: String): Boolean = requiresOutboundTrackingParams(SiteKey(siteGuid))

  def requiresOutboundTrackingParams(siteKey: SiteKey): Boolean = siteMeta(siteKey) match {
    case Some(s) => s.requiresOutboundTrackingParams
    case None => false
  }

  /**
   * Is the specified `siteGuid` configured to have the redirect server perform redirects of organic recos via HTTP Status 302?
   * @param siteGuid the guid of the site you are inquiring about
   * @return `true` if the site redirects organic reco linnks with 302 and `false` if it does not or if the site is not found
   */
  def redirectsOrganicsUsingHttpStatus(siteGuid: String): Boolean = redirectsOrganicsUsingHttpStatus(SiteKey(siteGuid))

  /**
   * Is the specified `siteKey` configured to have the redirect server perform redirects of organic recos via HTTP Status 302?
   * @param siteKey key of the site you are inquiring about
   * @return `true` if the site redirects organic reco linnks with 302 and `false` if it does not or if the site is not found
   */
  def redirectsOrganicsUsingHttpStatus(siteKey: SiteKey): Boolean = siteMeta(siteKey) match {
    case Some(s) => s.redirectOrganics && s.redirectOrganicsUsingHttpStatus
    case None => false
  }

  /**
   * Does the specified `siteGuid` support recent articles?
   * @param siteGuid the guid of the site you are inquiring about
   * @return `true` if the site supports recent articles and `false` if it does not or if the site is not found
   */
  def supportsRecentArticles(siteGuid: String): Boolean = supportsRecentArticles(SiteKey(siteGuid))

  /**
   * Does the specified `siteKey` support recent articles?
   * @param siteKey the key of the site you are inquiring about
   * @return `true` if the site supports recent articles and `false` if it does not or if the site is not found
   */
  def supportsRecentArticles(siteKey: SiteKey): Boolean = siteMeta(siteKey) match {
    case Some(s) => s.supportsRecentArticles
    case None => false
  }

  /**
   * Is the specified `siteKey` configured to accept beacons from any host?
   * @param siteKey key of the site you are inquiring about
   * @return `true` if the site accepts beacons from all hosts, including hosts not associated with that site
   */
  def ignoreDomainCheck(siteKey: SiteKey): Boolean = siteMeta(siteKey) match {
    case Some(s) => s.ignoreDomainCheck
    case None => false
  }

  def maximumRecommendationsStoredPerUser(siteKey: SiteKey): Int = 60

  def minimumClicksToRecommendLive(siteGuid: String): Int = {
    8
  }

  def onlyRecommendToTestUserGroup(siteGuid: String): Boolean = false

  def getMostRecentArticles(siteGuid: String, minimumArticlesRequired: Int = -1, printTrace: Boolean = false, maximumArticles: Int = 1000)(oldestAllowed: DateTime, resultFamilies: ((ArticlesTable) => ColumnFamily[ArticlesTable, ArticleKey, String, _, _])*)(articleCacheTtl: Int, resultColumns: ((ArticlesTable) => Column[ArticlesTable, ArticleKey, String, _, _])*): Seq[ArticleRow] = {

    val sk = SiteKey(siteGuid)
    val minStamp = oldestAllowed.getMillis

    val aks = if (SiteService.supportsRecentArticles(siteGuid)) {
      if (printTrace) println("Attempting recentArticles")
      Schema.Sites.query2.withKey(sk).withFamilies(_.recentArticles).filter(
        _.or(
          _.allInFamilies(_.meta),
          _.lessThanColumnKey(_.recentArticles, PublishDateAndArticleKey.partialByStartDate(oldestAllowed.minusMinutes(1)))
        ),
        _.or(
          _.withPaginationForFamily(_.recentArticles, maximumArticles, 0),
          _.allInFamilies(_.meta)
        )
      ).singleOption() match {
        case Some(s) => {

          val recentArticleSeq = s.family(_.recentArticles).toSeq

          if (printTrace) {
            println(recentArticleSeq.size + " recentArticles returned. Most recent 20 below:")
            recentArticleSeq.sortBy(-_._1.publishDate.getMillis).take(20).foreach(ra => println("\t" + ra._1.publishDate.toString("yyyy-MM-dd'-T-'HH:mm:ss") + " " + ra._2))
          }

          val articleKeyCandidates = recentArticleSeq.filter(_._1.publishDate.getMillis >= minStamp)

          if (printTrace) println(articleKeyCandidates.size + " recentArticles more recent than oldestAllowed " + oldestAllowed.toString("yyyy-MM-dd'-T-'HH:mm:ss"))

          val articleKeys = articleKeyCandidates.map(_._2)


          articleKeys
        }
        case None => Seq.empty[ArticleKey]
      }
    } else Seq.empty[ArticleKey]

    val articleKeyset = if ((minimumArticlesRequired > -1 && aks.size < minimumArticlesRequired) || (minimumArticlesRequired < 0 && aks.isEmpty)) {
     // UserServiceRemoteCounters.mostRecentArticlesMiss.increment
      println("Site: '" + siteGuid + "' is falling back to topSortedArticles since recentArticles failed to return the minimumArticlesRequired: " + minimumArticlesRequired)
      val arsk = ArticleRangeSortedKey.mostRecentArticlesKey
      Schema.Sites.query2.withKey(sk).withColumn(_.topSortedArticles, arsk).singleOption() match {
        case Some(s) => s.columnFromFamily(_.topSortedArticles, arsk) match {
          case Some(tsaks) => tsaks.toSet
          case None => Set.empty[ArticleKey]
        }
        case None => Set.empty[ArticleKey]
      }
    } else aks.toSet

    if (printTrace) println("Retrieved %,d article keys to query against articles table".format(articleKeyset.size))

    val finalFamilies = if (resultFamilies.isEmpty) SiteService.defaultArticleFamilies else resultFamilies
    val allInFamilies = {
      val famNames = mutable.HashSet[String]()
      famNames += Schema.Articles.standardMetricsHourlyOld.familyName
      val fromRequested = for {
        lam <- finalFamilies
        fam = lam(Schema.Articles)
        if (fam.familyName != Schema.Articles.standardMetricsHourlyOld.familyName)
      } yield {
        famNames += fam.familyName
        lam
      }

      if (resultColumns.isEmpty) {
        fromRequested
      } else {
        val fromColumns = for {
          cl <- resultColumns
          fam = cl(Schema.Articles).family.asInstanceOf[ColumnFamily[ArticlesTable, ArticleKey, String, _, _]]
          if (famNames.add(fam.familyName))
        } yield {
          (a: ArticlesTable) => fam
        }
        fromRequested ++ fromColumns
      }
    }

    val query = Schema.Articles.query2
      .withKeys(articleKeyset)
      .withFamilies(finalFamilies.head, finalFamilies.tail: _*)
      .filter(
      _.or(
        _.greaterThanColumnKey(_.standardMetricsHourlyOld, oldestAllowed.toGrvDateMidnight.minusDays(1).toDateHour),
        _.allInFamilies(allInFamilies: _*)
      )
    )

    if (!resultColumns.isEmpty) {
      query.withColumns(resultColumns.head, resultColumns.tail: _*)
    }


    val (sc, ttl) = if (articleCacheTtl > 0) (false, articleCacheTtl) else (true, 0)

    query.executeMap(skipCache = sc, ttl = ttl).values.toSeq
  }

  private def getSiteFromHbase(siteGuid: String): Option[SiteRow] = getSiteFromHbase(SiteKey(siteGuid))

  private def getSiteFromHbase(siteKey: SiteKey): Option[SiteRow] = {
    fetchAndCacheEmpties(siteKey)(_.withFamilies(_.meta, _.standardMetricsHourlyOld, _.viralMetricsHourly, _.sections, _.sponsoredPoolSettingsFamily))(60 * 5).toOption
  }

  def siteNameByGuid(guid: String): String = {
    require(guid != null && guid.length > 0, "Attempted to get a site name with an empty guid")
    nameForSite(siteMeta(guid))
  }

  def siteName(key: SiteKey): String = {
    nameForSite(siteMeta(key))
  }

  def siteName(sg: SiteGuid): SiteName = {
    nameForSite(siteMeta(sg.siteKey)).asSiteName
  }

  private def nameForSite(siteOption: Option[SiteRow]): String = siteOption match {
    case Some(s) => s.name.getOrElse("NO_NAME")
    case None => "NOT_IN_SITES_TABLE"
  }

  def siteExists(guid: String): Boolean = siteExists(SiteKey(guid))

  def siteExists(key: SiteKey): Boolean = PermaCacher.getOrRegister("SiteService.siteExists(" + key.siteId + ")", {
    Schema.Sites.query2.withKey(key).withColumn(_.guid).singleOption().isDefined.toString
  }, refreshSecondsFiveMinutes) == true.toString

  def siteIsEnabled(guid: String): Boolean = siteIsEnabled(SiteKey(guid))

  def siteIsEnabled(key: SiteKey): Boolean = PermaCacher.getOrRegister("SiteService.siteIsEnabled(" + key.siteId + ")", {
    siteMeta(key) match {
      case Some(site) => site.isEnabled.toString
      case None => false.toString
    }
  }, refreshSecondsFiveMinutes) == true.toString

  def siteBeaconCrawlingIsDisabled(guid: String): Boolean = siteBeaconCrawlingIsDisabled(SiteKey(guid))

  def siteBeaconCrawlingIsDisabled(key: SiteKey): Boolean = siteMeta(key) match {
    case Some(site) => site.isBeaconCrawlingDisabled
    case None => false
  }

  def supportsLiveRecommendationMetrics(siteGuid: String): Boolean = supportsLiveRecommendationMetrics(SiteKey(siteGuid))

  def supportsLiveRecommendationMetrics(siteKey: SiteKey): Boolean = siteMeta(siteKey) match {
    case Some(site) => site.supportsLiveRecommendationMetrics
    case None => false
  }

  def site(guid: String): Option[SiteRow] = getSiteFromHbase(guid)

  def site(key: SiteKey): Option[SiteRow] = getSiteFromHbase(key)

//  def isSuperSite(siteKey: SiteKey): Boolean = siteMeta(siteKey) match {
//    case Some(s) => s.isSuperSite
//    case None => false
//  }

  lazy val financeSuperSiteUri = "http://insights.gravity.com/rollup/Finance"
  lazy val financeSuperSiteGuid = "ROLL6afd1f4b40dfd9f5731e35302fe5"
  lazy val financeSuperSiteKey: SiteKey = SiteKey(financeSuperSiteGuid)

//  /**
//   * Returns the sites result
//   */
//  def getSuperSite(superUri: String): Option[SiteRow] = if (superUri == financeSuperSiteUri) {
//    site(financeSuperSiteKey)
//  } else {
//    None
//  }

  def uniques(siteKey: SiteKey, range: DateMidnightRange): Long = Schema.Sites.query2.withKey(siteKey).withColumn(_.uniques, range).singleOption() match {
    case Some(res) => res.columnFromFamily(_.uniques, range).getOrElse(0l)
    case None => 0l
  }

  def recoMetricsSince(startHour: DateHour): Map[SiteGuid, (SiteRow, RecommendationMetrics)] = {
    val siteRows = TempCacher.getOrUpdate("sitemetricslist"+startHour, {

      Schema.Sites.query2.withFamilies(
        _.rollupRecommendationMetrics, _.meta
      ).filter(
        _.or(
          _.lessThanColumnKey(_.rollupRecommendationMetrics, RollupRecommendationMetricKey.partialByStartDate(startHour))
        ,_.allInFamilies(_.meta)
      )).scanToIterable(item => item).filter(_.family(_.rollupRecommendationMetrics).size > 0)}, ttlSeconds = 60*60)

    import com.gravity.domain.grvmetrics._

    val metrics = for {
      siteRow <- siteRows
      (sk, metr) <- siteRow.rollupRecommendationMetrics.asRecoMetrics.groupReduceSeqBy(x => x._1.siteKey)
      guid <- siteRow.guid
    } yield {
      (guid, (siteRow, metr))
    }

    metrics.toMap
  }

  private val defaultSortby = (rm:RecommendationMetrics) => -1 * (rm.clicks + rm.unitImpressionsViewed)
  def siteGuidsRankedSince[T: scala.Ordering](startHour: DateHour, sortBy: RecommendationMetrics => T = defaultSortby): IndexedSeq[SiteGuid] = {
    recoMetricsSince(startHour).toIndexedSeq.sortBy {
      case (sg, (row, metric)) => sortBy(metric)
    }.flatMap {
      case (sg, (row, metric)) => row.guid
    }
  }

  def siteRank(site: SiteGuid, startHour: DateHour = (24.hoursAgo).toDateHour): Int = {
    val rank = siteGuidsRankedSince(startHour).indexOf(site)
    if(rank == -1) Integer.MAX_VALUE else rank
  }

  def validateNameUnique(siteName: String, excludeSiteGuid: Option[String] = None): ValidationNel[FailureResult, String] = withMaintenance {
    Schema.Sites.query2.withColumns(_.guid, _.name).scanUntil(siteFromDb => siteFromDb.name match {
      // Name is not unique and this site wasn't excluded
      case Some(nameFromDb) if nameFromDb == siteName && (excludeSiteGuid.isEmpty || excludeSiteGuid != siteFromDb.siteGuid) =>
        return SiteAlreadyExistsFailureResult(siteFromDb.siteGuidOrNoGuid, siteFromDb.nameOrNoName).failureNel

      // Name still unique...
      case _ =>
        true
    })

    siteName.successNel
  }

  def siteDoingConversions(siteKey: SiteKey) : Boolean = {
    AllSiteMeta.allSiteMeta.get(siteKey) match {
      case Some(row) => row.conversionTrackingParams.nonEmpty
      case None => false
    }
  }

  def isMediaIQSite(siteKey: SiteKey): Boolean = {
    siteKey == SiteKey("381307cc7ee3235d296537715d66e7bb")
  }

  def isASLSite(siteKey: SiteKey): Boolean = {
    val sn = siteName(siteKey)
    sn.contains("(ASL)") || sn.startsWith("ASL -")
  }

  def siteUsesAuxiliaryClickEvent(siteKey: SiteKey): Boolean = {
    CampaignService.allCampaignMetaBySiteKey.getOrElse(siteKey, Seq.empty).exists(_.enableAuxiliaryClickEvent)
  }
}
