package com.gravity.interests.jobs.intelligence

import com.gravity.hbase.schema._
import com.gravity.interests.jobs.intelligence.SchemaContext._
import com.gravity.interests.jobs.intelligence.SchemaTypes._
import com.gravity.interests.jobs.intelligence.blacklist.{BlacklistRow, BlacklistColumns}
import com.gravity.interests.jobs.intelligence.hbase.{ConnectionPoolingTableManager, MetricsKeyLite, ScopedKey}
import com.gravity.interests.jobs.intelligence.operations.graphing.SiteConceptMetrics
import com.gravity.interests.jobs.intelligence.schemas.{ConversionTrackingParam, DollarValue}
import com.gravity.utilities.SplitHost
import com.gravity.utilities.analytics.{DateHourRange, DateMidnightRange, ReferrerSites, TimeSliceResolution}
import com.gravity.utilities.grvstrings._
import com.gravity.utilities.time.{DateHour, GrvDateMidnight}
import com.gravity.valueclasses.PubId
import com.gravity.valueclasses.ValueClassesForDomain._
import com.gravity.valueclasses.ValueClassesForUtilities._
import org.joda.time.DateTime
import play.api.libs.json._

import scala.collection._
import scalaz.std.option._
import scalaz.syntax.apply._



/*             )\._.,--....,'``.
 .b--.        /;   _.. \   _\  (`._ ,.
`=,-,-'~~~   `----(,_..'--(,_..'`-.;.'  */

class SitesTable extends HbaseTable[SitesTable, SiteKey, SiteRow](tableName = "sites", rowKeyClass = classOf[SiteKey], logSchemaInconsistencies = false, tableConfig = defaultConf)
with StandardMetricsColumns[SitesTable, SiteKey]
with RecommendationMetricsColumns[SitesTable, SiteKey]
with TopArticleRecommendationMetricsColumns[SitesTable, SiteKey]
with SponsoredMetricsColumns[SitesTable, SiteKey]
with RollupRecommendationMetricsColumns[SitesTable, SiteKey]
with BlacklistColumns[SitesTable, SiteKey]
with HasArticles[SitesTable, SiteKey]
with SponsoredPoolColumns[SitesTable, SiteKey] // can serve as a pool
with SponsoredPoolSponseeColumns[SitesTable, SiteKey] // can serve as a publisher in a pool
with SponsoredPoolSponsorColumns[SitesTable, SiteKey] // can serve as an advertiser in a pool
with InterestGraph[SitesTable, SiteKey]
with PartnerTaggingColumns[SitesTable, SiteKey]
with ConnectionPoolingTableManager
{
  override def rowBuilder(result: DeserializedResult): SiteRow = new SiteRow(result, this)

  val meta: Fam[String, Any] = family[String, Any]("meta", compressed = true)
  val guid: Col[String] = column(meta, "siteGuid", classOf[String])
  val name: Col[String] = column(meta, "name", classOf[String])
  val url: Col[String] = column(meta, "url", classOf[String])
  val logoImageUrl : Col[String]= column(meta, "liu", classOf[String])
  val keyPages: Col[CommaSet] = column(meta, "keyPages", classOf[CommaSet])
  val subSites: Col[Set[SiteKey]] = column(meta, "subSites", classOf[Set[SiteKey]])
  val superSites: Col[Set[SiteKey]] = column(meta, "superSites", classOf[Set[SiteKey]])
  val usesKeyPages: Col[Boolean] = column(meta, "usesKeyPages", classOf[Boolean])
  val partnerSite: Col[Boolean] = column(meta, "partnerSite", classOf[Boolean])
  val publicPages: Col[Boolean] = column(meta, "publicPages", classOf[Boolean])
  val hasBeacon: Col[Boolean] = column(meta, "hbe", classOf[Boolean])
  val isBeaconCrawlingDisabled: Col[Boolean] = column(meta, "bcd", classOf[Boolean])
  val isEnabled: Col[Boolean] = column(meta, "on", classOf[Boolean])
  // used to identify sites we are targeting for the IGI Product
  val siteType: Col[String] = column(meta, "siteType", classOf[String])
  // used to define forum type articles
  val feedUrl: Col[String] = column(meta, "fu", classOf[String])
  val feedSettings: Col[RssFeedSettings] = column(meta, "fs", classOf[RssFeedSettings])
  val lang: Col[String] = column(meta, "ln", classOf[String])
  // primarily for wordpress sites only, but this states the written language of a site
  val publisherTier: Col[String] = column(meta, "pt", classOf[String])
  val advertiserClass: Col[String] = column(meta, "ac", classOf[String])
  val redirectOrganics: Col[Boolean] = column(meta, "ro", classOf[Boolean])
  val redirectOrganicsUsingHttpStatus: Col[Boolean] = column(meta, "rohs", classOf[Boolean])
  val ignoreDomainCheck: Col[Boolean] = column(meta, "idc", classOf[Boolean])
  val saveCachedImages: Col[Boolean] = column(meta, "sci", classOf[Boolean])
  val chubClientId: Col[String] = column(meta, "chcl", classOf[String])
  val teamId: Col[String] = column(meta, "tid", classOf[String])

  /**
   * Maximum recommendations stored per user.  Will default to 20.
   */
  val maximumRecommendationsPerUser: Col[Int] = column(meta, "mur", classOf[Int])
  val frequencyCap: Col[Short] = column(meta, "fc", classOf[Short])
  val frequencyCapReuseProbability: Col[Float] = column(meta, "fcp", classOf[Float]) // once cap is reached, this is the probability an article will still be considered for recos

  // site config booleans
  val isAbstract: Col[Boolean] = column(meta, "abs", classOf[Boolean])
  // whether this site is just a placeholder for default settings or segments, with no actual data tied to it
  val supportsLiveRecommendations: Col[Boolean] = column(meta, "slr", classOf[Boolean])
  val dateCreated: Col[DateTime] = column(meta, "dcd", classOf[DateTime])
  val supportsLiveRecommendationMetrics: Col[Boolean] = column(meta, "slrm", classOf[Boolean])
  val supportsRecommendations: Col[Boolean] = column(meta, "sr", classOf[Boolean])
  val supportsRecentArticles: Col[Boolean] = column(meta, "sra", classOf[Boolean])
  val supportsArticleClustering: Col[Boolean] = column(meta, "sac", classOf[Boolean])
  val supportsTopicClustering: Col[Boolean] = column(meta, "sat", classOf[Boolean])
  val supportsArticleImageExtraction: Col[Boolean] = column(meta, "sim", classOf[Boolean])
  val requiresOutboundTrackingParams: Col[Boolean] = column(meta, "sobct", classOf[Boolean])
  val recommendationsRequireImages: Col[Boolean] = column(meta, "rar", classOf[Boolean])
  val isRssIngested: Col[Boolean] = column(meta, "rss", classOf[Boolean])
  val removeClickedArticlesFromRecommendations: Col[Boolean] = column(meta, "rac", classOf[Boolean])
  val googleAnalyticsWebPropertyId: Col[String] = column(meta, "gaid", classOf[String])
  val googleAnalyticsOrganicClicks: Col[Boolean] = column(meta, "gaoc", classOf[Boolean])
  val minArticleContentLength: Col[Int] = column(meta, "mac", classOf[Int])

  // Publisher Revenue Config
  val revenueGuarantee: Col[DollarValue] = column(meta, "hrg", classOf[DollarValue])
  val revenueSharePercentageAsByte: Col[Byte] = column(meta, "rsp", classOf[Byte])
  val revenueFixed: Col[DollarValue] = column(meta, "rfpm", classOf[DollarValue])
  val revenueModelType: Col[RevenueModelTypes.Type] = column(meta, "rmt", classOf[RevenueModelTypes.Type])

  val sections: Fam[SectionKey, String] = family[SectionKey, String]("sections", compressed = true)

  val topTopicsByUniques: Fam[DateMidnightRange, scala.Seq[SiteTopicKey]] = family[DateMidnightRange, Seq[SiteTopicKey]]("ttu", rowTtlInSeconds = 2592000, compressed = true)
  val topConceptsByUniques: Fam[DateMidnightRange, scala.Seq[SiteTopicKey]] = family[DateMidnightRange, Seq[SiteTopicKey]]("tcu", rowTtlInSeconds = 2592000, compressed = true)

  val igiReports: Fam[ReportKey, scala.Seq[ReportInterest]] = family[ReportKey, Seq[ReportInterest]]("ir", rowTtlInSeconds = 2592000, compressed = true)

  val uniques: Fam[DateMidnightRange, Long] = family[DateMidnightRange, Long]("un", rowTtlInSeconds = 2592000, compressed = true)
  val uniquesWithGraphs: Fam[DateMidnightRange, Long] = family[DateMidnightRange, Long]("ung", rowTtlInSeconds = 2592000, compressed = true)
  val uniquesHourly: Fam[DateHour, Long] = family[DateHour, Long]("unh", rowTtlInSeconds = 777600, compressed = true) // ttl of 9 days

  val activeDeals: Col[Set[ArticleKey]] = column(meta, "activedeals", classOf[Set[ArticleKey]])

  val homeLinks: Col[Set[ArticleKey]] = column(meta, "homelinks", classOf[Set[ArticleKey]])
  val homeLinksHourly: Fam[DateHour, Set[ArticleKey]] = family[DateHour, Set[ArticleKey]]("hlh", rowTtlInSeconds = 777600, compressed = true)

  val generalReports : Fam[String,Any]= family[String, Any]("rpt", compressed = true)
  val topSections: Col[Seq[SectionKey]] = column(generalReports, "ts", classOf[Seq[SectionKey]])
  val topSiteConcepts: Col[Seq[SiteConceptMetrics]] = column(generalReports, "tsc", classOf[Seq[SiteConceptMetrics]])

  val campaigns: Fam[CampaignKey, CampaignStatus.Type] = family[CampaignKey, CampaignStatus.Type]("cms", compressed = true)
  val campaignsByName: Fam[String,CampaignKey] = family[String, CampaignKey]("cbn", compressed = true)

  val blockedSites:Col[Set[SiteKey]] = column(meta, "blksites", classOf[Set[SiteKey]])
  val blockedCampaigns:Col[Set[CampaignKey]] = column(meta, "blkcamps", classOf[Set[CampaignKey]])
  val blockedArticles:Col[Set[(CampaignKey,ArticleKey)]] = column(meta, "blkarts", classOf[Set[(CampaignKey, ArticleKey)]])
  val blockedDomains:Col[Set[String]] = column(meta, "blkdoms", classOf[Set[String]])

  val existsInWarehouse:Col[Boolean] = column(meta, "wh", classOf[Boolean])

  val conversionTrackingParams:Col[Set[ConversionTrackingParam]] = column(meta, "ct", classOf[Set[ConversionTrackingParam]])

  val defaultRevenueModel:Col[RevenueModelData] = column(meta, "drm", classOf[RevenueModelData])
}

class SiteRow(result: DeserializedResult, table: SitesTable) extends HRow[SitesTable, SiteKey](result, table)
with StandardMetricsRow[SitesTable, SiteKey, SiteRow]
with RecommendationMetricsRow[SitesTable, SiteKey, SiteRow]
with TopArticleRecommendationMetricsRow[SitesTable, SiteKey, SiteRow]
with SponsoredMetricsRow[SitesTable, SiteKey, SiteRow]
with HasArticlesRow[SitesTable, SiteKey, SiteRow]
with BlacklistRow[SitesTable, SiteKey, SiteRow]
with RollupRecommendationMetricsRow[SitesTable, SiteKey, SiteRow]
with SponsoredPoolRow[SitesTable, SiteKey, SiteRow]
with SponsoredPoolSponseeRow[SitesTable, SiteKey, SiteRow]
with SponsoredPoolSponsorRow[SitesTable, SiteKey, SiteRow]
with PartnerTaggingRow[SitesTable, SiteKey, SiteRow]
with InterestGraphedRow[SitesTable, SiteKey, SiteRow] {
  import com.gravity.logging.Logging._

  lazy val siteKey:SiteKey = rowid
  lazy val siteGuid:Option[String] = column(_.guid)
  lazy val sg:SiteGuid = siteGuidOrNoGuid.asSiteGuid
  lazy val siteGuidOrNoGuid:String = siteGuid.getOrElse {
    warn(new Exception, s"Encountered a site $siteKey with no site GUID")
    "NO_GUID"
  }
  lazy val guid:Option[SiteGuid] = siteGuid map SiteGuid.apply
  lazy val reportMap:Map[ReportKey, Seq[ReportInterest]] = family(_.igiReports)
  lazy val reportKeySet:Set[ReportKey] = familyKeySet(_.igiReports)
  lazy val name:Option[String] = column(_.name)
  lazy val nameOrNoName:String = name.getOrElse {
    warn(s"Encountered a site $siteKey with no site name")
    "NO NAME"
  }
  lazy val url:Option[String] = column(_.url)
  lazy val urlDomain:Option[String] = url.flatMap(_.tryToURL).map(ReferrerSites.normalizedHost)
  lazy val domain: Option[Domain] = url.flatMap(u => SplitHost.domain(u.asUrl))
  lazy val domainOrEmpty:Domain = domain.getOrElse(Domain.empty)
  lazy val defaultPubId:PubId = PubId.apply(sg, domain.getOrElse(Domain.empty), PartnerPlacementId.empty)
  lazy val logoImageUrlOption:Option[String] = column(_.logoImageUrl)
  lazy val keyPages:Option[CommaSet] = column(_.keyPages)
  lazy val hasBeacon: Boolean = column(_.hasBeacon).getOrElse(true)
  lazy val isBeaconCrawlingDisabled:Boolean = column(_.isBeaconCrawlingDisabled).getOrElse(false)
  lazy val isEnabled:Boolean = column(_.isEnabled).getOrElse(false) //No longer meaningful
  lazy val isPartner:Boolean = column(_.partnerSite).getOrElse(false)
  lazy val isAbstract:Boolean = column(_.isAbstract).getOrElse(false)
  lazy val usesKeyPages:Boolean = column(_.usesKeyPages).getOrElse(false)
  lazy val supportsLiveRecommendations:Boolean = column(_.supportsLiveRecommendations).getOrElse(false)
  lazy val supportsLiveRecommendationMetrics:Boolean = column(_.supportsLiveRecommendationMetrics).getOrElse(false)
  lazy val supportsRecommendations:Boolean = column(_.supportsRecommendations).getOrElse(false)
  lazy val supportsRecentArticles:Boolean = column(_.supportsRecentArticles).getOrElse(true)
  lazy val supportsArticleClustering:Boolean = column(_.supportsArticleClustering).getOrElse(false)
  lazy val supportsTopicClustering:Boolean = column(_.supportsTopicClustering).getOrElse(false)
  lazy val supportsArticleImageExtraction:Boolean = column(_.supportsArticleImageExtraction).getOrElse(false)
  lazy val requiresOutboundTrackingParams:Boolean = column(_.requiresOutboundTrackingParams).getOrElse(false)
  
  lazy val recommendationsRequireImages:Boolean = column(_.recommendationsRequireImages).getOrElse(false)
  lazy val googleAnalyticsWebPropertyId:String = column(_.googleAnalyticsWebPropertyId).getOrElse(emptyString)
  lazy val googleAnalyticsOrganicClicks:Boolean = column(_.googleAnalyticsOrganicClicks).getOrElse(false)
  lazy val removeClickedArticlesFromRecommendations:Boolean = column(_.removeClickedArticlesFromRecommendations).getOrElse(false)
  lazy val redirectOrganics:Boolean = column(_.redirectOrganics).getOrElse(false)
  lazy val redirectOrganicsUsingHttpStatus:Boolean = column(_.redirectOrganicsUsingHttpStatus).getOrElse(false)
  lazy val ignoreDomainCheck:Boolean = column(_.ignoreDomainCheck).getOrElse(false)
  lazy val saveCachedImages:Boolean = column(_.saveCachedImages).getOrElse(false)
  lazy val chubClientId:String = column(_.chubClientId).getOrElse(emptyString)

  lazy val uniques:Map[DateMidnightRange,Long] = family(_.uniques)
  lazy val uniquesHourly:Map[DateHour,Long] = family(_.uniquesHourly)
  lazy val uniquesWithGraphs:Map[DateMidnightRange,Long] = family(_.uniquesWithGraphs)
  lazy val maximumRecommendationsPerUser:Int = column(_.maximumRecommendationsPerUser).getOrElse(20)
  lazy val frequencyCap:Short = column(_.frequencyCap).getOrElse(0.toShort)
  // freq cap of zero means no freq cap is set
  lazy val frequencyCapReuseProbability:Float = column(_.frequencyCapReuseProbability).getOrElse(0.5f)
  // default of 50% chance an already read article will be recommended
  lazy val sectionKeys:Set[SectionKey] = familyKeySet(_.sections)
  lazy val feedUrl:Option[String] = column(_.feedUrl)
  lazy val feedSettings:Option[RssFeedSettings] = column(_.feedSettings)
  lazy val lang:String = column(_.lang).getOrElse("en")

  lazy val teamId:Option[String] = column(_.teamId)

  def checkColumnStringEquals(col: Option[String], value: String): Boolean = col match {
    case Some(str) if str == value => true
    case _ => false
  }

  lazy val publisherTier:Option[String] = column(_.publisherTier)
  lazy val advertiserClass:Option[String] = column(_.advertiserClass)

  lazy val tier1:Boolean = checkColumnStringEquals(publisherTier, "1")
  lazy val tier2:Boolean = checkColumnStringEquals(publisherTier, "2")
  lazy val tier3:Boolean = checkColumnStringEquals(publisherTier, "3")
  lazy val tier4:Boolean = checkColumnStringEquals(publisherTier, "4")
  lazy val class1:Boolean = checkColumnStringEquals(advertiserClass, "1")
  lazy val class2:Boolean = checkColumnStringEquals(advertiserClass, "2")
  lazy val class3:Boolean = checkColumnStringEquals(advertiserClass, "3")

  lazy val campaigns:Map[CampaignKey, CampaignStatus.Type] = family(_.campaigns)
  lazy val campaignKeys:Set[CampaignKey] = familyKeySet(_.campaigns)
  lazy val campaignsByName:Map[String,CampaignKey] = family(_.campaignsByName)

  // blacklist settings
  lazy val blockedSites:Set[SiteKey] = column(_.blockedSites).getOrElse(Set.empty[SiteKey])
  lazy val blockedCampaigns:Set[CampaignKey] = column(_.blockedCampaigns).getOrElse(Set.empty[CampaignKey])
  lazy val blockedArticles:Set[(CampaignKey,ArticleKey)] = column(_.blockedArticles).getOrElse(Set.empty[(CampaignKey, ArticleKey)])
  lazy val blockedDomains:Set[String] = column(_.blockedDomains).getOrElse(Set.empty[String])

  lazy val topSectionKeys:Seq[SectionKey] = column(_.topSections).getOrElse(Seq.empty[SectionKey])
  lazy val topSiteConcepts:Seq[SiteConceptMetrics] = column(_.topSiteConcepts).getOrElse(Seq.empty[SiteConceptMetrics])
  // fuhgetuhboudit crawler... we be RSSin' dis now!
  lazy val isRssIngested:Boolean = column(_.isRssIngested).getOrElse(false)

  lazy val feedSettingsOrDefault:RssFeedSettings = feedSettings.getOrElse(RssFeedSettings.default)

  // we have an article, from article we get site, which is this siterow, from this siterow, we want all it's sponso
  lazy val sponsorPoolSiteKeys:Set[SiteKey] = column(_.poolsSponsorIn).getOrElse(Set.empty[SiteKey])

  lazy val supportsSponsoredRecommendations:Boolean = column(_.poolsSponseeIn) match {
    case Some(sks) if sks.nonEmpty => true
    case _ => false
  }

  lazy val activeDeals:Set[ArticleKey] = column(_.activeDeals).getOrElse(Set.empty[ArticleKey])

  lazy val superSites:Set[SiteKey] = column(_.superSites).getOrElse(Set.empty[SiteKey])

  lazy val subSites:Set[SiteKey] = column(_.subSites).getOrElse(Set.empty[SiteKey])

  def isKeyPage(url: String): Boolean = keyPages match {
    case Some(kps) if kps.items.contains(url) => true
    case _ => false
  }

  def isSuperSite: Boolean = subSites.nonEmpty

  def numberOfUniqueGraphedUsers(range: DateMidnightRange): Long = {
    uniquesWithGraphs.getOrElse(range, uniques.getOrElse(range, 0l))
  }

  def uniqueGraphedUsers(ranges: Set[DateMidnightRange]): Map[DateMidnightRange, Long] = {
    ranges.map(r => r -> numberOfUniqueGraphedUsers(r)).toMap
  }

  /** Return site's key as a string of individual stringified bytes, useful for heap dumps
    *
    */
  def keyAsByteString():String = {
    val siteId = siteKey.siteId
    val siteByteArray = new Array[Byte](8)
    var i = 0
    var siteBytes = siteId
    while (i < 8) {
      val byte = siteBytes.toByte
      siteByteArray(i) = byte
      siteBytes = siteBytes >> 8
      i = i + 1
    }
    siteByteArray.mkString("(", ",", ")")
  }

  //Call in a way that's standard with the stuff in HasArticles
  //    def getRecentArticlesAndData() : Option[Seq[(ArticleAndMetrics,ArticleRow)]] = {
  //
  //    }


  lazy val recentArticlesWithViralMetrics:Map[ArticleKey,ArticleRow] = Schema.Articles.query2.withKeys(recentArticleKeys).withFamilies(_.meta, _.viralMetrics).executeMap()

  def hourlyMetricsWithinSorted(period: TimeSliceResolution, sg: String):Seq[(DateHour,MetricsWithVisitors)] = hourlyMetricsWithin(period, sg).toSeq.sortBy(_._1.getMillis)

  def hourlyMetricsWithinSorted(hourRange: DateHourRange, sg: String):Seq[(DateHour,MetricsWithVisitors)] = hourlyMetricsWithin(hourRange, sg).toSeq.sortBy(_._1.getMillis)

  def hourlyMetricsWithin(period: TimeSliceResolution, sg: String): Map[DateHour, MetricsWithVisitors] = MetricsWithVisitors.fromHourlyWithin(sg, standardMetricsHourlyOld, uniquesHourly, period)

  def hourlyMetricsWithin(hourRange: DateHourRange, sg: String): Map[DateHour, MetricsWithVisitors] = MetricsWithVisitors.fromHourlyWithin(sg, standardMetricsHourlyOld, uniquesHourly, hourRange)

  // default to no existence in warehouse
  lazy val existsInWarehouse:Boolean = column(_.existsInWarehouse).getOrElse(false)


  val poolsSponsoredIn:Set[SiteKey] = column(_.poolsSponsorIn).getOrElse(Set.empty[SiteKey])
  val sponsorInPools:Set[SiteKey] = column(_.poolsSponseeIn).getOrElse(Set.empty[SiteKey])

  val isPoolWithSponsees: Set[ScopedKey] = column(_.sponseesInPool).getOrElse(Set.empty[ScopedKey])
  val isPoolWithSponsors:Set[ScopedKey] = column(_.sponsorsInPool).getOrElse(Set.empty[ScopedKey])

  def isAdvertiser: Boolean = poolsSponsoredIn.nonEmpty
  def isPublisher: Boolean = sponsorInPools.nonEmpty
  def isSponsoredPool: Boolean = isPoolWithSponsors.nonEmpty

  def sponsoredMetricsWithRollupsGroupedBy[K](grouper: (MetricsKeyLite) => K, filter: (MetricsKeyLite) => Boolean = s => true): Map[K, RecommendationMetrics] = {

    val groupedSponsored = sponsoredMetrics.filterKeys(filter).groupBy(kv => grouper(kv._1))
    val groupedRollups = rollupRecommendationMetrics.filterKeys(filter).groupBy(kv => grouper(kv._1))

    val allKeys = groupedSponsored.keySet ++ groupedRollups.keySet

    if (allKeys.isEmpty) return Map.empty[K, RecommendationMetrics]

    (for (key <- allKeys) yield {
      val articleMetrics = groupedSponsored.getOrElse(key, Map.empty[SponsoredMetricsKey, Long]).toSeq.foldLeft(RecommendationMetrics.empty) {
        case (rm: RecommendationMetrics, kv: (SponsoredMetricsKey, Long)) => rm + kv._1.recoMetrics(kv._2)
      }

      val unitMetrics = groupedRollups.getOrElse(key, Map.empty[RollupRecommendationMetricKey, Long]).toSeq.foldLeft(RecommendationMetrics.empty) {
        case (rm: RecommendationMetrics, kv: (RollupRecommendationMetricKey, Long)) => rm + kv._1.recoMetrics(kv._2)
      }

      key -> articleMetrics.withRollups(unitMetrics)
    }).toMap
  }

  lazy val sponsoredMetricsByDayWithRollupSorted: scala.Seq[(GrvDateMidnight, RecommendationMetrics)] = sponsoredMetricsWithRollupsGroupedBy(_.dateHour.toGrvDateMidnight).toSeq.sortBy(-_._1.getMillis)

  lazy val sponsoredMetricsByHourWithRollupSorted: scala.Seq[(DateHour, RecommendationMetrics)] = sponsoredMetricsWithRollupsGroupedBy(_.dateHour).toSeq.sortBy(-_._1.getMillis)

  lazy val revenueConfig: RevenueConfig = column(_.revenueModelType) tuple column(_.revenueSharePercentageAsByte) tuple column(_.revenueFixed) tuple column(_.revenueGuarantee) match {
    case Some((((revenueModelType, revenuePercentage), revenueFixed), revenueGuarantee)) => RevenueConfig(revenueModelType, revenuePercentage.toDouble / 100, revenueFixed, revenueGuarantee)
    case _ => RevenueConfig.default
  }

  lazy val defaultRevenueModel: Option[RevenueModelData] = column(_.defaultRevenueModel)

  lazy val conversionTrackingParams: Set[ConversionTrackingParam] = column(_.conversionTrackingParams).getOrElse(Set.empty[ConversionTrackingParam])
}

object SiteRow {
 import com.gravity.logging.Logging._
  val basicSiteJsonWrites: Writes[SiteRow] = Writes[SiteRow](sr => Json.obj(
    "key" -> sr.rowid.stringConverterSerialized,
    "guid" -> sr.siteGuidOrNoGuid,
    "name" -> sr.nameOrNoName
  ))
}