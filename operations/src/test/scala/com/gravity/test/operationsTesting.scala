package com.gravity.test

import java.io.File
import java.util.concurrent.atomic.AtomicInteger

import com.gravity.algorithms.model.{ArticleCandidateKey, FeatureSettings}
import com.gravity.data.configuration.{ConfigurationQueriesTestSupport, ConfigurationQueryService, ContentGroupInsert}
import com.gravity.domain.aol._
import com.gravity.domain.articles.{ContentGroupSourceTypes, ContentGroupStatus}
import com.gravity.domain.gms.{GmsArticleStatus, GmsRoute}
import com.gravity.domain.recommendations.ContentGroup
import com.gravity.domain.rtb.gms.ContentGroupIdToPinnedSlot
import com.gravity.domain.{BeaconEvent, GrvDuration}
import com.gravity.interests.jobs.hbase.HBaseConfProvider
import com.gravity.interests.jobs.intelligence._
import com.gravity.interests.jobs.intelligence.hbase.{GravityTestDataHelpers, HBaseTestEnvironment, HBaseTestTableBroker, ScopedKey}
import com.gravity.interests.jobs.intelligence.operations._
import com.gravity.interests.jobs.intelligence.schemas.DollarValue
import com.gravity.logging.Logging._
import com.gravity.utilities.analytics.URLUtils
import com.gravity.utilities.analytics.articles.AolMisc
import com.gravity.utilities.cache.PermaCacher
import com.gravity.utilities.components.FailureResult
import com.gravity.utilities.grvz._
import com.gravity.utilities.time.DateHour
import com.gravity.utilities.{Settings => GrvSettings, _}
import com.gravity.valueclasses.ValueClassesForDomain.SiteGuid
import com.sksamuel.elastic4s.{ElasticClient, ElasticsearchClientUri}
import org.apache.hadoop.fs.Path
import org.elasticsearch.action.admin.cluster.node.info.NodesInfoRequest
import org.elasticsearch.common.settings.Settings
import org.elasticsearch.common.settings.{Settings => ESSettings}
import org.joda.time.DateTime
import org.junit.Assert._
import play.api.libs.json.{Json, Writes}

import scala.collection.{Map, Seq, Set}
import scala.util.{Random, Try}
import scalaz.syntax.std.option._
import scalaz.syntax.validation._
import scalaz.{Failure, NonEmptyList, Success, Validation, ValidationNel}

/*             )\._.,--....,'``.
.b--.        /;   _.. \   _\  (`._ ,.
`=,-,-'~~~   `----(,_..'--(,_..'`-.;.'  */


trait operationsTesting extends GravityTestDataHelpers with domainTesting with HBaseTestEnvironment with sparkTesting with ConfigurationQueriesTestSupport with DataMartTesting {

  def configQuerySupport = ConfigurationQueryService.queryRunner

  /**
   * Gets a temp dir that is randomized and local hdfs friendly.
    *
    * @param relativePath This is forced to be relative, a leading / will be trimmed.
   * @return
   */
  def getHdfsTestPath(relativePath: String) = {
    val path = if (relativePath.startsWith("/")) relativePath.substring(1, relativePath.length) else relativePath
    val guid = HashUtils.randomMd5
    s"file://${com.gravity.utilities.Settings.tmpDir}/$guid/$path"
  }

  /**
   * Runs code in the context of expecting an hdfs test path.
   */
  def withHdfsTestPath[T](relativePath:String)(work: String => T) = {
    val path =getHdfsTestPath(relativePath)

    val res = work(path)

    HBaseConfProvider.getConf.fs.delete(new Path(path), true)
    res
  }

  private def getHdfsTestPathAndPrefix(relativePath: String) = {
    val path = if (relativePath.startsWith("/")) relativePath.substring(1, relativePath.length) else relativePath
    val guid = HashUtils.randomMd5
    val prefix = s"file://${com.gravity.utilities.Settings.tmpDir}/$guid"
    val resultPath = s"$prefix/$path"
    (prefix, resultPath)
  }

  def withHdfsTestPathAndPrefix[T](relativePath:String)(work: (String, String) => T) = {
    val (prefix, path) = getHdfsTestPathAndPrefix(relativePath)

    val res = work(prefix, path)

    HBaseConfProvider.getConf.fs.delete(new Path(path), true)
    res
  }

  case class ElasticTestProperties(client: ElasticClient, clusterName: String, httpPort: Int, transportPort: Int, clusterFullName: String = "localhost")

  def withLocallyRunningExternalProcessElasticSearch(work: (ElasticTestProperties) => Unit): Unit = {

    val clusterName = "elasticsearch"

    val settings = Settings.settingsBuilder()
      .put("cluster.name", clusterName)
    val client = ElasticClient.transport(ElasticsearchClientUri("localhost", 9300))
    val etp = ElasticTestProperties(client, clusterName, 9200, 9300)

    try {
      work(etp)
    } finally Try(client.close())
  }

  def withAwsDevElasticSearch(work: (ElasticTestProperties) => Unit): Unit = {

    val clusterName: String = GrvSettings.getProperty("grv-prod-es-data.es.cluster.name")
    val clusterFullName: String = GrvSettings.getProperty("grv-prod-es-data.es.cluster.fullname")

    val settings = ESSettings.settingsBuilder()
      .put("cluster.name", clusterName)
      .build

    val e = ElasticsearchClientUri(clusterFullName, 9300)
    val client = ElasticClient.transport(settings, e)

    val etp = ElasticTestProperties(client, clusterName, 9200, 9300, clusterFullName = clusterFullName)

    try {
      work(etp)
    } finally Try(client.close())
  }

  def withInMemoryElasticSearchClient(work: (ElasticTestProperties) => Unit): Unit = {
    val clusterName = HashUtils.randomMd5
    val clusterLocation = s"${com.gravity.utilities.Settings.tmpDir}/$clusterName"

    val dir = new File(clusterLocation)
    assertTrue(dir.mkdir)

    try {
      val settings = Settings.settingsBuilder()
        .put("cluster.name", clusterName)
        .put("path.home", s"$clusterLocation/")
      val client = ElasticClient.local(settings.build)

      // in a local client context, there is only one node, so we'll just get "all"
      val resp = client.client.admin().cluster().nodesInfo(new NodesInfoRequest().all()).actionGet()
      val info = resp.iterator().next()

      // in a local client context, local transport is used, so port isn't particularly relevant, will always be zero
      // but we'll get it anyway, just for clarity's sake
      val defaultTransportPort: Int = info.getTransport.getAddress.publishAddress().getPort
      val defaultHttpPort: Int = info.getHttp.getAddress.publishAddress().getPort

      val etp = ElasticTestProperties(client, clusterName, defaultHttpPort, defaultTransportPort)

      try {
        work(etp)
      } finally Try(client.close())
    } finally Try(dir.delete())
  }

  def getGuids(num: Int): String = {
    (1 to num).map{_ => java.util.UUID.randomUUID.toString}.mkString("!")
  }

  case class SiteContext(name: String, guid: String, baseUrl: String, row: SiteRow)

  case class MultiSiteContext(siteRows: Seq[SiteContext]) {
    def +(that: MultiSiteContext): MultiSiteContext = MultiSiteContext(siteRows ++ that.siteRows)
  }

  def withCleanup(work: => Unit) {
    PermaCacher.restart()
    HBaseTestTableBroker.reset()
    GmsPinnedDataCache.clear()

    work

    PermaCacher.restart()
    HBaseTestTableBroker.reset()
    GmsPinnedDataCache.clear()
  }

  def withSites[T](metas: MultiSiteMetaContext)(work: (MultiSiteContext) => T): T = {
    val sites = metas.metas.map(meta=>{
        val row = makeSite(meta.siteGuid, meta.siteName, meta.baseUrl).valueOr(fails => throw new RuntimeException(fails.mkString(", ")))
        SiteContext(meta.siteName, meta.siteGuid, meta.baseUrl, row)
      })
    val res = work(MultiSiteContext(sites))
    res
  }


  def withSites[T](count: Int = 1)(work: (MultiSiteContext) => T): T = {
    withSiteMetas(count) {siteMetas=>
      withSites(siteMetas) {sites=>
        work(sites)
      }
    }
  }

  def withSiteHourlyRecoMetrics[T](hoursPerSite: Int, lastHour: DateHour = DateHour.currentHour)(context: MultiSiteContext)(work: (Map[SiteContext, Map[ArticleRecommendationMetricKey, Long]]) => T): T = {
    val outputMap = Map((for {
      sc <- context.siteRows
    } yield {
        val metricsMap = Map((for {
          hour <- 0 until hoursPerSite
        } yield {
            // probably should do something special and use real placement id and bucket ids and maybe even allow specifying the countBy type and perhaps the value, but keeping it simple for now
            val key = ArticleRecommendationMetricKey(lastHour.minusHours(hour), RecommendationMetricCountBy.articleImpression, -1, sc.row.rowid, -1)
            key -> 1L
        }): _*)
        // put to SitesTable
        val putOp = Schema.Sites.put(sc.row.rowid, writeToWAL = false)
        putOp.valueMap(_.recommendationMetrics, metricsMap)
        putOp.execute()
        sc -> metricsMap
    }): _*)
    work(outputMap)
  }
  def withSiteHourlyRollupMetrics[T](hoursPerSite: Int, lastHour: DateHour = DateHour.currentHour)(context: MultiSiteContext)(work: (Map[SiteContext, Map[RollupRecommendationMetricKey, Long]]) => T): T = {
    val outputMap = Map((for {
      sc <- context.siteRows
    } yield {
        val metricsMap = Map((for {
          hour <- 0 until hoursPerSite
        } yield {
            // probably should do something special and use real placement id and bucket ids and maybe even allow specifying the countBy type and perhaps the value, but keeping it simple for now
            val key = RollupRecommendationMetricKey(lastHour.minusHours(hour), sc.row.rowid, -1, -1, countBy = RecommendationMetricCountBy.articleImpression)
            key -> 1L
          }): _*)
        // put to SitesTable
        val putOp = Schema.Sites.put(sc.row.rowid, writeToWAL = false)
        putOp.valueMap(_.rollupRecommendationMetrics, metricsMap)
        putOp.execute()
        sc -> metricsMap
      }): _*)
    work(outputMap)
  }
  def withSiteHourlySponsoredMetrics[T](hoursPerSite: Int, lastHour: DateHour = DateHour.currentHour)(context: MultiSiteContext)(work: (Map[SiteContext, Map[SponsoredMetricsKey, Long]]) => T): T = {
    val outputMap = Map((for {
      sc <- context.siteRows
    } yield {
        val metricsMap = Map((for {
          hour <- 0 until hoursPerSite
        } yield {
            // probably should do something special and use real placement id and bucket ids and maybe even allow specifying the countBy type and perhaps the value, but keeping it simple for now
            //val key = RollupRecommendationMetricKey(lastHour.minusHours(hour), sc.row.rowid, -1, -1, countBy = RecommendationMetricCountBy.articleImpression)
            val key = SponsoredMetricsKey(lastHour.minusHours(hour), RecommendationMetricCountBy.articleImpression, -1, sc.row.rowid, -1, -1, -1, CampaignKey.minValue, DollarValue.minValue)
            key -> 1L
          }): _*)
        // put to SitesTable
        val putOp = Schema.Sites.put(sc.row.rowid, writeToWAL = false)
        putOp.valueMap(_.sponsoredMetrics, metricsMap)
        putOp.execute()
        sc -> metricsMap
      }): _*)
    work(outputMap)
  }

  case class ArticleMeta(url: String, title: String, siteGuid: String) {
    def articleKey = ArticleKey(url)
  }

  case class ArticleContext(url: String, title: String, site: SiteContext, row: ArticleRow)

  case class MultiArticleContext(articles: Seq[ArticleContext])

  case class BeaconContext(beacon: BeaconEvent, article: ArticleContext)

  case class MultiBeaconContext(beacons: Seq[BeaconContext])

  case class UserSiteContext(userGuid: String, siteGuid: String, row: UserSiteRow)

  case class MultiUserSiteContext(users: Seq[UserSiteContext])

  case class MultiCampaignContext(campaigns: Seq[CampaignContext], sites:MultiSiteContext) {
    def +(that: MultiCampaignContext): MultiCampaignContext = MultiCampaignContext(campaigns ++ that.campaigns, sites + that.sites)
  }

  case class ContentGroupArticleContext(candidateKey: ArticleCandidateKey, article: ArticleContext)
  case class ContentGroupContext(contentGroup: ContentGroup, articles: Seq[ContentGroupArticleContext])

  case class MultiContentGroupContext(contentGroups: Seq[ContentGroupContext]) {
    def bySourceKey: Map[ScopedKey, Seq[ContentGroupContext]] = contentGroups.groupBy(_.contentGroup.sourceKey)
    def allContentGroups: NonEmptyList[ContentGroup] = contentGroups.map(_.contentGroup).toNel.getOrElse(fail("Unable to get content groups"))
    def +(that: MultiContentGroupContext): MultiContentGroupContext = MultiContentGroupContext(contentGroups ++ that.contentGroups)
  }

  case class CampaignContext(campaign: CampaignRow, site: SiteContext, campaignName: String)

  case class CampaignArticlesContext(campaign: CampaignContext, articles: MultiArticleContext)

  case class MultiCampaignArticlesContext(articleSets: Seq[CampaignArticlesContext], campaigns: MultiCampaignContext) {
    def +(that: MultiCampaignArticlesContext): MultiCampaignArticlesContext = MultiCampaignArticlesContext(articleSets ++ that.articleSets, campaigns + that.campaigns)
  }

  case class ExchangeContext(exchange: ExchangeRow, sites: MultiSiteContext, hostSite: SiteContext, siteContentGroups: MultiContentGroupContext, exchangeContentGroup: ContentGroupContext) {
    lazy val nonHostSites: MultiSiteContext = sites.copy(siteRows = sites.siteRows.filter(_.guid != hostSite.guid))
  }

  def getSiteContext(guid: String): Option[SiteContext] = {
    Schema.Sites.query2.withKey(SiteKey(guid)).withAllColumns.singleOption().map(site => SiteContext(site.name.getOrElse(""), site.siteGuidOrNoGuid, site.url.getOrElse(""), site))

  }

  // Create a bunch of Sites, with Content Groups and Campaigns.
  def withSitesCampaignsAndContentGroups(numSites: Int, numCampaigns: Int, isGmsManaged: Boolean)(work: List[(SiteRow, CampaignRow, ContentGroup)] => Unit): Unit = {
    withSites(numSites) { multiSiteCtx =>
      withCampaigns(numCampaigns, multiSiteCtx) { multiCampaignCtx =>
        withCampaignArticles(0, multiCampaignCtx) { multiCampaignArticlesContext =>
          withContentGroups(multiCampaignArticlesContext, isGmsManaged = isGmsManaged) { multiContentGroupCtx =>
            val contentGroupCtxBySourceKey = multiContentGroupCtx.bySourceKey
            val list = (for {
              CampaignArticlesContext(campaign, _) <- multiCampaignArticlesContext.articleSets
              contentGroupContext <- contentGroupCtxBySourceKey.get(campaign.campaign.campaignKey.toScopedKey).toList.flatten
              (siteRow, campaignRow, contentGroup) = (campaign.site.row, campaign.campaign, contentGroupContext.contentGroup)
            } yield {
              (siteRow, campaignRow, contentGroup)
            }).toList

            assert(list.size == multiContentGroupCtx.contentGroups.size)

            work(list)
          }
        }
      }
    }
  }

  var grvMapCounter = new AtomicInteger(0)

  /**
   * Generates a grv:map for an ArtGrvMap.OneScopeKey that is either empty or has a small number of key/value pairs.
   *
   * @param gmScopeKey The ArtGrvMap.OneScopeKey to be used as the HBase key for this grv:map.
   * @return A Map[ArtGrvMap.OneScopeKey, ArtGrvMapMetaVal] that is either empty or that has one ArtGrvMap.OneScopeKey and between 1 and 4 key-value pairs.
   */
  def withGrvMap(gmScopeKey: ArtGrvMap.OneScopeKey): ArtGrvMap.AllScopesMap = {
    val num = grvMapCounter.getAndIncrement

    val allEntries = List(
      "answer"    -> ArtGrvMapMetaVal(isPrivate = false, XsdIntType.typeString    , (num * 1000 + 42).toString),
      "question"  -> ArtGrvMapMetaVal(isPrivate = true , XsdStringType.typeString , s"Number #$num: What's 6x9?"),
      "cat.name"  -> ArtGrvMapMetaVal(isPrivate = false, XsdStringType.typeString , "Tiger Lily"),
      "cat.born"  -> ArtGrvMapMetaVal(isPrivate = false, XsdIntType.typeString    , "1996"),
      "cat.alive" -> ArtGrvMapMetaVal(isPrivate = false, XsdBooleanType.typeString, "false")
      )

    val useEntries = allEntries.take(num % 6)

    if (useEntries.isEmpty)
      Map()
    else
      Map(gmScopeKey -> Map(useEntries:_*))
  }

  def withDlugGrvMap = {
    withUniGrvMap(GmsRoute(AolMisc.dynamicLeadDLUGCampaignKey))
  }

  def withGmsGrvMap(siteKey: SiteKey) = {
    withUniGrvMap(GmsRoute(siteKey)).mapValues (_ ++ Map(
      AolGmsFieldNames.SiteId -> ArtGrvMapMetaVal(siteKey.siteId.toString, isPrivate = false)
    ))
  }

  def withUniGrvMap(gmsRoute: GmsRoute): ArtGrvMap.AllScopesMap = Map(gmsRoute.oneScopeKey -> Map(
    AolUniFieldNames.Title -> ArtGrvMapMetaVal("Article title" + math.random, isPrivate = false),
    AolUniFieldNames.Status -> ArtGrvMapMetaVal(GmsArticleStatus.Live.name, isPrivate = false),
    AolUniFieldNames.CategoryText -> ArtGrvMapMetaVal("Test Category", isPrivate = false),
    AolUniFieldNames.SourceText -> ArtGrvMapMetaVal("Test Source", isPrivate = false),
    AolUniFieldNames.SourceLink -> ArtGrvMapMetaVal("http://example.com/source", isPrivate = false),
    AolUniFieldNames.Summary -> ArtGrvMapMetaVal("Test summary", isPrivate = false),
    AolUniFieldNames.Headline -> ArtGrvMapMetaVal("Test headline" + math.random, isPrivate = false),
    AolUniFieldNames.SecondaryHeader -> ArtGrvMapMetaVal("Test secondary header", isPrivate = false),
    AolUniFieldNames.Image -> ArtGrvMapMetaVal("http://example.com/image", isPrivate = false),
    AolUniFieldNames.ShowVideoIcon -> ArtGrvMapMetaVal(value = false, isPrivate = false),
    AolUniFieldNames.UpdatedTime -> ArtGrvMapMetaVal((new DateTime().getMillis / 1000).toInt, isPrivate = false)
  ))

  def withGrvMap(optScopeKey: Option[ScopedKey], namespace: String = ""): ArtGrvMap.AllScopesMap = withGrvMap(ArtGrvMap.toOneScopeKey(optScopeKey, namespace))

  case class ArticleMetaContext(title:String, url:String, grvMapKey: Option[ScopedKey], site: SiteContext) {
    val articleKey = ArticleKey(url)
  }
  case class MultiArticleMetaContext(articles: Seq[ArticleMetaContext]) {

    def bySite = articles.groupBy(_.site)
  }

  def withArticles[T](articleMetas: MultiArticleMetaContext)(work: (MultiArticleContext) => T): T = {

    articleMetas.articles.foreach{articleMeta=>
      val articleKey = makeArticle(
        url = articleMeta.url,
        siteGuid = articleMeta.site.guid,
        title = articleMeta.title,
        allArtGrvMap = withGrvMap(articleMeta.grvMapKey)
      )
      SiteService.addToRecentArticles(SiteKey(articleMeta.site.guid), Seq(articleKey -> new DateTime()))

    }

    val articleContexts = Schema.Articles.query2.withKeys(articleMetas.articles.map(_.articleKey).toSet).withAllColumns.executeMap().values.toSeq.map(row => ArticleContext(row.url, row.title, getSiteContext(row.siteGuid).get, row))
    val multiArticleContext = MultiArticleContext(articleContexts)
    work(multiArticleContext)

  }


  def withArticles[T](count: Int = 10, context: MultiSiteContext)(work: (MultiArticleContext) => T): T = {
    val articleMetas = context.siteRows.flatMap(site => {
      val articles = (0 until count).map {
        idx =>
          val title = s"Article $idx from test site ${site.name} without campaign"
          val url = s"${site.baseUrl}/article${idx}_nocampaign.html"

          val grvMapKey =
            if ((idx & 1) != 0) None else Option(SiteKey(site.guid).toScopedKey)

          ArticleMetaContext(url, title, grvMapKey, site)
      }

      articles
    })
    val multiContext = MultiArticleMetaContext(articleMetas)
    withArticles(multiContext)(work)

  }

  def withArticleHourlyRecoMetrics[T](hoursPerArticle: Int, lastHour: DateHour = DateHour.currentHour)(context: MultiArticleContext)(work: (Map[ArticleContext, Map[ArticleRecommendationMetricKey, Long]]) => T): T = {
    val outputMap = Map((for {
      sc <- context.articles
    } yield {
        val metricsMap = Map((for {
          hour <- 0 until hoursPerArticle
        } yield {
            // probably should do something special and use real placement id and bucket ids and maybe even allow specifying the countBy type and perhaps the value, but keeping it simple for now
            val key = ArticleRecommendationMetricKey(lastHour.minusHours(hour), RecommendationMetricCountBy.articleImpression, -1, sc.site.row.rowid, -1)
            key -> 1L
          }): _*)
        // put to SitesTable
        val putOp = Schema.Articles.put(sc.row.rowid, writeToWAL = false)
        putOp.valueMap(_.recommendationMetrics, metricsMap)
        putOp.execute()
        sc -> metricsMap
      }): _*)
    work(outputMap)
  }
  def withArticleHourlySponsoredMetrics[T](hoursPerArticle: Int, lastHour: DateHour = DateHour.currentHour)(context: MultiArticleContext)(work: (Map[ArticleContext, Map[SponsoredMetricsKey, Long]]) => T): T = {
    val outputMap = Map((for {
      sc <- context.articles
    } yield {
        val metricsMap = Map((for {
          hour <- 0 until hoursPerArticle
        } yield {
            // probably should do something special and use real placement id and bucket ids and maybe even allow specifying the countBy type and perhaps the value, but keeping it simple for now
            val key = SponsoredMetricsKey(lastHour.minusHours(hour), RecommendationMetricCountBy.articleImpression, -1, sc.site.row.rowid, -1, -1, -1, CampaignKey.minValue, DollarValue.minValue)
            key -> 1L
          }): _*)
        // put to SitesTable
        val putOp = Schema.Articles.put(sc.row.rowid, writeToWAL = false)
        putOp.valueMap(_.sponsoredMetrics, metricsMap)
        putOp.execute()
        sc -> metricsMap
      }): _*)
    work(outputMap)
  }


  /**
   * Will return X campaigns per site in SiteContext.  For now returns organic campaigns, add some extra paramaterage to get non-organics!
    *
    * @param countPerSite Number of campaigns you want per site
   * @return
   */
  def withCampaigns[T](countPerSite: Int = 2, context: MultiSiteContext)(work: (MultiCampaignContext) => T): T = {
    val campaigns = for {
      site <- context.siteRows
      campaignIndex <- 0 until countPerSite
      campaign <- CampaignService.createOrganicCampaign(site.guid, site.guid + "_campaign" + campaignIndex, 1, None,
        None, None, None).toOption
    } yield CampaignContext(campaign, site, campaign.nameOrNotSet)

    work(MultiCampaignContext(campaigns, context))
  }

  def withCampaignHourlySponsoredMetrics[T](hoursPerCampaign: Int, lastHour: DateHour = DateHour.currentHour)(context: MultiCampaignContext)(work: (Map[CampaignContext, Map[SponsoredMetricsKey, Long]]) => T): T = {
    val outputMap = Map((for {
      sc <- context.campaigns
    } yield {
        val metricsMap = Map((for {
          hour <- 0 until hoursPerCampaign
        } yield {
            // probably should do something special and use real placement id and bucket ids and maybe even allow specifying the countBy type and perhaps the value, but keeping it simple for now
            //val key = RollupRecommendationMetricKey(lastHour.minusHours(hour), sc.row.rowid, -1, -1, countBy = RecommendationMetricCountBy.articleImpression)
            val key = SponsoredMetricsKey(lastHour.minusHours(hour), RecommendationMetricCountBy.articleImpression, -1, sc.site.row.rowid, -1, -1, -1, sc.campaign.rowid, DollarValue.minValue)
            key -> 1L
          }): _*)
        // put to SitesTable
        val putOp = Schema.Campaigns.put(sc.campaign.rowid, writeToWAL = false)
        putOp.valueMap(_.sponsoredMetrics, metricsMap)
        putOp.execute()
        sc -> metricsMap
      }): _*)
    work(outputMap)
  }


  /**
   * Will return X campaigns per site in SiteContext.  For now returns organic campaigns, add some extra paramaterage to get non-organics!
    *
    * @param organicsPerSite Number of organic campaigns you want per site
   * @param sponsoredPerSite Number of organic campaigns you want per site
   * @return
   */
  def withOrganicAndSponsoredCampaigns[T](organicsPerSite: Int = 2, sponsoredPerSite: Int = 2, siteContext: MultiSiteContext)(work: (MultiCampaignContext) => T): T = {
    val organics = for {
      site <- siteContext.siteRows
      campaignIndex <- 0 until organicsPerSite
      campaign <- CampaignService.createOrganicCampaign(site.guid, site.guid + "_campaign_organic_" + campaignIndex + "_" + Random.alphanumeric.take(5).mkString, 1, None, None, None, None).toOption
    } yield CampaignContext(campaign, site, campaign.nameOrNotSet)

    val sponsoreds = for {
      site <- siteContext.siteRows
      campaignIndex <- 0 until sponsoredPerSite
      campaign <- CampaignService.createCampaign(site.guid, site.guid + "_campaign_sponsored_" + campaignIndex + "_" + Random.alphanumeric.take(5).mkString, Option(CampaignType.sponsored), CampaignDateRequirements.empty,
                  DollarValue(10), Some(DollarValue(6)), isGravityOptimized = false, None).toOption
      _ <- CampaignService.updateStatus(campaign.campaignKey, CampaignStatus.active, -1).toOption
      refetched <- CampaignService.fetch(campaign.campaignKey)(_.withFamilies(_.meta)).toOption
    } yield CampaignContext(refetched, site, refetched.nameOrNotSet)

    work(MultiCampaignContext(organics ++ sponsoreds, siteContext))
  }

  // TODO-CG: Extend
  def makeContentGroupForCampaign(siteGuidStr: String, campKey: CampaignKey, campName: String, isGmsManaged: Boolean) = {
    val cgRow = try {
      val cgInsert = ContentGroupInsert(campName, ContentGroupSourceTypes.campaign, campKey.toScopedKey, siteGuidStr, ContentGroupStatus.active, isGmsManaged,
        false, "", "", "")

      configQuerySupport.insertContentGroup(cgInsert)
    } catch {
      case ex: Exception if ex.getMessage.contains("Unique index or primary key violation") =>
        warn("Content group already exists: " + campName)
        configQuerySupport.getContentGroupsForSite(siteGuidStr).find(cg => cg._2.name == campName && cg._2.sourceKey == campKey.toScopedKey).map(_._2)
          .getOrElse(throw new RuntimeException("Could not fetch existing content group: " + campName))
    }

    cgRow.asContentGroup
  }

  def makeContentGroupForExchange(exchangeKey: ExchangeKey, exchangeName: String, isGmsManaged: Boolean) = {
    val guid = ExchangeMisc.exchangeContentGroupSiteGuid
    val cgRow = try {
      val cgInsert = ContentGroupInsert(exchangeName, ContentGroupSourceTypes.exchange, exchangeKey.toScopedKey, guid, ContentGroupStatus.active, isGmsManaged,
        false, "", "", "")
      configQuerySupport.insertContentGroup(cgInsert)
    } catch {
      case ex: Exception if ex.getMessage.contains("Unique index or primary key violation") =>
        warn("Content group already exists: " + exchangeName)
        configQuerySupport.getContentGroupsForSite(guid).find(cg => cg._2.name == exchangeName && cg._2.sourceKey == exchangeKey.toScopedKey).map(_._2)
          .getOrElse(throw new RuntimeException("Could not fetch existing content group: " + exchangeName))
    }

    cgRow.asContentGroup
  }

  def withContentGroups[T](context: MultiCampaignArticlesContext, isGmsManaged: Boolean = false)(work: (MultiContentGroupContext) => T): T = {
    val contentGroups = context.articleSets.map { case campaignArticles =>
      val camp = campaignArticles.campaign
      val cg   = makeContentGroupForCampaign(camp.site.guid, camp.campaign.campaignKey, camp.campaignName, isGmsManaged)
      val articles = for {
        article <- campaignArticles.articles.articles
        articleKey = article.row.articleKey
        campaignKey: CampaignKey = campaignArticles.campaign.campaign.campaignKey
      } yield ContentGroupArticleContext(ArticleCandidateKey(articleKey, campaignKey, Option(cg), exchangeGuid = None), article)

      ContentGroupContext(cg, articles)
    }
    work(MultiContentGroupContext(contentGroups))
  }

  def withCampaignArticles[T](countPerCampaign: Int = 10, context: MultiCampaignContext, params: Map[String, String] = Map.empty,
                              withDlugGrvMapMeta: Boolean = false, withGmsGrvMapMeta: Boolean = false)(work: (MultiCampaignArticlesContext) => T): T = {
    val fullSet = context.campaigns.map(campaign => {
      val ck = campaign.campaign.rowid
      val articles = (0 until countPerCampaign).map(articleIdx => {
        val url = URLUtils.appendParameters(s"http://testsite.${campaign.site.guid}.com/campaign.${campaign.campaignName}/article$articleIdx.html", params.toSeq: _*)
        val title = s"Article $articleIdx of campaign ${campaign.campaignName}"
        val newArticle = makeArticle(url, campaign.site.guid, title = title, allArtGrvMap = {
          val maybeDlug = if (withDlugGrvMapMeta) withDlugGrvMap else Map.empty
          val maybeGms  = if (withGmsGrvMapMeta) withGmsGrvMap(ck.siteKey) else Map.empty

          if (maybeDlug.nonEmpty || maybeGms.nonEmpty)
            maybeDlug ++ maybeGms
          else
            withGrvMap(Option(ck.toScopedKey))
        }.toMap)
        val campaignSettings = Some(CampaignArticleSettings(CampaignArticleStatus.active, isBlacklisted = false))
        CampaignService.addArticleKey(ck, newArticle, settings = campaignSettings).toOption.get
        val article = ArticleService.fetch(newArticle)(_.withAllColumns).toOption.get
        ArticleContext(article.url, article.title, campaign.site, article)
      })

      val campaignArticles = CampaignArticlesContext(campaign, MultiArticleContext(articles))
      campaignArticles
    })

    work(MultiCampaignArticlesContext(fullSet, context))
  }

  //  def withContentGroup(articles:ArticleContext, site:SiteContext) = {
  //    CampaignService.createCampaign()
  //    CampaignService.addArticleKey()
  //  }

  def withUsersForCampaigns[T](usersPerSite: Int = 10, context: MultiCampaignArticlesContext)(work: (MultiUserSiteContext) => T): T = {
    val multiArticleContext = MultiArticleContext(context.articleSets.flatMap(_.articles.articles))
    withUsers(usersPerSite, multiArticleContext)(work)
  }

  /**
   * Make count random users per site who have viewed the articles in question
   */
  def withUsers[T](usersPerSite: Int = 10, articles: MultiArticleContext)(work: (MultiUserSiteContext) => T): T = {
    val dateTime = grvtime.currentTime

    val articlesBySite = articles.articles.groupBy(_.site)

    val users = articlesBySite.flatMap {
      case (site, articles) =>
        val articleKeysInSite = articles.map(_.row.rowid)

        val userKeys = (0 until usersPerSite).map {
          idx =>
            val userGuid = HashUtils.randomMd5
            val siteGuid = site.guid

            val viewedArticles = articleKeysInSite.map(ak => dateTime -> ak).toMap

            val userKey = makeUser(userGuid, siteGuid, viewedArticles)
            userKey
        }
        userKeys
    }

    val userRows = Schema.UserSites.query2.withKeys(users.toSet).withAllColumns.executeMap().values.toSeq.map(row => {
      UserSiteContext(row.userGuid, row.siteGuid, row)
    })

    val userContext = MultiUserSiteContext(userRows)

    work(userContext)
  }
  def withSitesUsers[T](usersPerSite: Int = 10, sites: MultiSiteContext)(work: (MultiUserSiteContext) => T): T = {
    val users = for {
      siteCtx <- sites.siteRows
      idx <- 1 to usersPerSite
    } yield {
      val userGuid = HashUtils.randomMd5
      val siteGuid = siteCtx.guid
      val userKey = makeUser(userGuid, siteGuid)
      userKey
    }

    val userRows = Schema.UserSites.query2.withKeys(users.toSet).withAllColumns.executeMap().values.toSeq.map(row => {
      UserSiteContext(row.userGuid, row.siteGuid, row)
    })

    val userContext = MultiUserSiteContext(userRows)

    work(userContext)
  }

  def withBeacons[T](viewsPerArticle: Int, articles: MultiArticleContext)(work: (MultiBeaconContext) => T): T = {
    val beacons = for {
      article <- articles.articles
      index <- 0 until viewsPerArticle
    } yield {
        val userGuid = HashUtils.randomMd5

        val beacon = BeaconEvent(action = "beacon", siteGuid = article.site.guid, userGuid = userGuid, articleId = article.row.rowid.articleId.toString, articleTitle = article.title, pageUrl = article.url)

        BeaconService.extractMetrics(beacon) match {
        case Success(data) => BeaconService.writeMetrics(data, writeMetaDataBuffered = true)
        case Failure(failure) => println("Failure to write beacon")
      }
      //BeaconService.handleBeaconForArticle(beacon)

      BeaconContext(beacon, article)
    }

    work(MultiBeaconContext(beacons))
  }

  def withExchange[T](numSites: Int = 4, campaignsPerSite: Int = 2, articlesPerCampaign: Int = 4, exchangeStatus: ExchangeStatus.Type = ExchangeStatus.defaultValue,
                      exchangeType: ExchangeType.Type = ExchangeType.twoWayTrafficExchange, scheduledStartTime: DateTime = grvtime.currentTime, scheduledEndTime: DateTime = grvtime.currentTime.plusMonths(1),
                      goal: ExchangeGoal = new ClicksUnlimitedGoal(), throttle: ExchangeThrottle = new NoExchangeThrottle(),
                      enableOmnitureTrackingParamsForAllSites: Option[Boolean] = None, trackingParamsForAllSites: Option[Map[String,String]] = None
                     )(work: (ExchangeContext) => T): T = {

    if(numSites < 1) throw new IllegalArgumentException("Exchange must have at least one site")

    withSites(numSites) { multiSiteCtx =>
      //make sure all of the sites don't have AlgoSettings.oneAdvertiserPerSlot set
      multiSiteCtx.siteRows.foreach{ site => FeatureSettings.setScopedSwitch(FeatureSettings.oneAdvertiserPerSlot, site.row.siteKey.toScopedKey, false) }
      withCampaigns(campaignsPerSite, multiSiteCtx) { multiCampaignCtx =>
        withCampaignArticles(articlesPerCampaign, multiCampaignCtx) { multiCampaignArticlesContext =>
          withContentGroups(multiCampaignArticlesContext) { multiContentGroupCtx =>

            val sponsorSite = multiSiteCtx.siteRows.headOption.getOrElse(throw new RuntimeException("Exchange creation unable to create any sites"))
            val exchangeName = s"exchange for site ${sponsorSite.guid}"
            val allSites = multiSiteCtx.siteRows.map(row => SiteKey(row.guid)).toSet.some
            val contentKeys = multiContentGroupCtx.contentGroups.map(cg => ContentGroupKey(cg.contentGroup.id)).toSet.some
            val exchange = ExchangeService.createExchange(SiteGuid(sponsorSite.guid), exchangeName, exchangeType, 1, scheduledStartTime, scheduledEndTime, exchangeStatus.some,
              goal.some, throttle.some, allSites, contentKeys, enableOmnitureTrackingParamsForAllSites, trackingParamsForAllSites
            ).valueOr( fail => throw new RuntimeException("Could not create exchange: " + exchangeName + ". Reason: " + fail.head.messageWithExceptionInfo) )

            val exchangeContentGroup = makeContentGroupForExchange(exchange.exchangeKey, exchangeName, isGmsManaged = false)
            val articlesContext = for {
              contentGroupContext <- multiContentGroupCtx.contentGroups
              article <- contentGroupContext.articles
            } yield article.copy(candidateKey = article.candidateKey.copy(exchangeGuid = Option(exchange.exchangeGuid)))

            work(ExchangeContext(exchange, multiSiteCtx, sponsorSite, multiContentGroupCtx, ContentGroupContext(exchangeContentGroup, articlesContext)))
          }
        }
      }
    }
  }

  def makeExampleUrl(txt: String): String = s"http://example.com/${txt.replaceAll(" ", "_")}"

//  def createDlArticle(title: String, channels: Set[AolDynamicLeadChannels.Type] = Set(AolDynamicLeadChannels.Home),
//                      secondaryLinks: List[AolLink] = Nil, startDate: Option[DateTime] = None, endDate: Option[DateTime] = None,
//                      duration: Option[GrvDuration] = None, statusOption: Option[GmsArticleStatus.Type] = None): ValidationNel[FailureResult, AolDynamicLeadArticle] = {
//
//    if (GmsAlgoSettings.aolComDlugUsesMultisiteGms)
//      return FailureResult("Old-Style DLUG has been disabled; use GmsService instead.").failureNel
//
//    val nonEmptySecondaryLinks = secondaryLinks match {
//      case Nil => List(AolLink(makeExampleUrl("link1"), "link1"), AolLink(makeExampleUrl("link2"), "link2"))
//      case _ => secondaryLinks
//    }
//    val url = makeExampleUrl(title)
//    val fields = AolDynamicLeadModifyFields(
//      title, "Category", makeExampleUrl("Category"), 0, "", "Source", makeExampleUrl("Source"), "Summary", "Headline",
//      "Secondary Header", nonEmptySecondaryLinks, makeExampleUrl(title + "/image.jpg"), showVideoIcon = false,
//      aolDynamicLeadCampaign = None, startDate = startDate, endDate = endDate, duration = duration, statusOption = statusOption, channels = channels)
//
//    val annotatedResult = AolDynamicLeadService.saveDynamicLeadArticle(url, fields, 0L, isNewlySubmitted = true)
//    if (annotatedResult.value.isFailure) {
//      println(annotatedResult.notes.mkString("\n"))
//    }
//
//    annotatedResult.value
//  }
//
//  def createDlArticlesInChannel(baseTitle: String, numArticles: Int, channels: Set[AolDynamicLeadChannels.Type] = Set(AolDynamicLeadChannels.Home), secondaryLinks: List[AolLink] = Nil): Seq[AolDynamicLeadArticle] = {
//    for {
//      i <- 1 to numArticles
//      title = s"$baseTitle $i"
//      dl <- createDlArticle(title, channels, secondaryLinks) match {
//        case Success(a) => a.some
//        case Failure(fails) =>
//          fail("Failed to create unit: " + fails.list.map(_.messageWithExceptionInfo).mkString(" AND "))
//          None
//      }
//    } yield dl
//  }
//
//  def convertDlPinsToPinMap(channel: AolDynamicLeadChannels.Type, pins: Map[Int, ArticleKey]): Map[ArticleKey, List[ChannelToPinnedSlot]] = {
//    (for ((slot, ak) <- pins) yield ak -> List(ChannelToPinnedSlot(channel, slot))).toMap
//  }

  def saveTestGmsArticle(url: String, fields: AolGmsModifyFields, userId: Long, isNewlySubmitted: Boolean): ValidationNel[FailureResult, AolGmsArticle] = {
    val annotatedResult = GmsService.saveGmsArticle(url, fields, userId, isNewlySubmitted)

    if (annotatedResult.value.isFailure)
      println(annotatedResult.notes.mkString("\n"))

    annotatedResult.value
  }

  def saveTestGmsArticle(siteKey: SiteKey, title: String, contentGroupIds: Set[Long],
                         secondaryLinks: List[AolLink] = Nil, startDate: Option[DateTime] = None, endDate: Option[DateTime] = None,
                         duration: Option[GrvDuration] = None, statusOption: Option[GmsArticleStatus.Type] = None, isNewlySubmitted: Boolean = true): ValidationNel[FailureResult, AolGmsArticle] = {

    val nonEmptySecondaryLinks = secondaryLinks match {
      case Nil => List(AolLink(makeExampleUrl("link1"), "link1"), AolLink(makeExampleUrl("link2"), "link2"))
      case _ => secondaryLinks
    }
    val url = makeExampleUrl(title)
    val fields = AolGmsModifyFields(
      siteKey, title, "Category", makeExampleUrl("Category"), 0, "", "Source", makeExampleUrl("Source"), "Summary", "Headline",
      "Secondary Header", nonEmptySecondaryLinks, makeExampleUrl(title + "/image.jpg"), showVideoIcon = false,
      startDate = startDate, endDate = endDate, duration = duration, statusOption = statusOption, contentGroupIds = contentGroupIds)

    saveTestGmsArticle(url, fields, userId = 0L, isNewlySubmitted = isNewlySubmitted)
  }

  def createTestGmsArticlesInContentGroup(siteKey: SiteKey, baseTitle: String, numArticles: Int, contentGroupIds: Set[Long], secondaryLinks: List[AolLink] = Nil): Seq[AolGmsArticle] = {
    for {
      i <- 1 to numArticles
      title = s"$baseTitle $i"
      gmsArticle <- saveTestGmsArticle(siteKey, title, contentGroupIds, secondaryLinks) match {
        case Success(a) => a.some
        case Failure(fails) =>
          fail("Failed to create unit: " + fails.list.map(_.messageWithExceptionInfo).mkString(" AND "))
          None
      }
    } yield gmsArticle
  }

  def convertGmsPinsToPinMap(contentGroupId: Long, pins: Map[Int, ArticleKey]): Map[ArticleKey, List[ContentGroupIdToPinnedSlot]] = {
    (for ((slot, ak) <- pins) yield ak -> List(ContentGroupIdToPinnedSlot(contentGroupId, slot))).toMap
  }

  def validateAuditEvents[T](logs: Iterable[AuditRow2],
                             eventTypeGetter: AuditEvents.type => AuditEvents.Type,
                             fromValue: T,
                             toValue: T)(implicit tjs: Writes[T]): Validation[String, Unit] = {
    val eventType = eventTypeGetter(AuditEvents)
    val fromValueStr = Json.stringify(Json.toJson(fromValue))
    val toValueStr = Json.stringify(Json.toJson(toValue))
    val fromValueOpt = fromValueStr.some
    val toValueOpt = toValueStr.some


    logs.filter(_.eventType.contains(eventType)).toList match {
      case List() =>
        s"no ${eventType.n} in events to try matching $fromValueStr -> $toValue".failure
      case eventsForType if !eventsForType.exists(row => row.fromValue == fromValueOpt && row.toValue == toValueOpt) =>
        s"no ${eventType.n} matching $fromValueStr -> $toValueStr in events for that type: \n${eventsForType.map(row => row.fromValue.toString + " -> " + row.toValue.toString).mkString("; ")}".failure
      case _: List[AuditRow2] =>
        println(s"Found an AuditRow for ${eventType.n} from $fromValueStr to $toValueStr").success
    }
  }

}
