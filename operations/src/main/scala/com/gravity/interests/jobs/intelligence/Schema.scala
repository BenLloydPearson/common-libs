package com.gravity.interests.jobs.intelligence

import com.esotericsoftware.kryo.Serializer
import com.gravity.grvlogging._
import com.gravity.hbase.schema.{HRow, HbaseTable}
import com.gravity.interests.jobs.intelligence.SchemaTypes.TaggerWorkKey
import com.gravity.interests.jobs.intelligence.hbase.{ClusterKey, ClusterScopeKey, _}
import com.gravity.interests.jobs.intelligence.helpers.grvkryo
import com.gravity.interests.jobs.intelligence.operations.recommendations.model.RecommendedScopeKey
import com.gravity.recommendations.storage.{RecommendationsRow, RecommendationsTable}
import com.gravity.service.grvroles

import scala.collection._
import scala.reflect.ClassTag

/*             )\._.,--....,'``.
.b--.        /;   _.. \   _\  (`._ ,.
`=,-,-'~~~   `----(,_..'--(,_..'`-.;.'  */


object Schema extends com.gravity.hbase.schema.Schema with SchemaRegistry  {

  private val secondsInYear = 31536000
  private val cacheSecondsDefault = 60
  info("Setting default cache ttl to " + cacheSecondsDefault + " seconds")

  val TaskResults: TaskResultTable = registerTable(new TaskResultTable)
  val Jobs: JobSupportTable = registerTable(new JobSupportTable)
  val SiteTopics: SiteTopicsTable = registerTable(new SiteTopicsTable)
  SiteTopics.cache = new IntelligenceSchemaCacher[SiteTopicsTable, SiteTopicKey, SiteTopicRow](SiteTopics, 15000, 60 * 60)

  val Sites: SitesTable = registerTable(new SitesTable)
  Sites.cache = new IntelligenceSchemaCacher[SitesTable, SiteKey, SiteRow](Sites, 2000, 60 * 45)

  val ArticleCrawling: ArticleCrawlingTable = registerTable(new ArticleCrawlingTable)

  ArticleCrawling.cache =
    if (grvroles.isInRole(grvroles.IIO))
      new IntelligenceSchemaCacher[ArticleCrawlingTable, ArticleKey, ArticleCrawlingRow](ArticleCrawling, 100000, 60 * 60 * 24)
    else
      new IntelligenceSchemaCacher[ArticleCrawlingTable, ArticleKey, ArticleCrawlingRow](ArticleCrawling, 10000, 6000)


  val ArticleIngesting: ArticleIngestingTable = registerTable(new ArticleIngestingTable)
  ArticleIngesting.cache = new IntelligenceSchemaCacher[ArticleIngestingTable, ArticleIngestingKey, ArticleIngestingRow](ArticleIngesting, 10000, 6000)

  private def articlesTableCacher(maxRows: Int, defaultTtlSeconds: Int): IntelligenceSchemaCacher[ArticlesTable, ArticleKey, ArticleRow] = {
    new IntelligenceSchemaCacher[ArticlesTable, ArticleKey, ArticleRow](Articles, maxRows, defaultTtlSeconds)
  }

  val Articles: ArticlesTable = registerTable(new ArticlesTable)
  private val articlesCache = if (grvroles.isInRole(grvroles.REMOTE_RECOS)) {
    articlesTableCacher(35000, 60 * 10)
  }
  else if (grvroles.isInRole(grvroles.IIO)) {
    articlesTableCacher(30000, cacheSecondsDefault)
  }
  else if (grvroles.isInRole(grvroles.POC)) {
    articlesTableCacher(100000, 60 * 60 * 24)
  }
  else if (grvroles.isInRole(grvroles.HIGHLIGHTER)) {
    articlesTableCacher(100000, 60 * 30)
  }
  else if (grvroles.isInRole(grvroles.RECOGENERATION)) {
    articlesTableCacher(100000, 60 * 5)
  }
  else if (grvroles.isInRole(grvroles.RECOGENERATION2)) {
    articlesTableCacher(100000, 60 * 5)
  }
  else if (grvroles.isInApiRole) {
    articlesTableCacher(0, 60 * 10) //we have another cache layer in play for the only articles call API makes (ArticleDataLiteCache), but we still want to use memcached
  }
  else if (grvroles.isInRole(grvroles.STATIC_WIDGETS)) {
    articlesTableCacher(20000, 1)
  }
  else {
    articlesTableCacher(10000, cacheSecondsDefault)
  }

  Articles.cache = articlesCache


  val Sections: SectionsTable = registerTable(new SectionsTable)
  Sections.cache = new IntelligenceSchemaCacher[SectionsTable, SectionKey, SectionRow](Sections, 5000, cacheSecondsDefault)

  val UserSites: UserSitesTable = registerTable(new UserSitesTable)
  UserSites.cache = {
    if (grvroles.isInRole(grvroles.RECO_STORAGE)) {
      new IntelligenceSchemaCacher[UserSitesTable, UserSiteKey, UserSiteRow](UserSites, 100000, cacheSecondsDefault)
    }
    else {
      new IntelligenceSchemaCacher[UserSitesTable, UserSiteKey, UserSiteRow](UserSites, 5000, cacheSecondsDefault)
    }
  }

  val Visitors: VisitorsTable = registerTable(new VisitorsTable) //Not in use?
  Visitors.cache = new IntelligenceSchemaCacher[VisitorsTable, UserSiteHourKey, VisitorsRow](Visitors, 15000000, cacheSecondsDefault)

  val FBOpenGraphDomains: FBOpenGraphDomainsTable = registerTable(new FBOpenGraphDomainsTable)
  val FBOpenGraphLinks: FBOpenGraphLinksTable = registerTable(new FBOpenGraphLinksTable)
  val LSHClusterToUsers: LSHClusterToUsersTable = registerTable(new LSHClusterToUsersTable)

  val Campaigns: CampaignsTable = registerTable(new CampaignsTable)
  Campaigns.cache = new IntelligenceSchemaCacher[CampaignsTable, CampaignKey, CampaignRow](Campaigns, 5000, 60 * 10)

  val ScopedRecommendationMetrics: ScopedRecommendationMetricsTable = registerTable(new ScopedRecommendationMetricsTable)
  ScopedRecommendationMetrics.cache = new IntelligenceSchemaCacher[ScopedRecommendationMetricsTable, ScopedFromToKey, ScopedRecommendationMetricsRow](ScopedRecommendationMetrics, 20000, cacheSecondsDefault)
  val Audits2: AuditTable2 = registerTable(new AuditTable2)

  val DomainSiteLookup: DomainSiteLookupTable = registerTable(new DomainSiteLookupTable)
  DomainSiteLookup.cache = new IntelligenceSchemaCacher[DomainSiteLookupTable, String, DomainSiteLookupRow](DomainSiteLookup, 10000, cacheSecondsDefault)

  val RegistryDataTable: RegistryTable = registerTable(new RegistryTable)
  RegistryDataTable.cache = new IntelligenceSchemaCacher[RegistryTable, Array[Byte], RegistryTableRow](RegistryDataTable, 10000, cacheSecondsDefault)

  val VirtualSitesTable: VirtualSitesTable = registerTable(new VirtualSitesTable)
  VirtualSitesTable.cache = new IntelligenceSchemaCacher[VirtualSitesTable, SiteKey, VirtualSiteRow](VirtualSitesTable, 10000, cacheSecondsDefault)

  val Clusters: ClusterTable = registerTable(new ClusterTable())
  Clusters.cache = new IntelligenceSchemaCacher[ClusterTable, ClusterKey, ClusterRow](Clusters, 10000, cacheSecondsDefault)

  val ClusterScopes: ClusterScopeTable = registerTable(new ClusterScopeTable)
  ClusterScopes.cache = new IntelligenceSchemaCacher[ClusterScopeTable, ClusterScopeKey, ClusterScopeRow](ClusterScopes, 10000, cacheSecondsDefault)

  val ScopedToMetrics: ScopedToMetricsTable = registerTable(new ScopedToMetricsTable)
  ScopedToMetrics.cache = new IntelligenceSchemaCacher[ScopedToMetricsTable, ScopedKey, ScopedToMetricsRow](ScopedToMetrics, 10000, cacheSecondsDefault)

  val ScopedMetrics: ScopedMetricsTable = registerTable(new ScopedMetricsTable)
  ScopedMetrics.cache = if (grvroles.isInRole(grvroles.REMOTE_RECOS)) {
    new IntelligenceSchemaCacher[ScopedMetricsTable, ScopedFromToKey, ScopedMetricsRow](ScopedMetrics, 250000, cacheSecondsDefault)
  } else {
    new IntelligenceSchemaCacher[ScopedMetricsTable, ScopedFromToKey, ScopedMetricsRow](ScopedMetrics, 20000, cacheSecondsDefault)
  }

  val ScopedData: ScopedDataTable = registerTable(new ScopedDataTable)
  ScopedData.cache = new IntelligenceSchemaCacher[ScopedDataTable, ScopedKey, ScopedDataRow](ScopedData, 25000, 60 * 10)

  val OntologyNodes: OntologyNodesTable = registerTable(new OntologyNodesTable)
  OntologyNodes.cache = new IntelligenceSchemaCacher[OntologyNodesTable, OntologyNodeKey, OntologyNodeRow](OntologyNodes, 10000, secondsInYear)

  val SitePlacements: SitePlacementsTable = registerTable(new SitePlacementsTable)
  SitePlacements.cache = new IntelligenceSchemaCacher[SitePlacementsTable, SitePlacementKey, SitePlacementRow](SitePlacements, 10000, cacheSecondsDefault)

  val AlgoSettings: AlgoSettingsTable = registerTable(new AlgoSettingsTable)
  AlgoSettings.cache = new IntelligenceSchemaCacher[AlgoSettingsTable, AlgoSettingKey, AlgoSettingsRow](AlgoSettings, 2000, 60 * 5)

  val Stories: StoriesTable = registerTable(new StoriesTable)
  Stories.cache = new IntelligenceSchemaCacher[StoriesTable, Long, StoryRow](Stories, 10000, secondsInYear)

  val StoryWork: StoryWorkTable = registerTable(new StoryWorkTable)
  StoryWork.cache = new IntelligenceSchemaCacher[StoryWorkTable, Long, StoryWorkRow](StoryWork, 1000, 60)

  val TaggerWork: TaggerWorkTable = registerTable(new TaggerWorkTable)
  TaggerWork.cache = new IntelligenceSchemaCacher[TaggerWorkTable, TaggerWorkKey, TaggerWorkRow](TaggerWork, 1000, 60)

  val TopicModelMembership: TopicModelMembershipTable = registerTable(new TopicModelMembershipTable)
  TopicModelMembership.cache = new IntelligenceSchemaCacher[TopicModelMembershipTable, TopicModelMemberKey, TopicModelMemberRow](TopicModelMembership, 10000, secondsInYear)

  val TopicModelTopics: TopicModelTopicTable = registerTable(new TopicModelTopicTable)
  TopicModelTopics.cache = new IntelligenceSchemaCacher[TopicModelTopicTable, TopicModelTopicKey, TopicModelTopicRow](TopicModelTopics, 10000, secondsInYear)

  val TopicModels: TopicModelTable = registerTable(new TopicModelTable)
  TopicModels.cache = new IntelligenceSchemaCacher[TopicModelTable, TopicModelKey, TopicModelRow](TopicModels, 10000, secondsInYear)

  val ArticlesToArticles: ArticlesToArticlesTable = registerTable(new ArticlesToArticlesTable)
  ArticlesToArticles.cache = new IntelligenceSchemaCacher[ArticlesToArticlesTable, ArticleArticleKey, ArticlesToArticlesRow](ArticlesToArticles, 1000, 3600)

  val ArticlesToUsers: ArticlesToUsersTable = registerTable(new ArticlesToUsersTable)
  ArticlesToUsers.cache = new IntelligenceSchemaCacher[ArticlesToUsersTable, ArticleKey, ArticlesToUsersRow](ArticlesToUsers, 1000, 3600)

  val SectionalTopics: SectionalTopicsTable = registerTable(new SectionalTopicsTable)
  SectionalTopics.cache = new IntelligenceSchemaCacher[SectionalTopicsTable, SectionTopicKey, SectionalTopicsRow](SectionalTopics, 1000, 3600)

  val Recommendations: RecommendationsTable = registerTable(new RecommendationsTable)
  val recommendationsCache: IntelligenceSchemaCacher[RecommendationsTable, RecommendedScopeKey, RecommendationsRow] = if (grvroles.isInRole(grvroles.STATIC_WIDGETS)) {
    new IntelligenceSchemaCacher[RecommendationsTable, RecommendedScopeKey, RecommendationsRow](Recommendations, 5000, 1)
  } else if(grvroles.isInRole(grvroles.RECO_STORAGE)) {
    new IntelligenceSchemaCacher[RecommendationsTable, RecommendedScopeKey, RecommendationsRow](Recommendations, 10000, 120)
  }
  else {
    new IntelligenceSchemaCacher[RecommendationsTable, RecommendedScopeKey, RecommendationsRow](Recommendations, 5000, 120)
  }
  Recommendations.cache = recommendationsCache

  val RevenueModels: RevenueModelsTable = registerTable(new RevenueModelsTable)
  RevenueModels.cache = new IntelligenceSchemaCacher[RevenueModelsTable, RevenueModelKey, RevenueModelRow](RevenueModels, 10000, 24 * 60 * 60)

  val SortedArticles: SortedArticlesTable = registerTable(new SortedArticlesTable)
  SortedArticles.cache = new IntelligenceSchemaCacher[SortedArticlesTable, ScopedFromToKey, SortedArticlesRow](SortedArticles, 1000, cacheSecondsDefault)

  val CachedImages: CachedImagesTable = registerTable(new CachedImagesTable)
  CachedImages.cache = new IntelligenceSchemaCacher[CachedImagesTable, MD5HashKey, CachedImageRow](CachedImages, 10 * 1000, 5 * 60)

  val ImageBackups: ImageBackupsTable = registerTable(new ImageBackupsTable)
  ImageBackups.cache = new IntelligenceSchemaCacher[ImageBackupsTable, ArticleKey, ImageBackupRow](ImageBackups, 10 * 1000, 5 * 60)

  val TraceTable: TraceTable = registerTable(new TraceTable())

  val SitePartnerPlacementDomains: SitePartnerPlacementDomainsTable = registerTable(new SitePartnerPlacementDomainsTable)
  SitePartnerPlacementDomains.cache = new IntelligenceSchemaCacher[SitePartnerPlacementDomainsTable, SitePartnerPlacementDomainKey, SitePartnerPlacementDomainRow](SitePartnerPlacementDomains, 100000, 60 * 5)

  val PPIDs: PPIDsTable = registerTable(new PPIDsTable)
  PPIDs.cache = new IntelligenceSchemaCacher[PPIDsTable, SiteKey, PPIDRow](PPIDs, 1000, 60 * 5)

  val Users: UsersTable = registerTable(new UsersTable)
  Users.cache = new IntelligenceSchemaCacher[UsersTable, UserKey, UserRow](Users, 10 * 1000, 5 * 60)

  val ExternalUsers: ExternalUsersTable = registerTable(new ExternalUsersTable)
  ExternalUsers.cache = new IntelligenceSchemaCacher[ExternalUsersTable, ExternalUserKey, ExternalUserRow](ExternalUsers, 10 * 1000, 5 * 60)

  val Exchanges: ExchangesTable = registerTable(new ExchangesTable)
  Exchanges.cache = new IntelligenceSchemaCacher[ExchangesTable, ExchangeKey, ExchangeRow](Exchanges, 1000, 5 * 60)

}


case class TableReference[T <: HbaseTable[T, R, RR], R, RR <: HRow[T, R]] protected[intelligence] (table: HbaseTable[T, R, RR])(implicit tct: ClassTag[T], kct: ClassTag[R], rct: ClassTag[RR]) {

  import grvkryo._

  def tableClass: Class[_] = tct.runtimeClass
  def keyClass: Class[_] = kct.runtimeClass
  def rowClass: Class[_] = rct.runtimeClass
  def kryoKeySerializer: Serializer[R] = byteConverterSerializer(table.rowKeyConverter)
  def kryoRowSerializer: Serializer[RR] = HRowKryoSerializer[T, R, RR](table)
  def tableName: String = table.tableName

  def writeReplace: Any = SerializedTableReference[T, R, RR](tableName)

}

protected[intelligence] case class SerializedTableReference[T <: HbaseTable[T, R, RR], R, RR <: HRow[T, R]](tableName: String) {
  def readResolve: Any = {
    // this ensures tables are loaded if TableReference is deserialized before Schema is referenced
    Schema.getTableReference(tableName)
  }
}

trait SchemaRegistry {

  def registerTable[T <: HbaseTable[T, R, RR] : ClassTag, R : ClassTag, RR <: HRow[T, R] : ClassTag](tbl: HbaseTable[T, R, RR]): T = {
    Schema.table(tbl.asInstanceOf[T])
    entries += TableReference[T, R, RR](tbl)
    tbl.asInstanceOf[T]
  }

  protected[intelligence] val entries: mutable.ListBuffer[TableReference[_, _, _]] = new mutable.ListBuffer[TableReference[_, _, _]]()

  def getTableReferences: Seq[TableReference[_, _, _]] = entries

  implicit def getTableReference[T <: HbaseTable[T, R, RR], R, RR <: HRow[T, R]](implicit tc: ClassTag[T], kc: ClassTag[R], rc: ClassTag[RR]): TableReference[T, R, RR] = {
    val types = Seq(tc.runtimeClass, kc.runtimeClass, rc.runtimeClass)
    entries.find(e => Seq(e.tableClass, e.keyClass, e.rowClass) == types)
      .getOrElse(throw new RuntimeException(s"Table with types: ${types.map(_.getName)} not found in registry"))
      .asInstanceOf[TableReference[T, R, RR]]
  }

//  implicit def getTableReference[T <: HbaseTable[T, _, _]](implicit tc: ClassTag[T]): TableReference[T, _, _] = {
//    entries.find(_.tableClass == tc.runtimeClass)
//      .getOrElse(throw new RuntimeException(s"Table with type: ${tc.runtimeClass} not found in registry"))
//      .asInstanceOf[TableReference[T, _, _]]
//  }
//
  def getTableReference(tableName: String): TableReference[_, _, _] = {
    entries.find(_.tableName == tableName).getOrElse(throw new RuntimeException(s"Table '$tableName' not found in registry"))
  }

}















