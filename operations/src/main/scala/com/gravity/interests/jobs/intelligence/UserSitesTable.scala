package com.gravity.interests.jobs.intelligence

import com.gravity.hbase.schema.{DeserializedResult, HRow, HbaseTable, Query2Builder}
import com.gravity.interests.jobs.intelligence.SchemaContext._
import com.gravity.interests.jobs.intelligence.SchemaTypes._
import com.gravity.interests.jobs.intelligence.hbase.ConnectionPoolingTableManager
import com.gravity.interests.jobs.intelligence.operations.recommendations.LSHCluster
import com.gravity.interests.jobs.intelligence.operations.{FacebookLike, GraphAnalysisService, SiteService, UserGraphGenerationService}
import com.gravity.utilities.analytics.articles.ArticleWhitelist
import com.gravity.utilities.grvstrings._
import com.gravity.utilities.time.DateHour.asReadableInstant
import com.gravity.utilities.time.{DateHour, GrvDateMidnight}
import org.joda.time.{DateTime, Days}

import scala.collection._

/*             )\._.,--....,'``.
 .b--.        /;   _.. \   _\  (`._ ,.
`=,-,-'~~~   `----(,_..'--(,_..'`-.;.'  */


class UserSitesTable extends HbaseTable[UserSitesTable, UserSiteKey, UserSiteRow]("site_users", rowKeyClass = classOf[UserSiteKey], logSchemaInconsistencies = false, tableConfig = defaultConf)
with InterestGraph[UserSitesTable, UserSiteKey]
with ClickStream[UserSitesTable, UserSiteKey]
with ArticleSponsoredMetricsColumns[UserSitesTable, UserSiteKey]
with ConnectionPoolingTableManager {
  override def rowBuilder(result: DeserializedResult): UserSiteRow = new UserSiteRow(result, this)

  val meta: Fam[String, Any] = family[String, Any]("meta", compressed = true)
  val userGuid: Col[String] = column(meta, "ug", classOf[String])
  val siteGuid: Col[String] = column(meta, "sg", classOf[String])
  val doNotTrack : Col[Boolean]= column(meta, "dnt", classOf[Boolean])
  val doRecommendations: Col[Boolean] = column(meta, "dr", classOf[Boolean])
  val facebookAccessToken: Col[String] = column(meta, "fb", classOf[String])
  val facebookLikes: Col[Set[FacebookLike]] = column(meta, "fl", classOf[Set[FacebookLike]])
  val recos3: Fam[SegmentRecoKey, ArticleRecommendations] = family[SegmentRecoKey, ArticleRecommendations]("r3", compressed = true, rowTtlInSeconds = 2629743)

  // stores what LSH cluster this user belongs to
  val LSHClustersUserIsIn: Col[scala.Seq[LSHCluster]] = column(meta, "cl", classOf[Seq[LSHCluster]])


  val userRelationships: Fam[UserRelationshipKey, DateTime] = family[UserRelationshipKey, DateTime]("ur", compressed = true)

}


class UserSiteRow(result: DeserializedResult, table: UserSitesTable) extends HRow[UserSitesTable, UserSiteKey](result, table)
with InterestGraphedRow[UserSitesTable, UserSiteKey, UserSiteRow]
with ArticleSponsoredMetricsRow[UserSitesTable, UserSiteKey, UserSiteRow] {
  lazy val siteGuid: String = column(_.siteGuid).getOrElse(emptyString)

  lazy val clickStream: Map[ClickStreamKey, Long] = family(_.clickStream)

  lazy val interactionClickStream : Map[ClickStreamKey, Long] = family(_.clickStream).filterKeys(streamKey => ClickType.isInteraction(streamKey.clickType))

  lazy val clickStreamKeys: Set[ClickStreamKey] = familyKeySet(_.clickStream)

  lazy val latestClickDateOption: Option[DateHour] = if (clickStreamKeys.isEmpty) None
  else {
    var retdate: DateHour = null
    for (ck <- clickStreamKeys) {
      if (retdate == null || retdate.isBefore(ck.hour)) {
        retdate = ck.hour
      }
    }
    Some(retdate)
  }

  def hasClicksWithinXDays(x: Int, dm: GrvDateMidnight): Boolean = {
    latestClickDateOption match {
      case Some(dh) =>
        if (math.abs(Days.daysBetween(dm, dh).getDays) <= x) {
          true
        } else {
          false
        }
      case None => false
    }
  }

  lazy val viewedArticleKeys: Set[ArticleKey] = for {
    csk <- clickStreamKeys
    if csk.clickType.id == ClickType.viewed.id
  } yield csk.articleKey

  lazy val impressionServedKeys: Set[ArticleKey] = for {
    csk <- clickStreamKeys
    if csk.clickType.id == ClickType.impressionserved.id
  } yield csk.articleKey

  lazy val impressionViewedKeys: Set[ArticleKey] = for {
    csk <- clickStreamKeys
    if csk.clickType.id == ClickType.impressionviewed.id
  } yield csk.articleKey

  lazy val clickedArticleKeys: Set[ArticleKey] = family(_.articleSponsoredMetrics).flatMap(_._1.articleKeyOption).toSet

  lazy val viewedAndClickedArticleKeys: Set[ArticleKey] = viewedArticleKeys ++ clickedArticleKeys

  //def isArticleRead(key: ArticleKey) = viewedArticleKeys.contains(key)

  lazy val totalArticlesViewed: Int = viewedArticleKeys.size

  lazy val topviewedArticleKeys: Set[ArticleKey] = clickStream.view.filter(_._1.clickType.id == ClickType.viewed.id).toSeq.sorted(ClickStreamKey.keyValOrdering).take(1000).map(_._1.articleKey).toSet

  lazy val topimpressionServedKeys: Set[ArticleKey] = clickStream.view.filter(_._1.clickType.id == ClickType.impressionserved.id).toSeq.sorted(ClickStreamKey.keyValOrdering).take(1000).map(_._1.articleKey).toSet

  lazy val topimpressionViewedKeys: Set[ArticleKey] = clickStream.view.filter(_._1.clickType.id == ClickType.impressionviewed.id).toSeq.sorted(ClickStreamKey.keyValOrdering).take(1000).map(_._1.articleKey).toSet

  lazy val viewedArticleQuery: Query2Builder[ArticlesTable, ArticleKey, ArticleRow] = {
    if (viewedArticleKeys.size <= 1000) {
      Schema.Articles.query2.withKeys(viewedArticleKeys)
    }
    else {
      Schema.Articles.query2.withKeys(topviewedArticleKeys)
    }
  }

  lazy val impressionServedQuery: Query2Builder[ArticlesTable, ArticleKey, ArticleRow]  = {
    if (impressionServedKeys.size <= 1000) {
      Schema.Articles.query2.withKeys(impressionServedKeys)
    }
    else {
      Schema.Articles.query2.withKeys(topimpressionServedKeys)
    }
  }

  lazy val impressionViewedQuery: Query2Builder[ArticlesTable, ArticleKey, ArticleRow]  = {
    if (impressionViewedKeys.size <= 1000) {
      Schema.Articles.query2.withKeys(impressionViewedKeys)
    }
    else {
      Schema.Articles.query2.withKeys(topimpressionViewedKeys)
    }
  }

  private var _viewedArticles: Map[ArticleKey, ArticleRow] = null
  private var _viewedArticlesWithContent: Map[ArticleKey, ArticleRow] = null
  private var _viewedArticlesWithGraphs: Map[ArticleKey, ArticleRow] = null
  private var _viewedArticlesWithMetrics: Map[ArticleKey, ArticleRow] = null
  private var _viewedArticlesWithGraphsAndMetrics: Map[ArticleKey, ArticleRow] = null
  private var _viewedArticlesWithClustersAndMetrics: Map[ArticleKey, ArticleRow] = null
  private var _impressionsServed: Map[ArticleKey, ArticleRow] = null
  private var _impressionsViewed: Map[ArticleKey, ArticleRow] = null


  def viewedArticles: Map[ArticleKey, ArticleRow] = {
    if (_viewedArticles == null)
      _viewedArticles = viewedArticleQuery.withFamilies(_.meta).executeMap(skipCache = false, ttl = 60000)

    _viewedArticles
  }

  def viewedArticlesWithText: Map[ArticleKey, ArticleRow] = {
    if (_viewedArticles == null)
      _viewedArticles = viewedArticleQuery.withFamilies(_.meta, _.text).executeMap(skipCache = false, ttl = 60000)

    _viewedArticles
  }

  def viewedArticlesWithClustersAndMetrics: Map[ArticleKey, ArticleRow] = {
    if (_viewedArticlesWithClustersAndMetrics == null) {
      _viewedArticlesWithClustersAndMetrics = viewedArticleQuery.withFamilies(_.meta, _.standardMetricsHourlyOld).withColumns(_.similarArticles).executeMap(skipCache = false)
    }
    _viewedArticlesWithClustersAndMetrics
  }

  def impressionsServed: Map[ArticleKey, ArticleRow] = {
    if (_impressionsServed == null)
      _impressionsServed = impressionServedQuery.withFamilies(_.meta).executeMap(skipCache = false, ttl = 60000)

    _impressionsServed
  }

  def impressionsViewed: Map[ArticleKey, ArticleRow] = {
    if (_impressionsViewed == null)
      _impressionsViewed = impressionViewedQuery.withFamilies(_.meta).executeMap(skipCache = false, ttl = 60000)

    _impressionsViewed
  }

  /**
   * Retrieve articles that were viewed after a certain date.  This call will not be cached.
   * @param date
   * @return Map of article keys to rows
   */
  def getViewedArticlesWithGraphsAfterOrDuringDate(date: DateTime): Map[ArticleKey, ArticleRow] = {
    if (clickStreamKeys.isEmpty) return Map[ArticleKey, ArticleRow]()

    val viewsAfterDate = for {
      csk <- clickStreamKeys
      if date.isEqual(csk.hour.getMillis) || date.isBefore(csk.hour.getMillis)
    } yield csk.articleKey

    Schema.Articles.query2.withKeys(viewsAfterDate).withFamilies(_.meta, _.storedGraphs).executeMap(skipCache = false)
  }

  def viewedArticlesWithContent(): Map[ArticleKey, ArticleRow] = {
    if (_viewedArticlesWithContent == null) {
      _viewedArticlesWithContent = viewedArticleQuery.withFamilies(_.meta).withColumns(_.content).executeMap(skipCache = false)
      _viewedArticles = _viewedArticlesWithContent
    }
    _viewedArticlesWithContent
  }


  def viewedArticlesWithGraphs: Map[ArticleKey, ArticleRow] = {
    if (_viewedArticlesWithGraphs == null) {
      _viewedArticlesWithGraphs = viewedArticleQuery.withFamilies(_.meta, _.storedGraphs).executeMap(skipCache = false)
      _viewedArticles = _viewedArticlesWithGraphs
    }
    _viewedArticlesWithGraphs
  }

  def viewedArticlesWithMetrics: Map[ArticleKey, ArticleRow] = {
    if (_viewedArticlesWithMetrics == null) {
      _viewedArticlesWithMetrics = viewedArticleQuery.withFamilies(_.meta, _.standardMetricsHourlyOld).executeMap(skipCache = false)
      _viewedArticles = _viewedArticlesWithMetrics
    }
    _viewedArticlesWithMetrics
  }


  def viewedArticlesWithGraphsAndMetrics: Map[ArticleKey, ArticleRow] = {
    if (_viewedArticlesWithGraphsAndMetrics == null) {
      _viewedArticlesWithGraphsAndMetrics = viewedArticleQuery.withFamilies(_.meta, _.storedGraphs, _.standardMetricsHourlyOld).executeMap(skipCache = false)
      _viewedArticlesWithGraphs = _viewedArticlesWithGraphsAndMetrics
      _viewedArticles = _viewedArticlesWithGraphsAndMetrics
      _viewedArticlesWithMetrics = _viewedArticlesWithGraphsAndMetrics
    }
    _viewedArticlesWithGraphsAndMetrics
  }

  def articleForClick(ck: ClickStreamKey): Option[ArticleRow] = viewedArticles.get(ck.articleKey)

  def impressionServedForClick(ck: ClickStreamKey): Option[ArticleRow] = impressionsServed.get(ck.articleKey)

  def impressionViewedForClick(ck: ClickStreamKey): Option[ArticleRow] = impressionsViewed.get(ck.articleKey)

  def itemForClick(ck: ClickStreamKey) : Option[ArticleRow] = {
    articleForClick(ck) match {
      case haveArticle@Some(_) => haveArticle
      case None => impressionServedForClick(ck) match {
        case haveServed@Some(_) => haveServed
        case None => impressionViewedForClick(ck) match {
          case haveViewed@Some(_) => haveViewed
          case None => None
        }
      }
    }
  }

  def articleTitleAndUrlForKey(articleKey: ArticleKey): (String, String) = {
    viewedArticles.get(articleKey) match {
      case Some(article) => (article.titleOrUrl, article.url)
      case None =>
        Schema.Articles.query2.withKey(articleKey).withFamilies(_.meta).singleOption() match {
          case Some(article) => (article.titleOrUrl, article.url)
          case None => (articleKey.articleId.toString, emptyString)
        }
    }
  }

  lazy val userSiteKey: UserSiteKey = rowid

  def siteKey: SiteKey = SiteKey(userSiteKey.siteId)

  def aggregationSiteKeys(): Set[SiteKey] = {
    val sk = siteKey
    SiteService.siteMeta(sk) match {
      case Some(site) => site.superSites ++ Set(sk)
      case None => Set(sk)
    }
  }

  val doNotTrack :Boolean= column(_.doNotTrack).getOrElse(false)
  val doRecommendations:Boolean = siteGuid != ArticleWhitelist.siteGuid(_.YAHOO_NEWS) || column(_.doRecommendations).getOrElse(false)
  lazy val userGuid:String = column(_.userGuid).getOrElse(emptyString)

  // use dynamically generated graphs (NOT the actual stored graphs)
  override lazy val conceptGraph: StoredGraph = getConceptGraph
  override lazy val phraseConceptGraph: StoredGraph = getPhraseConceptGraph

  override lazy val liveTfIdfGraph: StoredGraph = GraphAnalysisService.copyGraphToTfIdfGraph(conceptGraph)
  override lazy val liveTfIdfPhraseGraph: StoredGraph = GraphAnalysisService.copyGraphToTfIdfGraph(phraseConceptGraph)
  override lazy val allGraphs: StoredGraph = annoGraph + autoGraph + conceptGraph

  // user graph based on 10 quality recent articles
  lazy val qualityConceptGraph: StoredGraph = UserGraphGenerationService.dynamicClickstreamQualityGraph(this, GraphType.ConceptGraph, 100, 10)
  lazy val liveTfIdfQualityConceptGraph: StoredGraph = GraphAnalysisService.copyGraphToTfIdfGraph(qualityConceptGraph)
  lazy val qualityPhraseConceptGraph: StoredGraph = UserGraphGenerationService.dynamicClickstreamQualityGraph(this, GraphType.PhraseConceptGraph, 100, 10)
  lazy val liveTfIdfQualityPhraseConceptGraph: StoredGraph = GraphAnalysisService.copyGraphToTfIdfGraph(qualityPhraseConceptGraph)


  def getConceptGraph: StoredGraph = {
    val graph = UserGraphGenerationService.dynamicClickstreamGraph(this, GraphType.ConceptGraph)
    graph
  }

  def getPhraseConceptGraph: StoredGraph = {
    val graph = UserGraphGenerationService.dynamicClickstreamGraph(this, GraphType.PhraseConceptGraph)
    graph
  }


  lazy val facebookAccessToken: String = column(_.facebookAccessToken).getOrElse(emptyString)
}


