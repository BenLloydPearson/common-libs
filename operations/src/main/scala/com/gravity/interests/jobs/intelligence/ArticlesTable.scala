package com.gravity.interests.jobs.intelligence

import java.net.URL

import com.gravity.domain.articles.{Author, AuthorData}
import com.gravity.domain.gms.GmsRoute
import com.gravity.domain.recommendations.ContentGroup
import com.gravity.hbase.schema.{CommaSet, DeserializedResult, HRow, HbaseTable}
import com.gravity.interests.graphs.graphing.ScoredTerm
import com.gravity.interests.jobs.intelligence.ArtGrvMap.OneScopeMap
import com.gravity.interests.jobs.intelligence.SchemaContext._
import com.gravity.interests.jobs.intelligence.SchemaTypes._
import com.gravity.interests.jobs.intelligence.hbase.{ClusterParticipant, ClusterParticipantRow, ConnectionPoolingTableManager, ScopedKey}
import com.gravity.interests.jobs.intelligence.operations._
import com.gravity.interests.jobs.intelligence.operations.articles.CovisitationEvent
import com.gravity.interests.jobs.intelligence.operations.recommendations.CampaignRecommendationData
import com.gravity.interests.jobs.intelligence.schemas.{ArticleImage, ArticleIngestionData, GiltDeal}
import com.gravity.ontology.OntologyGraph2
import com.gravity.utilities._
import com.gravity.utilities.grvstrings._
import org.joda.time.DateTime

import scala.collection._
import scalaz.std.option._
import scalaz.syntax.apply._
import scalaz.syntax.std.option._
import scalaz.{Failure, Success}



/*             )\._.,--....,'``.
.b--.        /;   _.. \   _\  (`._ ,.
`=,-,-'~~~   `----(,_..'--(,_..'`-.;.'  */




class ArticlesTable extends HbaseTable[ArticlesTable, ArticleKey, ArticleRow](tableName = "articles", rowKeyClass = classOf[ArticleKey],logSchemaInconsistencies=false,tableConfig=defaultConf)
with StandardMetricsColumns[ArticlesTable, ArticleKey] with InterestGraph[ArticlesTable, ArticleKey] with RecommendationMetricsColumns[ArticlesTable, ArticleKey]
with SponsoredMetricsColumns[ArticlesTable, ArticleKey]
with ClusterParticipant[ArticlesTable, ArticleKey]
with ArticleVideoColumns[ArticlesTable, ArticleKey]
with ConnectionPoolingTableManager
{
  override def rowBuilder(result: DeserializedResult): ArticleRow = new ArticleRow(result, this)

  val meta: Fam[String, Any] = family[String,Any]("meta",compressed=true)
  val isSponsored: Col[Boolean] = column(meta, "isp", classOf[Boolean])
  val behindPaywall: Col[Boolean] = column(meta, "behindPaywall", classOf[Boolean])
  val siteGuid: Col[String] = column(meta, "siteGuid", classOf[String])
  val virtualSiteGuid:Col[String] = column(meta,"vsg",classOf[String])
  val author:Col[String] = column(meta, "author", classOf[String])
  val authorLink:Col[String] = column(meta, "authorLink", classOf[String])
  val authorData:Col[AuthorData] = column(meta, "ad", classOf[AuthorData])
  val title:Col[String] = column(meta, "title", classOf[String])
  val altTitle:Col[String] = column(meta, "altTitle", classOf[String]) // wsj has custom titles for items in the buckets
  val image:Col[String] = column(meta, "image", classOf[String])
  val category:Col[String] = column(meta, "category", classOf[String])
  val url:Col[String] = column(meta, "url", classOf[String])
  val beaconTime:Col[DateTime] = column(meta, "beaconTime", classOf[DateTime])
  val publishTime:Col[DateTime] = column(meta, "publishDate", classOf[DateTime])
  val seenOnHomePageDate:Col[DateTime] = column(meta, "seenOnHomePageDate", classOf[DateTime])
  val sectionsOld:Col[Set[SectionKey]] = column(meta, "sections", classOf[Set[SectionKey]])
  val FBShareCnt:Col[Long] = column(meta, "FBShareCnt", classOf[Long])
  val FBUpdateTime:Col[Long] = column(meta, "FBUpdateTime", classOf[Long])
  val timesReSubmittedToCrawler:Col[Long] = column(meta, "resubmitcrawls", classOf[Long]) // how many times have we resubmitted this link to crawl, indicates crawling failure
  val images:Col[Seq[ArticleImage]] = column(meta, "images", classOf[Seq[ArticleImage]])
  val metaLink:Col[URL] = column(meta, "metaLink", classOf[URL]) // Link to metadata about the article, for example WSJ's DJML
  val attributionName :Col[String]= column(meta, "attrName", classOf[String]) // when the article is sourced from elsewhere, this will be the name of that source (like: Reuters)
  val attributionLogo:Col[String] = column(meta, "attrLogo", classOf[String]) // when the article is sourced from elsewhere, this will be the logo for that source
  val attributionSite:Col[String] = column(meta, "attrSite", classOf[String]) // when the article is sourced from elsewhere, this will be the site (root URL) for that source
  val ontologyDate:Col[DateTime] = column(meta, "d", classOf[DateTime])
  val invalidArticle:Col[Boolean] = column(meta, "invalidArticle", classOf[Boolean])
  val relegenceStoryId:Col[Long] = column(meta, "rgSt")
  val relegenceStoryInfo:Col[ArtStoryInfo] = column(meta, "rgSi")
  val authorityScore:Col[Double] = column(meta, "auSc", classOf[Double])
  val contentQualityIndexNumeric:Col[Double] = column(meta, "cqiNum", classOf[Double])
  val contentQualityIndexPositive:Col[Double] = column(meta, "cqiPos", classOf[Double])
  val contentQualityIndexNegative:Col[Double] = column(meta, "cqiNeg", classOf[Double])


  val siteSections: Fam[SiteKey, scala.Seq[String]] = family[SiteKey, Seq[String]]("s", compressed = true, versions = 1)

  lazy val text: Fam[String, Any] = family[String,Any]("text", compressed = true, versions = 1)
  val crawlFailed:Col[String] = column(text, "cf", classOf[String])
  val keywords:Col[String] = column(text, "keywords", classOf[String])
  val content:Col[String] = column(text, "content", classOf[String])
  val tags:Col[CommaSet] = column(text, "tags", classOf[CommaSet])
  val summary:Col[String] = column(text, "summary", classOf[String])
  val termVector:Col[Seq[ScoredTerm]] = column(text, "tv", classOf[Seq[ScoredTerm]])
  val termVector2:Col[Seq[ScoredTerm]] = column(text, "tv2", classOf[Seq[ScoredTerm]])
  val termVector3:Col[Seq[ScoredTerm]] = column(text, "tv3", classOf[Seq[ScoredTerm]])
  val termVectorG:Col[Seq[ScoredTerm]] = column(text, "tvG", classOf[Seq[ScoredTerm]])
  val relegenceEntities:Col[Seq[ArtRgEntity]] = column(text, "rgEnt", classOf[Seq[ArtRgEntity]])
  val relegenceSubjects:Col[Seq[ArtRgSubject]] = column(text, "rgSub", classOf[Seq[ArtRgSubject]])

  val phraseVectorKea:Col[Seq[ScoredTerm]] = column(text, "pv", classOf[Seq[ScoredTerm]])

  val similarArticles:Col[Seq[ArticleKey]] = column(text, "sa", classOf[Seq[ArticleKey]])
  val FBimage:Col[String] = column(meta, "FBimage", classOf[String])
  val globalViralityRank:Col[Int]= column(meta,"gvr", classOf[Int])
  // allows you to store a custom data object against an article, for instance cnn has cnn specific data fields in their data feeds

  val covisitationForward: Fam[CovisitationKey, Long] = family[CovisitationKey, Long]("cvf", compressed = true, rowTtlInSeconds=518400)
  val covisitationReverse: Fam[CovisitationKey, Long] = family[CovisitationKey, Long]("cvr", compressed = true, rowTtlInSeconds=518400)

  val churnPriority : Col[Int] = column(text, "pch", classOf[Int])

  // added for gilt
  // the following columns are now rolled into GiltDeal
  //val id = column(text, "id", classOf[String])
  //val expireTime = column(text, "expireDate", classOf[DateTime])
  //val price = column(text, "price", classOf[Double])
  //val value = column(text, "value", classOf[Double])
  //val soldOut = column(text, "soldOut", classOf[String])
  val tagLine : Col[String] = column(text, "tagLine", classOf[String])
  val purchaseUrl : Col[String] = column(text, "purchaseUrl", classOf[String])
  val detailsUrl  : Col[String]= column(text, "detailsUrl", classOf[String])
  val city : Col[String] = column(text, "city", classOf[String])
  val deals  : Col[Seq[GiltDeal]]= column(text, "deals", classOf[Seq[GiltDeal]])

  // The next three families store per-campaign data for articles.
  val campaigns: Fam[CampaignKey, CampaignStatus.Type] = family[CampaignKey, CampaignStatus.Type]("c", compressed = true)
  val campaignSettings: Fam[CampaignKey, CampaignArticleSettings] = family[CampaignKey, CampaignArticleSettings]("cas", compressed = true)
  val allArtGrvMap: Fam[(Option[ScopedKey], String), OneScopeMap] = family[ArtGrvMap.OneScopeKey, ArtGrvMap.OneScopeMap]("gm", compressed = true)

  val doRecommendations : Col[Boolean] = column(meta, "dr", classOf[Boolean])

  val lastGraphedMessage : Col[String] = column(text,"lgm", classOf[String])

  val existsInWarehouse : Col[Boolean] = column(meta, "wh", classOf[Boolean])

  val ingestedData: Fam[ArticleIngestionKey, ArticleIngestionData] = family[ArticleIngestionKey, ArticleIngestionData]("aid", compressed = true)

  val userFeedbackClicksByDay: Fam[UserFeedbackByDayKey, Long] = family[UserFeedbackByDayKey, Long]("ufcbd", compressed = true)
  val discountedLifetimeClicks: Fam[UserFeedbackKey, Double] = family[UserFeedbackKey, Double]("dlc", compressed = true)

  val articleAggregatesByDay: Fam[ArticleAggregateByDayKey, Double] = family[ArticleAggregateByDayKey, Double]("aa", compressed = true)
  val timeSpentByDay: Fam[ArticleTimeSpentByDayKey, Int] = family[ArticleTimeSpentByDayKey, Int]("tsbd", compressed = true)
  val timeSpent: Fam[ArticleTimeSpentKey, Long] = family[ArticleTimeSpentKey, Long]("tspnt", compressed = true)
}

trait HasCampaignSettings {
  // changed var to def to prevent overhead opaque state switcheroos
  // (def is r/o); state changes are managed in ArticleCandidate.scala
  def campaign: CampaignRecommendationData

  def campaignArticleSettings: Option[CampaignArticleSettings] = campaign.settings

}

trait HasContentGroupSettings {

  def contentGroup: Option[ContentGroup]

}

trait HasDeals {
  // val=>def per Mr. B. recommendation
  def deals : Option[Seq[GiltDeal]]
  // only deals that are still valid
  def liveDeals: Seq[GiltDeal] = {
    val now = new DateTime()
    for {
      deals <- deals.toSeq
      deal <- deals
      expired = deal.expireTime isBefore now
      if !expired
    } yield deal
  }

}

trait HasCovisitationData {
  def covisitationReverse : Map[CovisitationKey, Long]
  def covisitationForward : Map[CovisitationKey, Long]
  def covisitationEvents: Seq[CovisitationEvent]
  def articleKey : ArticleKey

  lazy val covisitationIncoming: scala.Seq[CovisitationEventLite] = covisitationReverse.map{case (key: CovisitationKey, l: Long) =>  CovisitationEventLite(key.key, this.articleKey, key.hour, key.distance, l)}.toSeq
  lazy val covisitationOutgoing: scala.Seq[CovisitationEventLite] = covisitationForward.map{case (key: CovisitationKey, l: Long) =>  CovisitationEventLite(this.articleKey, key.key, key.hour, key.distance, l)}.toSeq

}

class ArticleRow(result: DeserializedResult, table: ArticlesTable) extends HRow[ArticlesTable, ArticleKey](result, table)
with ArticleLike
with InterestGraphedRow[ArticlesTable, ArticleKey, ArticleRow]
with RecommendationMetricsRow[ArticlesTable, ArticleKey, ArticleRow]
with StandardMetricsRow[ArticlesTable, ArticleKey, ArticleRow]
with SponsoredMetricsRow[ArticlesTable, ArticleKey, ArticleRow]
with ClusterParticipantRow[ArticlesTable,ArticleKey, ArticleRow]
with ArticleVideoRow[ArticlesTable, ArticleKey, ArticleRow]
with HasDeals
with HasCovisitationData
{
  lazy val articleKey: ArticleKey = rowid

  override def toString: String = title + " (" + url + ")"

  def makeClone: ArticleRow = table.rowBuilder(result)

  def mutateGrvMap(useThisGrvMap: ArtGrvMap.AllScopesMap): ArticleRow = new ArticleRow(result, table) {
    override val underlyingAllArtGrvMap: ArtGrvMap.AllScopesMap = useThisGrvMap
  }

  lazy val shouldBeGraphed: Boolean = {
    column(_.ontologyDate) match {
      case Some(ontologyDate) =>
        if (ontologyDate.isBefore(OntologyGraph2.graph.ontologyCreationDate)) {
          true
        } else {
          false
        }
      case None =>
        true
    }
  }

  lazy val ingestedData: Map[ArticleIngestionKey, ArticleIngestionData] = family(_.ingestedData)

  lazy val ingestionInfo: scala.Seq[String] = ingestedData.toSeq.sortBy(-_._1.timestamp.getMillis).map(kv => kv._1.toString + " " + kv._2.toString)

  lazy val virtualSiteGuidOrSiteGuid: Option[String] = column(_.virtualSiteGuid) orElse column(_.siteGuid)

  lazy val campaigns: Map[CampaignKey, CampaignStatus.Type] = family(_.campaigns)
  lazy val campaignSettings: Map[CampaignKey, CampaignArticleSettings] = family(_.campaignSettings)

  /**
    * This exists solely to allow a non-lazy override for `mutateGrvMap`
    */
  def underlyingAllArtGrvMap: ArtGrvMap.AllScopesMap = family(_.allArtGrvMap)
  lazy val allArtGrvMap: ArtGrvMap.AllScopesMap = underlyingAllArtGrvMap

  def articleReviewStatus(ck: CampaignKey): ArticleReviewStatus.Type = {
//    campaignSettings.get(ck).flatMap(_.articleReviewStatus).getOrElse(ArticleReviewStatus.defaultValue)
    ArticleReviewStatus.defaultValue
  }

  /**
    * Until refactoring, this is where covisitation events are stored when fetched in an ancillary fashion by the candidate providers
    */
  var covisitationEvents: Seq[CovisitationEvent] = Nil

  lazy val covisitationReverse: Map[CovisitationKey, Long] = family(_.covisitationReverse)
  lazy val covisitationForward: Map[CovisitationKey, Long] = family(_.covisitationForward)

  def siteSectionPaths: Map[SiteKey, SectionPath] = for {
    (sk, paths) <- family(_.siteSections)
    if paths.nonEmpty
  } yield {
    sk -> SectionPath(paths)
  }

  lazy val sectionKeys: Predef.Set[SectionKey] = (for ((sk, sectPath) <- siteSectionPaths) yield sectPath.sectionKeys(sk)).flatten.toSet

  /**
    * Given a specific [[com.gravity.interests.jobs.intelligence.SiteKey]] `sk`, return the the possible first section name as an `Option`
    *
    * @param sk The specific [[com.gravity.interests.jobs.intelligence.SiteKey]] to lookup sections for since an article may be associated to multiple site/sections
    * @return The optional first section name for this article and specific [[com.gravity.interests.jobs.intelligence.SiteKey]]
    */
  def sectionNameHeadOption(sk: SiteKey): Option[String] = siteSectionPaths.get(sk).flatMap(_.sectionStrings.headOption)

  lazy val sectionsOld: Set[SectionKey] = column(_.sectionsOld).getOrElse(Set[SectionKey]())

  def isBehindPaywall: Boolean = column(_.behindPaywall).getOrElse(false)

  def isInvalidArticle: Boolean = column(_.invalidArticle).getOrElse(false)

  lazy val doRecommendations: Boolean = column(_.doRecommendations).getOrElse(true)

  def doNotRecommend: Boolean = !doRecommendations

  lazy val siteGuidOption: Option[String] = column(_.siteGuid)

  lazy val siteKeyOption: Option[SiteKey] = siteGuidOption.map(sg => SiteKey(sg))

  def mostRecentIngestedData(forSiteGuid: String): Option[(ArticleIngestionKey, ArticleIngestionData)] = ingestedData.filter(_._2.sourceSiteGuid == forSiteGuid).toSeq.sortBy(-_._1.timestamp.getMillis).headOption

  def topNRecentIngestedInfo(limit: Int = 10): Seq[String] = {
    ingestedData.groupBy(kv => kv._1.ingestionType -> kv._2.sourceSiteKey).toSeq.flatMap({
      case (_: (IngestionTypes.Type, SiteKey), kvs: Map[ArticleIngestionKey, ArticleIngestionData]) => kvs.toSeq.sortBy(-_._1.timestamp.getMillis).headOption
    }).sortBy(-_._1.timestamp.getMillis).take(limit).map(kv => kv._1.toString + " " + kv._2.toString)
  }

  // ATTN: Usages of this MUST first include the ingestedData family
  def articleTypeForSite(forSiteGuid: String): ArticleTypes.Type = {
    // In order to grandfather in previously ingested articles...
    if (ingestedData.isEmpty) return ArticleTypes.content

    mostRecentIngestedData(forSiteGuid) match {
      case Some(kv) => kv._2.articleType
      case None => ArticleTypes.unknown
    }
  }

  // ATTN: Usages of this MUST first include the siteGuid column as well as the ingestedData family
  lazy val articleTypeForThisSite: ArticleTypes.Type = siteGuidOption match {
    case Some(sg) => articleTypeForSite(sg)
    case None => ArticleTypes.unknown
  }

  lazy val similarArticleKeys: scala.Seq[ArticleKey] = column(_.similarArticles).getOrElse(Seq())


  def getAuthorObj: Option[Author] = column(_.author) map (authorName => Author(authorName, urlOption = column(_.authorLink)))

  def getAuthorName: Option[String] = column(_.authorData) flatMap (_.headAuthor.flatMap(_.name.noneForEmpty)) orElse {
    column(_.author).flatMap(_.noneForEmpty)
  }

  lazy val authorData: AuthorData = column(_.authorData).getOrElse(getAuthorObj.map(author => AuthorData(author)).getOrElse(AuthorData.empty))

  lazy val image: String = column(_.image).getOrElse(emptyString)
  lazy val FBimage: String = column(_.FBimage).getOrElse(emptyString)
  lazy val imageList: Seq[ArticleImage] = column(_.images).getOrElse(Nil)
  lazy val siteGuid: String = column(_.siteGuid).getOrElse(emptyString)
  lazy val title: String = column(_.title).getOrElse(emptyString)
  lazy val titleOrUrl: String = column(_.title).getOrElse(url)
  lazy val beaconDate: DateTime = column(_.beaconTime).getOrElse(grvtime.emptyDateTime)
  lazy val category: String = column(_.category).getOrElse(emptyString)
  lazy val tags: CommaSet = column(_.tags).getOrElse(CommaSet())
  lazy val author: String = authorData.plainOldAuthorName
  lazy val authorLink: String = authorData.plainOldAuthorLink
  lazy val attributionName: String = column(_.attributionName).getOrElse(emptyString)
  lazy val attributionLogo: String = column(_.attributionLogo).getOrElse(emptyString)
  lazy val attributionSite: String = column(_.attributionSite).getOrElse(emptyString)
  lazy val publisher: String = SplitHost.registeredDomainFromUrl(url).getOrElse(emptyString)
  lazy val keywords: String = column(_.keywords).getOrElse(emptyString)
  lazy val publishDateOption: Option[DateTime] = column(_.publishTime)
  lazy val contentQualityIndexNumericOpt: Option[Double] = column(_.contentQualityIndexNumeric)
  lazy val contentQualityIndexPositiveOpt: Option[Double] = column(_.contentQualityIndexPositive)
  lazy val contentQualityIndexNegativeOpt: Option[Double] = column(_.contentQualityIndexNegative)

  lazy val termVector1: List[ScoredTerm] = column(_.termVector) match {
    case Some(tv) => tv.toList
    case None => List[ScoredTerm]()
  }
  lazy val termVector2: List[ScoredTerm] = column(_.termVector2) match {
    case Some(tv) => tv.toList
    case None => List[ScoredTerm]()
  }
  lazy val termVector3: List[ScoredTerm] = column(_.termVector3) match {
    case Some(tv) => tv.toList
    case None => List[ScoredTerm]()
  }
  lazy val termVectorG: List[ScoredTerm] = column(_.termVectorG) match {
    case Some(tv) => tv.toList
    case None => List[ScoredTerm]()
  }

  lazy val relegenceStoryId: Option[Long]           = column(_.relegenceStoryId)
  lazy val relegenceStoryInfo: Option[ArtStoryInfo] = column(_.relegenceStoryInfo)
  lazy val relegenceEntities: Seq[ArtRgEntity]      = column(_.relegenceEntities).getOrElse(Nil)
  lazy val relegenceSubjects: Seq[ArtRgSubject]     = column(_.relegenceSubjects).getOrElse(Nil)
  lazy val authorityScore: Option[Double]           = column(_.authorityScore)

  // This is not currently used except to construct uniqueTermVectors, which is itself not currently used.
  lazy val termVectorsAll: List[ScoredTerm] = termVector1 ++ termVector2 ++ termVector3 ++ termVectorG

  lazy val phraseVectorKea: Seq[ScoredTerm] = column(_.phraseVectorKea).getOrElse(Nil)

  // This is not currently used.
  lazy val uniqueTermVectors: List[ScoredTerm] = {
    val uniquer = mutable.Map[String, (Int, Double)]()

    termVectorsAll.foreach(tv => {
      uniquer.get(tv.term) match {
        case Some((cnt, tot)) =>
          uniquer.update(tv.term, (cnt + 1) -> (tot + tv.score))
        case None => uniquer.put(tv.term, 1 -> tv.score)
      }
    })

    for ((term, (cnt, tot)) <- uniquer.toList) yield ScoredTerm(term, tot / cnt)
  }

  lazy val deals: Option[Seq[GiltDeal]] = column(_.deals)

  lazy val tagLine: Option[String] = column(_.tagLine)

  lazy val detailsUrl: Option[String] = column(_.detailsUrl)

  lazy val purchaseUrl: String = column(_.purchaseUrl).getOrElse(emptyString)

  lazy val city: Option[String] = column(_.city)

  lazy val urlOption: Option[String] = column(_.url)
  lazy val url: String = urlOption.getOrElse(emptyString)
  lazy val summary: String = column(_.summary).getOrElse(emptyString)
  lazy val content: String = column(_.content).getOrElse(emptyString)
  lazy val globalViralityRank: Int = column(_.globalViralityRank).getOrElse(0)

  //defaults to 1/1/2000
  lazy val publishTime: DateTime = publishDateOption.getOrElse(grvtime.emptyDateTime)

  lazy val crawlFailedReason: String = column(_.crawlFailed).getOrElse(emptyString)

  // default to no existence in warehouse
  lazy val existsInWarehouse: Boolean = column(_.existsInWarehouse).getOrElse(false)

  lazy val churnPriority: Int = column(_.churnPriority).getOrElse(0)

  def hasCrawlFailed: Boolean = !crawlFailedReason.isEmpty

  def aggregationSiteKeys(): Set[SiteKey] = siteKeyOption match {
    case Some(sk) => SiteService.siteMeta(sk) match {
      case Some(site) => site.superSites ++ site.sponsorPoolSiteKeys ++ Set(sk)
      case None => Set(sk)
    }
    case None => Set.empty[SiteKey]
  }

  def aggregationSiteGuids(): Set[String] = for {
    sk <- aggregationSiteKeys()
    site <- SiteService.siteMeta(sk)
    guid <- site.siteGuid
  } yield {
    guid
  }

  def getAolUniArticleInfo(optGmsRoute: Option[GmsRoute]): AolUniArticleInfo =
    AolUniService.getAolUniArticleInfo(articleKey, urlOption, AllArtGrvMapSource.articleRow, allArtGrvMap, optGmsRoute)

  def getAolUniArticleInfo(ck: CampaignKey): AolUniArticleInfo =
    getAolUniArticleInfo(GmsRoute.optGmsRoute(ck))

  lazy val userFeedbackClicksByDay: Map[UserFeedbackByDayKey, Long] = family(_.userFeedbackClicksByDay)
  lazy val discountedLifetimeClicks: Map[UserFeedbackKey, Double] = family(_.discountedLifetimeClicks)
  lazy val articleAggregates: Map[ArticleAggregateByDayKey, Double] = family(_.articleAggregatesByDay)
  lazy val timeSpentByDay: Map[ArticleTimeSpentByDayKey, Int] = family(_.timeSpentByDay)
  lazy val timeSpent: Map[ArticleTimeSpentKey, Long] = family(_.timeSpent)

  lazy val timeSpentMedian: Option[Float] = {
    val minRequiredViews = 100 // could go as low as 96 if necessary based on research from Christa
    val maxValidTime = 900

    val sum = timeSpent.filter(x => x._1.seconds.toInt <= maxValidTime).values.sum
    val mid = math.ceil(sum / 2.0).toLong

    var median: Option[Float] = None

    if (sum >= minRequiredViews) {
      val sortedTs = timeSpent.toSeq.sortBy(_._1.seconds)
      var found = false
      var progress = 0L
      var i = 0
      val len = sortedTs.size
      while ((i < len) && !found) {
        val tpl = sortedTs(i)
        progress += tpl._2
        if (progress >= mid) {
          median = if (sum % 2 == 0 && progress == mid) {
            Some((tpl._1.seconds.toFloat + sortedTs(i+1)._1.seconds.toFloat) / 2.0f)
          } else
            Some(tpl._1.seconds.toFloat)

          found = true
        }

        i += 1
      }
    }
    median
  }
}

trait ArticleLike extends HasArticleKey {

  def publishDateOption : Option[DateTime]
  def title : String
  def url : String
  def tags : CommaSet
  def siteGuid : String
  def content : String
  def summary : String
  def keywords : String
  def termVector1 : List[ScoredTerm]
  def termVector2 : List[ScoredTerm]
  def termVector3 : List[ScoredTerm]
  def termVectorG : List[ScoredTerm]
  def phraseVectorKea : Seq[ScoredTerm]
  def category: String
  def allArtGrvMap: ArtGrvMap.AllScopesMap
  def relegenceStoryId: Option[Long]
  def relegenceStoryInfo: Option[ArtStoryInfo]
  def relegenceEntities: Seq[ArtRgEntity]
  def relegenceSubjects: Seq[ArtRgSubject]
}

object ArticleRow {
 import com.gravity.logging.Logging._

}

trait HasArticleKey {
  def articleKey: ArticleKey
}


class WakDecorator(in: Long) extends ArticleKey(in) with HasArticleKey {
  val articleKey: ArticleKey = ArticleKey(in)
}
