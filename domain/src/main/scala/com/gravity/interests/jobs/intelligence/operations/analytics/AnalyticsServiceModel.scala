package com.gravity.interests.jobs.intelligence.operations.analytics

import scala.Predef
import scala.collection._
import com.gravity.interests.jobs.intelligence._
import com.gravity.ontology.OntologyGraph2

case class SiteSummary(guid: String, rollup: SiteRollup, scoredTopics: Seq[ScoredInterest])

case class InterestSummary(counts: InterestCounts, rollups: SiteRollup)

case class ArticleInterestSummary(
                                     uri: String,
                                     topicId: Long,
                                     name: String,
                                     publishCount: Long,
                                     viewCount: Long,
                                     searchReferrerCount: Long,
                                     socialReferrerCount: Long,
                                     keyPageReferrerCount: Long,
                                     publishPercentage: Float,
                                     viewPercentage: Float,
                                     searchPercentage: Float,
                                     socialPercentage: Float,
                                     publishZScore: Double,
                                     viewZScore: Double,
                                     searchReferrerZScore: Double,
                                     socialReferrerZScore: Double,
                                     keyPageReferrerZScore: Double,
                                     opportunityScore: Double)

 case class TopInterest(uri: String, count: Long)

case class InterestStats(
                                uri: String,
                                viewZScore: Double,
                                publishZScore: Double,
                                searchReferrerZScore: Double,
                                socialReferrerZScore: Double,
                                keyPageZScore: Double,
                                scores: ScoredInterest)

case class ScoredInterest(
                             uri: String,
                             topicId: Long,
                             name: String,
                             topicViewCount: Long,
                             totalViewCount: Long,
                             topicPublishCount: Long,
                             totalPublishCount: Long,
                             searchReferrerCount: Long,
                             totalSearchReferrerCount: Long,
                             socialReferrerCount: Long,
                             totalSocialReferrerCount: Long,
                             topicKeyPageCount: Long,
                             totalKeyPageViews: Long,
                             publishPercentage: Float,
                             viewPercentage: Float,
                             searchPercentage: Float,
                             socialPercentage: Float,
                             var writeMoreScore: Float,
                             var increaseVisibilityScore: Float,
                             var opportunityScore: Double,
                             var adjustedOpportunityScore: Double)

object InterestSortBy extends Enumeration {
  enum : InterestSortBy.type =>

  type Type = Value
  val OldOpportunityScore: InterestSortBy.Value = Value("opportunity")
  val FeatureOpportunityScore: InterestSortBy.Value = Value("kp-opportunity")
  val PublishOpportunityScore: InterestSortBy.Value = Value("n-opportunity")
  val Publishes: InterestSortBy.Value = Value("publishes")
  val Views: InterestSortBy.Value = Value("views")
  val Social: InterestSortBy.Value = Value("social")
  val Search: InterestSortBy.Value = Value("search")
  val Tweets: InterestSortBy.Value = Value("tweets")
  val Retweets: InterestSortBy.Value = Value("retweets")
  val FacebookClicks: InterestSortBy.Value = Value("fb-clicks")
  val FacebookLikes: InterestSortBy.Value = Value("fb-likes")
  val FacebookShares: InterestSortBy.Value = Value("fb-shares")
  val Influencers: InterestSortBy.Value = Value("influencers")
  val TotalViral: InterestSortBy.Value = Value("totalViral")
  val ViralVelocity: InterestSortBy.Value = Value("viralVelocity")
  val Visitors: InterestSortBy.Value = Value("visitors")
  val PublishDate: InterestSortBy.Value = Value("publishDate")
  val Impressions: InterestSortBy.Value = Value("impressions")
  val Clicks: InterestSortBy.Value = Value("clicks")
  val CTR: InterestSortBy.Value = Value("ctr")
  val SiteUniques: InterestSortBy.Value = Value("SiteUniques")
  val MostRecentlyPublished: InterestSortBy.Value = Value("MostRecentlyPublished")
  val Revenue: InterestSortBy.Value = Value("Revenue")

  // quick & dirty enums to expand the ArticleRangeSortedKey until a refactor can occur
  val OrganicPublishDate: InterestSortBy.Value = Value("organic-publishDate")
  val OrganicViews: InterestSortBy.Value = Value("organic-views")
  val OrganicClicks: InterestSortBy.Value = Value("organic-clicks")
  val SponsoredPublishDate: InterestSortBy.Value = Value("sponsored-publishDate")
  val SponsoredViews: InterestSortBy.Value = Value("sponsored-views")
  val SponsoredClicks: InterestSortBy.Value = Value("sponsored-clicks")

  object Keys {
    val OldOpportunityScore: String = enum.OldOpportunityScore.toString
    val FeatureOpportunityScore: String = enum.FeatureOpportunityScore.toString
    val PublishOpportunityScore: String = enum.PublishOpportunityScore.toString
    val Publishes: String = enum.Publishes.toString
    val Views: String = enum.Views.toString
    val Social: String = enum.Social.toString
    val Search: String = enum.Search.toString
    val Tweets: String = enum.Tweets.toString
    val Retweets: String = enum.Retweets.toString
    val FacebookClicks: String = enum.FacebookClicks.toString
    val FacebookLikes: String = enum.FacebookLikes.toString
    val FacebookShares: String = enum.FacebookShares.toString
    val Influencers: String = enum.Influencers.toString
    val TotalViral: String = enum.TotalViral.toString
    val ViralVelocity: String = enum.ViralVelocity.toString
    val Visitors: String = enum.Visitors.toString
    val PublishDate: String = enum.PublishDate.toString
    val Impressions: String = enum.Impressions.toString
    val Clicks: String = enum.Clicks.toString
    val CTR: String = enum.CTR.toString
    val SiteUniques: String = enum.SiteUniques.toString
    val MostRecentlyPublished: String = enum.MostRecentlyPublished.toString
    val Revenue: String = enum.Revenue.toString
    val OrganicPublishDate: String = enum.OrganicPublishDate.toString
    val OrganicViews: String = enum.OrganicViews.toString
    val OrganicClicks: String = enum.OrganicClicks.toString
    val SponsoredPublishDate: String = enum.SponsoredPublishDate.toString
    val SponsoredViews: String = enum.SponsoredViews.toString
    val SponsoredClicks: String = enum.SponsoredClicks.toString
  }

  def apply(key: String): Option[InterestSortBy.Value] = key match {
    case Keys.OldOpportunityScore => Some(OldOpportunityScore)
    case Keys.FeatureOpportunityScore => Some(FeatureOpportunityScore)
    case Keys.PublishOpportunityScore => Some(PublishOpportunityScore)
    case Keys.Publishes => Some(Publishes)
    case Keys.Views => Some(Views)
    case Keys.Social => Some(Social)
    case Keys.Search => Some(Search)
    case Keys.Tweets => Some(Tweets)
    case Keys.Retweets => Some(Retweets)
    case Keys.FacebookClicks => Some(FacebookClicks)
    case Keys.FacebookLikes => Some(FacebookLikes)
    case Keys.FacebookShares => Some(FacebookShares)
    case Keys.Influencers => Some(Influencers)
    case Keys.TotalViral => Some(TotalViral)
    case Keys.ViralVelocity => Some(ViralVelocity)
    case Keys.Visitors => Some(Visitors)
    case Keys.PublishDate => Some(PublishDate)
    case Keys.Impressions => Some(Impressions)
    case Keys.Clicks => Some(Clicks)
    case Keys.CTR => Some(CTR)
    case Keys.SiteUniques => Some(SiteUniques)
    case Keys.MostRecentlyPublished => Some(MostRecentlyPublished)
    case Keys.Revenue => Some(Revenue)
    case Keys.OrganicPublishDate => Some(OrganicPublishDate)
    case Keys.OrganicViews => Some(OrganicViews)
    case Keys.OrganicClicks => Some(OrganicClicks)
    case Keys.SponsoredPublishDate => Some(SponsoredPublishDate)
    case Keys.SponsoredViews => Some(SponsoredViews)
    case Keys.SponsoredClicks => Some(SponsoredClicks)
    case _ => None
  }

  def getByKey(key: String): InterestSortBy.Value = apply(key) match {
    case Some(value) => value
    case None => throw new IllegalArgumentException("The specified key: '%s' was not found!".format(key))
  }

  val enabledSorts: List[InterestSortBy.Value] = List(
    FeatureOpportunityScore,
    PublishOpportunityScore,
    Publishes,
    Views,
    Social,
    Search,
    Tweets,
    Retweets,
    TotalViral,
    ViralVelocity,
    Visitors,
    Impressions,
    Clicks,
    CTR,
    SiteUniques,
    MostRecentlyPublished,
    Revenue
  )
  
  val enabledSortKeys: List[String] = enabledSorts.map(_.toString)

  lazy val enabledSortSet: Predef.Set[InterestSortBy.Value] = enabledSorts.toSet

  def isEnabled(sortBy: InterestSortBy.Value): Boolean = enabledSortSet.contains(sortBy)
}

object ArticleSortBy extends Enumeration {
  enum : InterestSortBy.type =>

  type Type = Value
  val PerformancePercentage: ArticleSortBy.Value = Value("perf")
  val PublishDate: ArticleSortBy.Value = Value("publishDate")
  val DayViews: ArticleSortBy.Value = Value("dayViews")
  val HourViews: ArticleSortBy.Value = Value("hourViews")


  object Keys {
    val PerformancePercentage: String = enum.PerformancePercentage.toString
    val PublishDate: String = enum.PublishDate.toString
    val TotalViews: String = enum.DayViews.toString
    val HourViews: String = enum.HourViews.toString
  }

  def apply(key: String): Option[ArticleSortBy.Value] = key match {
    case Keys.PerformancePercentage => Some(PerformancePercentage)
    case Keys.PublishDate => Some(PublishDate)
    case Keys.TotalViews => Some(DayViews)
    case Keys.HourViews => Some(HourViews)
    case _ => None
  }

  def getByKey(key: String): ArticleSortBy.Value = apply(key) match {
    case Some(value) => value
    case None => throw new IllegalArgumentException("The specified key: '%s' was not found!".format(key))
  }

  val enabledSorts: List[ArticleSortBy.Value] = List(
    PerformancePercentage,
    PublishDate,
    DayViews,
    HourViews
  )

  val enabledSortKeys: List[String] = enabledSorts.map(_.toString)

  lazy val enabledSortSet: Predef.Set[ArticleSortBy.Value] = enabledSorts.toSet

  def isEnabled(sortBy: ArticleSortBy.Value): Boolean = enabledSortSet.contains(sortBy)
}

 case class Site(guid: String, topinterests: List[TopInterest])

 case class SiteStatistics(guid: String, totalViews: Long, uniques: Long, searchReferrers: Long, socialReferrers: Long, articlesPublished: Long, timeperiod: String, resolution: String)

 case class SiteRollup(uniqueUsers: Long, searchReferrerCount: Long, socialReferrerCount: Long, viewCount: Long, publishedArticles: Long, keyPageViewCount: Long)

 case class TopicArticleViews(name: String, articleViews: List[ArticleTitleView])


 case class SiteTopInterests(guid: String, rollup: SiteRollup, topicsByViews: Seq[TopInterest], topicsBySearchReferrers: Seq[TopInterest], topicsBySocialReferrers: Seq[TopInterest], topicsByPublishes: Seq[TopInterest], topisByKeyPageViews: Seq[TopInterest])


 case class ArticleView(articleUrl: String, var views: Long) {
 }

 case class ArticleTitleView(articleId: Long, url: String, title: String, views: Long, pubTimestamp: Long = 0l, var social: Long = 0l, var search: Long = 0l, var pub: Long = 0l, tweets: Long = 0l, retweets: Long = 0l, fbLikes: Long = 0l, fbClicks: Long = 0l, fbShares: Long = 0l, influencers: Long = 0l, summary: String = "")

 /**
  * Counts that we track--to be embedded by another object
  */


 case class OpportunityScoreWeights(KP_LP1: Int = 10, KP_LP2: Int = -5, KP_LP3 : Int = 2, KP_LP4: Int = 3, N_LP1: Int = 10, N_LP2: Int = -5, N_LP3: Int = 2, N_LP4: Int = 3)

// BEGIN: Result Case Classes - Here are the consolidated case classes. After all usages are moved to these, we can remove most of the others
case class SiteInterestData(totals: SiteTotals, interests: Iterable[InterestCounts])

case class Counts(views: Long, publishedArticles: Long, socialReferrers: Long, searchReferrers: Long) {
  override def toString: String = {
    "Views: %,d; Publishes: %,d; Social Refs: %,d; Search Refs: %,d".format(views, publishedArticles, socialReferrers, searchReferrers)
  }
}

object Counts {
  val empty: Counts = Counts(0l, 0l, 0l, 0l)
}

case class CountGroups(keyPage: Counts, nonKeyPage: Counts, topicTotal: Counts) {
  override def toString: String = "Interest Counts:%n\tKeypage: %s%n\tNon-Keypage: %s%n\tTotal: %s".format(keyPage, nonKeyPage, topicTotal)
}

object CountGroups {
  val empty: CountGroups = CountGroups(Counts.empty, Counts.empty, Counts.empty)
}

case class SiteTotals(guid: String, uniques: Long, views: Long, publishedArticles: Long, socialReferrers: Long, searchReferrers: Long, keyPageReferrers: Long)

case class InterestPercentages(published: Float, viewed: Float, socialReferred: Float, searchReferred: Float, keyPageReferred: Float) {
  override def toString: String = {
    "Interest Percentages:%n\tPublished: %.4f%%; Viewed: %.4f%%; Social Refs: %.4f%%; Search Refs: %.4f%%; Keypages: %.4f%%".format(published, viewed, socialReferred, searchReferred, keyPageReferred)
  }
}

case class SiteInterestDetails(totals: SiteTotals, stats: SiteStatRollup, interests: Seq[InterestDetails])

case class InterestCalculationData(counts: CountGroups, topic: ReportTopic)

case class InterestDetails(topicUri: String, topicId: Long, name: String, counts: CountGroups, scores: ReportInterestScores, viralMetrics: ViralMetrics = ViralMetrics.empty, viralVelocity: Double = 0) {
  override def toString: String = {
    val hr = "--------------------------------------------------"
    "Interest Details for \"%s\" (uri: '%s' id: %d):%n%s%n%s%n%s".format(name, topicUri, topicId, counts, scores, hr)
  }
}


case class SiteStatRollup(viewsStdDev : Double, publishesStdDev : Double, socialStdDev : Double, searchStdDev : Double, keyPageStdDev : Double)
//   END: Result Case Classes

case class SiteInterests(guid: String, totals:SiteRollup, interests: Seq[InterestCounts])
case class SiteInterestStats(guid: String, totals:SiteRollup, statTotals: SiteStatRollup, interests: Seq[InterestStats])

case class ReportNode(node: Node, metrics: StandardMetrics,
                      viralMetrics: ViralMetrics,
                      velocity: Double,
                      visitors: Long,
                      var graph: StoredGraph,
                      counts: CountGroups = CountGroups.empty,
                      zscores: ReportInterestScores = ReportInterestScores.empty,
                      measurements: StandardStatistics = StandardStatistics.empty,
                      siteMeasurements: StandardStatistics = StandardStatistics.empty) {

  val isTopic: Boolean = node.nodeType == NodeType.Topic
  val isConcept: Boolean = node.nodeType == NodeType.Interest

  lazy val interest: ReportInterest = ReportInterest(OntologyGraph2.getTopicNameWithoutOntology(node.uri), node.uri, node.id, node.level, ReportOpportunityScores(), metrics, mutable.Seq[ReportTopic](), mutable.Buffer[ReportInterest](), zscores, viralMetrics, velocity, visitors, measurements, siteMeasurements)

  lazy val topic: ReportTopic = ReportTopic(node.name, node.uri, node.id, ReportOpportunityScores(), metrics, zscores, viralMetrics, velocity, visitors, measurements, siteMeasurements)

  def scores: ReportOpportunityScores = if (isTopic) topic.scores else interest.scores
}