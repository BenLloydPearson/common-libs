package com.gravity.interests.jobs.intelligence.operations

import com.gravity.interests.jobs.intelligence.{CampaignArticleStatus, CampaignRow, CampaignStatus}
import com.gravity.utilities.analytics.{DateMidnightRange, TimeSliceResolution}
import com.gravity.utilities.grvstrings.emptyString
import com.gravity.utilities.thumby.ThumbyMode
import play.api.libs.json.Json

/** A content group's metrics without any content group meta. */
case class ContentGroupMetrics(feedUrls: Set[String], impressionsLastWeek: Long, numArticles: Int, numActiveArticles: Int,
                               mostRecentArticle: Option[ArticleMini]) {
  def +(that: ContentGroupMetrics): ContentGroupMetrics = ContentGroupMetrics(
    feedUrls ++ that.feedUrls,
    impressionsLastWeek + that.impressionsLastWeek,
    numArticles + that.numArticles,
    numActiveArticles + that.numActiveArticles,
    (mostRecentArticle.toSeq ++ that.mostRecentArticle.toSeq).sortBy(-_.publishTime).headOption
  )
}
object ContentGroupMetrics {
  lazy val empty = ContentGroupMetrics(Set.empty[String], 0l, 0, 0, None)

  implicit val cgmFormat = Json.format[ContentGroupMetrics]

  /** @return A content group metrics that represents metrics only from one campaign. */
  def toMetrics(cr: CampaignRow): ContentGroupMetrics = {
    val ras = cr.recentArticleSettings()
    val (activeFeedUrls, numActiveArticles) = cr.status match {
      case CampaignStatus.active =>
        (
          cr.activeFeeds.map(_.feedUrl).toSet,
          ras.count(_._2.status == CampaignArticleStatus.active)
        )

      case _ => (Set.empty[String], 0)
    }

    val impsLastWeek = DateMidnightRange.forTimeSlice(TimeSliceResolution.lastSevenDays).daysWithin.toSeq
                          .collect(cr.sponsoredMetricsByDay(_ => cr.isOrganic)).map(_.unitImpressions).sum

    ContentGroupMetrics(
      activeFeedUrls,
      impsLastWeek,
      ras.size,
      numActiveArticles,
      cr.recentArticlesByDate.headOption.map { case (dt, ak) => ArticleMini(ak.articleId.toString, dt.getMillis) }
    )
  }
}

/** A content group with some of its meta + metrics. */
case class ContentGroupWithMetrics(id: Long, name: String, isOrganic: Boolean, metrics: ContentGroupMetrics)
object ContentGroupWithMetrics{
  lazy val empty = ContentGroupWithMetrics(0l, emptyString, isOrganic = false, ContentGroupMetrics.empty)
  implicit val cgwmFormat = Json.format[ContentGroupWithMetrics]
}

case class ContentGroupMetricsLite(useThumby: Boolean, numActiveArticles: Int, trackingParams: Map[String, String]) {
  def +(that: ContentGroupMetricsLite): ContentGroupMetricsLite = ContentGroupMetricsLite(
    useThumby || that.useThumby,
    numActiveArticles + that.numActiveArticles,
    trackingParams ++ that.trackingParams
  )
}

object ContentGroupMetricsLite {
  implicit val jsonFormat = Json.format[ContentGroupMetricsLite]

  lazy val empty = ContentGroupMetricsLite(useThumby = false, 0, Map.empty[String, String])

  def build(cr: CampaignRow, keepTrackingParams: Boolean = true): ContentGroupMetricsLite = {
    ContentGroupMetricsLite(
      useThumby = cr.thumbyMode == ThumbyMode.on || cr.thumbyMode == ThumbyMode.withServerSideBlur,
      0,
      if (keepTrackingParams) cr.trackingParams.toMap else Map.empty
    )
  }
}