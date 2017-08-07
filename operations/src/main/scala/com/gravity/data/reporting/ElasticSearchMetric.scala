package com.gravity.data.reporting

import com.gravity.domain.recommendations.RecoArticleReportMetrics
import com.gravity.interests.jobs.intelligence.reporting.FullPlacementMetrics

trait ElasticSearchMetric {
  def group: Long
  def metrics: FullPlacementMetrics
}

case class UnitDataMartMetricByDay(
  millis: Long,
  metrics: FullPlacementMetrics) extends ElasticSearchMetric {
  override def group: Long = millis
}

case class UnitDataMartMetricByPlacement(
  placementId: Long,
  metrics: FullPlacementMetrics) extends ElasticSearchMetric {
  override def group: Long = placementId
}

case class ArticleDataMartMetricByArticle(
  articleId: Long,
  metrics: RecoArticleReportMetrics)

case class ArticleDataMartMetricByCampaignDay(
  millis: Long,
  campaignId: String,
  metrics: RecoArticleReportMetrics)

case class ArticleDataMartMetricByDay(
  millis: Long,
  metrics: RecoArticleReportMetrics)