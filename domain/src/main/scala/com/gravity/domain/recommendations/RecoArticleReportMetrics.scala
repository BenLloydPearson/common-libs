package com.gravity.domain.recommendations

import scalaz.{Monoid, Semigroup}
import com.gravity.interests.jobs.intelligence.{PluginDashboardMetrics, RecommendationMetrics}

/**
 * Created by IntelliJ IDEA.
 * Author: Robbie Coleman
 * Date: 5/13/14
 * Time: 10:52 AM
 *            _ 
 *          /  \
 *         / ..|\
 *        (_\  |_)
 *        /  \@'
 *       /     \
 *   _  /  \   |
 * \\/  \  | _\
 *   \   /_ || \\_
 *    \____)|_) \_)
 *
 */
case class RecoArticleReportMetrics(articleImpressionsClean: Long, articleImpressionsDiscarded: Long,
                                    articleImpressionsViewedClean: Long, articleImpressionsViewedDiscarded: Long,
                                    clicksClean: Long, clicksDiscarded: Long) extends PluginDashboardMetrics {

  def unitImpressions: Long = articleImpressionsClean

  def unitImpressionsViewed: Long = articleImpressionsViewedClean

  def unitImpressionsControl: Long = 0l

  def unitImpressionsNonControl: Long = 0l

  def unitImpressionsViewedControl: Long = 0l

  def unitImpressionsViewedNonControl: Long = 0l

  def organicClicks: Long = clicksClean

  def organicClicksControl: Long = 0l

  def organicClicksNonControl: Long = 0l

  def sponsoredClicks: Long = 0l

  def revenue: Double = 0d

  def totalRevenue: Double = 0d

  def +(that: PluginDashboardMetrics): PluginDashboardMetrics = {
    (RecoArticleReportMetrics.fromPluginDashboardMetrics(that) + this).asInstanceOf[PluginDashboardMetrics]
  }

  def +(that: RecoArticleReportMetrics): RecoArticleReportMetrics = RecoArticleReportMetrics(
    that.articleImpressionsClean + articleImpressionsClean,
    that.articleImpressionsDiscarded + articleImpressionsDiscarded,
    that.articleImpressionsViewedClean + articleImpressionsViewedClean,
    that.articleImpressionsViewedDiscarded + articleImpressionsViewedDiscarded,
    that.clicksClean + clicksClean, that.clicksDiscarded + clicksDiscarded
  )

  def articleClickThroughRate: Double = if (articleImpressionsClean == 0) 0.0 else clicksClean.toDouble / articleImpressionsClean

  override def isEmpty: Boolean = {
    articleImpressionsClean == 0 &&
    articleImpressionsDiscarded == 0 &&
    articleImpressionsViewedClean == 0 &&
    articleImpressionsViewedDiscarded == 0 &&
    clicksClean == 0 &&
    clicksDiscarded == 0
  }
}

object RecoArticleReportMetrics {
  val empty: RecoArticleReportMetrics = RecoArticleReportMetrics(0L, 0L, 0L, 0L, 0L, 0L)

  implicit val recoArticleReportMetricsSemigroup: Semigroup[RecoArticleReportMetrics] with Object {def append(f1: RecoArticleReportMetrics, f2: => RecoArticleReportMetrics): RecoArticleReportMetrics} = new Semigroup[RecoArticleReportMetrics] {
    def append(f1: RecoArticleReportMetrics, f2: => RecoArticleReportMetrics): RecoArticleReportMetrics = f1 + f2
  }

  implicit val recoArticleReportMetricsMonoid: Monoid[RecoArticleReportMetrics] = Monoid.instance(recoArticleReportMetricsSemigroup.append, empty)

  def apply(metrics: RecommendationMetrics): RecoArticleReportMetrics = RecoArticleReportMetrics(
    metrics.articleImpressions, 0L,
    metrics.unitImpressionsViewed, 0L,
    metrics.clicks, 0L
  )

  def fromPluginDashboardMetrics(metrics: PluginDashboardMetrics): RecoArticleReportMetrics = RecoArticleReportMetrics(
    metrics.unitImpressions, 0l,
    metrics.unitImpressionsViewed, 0l,
    metrics.organicClicks, 0l
  )
}
