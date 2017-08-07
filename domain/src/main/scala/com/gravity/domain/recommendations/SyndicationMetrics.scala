package com.gravity.domain.recommendations

import com.gravity.interests.jobs.intelligence.schemas.DollarValue

import scalaz.{Monoid, Semigroup}

/**
 * Created with IntelliJ IDEA.
 * Author: robbie
 * Date: 4/7/15
 * Time: 5:52 PM
 *            _ 
 *          /  \
 *         / ..|\
 *        (_\  |_)
 *        /  \@'
 *       /     \
 *   _  /  \   |
 *  \\/  \  | _\
 *   \   /_ || \\_
 *    \____)|_) \_)
 *
 */
trait SyndicationMetrics {
  def organicClicks: Long
  def sponsoredClicks: Long
  def unitImpressions: Long
  def unitImpressionsViewed: Long
  def publisherRevenue: DollarValue
  def +(that: SyndicationMetrics): SyndicationMetrics

  private def safeCtr(clicks: Long, impressions: Long): Double = if (impressions < 1) 0.0 else clicks.toDouble / impressions

  def totalClicks: Long = organicClicks + sponsoredClicks

  def organicCtr: Double = safeCtr(organicClicks, unitImpressions)

  def organicViewedCtr: Double = safeCtr(organicClicks, unitImpressionsViewed)

  def sponsoredCtr: Double = safeCtr(sponsoredClicks, unitImpressions)

  def sponsoredViewedCtr: Double = safeCtr(sponsoredClicks, unitImpressionsViewed)

  def totalCtr: Double = safeCtr(totalClicks, unitImpressions)

  def totalViewedCtr: Double = safeCtr(totalClicks, unitImpressionsViewed)
}

object SyndicationMetrics {

  val empty: SyndicationMetrics with Object {def publisherRevenue: DollarValue; def organicClicks: Long; def unitImpressionsViewed: Long; def unitImpressions: Long; def +(that: SyndicationMetrics): SyndicationMetrics; def sponsoredClicks: Long} = new SyndicationMetrics {
    def organicClicks: Long = 0L

    def sponsoredClicks: Long = 0L

    def unitImpressions: Long = 0L

    def unitImpressionsViewed: Long = 0L

    def publisherRevenue: DollarValue = DollarValue.zero

    def +(that: SyndicationMetrics): SyndicationMetrics = that
  }

  implicit val syndicationMetricsSemigroup: Semigroup[SyndicationMetrics] with Object {def append(f1: SyndicationMetrics, f2: => SyndicationMetrics): SyndicationMetrics} = new Semigroup[SyndicationMetrics] {
    def append(f1: SyndicationMetrics, f2: => SyndicationMetrics): SyndicationMetrics = f1 + f2
  }

  implicit val syndicationMetricsMonoid: Monoid[SyndicationMetrics with Object {def publisherRevenue: DollarValue; def organicClicks: Long; def unitImpressionsViewed: Long; def unitImpressions: Long; def +(that: SyndicationMetrics): SyndicationMetrics; def sponsoredClicks: Long}] = Monoid.instance(syndicationMetricsSemigroup.append, empty)
}
