package com.gravity.domain.recommendations

import com.gravity.interests.jobs.intelligence.schemas.DollarValue

/**
 * Created with IntelliJ IDEA.
 * Author: robbie
 * Date: 4/7/15
 * Time: 5:41 PM
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
case class SyndicationReportMetrics(
                              unitImpressionsClean: Long,
                              unitImpressionsDiscarded: Long,
                              unitImpressionsViewedClean: Long,
                              unitImpressionsViewedDiscarded: Long,
                              organicClicksClean: Long,
                              organicClicksDiscarded: Long,
                              sponsoredClicksClean: Long,
                              sponsoredClicksDiscarded: Long,
                              publisherRevenueClean: DollarValue,
                              publisherRevenueDiscarded: DollarValue) extends SyndicationMetrics {

  def organicClicks: Long = organicClicksClean

  def unitImpressions: Long = unitImpressionsClean

  def sponsoredClicks: Long = sponsoredClicksClean

  def unitImpressionsViewed: Long = unitImpressionsViewedClean

  def publisherRevenue: DollarValue = publisherRevenueClean

  def +(that: SyndicationMetrics): SyndicationMetrics = {
    (SyndicationReportMetrics.fromSyndicationMetrics(that) + this).asInstanceOf[SyndicationMetrics]
  }

  def +(that: SyndicationReportMetrics): SyndicationReportMetrics = SyndicationReportMetrics(
    that.unitImpressionsClean + unitImpressionsClean,
    that.unitImpressionsDiscarded + unitImpressionsDiscarded,
    that.unitImpressionsViewedClean + unitImpressionsViewedClean,
    that.unitImpressionsViewedDiscarded + unitImpressionsViewedDiscarded,
    that.organicClicksClean + organicClicksClean,
    that.organicClicksDiscarded + organicClicksDiscarded,
    that.sponsoredClicksClean + sponsoredClicksClean,
    that.sponsoredClicksDiscarded + sponsoredClicksDiscarded,
    that.publisherRevenueClean + publisherRevenueClean,
    that.publisherRevenueDiscarded + publisherRevenueDiscarded
  )
}

object SyndicationReportMetrics {
  val empty: SyndicationReportMetrics = SyndicationReportMetrics(0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L, DollarValue.zero, DollarValue.zero)

  def fromSyndicationMetrics(metrics: SyndicationMetrics): SyndicationReportMetrics = SyndicationReportMetrics(
    metrics.unitImpressions, 0L,
    metrics.unitImpressionsViewed, 0L,
    metrics.organicClicks, 0L,
    metrics.sponsoredClicks, 0L,
    metrics.publisherRevenue, DollarValue.zero
  )
}
