package com.gravity.domain.recommendations

import com.gravity.interests.jobs.intelligence.CampaignDashboardMetrics

/**
 * Created by IntelliJ IDEA.
 * Author: Robbie Coleman
 * Date: 2/10/14
 * Time: 2:43 PM
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
case class CampaignReportMetrics(articleImpressionsClean: Long,
    articleImpressionsDiscarded: Long,
    articleImpressionsViewedClean: Long,
    articleImpressionsViewedDiscarded: Long,
    clicksClean: Long,
    clicksDiscarded: Long,
    advertiserSpendClean: Long,
    advertiserSpendDiscarded: Long,
    conversionsClean: Long) extends CampaignDashboardMetrics {

  def spent: Long = advertiserSpendClean

  def clicks: Long = clicksClean

  def impressionsViewed: Long = articleImpressionsViewedClean

  def impressions: Long = articleImpressionsClean

  override def conversions: Long = conversionsClean

  def +(that: CampaignDashboardMetrics): CampaignDashboardMetrics = (CampaignReportMetrics(that) + this).asInstanceOf[CampaignDashboardMetrics]

  def +(that: CampaignReportMetrics): CampaignReportMetrics = CampaignReportMetrics(
    that.articleImpressionsClean + articleImpressionsClean,
    that.articleImpressionsDiscarded + articleImpressionsDiscarded,
    that.articleImpressionsViewedClean + articleImpressionsViewedClean,
    that.articleImpressionsViewedDiscarded + articleImpressionsViewedDiscarded,
    that.clicksClean + clicksClean,
    that.clicksDiscarded + clicksDiscarded,
    that.advertiserSpendClean + advertiserSpendClean,
    that.advertiserSpendDiscarded + advertiserSpendDiscarded,
    that.conversionsClean + conversionsClean
  )

  def articleImpressionsRaw: Long = articleImpressionsClean + articleImpressionsDiscarded
  def articleImpressionsViewedRaw: Long = articleImpressionsViewedClean + articleImpressionsViewedDiscarded
  def clicksRaw: Long = clicksClean + clicksDiscarded
  def advertiserSpendRaw: Long = advertiserSpendClean + advertiserSpendDiscarded
  def ctrRaw: Double = if (articleImpressionsRaw > 0) clicksRaw.toDouble / articleImpressionsRaw else 0d
  def ctrViewedRaw: Double = if (articleImpressionsViewedRaw > 0) clicksRaw.toDouble / articleImpressionsViewedRaw else 0d
  def averageCpcRaw: Long = if (clicksRaw > 0) math.round(advertiserSpendRaw.toDouble / clicksRaw) else 0l

  def isEmptyRaw: Boolean = articleImpressionsClean == 0 && articleImpressionsDiscarded == 0 &&
    articleImpressionsViewedClean == 0 && articleImpressionsViewedDiscarded == 0 &&
    clicksClean == 0 && clicksDiscarded == 0 &&
    advertiserSpendClean == 0 && advertiserSpendDiscarded == 0

  def nonEmptyRaw: Boolean = !isEmptyRaw

  override def toString: String = {
    val sb = new StringBuilder
    sb.append(f"Impressions:\n")
    sb.append(f"\t*Served: $articleImpressionsClean%,d (raw:$articleImpressionsRaw%,d - discarded:$articleImpressionsDiscarded%,d)\n")
    sb.append(f"\t*Viewed: $articleImpressionsViewedClean%,d (raw:$articleImpressionsViewedRaw%,d - discarded:$articleImpressionsViewedDiscarded%,d)\n")
    sb.append(f"Clicks: $clicksClean%,d (raw:$clicksRaw%,d - discarded:$clicksDiscarded%,d)\n")
    sb.append(f"Spend: $$${advertiserSpendClean.toDouble/100.0}%,.2f (raw:$$${advertiserSpendRaw.toDouble/100.0}%,.2f - discarded:$$${advertiserSpendDiscarded.toDouble/100.0}%,.2f)\n")
    sb.append(s"Conversions: $conversionsClean").toString()
  }
}

object CampaignReportMetrics {
  def apply(metrics: CampaignDashboardMetrics): CampaignReportMetrics = CampaignReportMetrics(metrics.impressions, 0l, metrics.impressionsViewed, 0l, metrics.clicks, 0l, metrics.spent, 0l, metrics.conversions)

  val empty: CampaignReportMetrics = CampaignReportMetrics(0l, 0l, 0l, 0l, 0l, 0l, 0l, 0l, 0l)
}
