package com.gravity.domain.recommendations

import com.gravity.interests.jobs.intelligence.schemas.DollarValue
import com.gravity.interests.jobs.intelligence.CampaignDashboardMetrics
import com.gravity.utilities.time.GrvDateMidnight
import com.gravity.utilities.grvtime._

/**
 * Created by IntelliJ IDEA.
 * Author: Robbie Coleman
 * Date: 3/18/14
 * Time: 11:23 AM
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
case class CampaignDashboardMetricsLite(override val impressions: Long, override val impressionsViewed: Long, override val clicks: Long, override val spent: Long, override val conversions: Long) extends CampaignDashboardMetrics {
  def +(that: CampaignDashboardMetrics): CampaignDashboardMetrics = CampaignDashboardMetricsLite(
    that.impressions + impressions,
    that.impressionsViewed + impressionsViewed,
    that.clicks + clicks,
    that.spent + spent,
    that.conversions + conversions
  )

  def +(that: CampaignDashboardMetricsLite): CampaignDashboardMetricsLite = CampaignDashboardMetricsLite(
    that.impressions + impressions,
    that.impressionsViewed + impressionsViewed,
    that.clicks + clicks,
    that.spent + spent,
    that.conversions + conversions
  )

  def dollarsSpent: DollarValue = DollarValue(spent)

  override def toString: String = f"""{"impressions":"$impressions%0,1d", "impressionsViewed":"$impressionsViewed%0,1d", "clicks":"$clicks%0,1d", "spent":"$dollarsSpent", "conversions":"$conversions%0,1d"}"""
}

object CampaignDashboardMetricsLite {
  // Since we already displayed un-cleaned metrics up to this date, we will continue to report them up to this date
  val lastDayOfRawMetrics: GrvDateMidnight = new GrvDateMidnight(2014, 3, 31)

  val empty: CampaignDashboardMetricsLite = CampaignDashboardMetricsLite(0l, 0l, 0l, 0l, 0l)

  def apply(row: AdvertiserCampaignReportRow): CampaignDashboardMetricsLite = if (row.logHour.toGrvDateMidnight.isAfter(lastDayOfRawMetrics)) {
    CampaignDashboardMetricsLite(row.articleImpressionsClean, row.articleImpressionsViewedClean, row.clicksClean, row.advertiserSpendClean.pennies, row.conversionsClean)
  } else {
    CampaignDashboardMetricsLite(row.articleImpressionsRaw, row.articleImpressionsViewedRaw, row.clicksRaw, row.advertiserSpendRaw, row.conversionsRaw)
  }

  def apply(row: AdvertiserCampaignArticleReportRow): CampaignDashboardMetricsLite = if (row.logDate.toGrvDateMidnight.isAfter(lastDayOfRawMetrics)) {
    CampaignDashboardMetricsLite(row.articleImpressionsClean, row.articleImpressionsViewedClean, row.clicksClean, row.advertiserSpendClean.pennies, row.conversionsClean)
  } else {
    CampaignDashboardMetricsLite(row.articleImpressionsRaw, row.articleImpressionsViewedRaw, row.clicksRaw, row.advertiserSpendRaw, row.conversionsRaw)
  }
}
