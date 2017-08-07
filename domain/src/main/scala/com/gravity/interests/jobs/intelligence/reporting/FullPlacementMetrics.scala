package com.gravity.interests.jobs.intelligence.reporting

import com.gravity.interests.jobs.intelligence.schemas.DollarValue
import com.gravity.interests.jobs.intelligence.{RevShareWithGuaranteedImpressionRPMFloorModelData, _}
import com.gravity.utilities.grvmath
import com.gravity.utilities.grvmath._
import org.apache.commons.lang.NotImplementedException
import play.api.libs.json.{Format, Json}

case class FullPlacementMetrics(
  basic: BasicPlacementMetrics = BasicPlacementMetrics.empty,
  organic: OrganicPlacementMetrics = OrganicPlacementMetrics.empty,
  sponsored: SponsoredPlacementMetrics = SponsoredPlacementMetrics.empty) extends PluginDashboardMetrics {
  override def isEmpty: Boolean = basic.isEmpty && organic.isEmpty && sponsored.isEmpty

  override def nonEmpty: Boolean = !isEmpty

  def +(that: PluginDashboardMetrics): PluginDashboardMetrics = {
    (FullPlacementMetrics.fromMetrics(that) + this).asInstanceOf[PluginDashboardMetrics]
  }

  def unitImpressionsViewed: Long = basic.impressionsViewed

  def unitImpressions: Long = basic.impressions

  def unitImpressionsControl: Long = organic.controlImpressions

  def unitImpressionsNonControl: Long = organic.nonControlImpressions

  def organicClicks: Long = organic.clicks

  def organicClicksControl: Long = organic.controlClicks

  def organicClicksNonControl: Long = organic.nonControlClicks

  def revenue: Double = basic.revenue

  def sponsoredClicks: Long = sponsored.clicks

  def totalRevenue: Double = basic.grossRevenue

  def unitImpressionsViewedControl: Long = organic.controlImpressionsViewed

  def unitImpressionsViewedNonControl: Long = organic.nonControlImpressionsViewed

  def +(that: FullPlacementMetrics): FullPlacementMetrics = {
    if (that.isEmpty) return this
    if (isEmpty) return that

    implicit val basicSummed = basic + that.basic
    FullPlacementMetrics(basicSummed, organic + that.organic, sponsored + that.sponsored)
  }
}

object FullPlacementMetrics {
  val empty: FullPlacementMetrics = FullPlacementMetrics.fromMetrics(PluginDashboardMetrics.empty)

  implicit val jsonFormatBasic: Format[BasicPlacementMetrics] = Json.format[BasicPlacementMetrics]
  implicit val jsonFormatOrganic: Format[OrganicPlacementMetrics] = Json.format[OrganicPlacementMetrics]
  implicit val jsonFormatSponsored: Format[SponsoredPlacementMetrics] = Json.format[SponsoredPlacementMetrics]
  implicit val jsonFormatFull: Format[FullPlacementMetrics] = Json.format[FullPlacementMetrics]

  def fromMetrics(metrics: PluginDashboardMetrics): FullPlacementMetrics = {
    // Round to two decimal places and convert to pennies. Looked at using math.floor (doesn't nicely round) or
    // setScale() (slower) but decided on this method.
    val dblRoundedToPennies = "%.2f".format(metrics.revenue).toDouble * 100
    val totalRevenuePennies = dblRoundedToPennies.toLong

    val totalViewRate = if (metrics.unitImpressions > 0) metrics.unitImpressionsViewed / metrics.unitImpressions.toDouble else 0.0
    val totalViewRpmGross = if (metrics.unitImpressionsViewed > 0) metrics.totalRevenue / metrics.unitImpressionsViewed.toDouble * 1000 else 0.0
    val totalCpmViewed = if (metrics.unitImpressionsViewed > 0) metrics.revenue / metrics.unitImpressionsViewed.toDouble * 1000 else 0.0

    FullPlacementMetrics(
      BasicPlacementMetrics(
        metrics.unitImpressions,
        metrics.unitImpressionsViewed,
        metrics.totalClicks,
        metrics.totalCtr,
        metrics.revenue,
        metrics.totalRevenue,
        metrics.rpc,
        metrics.rpm,
        metrics.rpmViewed,
        metrics.rpmGross,
        totalViewRate,
        totalViewRpmGross),
      OrganicPlacementMetrics(
        metrics.organicClicks,
        metrics.organicCtr,
        metrics.organicCtrViewed,
        metrics.unitImpressionsControl,
        metrics.unitImpressionsNonControl,
        metrics.organicClicksControl,
        metrics.organicClicksNonControl,
        metrics.organicCtrControl,
        metrics.organicCtrNonControl,
        metrics.organicCtrUplift,
        metrics.unitImpressionsViewedControl,
        metrics.unitImpressionsViewedNonControl,
        metrics.organicCtrViewedControl,
        metrics.organicCtrViewedNonControl),
      SponsoredPlacementMetrics(
        metrics.sponsoredClicks,
        metrics.sponsoredCtr,
        metrics.sponsoredCtrViewed,
        0.0,
        totalCpmViewed)
    )
  }

  def fromRecoMetrics(metrics: RecommendationMetrics, totalSpentPennies: Long, publisherRevenuePennies: Long): FullPlacementMetrics = {
    val totalSpentDollars = DollarValue(totalSpentPennies).dollars
    val publisherRevenueDollars = DollarValue(publisherRevenuePennies).dollars
    val thousandthImpressions = metrics.unitImpressions * 0.001
    val thousandthImpressionsViewed = metrics.unitImpressionsViewed * 0.001

    val totalViewRate = if (metrics.unitImpressions > 0) metrics.unitImpressionsViewed / metrics.unitImpressions.toDouble else 0.0
    val totalViewRpmGross = if (metrics.unitImpressionsViewed > 0) totalSpentDollars / metrics.unitImpressionsViewed.toDouble * 1000 else 0.0
    val totalCpmViewed = if (metrics.unitImpressionsViewed > 0) publisherRevenueDollars / metrics.unitImpressionsViewed.toDouble * 1000 else 0.0

    val rpc = if (metrics.sponsoredClicks > 0) {
      publisherRevenueDollars / metrics.sponsoredClicks
    } else {
      0.0
    }

    val rpm = if (metrics.unitImpressions > 0) {
      publisherRevenueDollars / thousandthImpressions
    } else {
      0.0
    }

    val rpmGross = if (metrics.unitImpressions > 0) {
      totalSpentDollars / thousandthImpressions
    } else {
      0.0
    }

    val rpmViewed = if (metrics.unitImpressionsViewed > 0)
      publisherRevenueDollars / thousandthImpressionsViewed
    else
      0.0

    FullPlacementMetrics(
      BasicPlacementMetrics(
        metrics.unitImpressions,
        metrics.unitImpressionsViewed,
        metrics.organicClicks + metrics.sponsoredClicks,
        metrics.organicClickThroughRate + metrics.sponsoredClickThroughRate,
        publisherRevenueDollars,
        totalSpentDollars,
        rpc,
        rpm,
        rpmViewed,
        rpmGross,
        totalViewRate,
        totalViewRpmGross),
      OrganicPlacementMetrics(
        metrics.organicClicks,
        metrics.organicClickThroughRate,
        metrics.organicClickThroughRateViewed,
        metrics.unitImpressionsControl,
        metrics.unitImpressionsNonControl,
        metrics.organicClicksControl,
        metrics.organicClicksNonControl,
        metrics.organicClickThroughRateControl,
        metrics.organicClickThroughRateNonControl,
        grvmath.upliftPercentageOpt(metrics.organicClickThroughRateControl, metrics.organicClickThroughRateNonControl),
        0L, // No value in RecommendationMetrics
        0L, // No value in RecommendationMetrics
        0L, // Requires impressionsViewedControl in RecoMetrics
        0L), // Requires impressionsViewedNonControl in RecoMetrics
      SponsoredPlacementMetrics(
        metrics.sponsoredClicks,
        metrics.sponsoredClickThroughRate,
        metrics.sponsoredClickThroughRateViewed,
        0.0,
        totalCpmViewed)
    )
  }

  def calculatePublisherRevenuePennies(unitImpressions: Long, totalSpent: Long, revenueConfig: RevenueModelData): Long = {

    val publisherRevenuePennies = revenueConfig match {
      case RevShareModelData(percentage: Double, tech: Double) =>
        val left = totalSpent * (1 - tech)
        percentage * left
      case GuaranteedImpressionRPMModelData(rpm: DollarValue) => rpm.pennies.toDouble * (unitImpressions * 0.001)
      case RevShareWithGuaranteedImpressionRPMFloorModelData(percentage: Double, rpm: DollarValue, tech: Double) =>
        val left = totalSpent * (1 - tech)
        val share = percentage * left
        val floor = rpm.pennies.toDouble * (unitImpressions * 0.001)
        math.max(share, floor)
      case wtf => throw new NotImplementedException("The following revenue model type is not implemented: " + wtf)
    }

    roundToLong(publisherRevenuePennies)
  }
}