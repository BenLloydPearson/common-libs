package com.gravity.interests.jobs.intelligence.reporting

import com.gravity.domain.grvmetrics
import com.gravity.utilities.grvmath

case class OrganicPlacementMetrics(
  clicks: Long,
  ctr: Double,
  ctrViewed: Double,
  controlImpressions: Long,
  nonControlImpressions: Long,
  controlClicks: Long,
  nonControlClicks: Long,
  controlCtr: Double,
  nonControlCtr: Double,
  ctrUplift: Option[Double],
  controlImpressionsViewed: Long,
  nonControlImpressionsViewed: Long,
  controlCtrViewed: Double,
  nonControlCtrViewed: Double) {
  def isEmpty: Boolean = clicks == 0 &&
    controlImpressions == 0 && nonControlImpressions == 0 &&
    controlClicks == 0 && nonControlClicks == 0 &&
    ctr == 0.0 && ctrViewed == 0.0 &&
    controlImpressionsViewed == 0 && nonControlImpressionsViewed == 0 &&
    controlCtrViewed == 0.0 && nonControlCtrViewed == 0.0

  def +(that: OrganicPlacementMetrics)(implicit basicSummed: BasicPlacementMetrics): OrganicPlacementMetrics = {
    val totalClicks = clicks + that.clicks
    val totalCtr = grvmetrics.safeCTR(totalClicks, basicSummed.impressions)
    val totalCtrViewed = grvmetrics.safeCTR(totalClicks, basicSummed.impressionsViewed)
    val totalControlImps = controlImpressions + that.controlImpressions
    val totalNonControlImps = nonControlImpressions + that.nonControlImpressions
    val totalControlClicks = controlClicks + that.controlClicks
    val totalNonControlClicks = nonControlClicks + that.nonControlClicks
    val totalControlCtr = grvmetrics.safeCTR(totalControlClicks, totalControlImps)
    val totalNonControlCtr = grvmetrics.safeCTR(totalNonControlClicks, totalNonControlImps)
    val totalCtrUplift = grvmath.upliftPercentageOpt(totalControlCtr, totalNonControlCtr)
    val totalControlImpressionsViewed = controlImpressionsViewed + that.controlImpressionsViewed
    val totalNonControlImpressionsViewed = nonControlImpressionsViewed + that.nonControlImpressionsViewed
    val totalControlCtrViewed = grvmetrics.safeCTR(totalControlClicks, totalControlImpressionsViewed)
    val totalNonControlCtrViewed = grvmetrics.safeCTR(totalNonControlClicks, totalNonControlImpressionsViewed)

    OrganicPlacementMetrics(
      totalClicks,
      totalCtr,
      totalCtrViewed,
      totalControlImps,
      totalNonControlImps,
      totalControlClicks,
      totalNonControlClicks,
      totalControlCtr,
      totalNonControlCtr,
      totalCtrUplift,
      totalControlImpressionsViewed,
      totalNonControlImpressionsViewed,
      totalControlCtrViewed,
      totalNonControlCtrViewed
    )
  }
}

object OrganicPlacementMetrics {
  val empty = OrganicPlacementMetrics(0, 0, 0, 0, 0, 0, 0, 0, 0, None, 0, 0, 0, 0)
}
