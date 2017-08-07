package com.gravity.interests.jobs.intelligence.reporting

import com.gravity.domain.grvmetrics

case class SponsoredPlacementMetrics(
  clicks: Long,
  ctr: Double,
  ctrViewed: Double,
  cpm: Double = 0.0,
  cpmViewed: Double = 0.0) {
  def isEmpty: Boolean = clicks == 0 && ctr == 0.0 && ctrViewed == 0.0

  def +(that: SponsoredPlacementMetrics)(implicit basicSummed: BasicPlacementMetrics): SponsoredPlacementMetrics = {
    if (that.isEmpty) return this
    if (isEmpty) return that

    val totalClicks = clicks + that.clicks
    val totalCtr = grvmetrics.safeCTR(totalClicks, basicSummed.impressions)
    val totalCtrViewed = grvmetrics.safeCTR(totalClicks, basicSummed.impressionsViewed)
    val totalCpm = if (basicSummed.impressions > 0) basicSummed.revenue / basicSummed.impressions.toDouble else 0.0
    val totalCpmViewed = if (basicSummed.impressionsViewed > 0) basicSummed.revenue / basicSummed.impressionsViewed.toDouble else 0.0

    SponsoredPlacementMetrics(totalClicks, totalCtr, totalCtrViewed, cpm = totalCpm, cpmViewed = totalCpmViewed)
  }
}

object SponsoredPlacementMetrics {
  val empty = SponsoredPlacementMetrics(0, 0, 0)
}