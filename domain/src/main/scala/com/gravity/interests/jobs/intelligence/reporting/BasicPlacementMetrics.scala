package com.gravity.interests.jobs.intelligence.reporting

case class BasicPlacementMetrics(
  impressions: Long,
  impressionsViewed: Long,
  clicks: Long,
  ctr: Double,
  revenue: Double = 0D,
  grossRevenue: Double = 0D,
  rpc: Double = 0D,
  rpm: Double = 0D,
  rpmViewed: Double = 0D,
  rpmGross: Double = 0D,
  viewRate: Double = 0D,
  viewRpmGross: Double = 0D,
  inboundClicks: Long = 0L,
  outboundClicks: Long = 0L,
  inboundVisitors: Long = 0L,
  outboundVisitors: Long = 0L,
  newVisitors: Long = 0L,
  returningVisitors: Long = 0L,
  totalVisitors: Long = 0L) {
  def isEmpty: Boolean = impressions == 0 && impressionsViewed == 0 && clicks == 0

  def +(that: BasicPlacementMetrics): BasicPlacementMetrics = {
    if (that.isEmpty) return this
    if (isEmpty) return that

    val totalImps = impressions + that.impressions
    val totalImpsViewed = impressionsViewed + that.impressionsViewed
    val totalClicks = clicks + that.clicks
    val totalRevenue = revenue + that.revenue
    val totalGrossRevenue = grossRevenue + that.grossRevenue

    val totalRpc = if (totalClicks > 0) totalRevenue / totalClicks.toDouble else 0.0

    val (totalCtr, totalRpm, totalRpmGross) = if (totalImps > 0) {
      val doubleImps = totalImps.toDouble
      (totalClicks / doubleImps, totalRevenue / doubleImps, totalGrossRevenue / doubleImps)
    } else {
      (0.0, 0.0, 0.0)
    }

    val totalRpmViewed = if (totalImpsViewed > 0) totalRevenue / totalImpsViewed.toDouble else 0.0

    val totalViewRate = if (totalImps > 0) totalImpsViewed / totalImps.toDouble else 0.0

    val totalViewRpmGross = if (totalImpsViewed > 0) totalGrossRevenue / totalImpsViewed.toDouble else 0.0

    val summedNewVisitors = newVisitors + that.newVisitors
    val summedReturningVisitors = returningVisitors + that.returningVisitors

    BasicPlacementMetrics(
      totalImps,
      totalImpsViewed,
      totalClicks,
      totalCtr,
      totalRevenue,
      totalGrossRevenue,
      totalRpc,
      totalRpm,
      totalRpmViewed,
      totalRpmGross,
      totalViewRate,
      totalViewRpmGross,
      inboundClicks + that.inboundClicks,
      outboundClicks + that.outboundClicks,
      inboundVisitors + that.inboundVisitors,
      outboundVisitors + that.outboundVisitors,
      summedNewVisitors,
      summedReturningVisitors,
      summedNewVisitors + summedReturningVisitors
    )
  }
}

object BasicPlacementMetrics {
  val empty = BasicPlacementMetrics(0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0)
}