package com.gravity.domain.recommendations

import com.gravity.interests.jobs.intelligence.schemas.DollarValue
import com.gravity.interests.jobs.intelligence.PluginDashboardMetrics

/**
 * Created by IntelliJ IDEA.
 * Author: Robbie Coleman
 * Date: 12/5/13
 * Time: 10:43 AM
 */
case class RecoReportMetrics(unitImpressionsClean: Long, unitImpressionsDiscarded: Long,
                             unitImpressionsViewedClean: Long, unitImpressionsViewedDiscarded: Long,
                             organicClicksClean: Long, organicClicksDiscarded: Long,
                             sponsoredClicksClean: Long, sponsoredClicksDiscarded: Long,
                             revenueClean: Long, revenueDiscarded: Long,
                             unitImpressionsControlClean: Long, unitImpressionsNonControlClean: Long,
                             organicClicksControlClean: Long, organicClicksNonControlClean: Long,
                             totalRevenueClean: Long, totalRevenueDiscarded: Long,
                             unitImpressionsViewedControlClean: Long, unitImpressionsViewedNonControlClean: Long) extends PluginDashboardMetrics {

  def unitImpressions: Long = unitImpressionsClean

  def unitImpressionsViewed: Long = unitImpressionsViewedClean

  def unitImpressionsControl: Long = unitImpressionsControlClean

  def unitImpressionsNonControl: Long = unitImpressionsNonControlClean

  def unitImpressionsViewedControl: Long = unitImpressionsViewedControlClean

  def unitImpressionsViewedNonControl: Long = unitImpressionsViewedNonControlClean

  def organicClicks: Long = organicClicksClean

  def organicClicksControl: Long = organicClicksControlClean

  def organicClicksNonControl: Long = organicClicksNonControlClean

  def sponsoredClicks: Long = sponsoredClicksClean

  def revenue: Double = DollarValue(revenueClean).dollars

  def totalRevenue: Double = DollarValue(totalRevenueClean).dollars

  def +(that: PluginDashboardMetrics): PluginDashboardMetrics = {
    (RecoReportMetrics.fromPluginDashboardMetrics(that) + this).asInstanceOf[PluginDashboardMetrics]
  }

  def unitImpressionsRaw: Long = unitImpressionsClean + unitImpressionsDiscarded
  def unitImpressionsViewedRaw: Long = unitImpressionsViewedClean + unitImpressionsViewedDiscarded
  def organicClicksRaw: Long = organicClicksClean + organicClicksDiscarded
  def sponsoredClicksRaw: Long = sponsoredClicksClean + sponsoredClicksDiscarded
  def totalClicksRaw: Long = organicClicksRaw + sponsoredClicksRaw
  def revenueRaw: Long = revenueClean + revenueDiscarded
  def totalRevenueRaw: Long = totalRevenueClean + totalRevenueDiscarded

  def +(that: RecoReportMetrics): RecoReportMetrics = RecoReportMetrics(
    unitImpressionsClean + that.unitImpressionsClean,
    unitImpressionsDiscarded + that.unitImpressionsDiscarded,
    unitImpressionsViewedClean + that.unitImpressionsViewedClean,
    unitImpressionsViewedDiscarded + that.unitImpressionsViewedDiscarded,
    organicClicksClean + that.organicClicksClean,
    organicClicksDiscarded + that.organicClicksDiscarded,
    sponsoredClicksClean + that.sponsoredClicksClean,
    sponsoredClicksDiscarded + that.sponsoredClicksDiscarded,
    revenueClean + that.revenueClean,
    revenueDiscarded + that.revenueDiscarded,
    unitImpressionsControlClean + that.unitImpressionsControlClean,
    unitImpressionsNonControlClean + that.unitImpressionsNonControlClean,
    organicClicksControlClean + that.organicClicksControlClean,
    organicClicksNonControlClean + that.organicClicksNonControlClean,
    totalRevenueClean + that.totalRevenueClean,
    totalRevenueDiscarded + that.totalRevenueDiscarded,
    unitImpressionsViewedControlClean + that.unitImpressionsViewedControlClean,
    unitImpressionsViewedNonControlClean + that.unitImpressionsViewedNonControlClean
  )

  override def toString: String = {
    val sb = new StringBuilder
    sb.append(f"Impressions:\n")
    sb.append(f"\t*Served: $unitImpressionsClean%,d (raw:$unitImpressionsRaw%,d - discarded:$unitImpressionsDiscarded%,d)\n")
    sb.append(f"\t*Viewed: $unitImpressionsViewedClean%,d (raw:$unitImpressionsViewedRaw%,d - discarded:$unitImpressionsViewedDiscarded%,d)\n")
    sb.append(f"\t*Control: $unitImpressionsControlClean%,d [viewed: $unitImpressionsViewedControl%,d]\n")
    sb.append(f"\t*Non-Control: $unitImpressionsNonControlClean%,d [viewed: $unitImpressionsViewedNonControl%,d]\n")
    sb.append("Clicks:\n")
    sb.append(f"\t*Organic: $organicClicksClean%,d (raw:$organicClicksRaw%,d - discarded:$organicClicksDiscarded%,d)\n")
    sb.append(f"\t*Organic Control: $organicClicksControlClean%,d\n")
    sb.append(f"\t*Organic Non-Control: $organicClicksNonControlClean%,d\n")
    sb.append(f"\t*Sponsored: $sponsoredClicksClean%,d (raw:$sponsoredClicksRaw%,d - discarded:$sponsoredClicksDiscarded%,d)\n")
    sb.append("Revenue:\n")
    sb.append(f"\t*Publisher: $$${revenueClean.toDouble/100.0}%,.2f (raw:$$${revenueRaw.toDouble/100.0}%,.2f - discarded:$$${revenueDiscarded.toDouble/100.0}%,.2f)\n")
    sb.append(f"\t*Total: $$${totalRevenueClean.toDouble/100.0}%,.2f (raw:$$${totalRevenueRaw.toDouble/100.0}%,.2f - discarded:$$${totalRevenueDiscarded.toDouble/100.0}%,.2f)").toString()
  }

  override def isEmpty: Boolean = unitImpressionsClean == 0 && unitImpressionsDiscarded == 0 &&
    unitImpressionsViewedClean == 0 && unitImpressionsViewedDiscarded == 0 &&
    unitImpressionsControlClean == 0 && unitImpressionsNonControlClean == 0 &&
    organicClicksControlClean == 0 && organicClicksNonControlClean == 0 &&
    organicClicksClean == 0 && organicClicksDiscarded == 0 &&
    sponsoredClicksClean == 0 && sponsoredClicksDiscarded == 0 &&
    revenueClean == 0 && revenueDiscarded == 0 &&
    totalRevenueClean == 0 && totalRevenueDiscarded == 0 &&
    unitImpressionsViewedControlClean == 0 && unitImpressionsViewedNonControlClean == 0

  def survivedCleansing: Boolean = unitImpressionsClean > 0 || unitImpressionsViewedClean > 0 || organicClicksClean > 0 || revenueClean > 0 || totalRevenueClean > 0
}

object RecoReportMetrics {
  val empty: RecoReportMetrics = RecoReportMetrics(0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L)
  def fromPluginDashboardMetrics(metrics: PluginDashboardMetrics): RecoReportMetrics = RecoReportMetrics(
    metrics.unitImpressions, 0l,
    metrics.unitImpressionsViewed, 0l,
    metrics.organicClicks, 0l,
    metrics.sponsoredClicks, 0l,
    (metrics.revenue * 100).toLong, 0l,
    metrics.unitImpressionsControl, metrics.unitImpressionsNonControl,
    metrics.organicClicksControl, metrics.organicClicksNonControl,
    (metrics.totalRevenue * 100).toLong, 0l,
    metrics.unitImpressionsViewedControl, metrics.unitImpressionsViewedNonControl
  )
}
