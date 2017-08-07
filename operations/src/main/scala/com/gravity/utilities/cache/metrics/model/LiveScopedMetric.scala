package com.gravity.utilities.cache.metrics.model

import scala.collection._

import com.gravity.interests.jobs.intelligence.hbase._

/**
 * Created by agrealish14 on 9/29/16.
 */
case class LiveScopedMetric(key:ScopedFromToKey) {

  // click, impression, view, spend, failures * hours
  val MAX_HOURLY_ITEMS: Int = 96 * 5 // approximately 2 days of hourly * number of event types
  val MAX_DAILY_ITEMS: Int = 32 * 5 // approximately 32 days of hourly * number of event types
  val MAX_MONTHLY_ITEMS: Int = 12 * 5 // approximately 2 days of hourly * number of event types

  val hourlyMetrics = LiveScopedMetricBucket(MAX_HOURLY_ITEMS)
  val dailyMetrics = LiveScopedMetricBucket(MAX_DAILY_ITEMS)
  val monthlyMetrics = LiveScopedMetricBucket(MAX_MONTHLY_ITEMS)

  /**
   * The newMetrics ScopedMetricsKey is hourly so we need to do some round up for daily and monthly
   * @param newMetrics
   */
  def add(newMetrics:Map[ScopedMetricsKey, Long]): Unit = {

    synchronized {
      hourlyMetrics.add(newMetrics)

      dailyMetrics.add(groupAndFilterScopedMetrics(newMetrics, ScopedMetricsKey.toDaily, ScopedMetricsTtl.getMillisThreshold(ScopedMetricsTtl.dailyTtlInSeconds)))

      monthlyMetrics.add(groupAndFilterScopedMetrics(newMetrics, ScopedMetricsKey.toMonthly, ScopedMetricsTtl.getMillisThreshold(ScopedMetricsTtl.monthlyTtlInSeconds)))
    }
  }

  /**
   * Because we query Hbase the ScopedMetricsKeys here do not need any roll up
   *
   * @param metrics
   */
  def setup(metrics:ScopedMetricsBuckets): Unit = {

    synchronized {
      hourlyMetrics.setup(metrics.hourly)
      dailyMetrics.setup(metrics.daily)
      monthlyMetrics.setup(metrics.monthly)
    }

  }

  def toScopedMetricsBuckets = ScopedMetricsBuckets(
    hourly = hourlyMetrics.toScopedMetricMap,
    daily =dailyMetrics.toScopedMetricMap,
    monthly = monthlyMetrics.toScopedMetricMap
  )

  private def groupAndFilterScopedMetrics(scopedFromTos: Map[ScopedMetricsKey, Long], groupBy: ScopedMetricsKey => ScopedMetricsKey, ttlThresholdMillis: Long): mutable.Map[ScopedMetricsKey, Long] = {

    val groupedScopedMetrics: mutable.Map[ScopedMetricsKey, Long] = mutable.Map[ScopedMetricsKey, Long]()
    scopedFromTos.foreach{
      case (k,v) =>
        val groupKey = groupBy(k)

        if(groupKey.dateTimeMs > ttlThresholdMillis) {

          val previous = groupedScopedMetrics.getOrElse(groupKey, 0l)
          groupedScopedMetrics.update(groupKey, previous + v)
        }
    }
    groupedScopedMetrics
  }

}

object LiveScopedMetric {

  def apply(key:ScopedFromToKey, metrics:ScopedMetricsBuckets): LiveScopedMetric = {
    val lsm = new LiveScopedMetric(key)
    lsm.setup(metrics)
    lsm
  }
}