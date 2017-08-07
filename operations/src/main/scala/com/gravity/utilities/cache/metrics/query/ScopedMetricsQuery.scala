package com.gravity.utilities.cache.metrics.query

import com.gravity.service
import com.gravity.interests.jobs.intelligence.hbase.{ScopedMetricsKey, ScopedMetricsService}
import com.gravity.service.grvroles
import com.gravity.utilities.grvtime
import com.gravity.utilities.grvtime._

/**
 * Created by agrealish14 on 11/10/16.
 */

object ScopedMetricsQuery extends ScopedMetricsQuery

trait ScopedMetricsQuery {

  val HOURLY_METRICS_DAYS_OLD: Int = if(service.grvroles.isInOneOfRoles(grvroles.MANAGEMENT, grvroles.DEVELOPMENT)) 9 else 2
  val DAILY_METRICS_DAYS_OLD: Int = 32
  val MONTHLY_METRICS_DAYS_OLD: Int = 365
  val MINUTELY_METRICS_HOURS_OLD: Int = if(service.grvroles.isInOneOfRoles(grvroles.MANAGEMENT, grvroles.DEVELOPMENT)) 12 else 2

  val ttl: Int = if(service.grvroles.isInOneOfRoles(grvroles.REMOTE_RECOS, grvroles.DEVELOPMENT)) 300 else 30

  def scopedMetricsQuery(): ScopedMetricsService.QuerySpec = (query) => {

    val currentTime = grvtime.currentTime
    val hourlyMetricsWillBeAfter = currentTime.minusDays(HOURLY_METRICS_DAYS_OLD).toDateHour
    val dailyMetricsWillBeAfter = currentTime.minusDays(DAILY_METRICS_DAYS_OLD).toDateHour
    val monthlyMetricsWillBeAfter = currentTime.minusDays(MONTHLY_METRICS_DAYS_OLD).toDateHour

    val q = query.withFamilies(_.scopedMetricsByHour, _.scopedMetricsByDay, _.scopedMetricsByMonth)
      .filter(
        _.or(
          _.lessThanColumnKey(_.scopedMetricsByHour, ScopedMetricsKey.partialByStartDate(hourlyMetricsWillBeAfter)),
          _.lessThanColumnKey(_.scopedMetricsByDay, ScopedMetricsKey.partialByStartDate(dailyMetricsWillBeAfter)),
          _.lessThanColumnKey(_.scopedMetricsByMonth, ScopedMetricsKey.partialByStartDate(monthlyMetricsWillBeAfter))
        ))
    q
  }

  def scopedMetricsQueryWithMinutes(): ScopedMetricsService.QuerySpec = (query) => {

    val currentTime = grvtime.currentTime
    val minutelyMetricsWillBeAfter = currentTime.minusHours(MINUTELY_METRICS_HOURS_OLD).toDateMinute
    val hourlyMetricsWillBeAfter = currentTime.minusDays(HOURLY_METRICS_DAYS_OLD).toDateHour
    val dailyMetricsWillBeAfter = currentTime.minusDays(DAILY_METRICS_DAYS_OLD).toDateHour
    val monthlyMetricsWillBeAfter = currentTime.minusDays(MONTHLY_METRICS_DAYS_OLD).toDateHour

    val q = query.withFamilies(_.scopedMetricsByMinute, _.scopedMetricsByHour, _.scopedMetricsByDay, _.scopedMetricsByMonth)
      .filter(
        _.or(
          _.lessThanColumnKey(_.scopedMetricsByMinute, ScopedMetricsKey.partialByStartDate(minutelyMetricsWillBeAfter)),
          _.lessThanColumnKey(_.scopedMetricsByHour, ScopedMetricsKey.partialByStartDate(hourlyMetricsWillBeAfter)),
          _.lessThanColumnKey(_.scopedMetricsByDay, ScopedMetricsKey.partialByStartDate(dailyMetricsWillBeAfter)),
          _.lessThanColumnKey(_.scopedMetricsByMonth, ScopedMetricsKey.partialByStartDate(monthlyMetricsWillBeAfter))
        ))
    q
  }

}
