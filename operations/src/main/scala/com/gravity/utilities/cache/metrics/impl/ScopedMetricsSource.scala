package com.gravity.utilities.cache.metrics.impl

import com.gravity.interests.jobs.intelligence.hbase._
import com.gravity.utilities.components.FailureResult

import scala.collection._
import scalaz.syntax.validation._
import scalaz.{Failure, Success, ValidationNel}

/**
 * Created by agrealish14 on 6/11/15.
 */
trait ScopedMetricsSource {
 import com.gravity.logging.Logging._

  def getFromHBase(keys: Set[ScopedFromToKey], skipCache: Boolean = true)(query: ScopedMetricsService.QuerySpec): ValidationNel[FailureResult, Map[ScopedFromToKey, ScopedMetricsBuckets]] = {

    val finalMetrics = mutable.Map[ScopedFromToKey, ScopedMetricsBuckets]().withDefaultValue(new ScopedMetricsBuckets())

    ScopedMetricsService.fetchMulti(keys, skipCache = true, returnEmptyResults = true)(query) match {
      case Success(hits) => {

        hits.foreach {
          case (key: ScopedFromToKey, row: ScopedMetricsRow) =>

            val minutelyRecoMetrics = row.scopedMetricsByMinute
            val hourlyRecoMetrics = row.scopedMetricsByHour
            val dailyRecoMetrics = row.scopedMetricsByDay
            val monthlyRecoMetrics = row.scopedMetricsByMonth
            finalMetrics += key -> ScopedMetricsBuckets(minutely=new ScopedMetricMap(minutelyRecoMetrics), hourly=new ScopedMetricMap(hourlyRecoMetrics), daily=new ScopedMetricMap(dailyRecoMetrics), monthly=new ScopedMetricMap(monthlyRecoMetrics))

        }
        finalMetrics.toMap.successNel
      }
      case Failure(failures) => {
        warn(failures, "Unable to fetch scoped metrics from Hbase")
        FailureResult("Unable to fetch scoped metrics from Hbase" + failures).failureNel
      }
    }
  }

}
