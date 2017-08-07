package com.gravity.utilities.cache.metrics.impl

import com.gravity.interests.jobs.intelligence.hbase.{ScopedFromToKey, ScopedMetricsBuckets, ScopedMetricsService}
import com.gravity.utilities.cache.metrics.ScopedMetricsCache
import com.gravity.utilities.components.FailureResult

import scala.collection._
import scalaz.ValidationNel

/**
 * Created by agrealish14 on 6/11/15.
 *
 * No cache for Adminpages so scoped metrics are up to the minute
 */
object ScopedMetricsSkipCache extends ScopedMetricsCache with ScopedMetricsSource {
 import com.gravity.logging.Logging._

  override def get(keys: Set[ScopedFromToKey], fromCacheOnly: Boolean = false)(query: ScopedMetricsService.QuerySpec): ValidationNel[FailureResult, Map[ScopedFromToKey, ScopedMetricsBuckets]] = {

    getFromHBase(keys)(query)
  }

  override def refresh()(query: ScopedMetricsService.QuerySpec): Unit = {}
}
