package com.gravity.utilities.cache.metrics

import com.gravity.interests.jobs.intelligence.hbase.{ScopedFromToKey, ScopedMetricsBuckets, ScopedMetricsService}
import com.gravity.utilities.components.FailureResult

import scala.collection.{Map, Set}
import scalaz._

/**
 * Created by agrealish14 on 2/23/15.
 */
trait ScopedMetricsCache {

  def get(keys: Set[ScopedFromToKey], fromCacheOnly: Boolean = false)(query: ScopedMetricsService.QuerySpec): ValidationNel[FailureResult,Map[ScopedFromToKey, ScopedMetricsBuckets]]

  def refresh()(query: ScopedMetricsService.QuerySpec)

}
