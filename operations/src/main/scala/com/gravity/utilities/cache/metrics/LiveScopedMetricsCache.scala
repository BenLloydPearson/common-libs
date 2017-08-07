package com.gravity.utilities.cache.metrics

import com.gravity.interests.jobs.intelligence.hbase.{ScopedMetricsKey, ScopedMetricsService, ScopedFromToKey}

import scala.collection.Map

/**
 * Created by agrealish14 on 10/3/16.
 */
trait LiveScopedMetricsCache {

  def add(scopedFromToKey: ScopedFromToKey, keyVals: Map[ScopedMetricsKey, Long])
}
