package com.gravity.utilities.cache.metrics

import com.gravity.interests.jobs.intelligence.hbase.ScopedFromToKey
import com.gravity.utilities.cache.metrics.model.LiveScopedMetric

/**
 * Created by agrealish14 on 10/17/16.
 */
trait LiveMetricsCacheDebugger {

  def getSize:Int

  def getHotKeys(size:Int = 100): Seq[ScopedFromToKey]

  def getFromCache(key: ScopedFromToKey): Option[LiveScopedMetric]
}
