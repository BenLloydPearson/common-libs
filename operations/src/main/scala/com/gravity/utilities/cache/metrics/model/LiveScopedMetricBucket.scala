package com.gravity.utilities.cache.metrics.model

import scala.collection.{Map, mutable}

import com.gravity.interests.jobs.intelligence.hbase.{ScopedMetricMap, ScopedMetricsBuckets, ScopedMetricsKey}

/**
 * Created by agrealish14 on 11/10/16.
 */
case class LiveScopedMetricBucket(maxItems: Int = 240)  {

  val metrics = scala.collection.concurrent.TrieMap[ScopedMetricsKey, Long]()

  // bounded collection FIFO
  var q: mutable.Queue[ScopedMetricsKey] = new mutable.Queue[ScopedMetricsKey]()

  def add(newMetrics:Map[ScopedMetricsKey, Long]): Unit = {

    newMetrics.map((metrics: (ScopedMetricsKey, Long)) => {
      get(metrics._1) match {
        case Some(count) =>
          updateCount(metrics._1, count + metrics._2)
        case None =>
          addNew(metrics._1, metrics._2)
      }
    })
  }

  def setup(metricMap:ScopedMetricMap): Unit = {

    // sort so oldest is added first
    metricMap.map.toList.sortBy(_._1.dateTimeMs).foreach(row => {
      addNew(row._1, row._2)
    })
  }

  def toScopedMetricMap = new ScopedMetricMap(metrics)

  private def get(key: ScopedMetricsKey): Option[Long] = {
    metrics.get(key)
  }

  private def updateCount(key: ScopedMetricsKey, count:Long) = {
    metrics.replace(key, count)
  }

  private def addNew(key: ScopedMetricsKey, count:Long = 1L) = {

    metrics.put(key, count)

    q.enqueue(key)

    if(q.size > maxItems) {

      metrics.remove(q.dequeue())
    }
  }

}

object LiveScopedMetricBucket {

  def apply(maxItems: Int, metrics: ScopedMetricMap): LiveScopedMetricBucket = {
    val lsm = new LiveScopedMetricBucket(maxItems)
    lsm.setup(metrics)
    lsm
  }
}
