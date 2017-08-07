package com.gravity.utilities.cache.metrics.impl

import java.text.SimpleDateFormat
import java.util.Date

import com.googlecode.concurrentlinkedhashmap.{ConcurrentLinkedHashMap, EvictionListener}
import com.gravity.interests.jobs.intelligence.HostnameKey
import com.gravity.interests.jobs.intelligence.Trace.Tag
import com.gravity.interests.jobs.intelligence.hbase.{ScopedFromToKey, ScopedMetricsBuckets, ScopedMetricsService}
import com.gravity.service
import com.gravity.service.grvroles
import com.gravity.trace.GravityTrace
import com.gravity.utilities.cache.metrics.ScopedMetricsCache
import com.gravity.utilities.components.FailureResult

import scala.collection.JavaConversions._
import scala.collection._
import scalaz.syntax.validation._
import scalaz.{Failure, Success, ValidationNel}

/**
 * Created by agrealish14 on 4/5/15.
 */
object LruScopedMetricsCache extends ScopedMetricsCache with ScopedMetricsSource {
 import com.gravity.logging.Logging._
  import com.gravity.utilities.Counters._
  val counterCategory = "LruScopedMetricsCache"

  val listener = new EvictionListener[ScopedFromToKey, ScopedMetricsBuckets]() {
    override def onEviction(key: ScopedFromToKey, value: ScopedMetricsBuckets): Unit = {
      ifTrace(trace("LruScopedMetricsCache.evicted: " + key))
      countPerSecond(counterCategory, "LruScopedMetricsCache.evicted")
    }
  }

  val MAX_ITEMS: Int = if(service.grvroles.isInOneOfRoles(grvroles.RECOGENERATION)) 500000 else 1500000

  val lruCache = new ConcurrentLinkedHashMap.Builder[ScopedFromToKey, ScopedMetricsBuckets]()
    .maximumWeightedCapacity(MAX_ITEMS)
    .listener(listener)
    .build()

  private def getFromCacheQuietly(keys: Set[ScopedFromToKey]): Map[ScopedFromToKey, ScopedMetricsBuckets] = {
    countPerSecond(counterCategory, "LruScopedMetricsCache.getFromCacheQuietly.called")
    keys.flatMap(k => Option(lruCache.getQuietly(k)).map(metrics => k -> metrics)).toMap
  }

  override def get(keys: Set[ScopedFromToKey], fromCacheOnly: Boolean = false)(query: ScopedMetricsService.QuerySpec): ValidationNel[FailureResult,Map[ScopedFromToKey, ScopedMetricsBuckets]] = {

    if (fromCacheOnly) return getFromCacheQuietly(keys).successNel

    var missKeys = mutable.ArrayBuffer[ScopedFromToKey]()

    val resultsMap = keys.map(k => {

      val metrics = lruCache.get(k)

      if (metrics == null) {

        missKeys += k
      }

      k -> metrics
    }).toMap


    if(missKeys.nonEmpty) {

      countPerSecond(counterCategory, "LruScopedMetricsCache.miss")

      fetchFromOrigin(missKeys.toSet)(query) match {
        case Success(hits: Map[ScopedFromToKey, ScopedMetricsBuckets]) => {

          (resultsMap ++ hits).successNel
        }
        case Failure(failures) => {
          warn(failures, "Unable to fetch scoped metrics from Hbase")
          countPerSecond(counterCategory, "LruScopedMetricsCache.error")
          FailureResult("Unable to fetch scoped metrics from Hbase").failureNel
        }
      }

    } else {

      countPerSecond(counterCategory, "LruScopedMetricsCache.hit")

      resultsMap.successNel
    }

  }

  def fetchFromOrigin(keys: Set[ScopedFromToKey])(query: ScopedMetricsService.QuerySpec): ValidationNel[FailureResult,Map[ScopedFromToKey, ScopedMetricsBuckets]] = {

    getFromHBase(keys)(query) match {
      case Success(hits: Map[ScopedFromToKey, ScopedMetricsBuckets]) => {

        hits.foreach(hit => {

          if(!lruCache.containsKey(hit._1)) {
            lruCache.put(hit._1, hit._2)
          }

        })

        hits.successNel
      }
      case Failure(failures) => {
        warn(failures, "Unable to fetch scoped metrics from Hbase")
        FailureResult("Unable to fetch  scoped metrics from Hbase").failureNel
      }
    }
  }

  val BATCH_SIZE = 20000

  val dateFormatter = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSZ")

  override def refresh()(query: ScopedMetricsService.QuerySpec) {

    try {

      val keysCopy = lruCache.keySet().toSet

      if(keysCopy.nonEmpty) {

        val trace = GravityTrace(dateFormatter.format(new Date()))
        val started = System.currentTimeMillis()

        val groupedKeys = keysCopy.grouped(BATCH_SIZE).toList

        groupedKeys.foreach(keySet => {

          getFromHBase(keySet)(query) match {
            case Success(hits: Map[ScopedFromToKey, ScopedMetricsBuckets]) => {

              hits.foreach(row => {

                lruCache.putQuietly(row._1, row._2)
              })

              countPerSecond(counterCategory, "LruScopedMetricsCache.refresh.batch")
            }
            case Failure(failures) => {
              warn(failures, "Unable to fetch scoped metrics from Hbase")
            }
          }

        })

        trace.timedEvent(
          HostnameKey(com.gravity.trace.hostname).toScopedKey,
          com.gravity.trace.RELOAD_SCOPED_METRICS,
          Seq[Tag](Tag(com.gravity.trace.KEY_COUNT, keysCopy.size.toString), Tag(com.gravity.trace.BATCH_COUNT, groupedKeys.size.toString)),
          started)

      }

    } catch {
      case ex: Exception => {
        warn(ex, "Unable to reload scoped metrics")
      }
    }

  }
}
