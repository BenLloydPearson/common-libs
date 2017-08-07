package com.gravity.utilities.cache.metrics.impl

import java.text.SimpleDateFormat
import java.util.Date

import com.googlecode.concurrentlinkedhashmap.{ConcurrentLinkedHashMap, EvictionListener}
import com.gravity.interests.jobs.intelligence.HostnameKey
import com.gravity.interests.jobs.intelligence.Trace.Tag
import com.gravity.interests.jobs.intelligence.hbase._
import com.gravity.service
import com.gravity.service.grvroles
import com.gravity.trace.GravityTrace
import com.gravity.utilities.cache.metrics.impl.LruScopedMetricsCache._
import com.gravity.utilities.cache.metrics.model.LiveScopedMetric
import com.gravity.utilities.cache.metrics.query.ScopedMetricsQuery
import com.gravity.utilities.cache.metrics.{LiveMetricsCacheDebugger, LiveScopedMetricsCache, ScopedMetricsCache}
import com.gravity.utilities.components.FailureResult

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._
import scala.collection._
import scalaz.syntax.validation._
import scalaz.{Failure, Success, ValidationNel}

/**
 * Created by agrealish14 on 9/28/16.
 */
object LruLiveScopedMetricsCache extends ScopedMetricsCache with LiveScopedMetricsCache with LiveMetricsCacheDebugger {
  import com.gravity.logging.Logging._
  import com.gravity.utilities.Counters._

  val counterCategory = "Lru Live Scoped Metrics Cache"
  
  val listener = new EvictionListener[ScopedFromToKey, LiveScopedMetric]() {
    override def onEviction(key: ScopedFromToKey, value: LiveScopedMetric): Unit = {
      ifTrace(trace("LruLiveScopedMetricsCache.evicted: " + key))
      countPerSecond(counterCategory, "LruLiveScopedMetricsCache.evicted")
    }
  }

  val MAX_ITEMS: Int = if (service.grvroles.isInOneOfRoles(grvroles.METRICS_LIVE)) 1000000 else 1500

  val lruCache = new ConcurrentLinkedHashMap.Builder[ScopedFromToKey, LiveScopedMetric]()
    .maximumWeightedCapacity(MAX_ITEMS)
    .listener(listener)
    .build()

  override def add(scopedFromToKey: ScopedFromToKey, keyVals: Map[ScopedMetricsKey, Long]): Unit = {

    internalGet(Set(scopedFromToKey))(ScopedMetricsQuery.scopedMetricsQuery()) match {
      case Success(hits: Map[ScopedFromToKey, LiveScopedMetric]) => {

        hits.foreach(row => {
          row._2.add(keyVals)
        })

        countPerSecond(counterCategory, "LruLiveScopedMetricsCache.add")
      }
      case Failure(failures) => {
        warn(failures, "Unable to add scoped metrics to live metrics")
        countPerSecond(counterCategory, "LruLiveScopedMetricsCache.add.error")
      }
    }

  }

  override def get(keys: Set[ScopedFromToKey], fromCacheOnly: Boolean = false)(query: ScopedMetricsService.QuerySpec): ValidationNel[FailureResult,Map[ScopedFromToKey, ScopedMetricsBuckets]] = {

    internalGet(keys, fromCacheOnly)(query) match {
      case Success(hits: Map[ScopedFromToKey, LiveScopedMetric]) => {
        hits.map(row => { row._1 -> row._2.toScopedMetricsBuckets}).successNel
      }
      case Failure(failures) => {
        warn(failures, "Unable to fetch scoped metrics from Hbase")
        countPerSecond(counterCategory, "LruLiveScopedMetricsCache.error")
        FailureResult("Unable to fetch scoped metrics from Hbase").failureNel
      }
    }
  }

  private def internalGet(keys: Set[ScopedFromToKey], fromCacheOnly: Boolean = false)(query: ScopedMetricsService.QuerySpec): ValidationNel[FailureResult,Map[ScopedFromToKey, LiveScopedMetric]] = {

    if (fromCacheOnly) return getFromCacheQuietly(keys).successNel

    var missKeys = mutable.ArrayBuffer[ScopedFromToKey]()

    val resultsMap: Map[ScopedFromToKey, LiveScopedMetric] = keys.map(k => {

      val metrics = lruCache.get(k)

      if (metrics == null) {

        missKeys += k

        k-> new LiveScopedMetric(k)
      } else {

        k -> metrics
      }
    }).toMap

    if(missKeys.nonEmpty) {

      countPerSecond(counterCategory, "LruLiveScopedMetricsCache.miss")

      getFromOrigin(missKeys.toSet)(query) match {
        case Success(hits: Map[ScopedFromToKey, ScopedMetricsBuckets]) => {

          val liveMetricsMap: Map[ScopedFromToKey, LiveScopedMetric] = hits.map(row => {

            val lsm = LiveScopedMetric(row._1, row._2)

            lruCache.put(row._1, LiveScopedMetric(row._1, row._2))

            row._1 -> lsm
          })

          (resultsMap ++ liveMetricsMap).successNel
        }
        case Failure(failures) => {
          warn(failures, "Unable to fetch scoped metrics from Hbase")
          countPerSecond(counterCategory, "LruLiveScopedMetricsCache.error")
          FailureResult("Unable to fetch scoped metrics from Hbase").failureNel
        }
      }

    } else {

      countPerSecond(counterCategory, "LruLiveScopedMetricsCache.hit")

      resultsMap.successNel
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

                lruCache.putQuietly(row._1, LiveScopedMetric(row._1, row._2))
              })

              countPerSecond(counterCategory, "LruLiveScopedMetricsCache.refresh.batch")
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

  private def getFromCacheQuietly(keys: Set[ScopedFromToKey]): Map[ScopedFromToKey, LiveScopedMetric] = {
    countPerSecond(counterCategory, "LruScopedMetricsCache.getFromCacheQuietly.called")
    keys.flatMap(k => Option(lruCache.getQuietly(k)).map(metrics => k -> metrics)).toMap
  }

  private def getFromOrigin(keys: Set[ScopedFromToKey])(query: ScopedMetricsService.QuerySpec): ValidationNel[FailureResult, Map[ScopedFromToKey, ScopedMetricsBuckets]] = {

    ScopedMetricsSkipCache.get(keys)(query)
  }

  // debug stuff
  override def getSize: Int = {

    lruCache.size()
  }

  override def getHotKeys(size: Int = 100): Seq[ScopedFromToKey] = {

    lruCache.descendingKeySetWithLimit(size).asScala.toSeq
  }

  override def getFromCache(key: ScopedFromToKey): Option[LiveScopedMetric] = {

    val user = lruCache.get(key)

    if (user != null) {

      Some(user)

    } else {
      None
    }

  }
}
