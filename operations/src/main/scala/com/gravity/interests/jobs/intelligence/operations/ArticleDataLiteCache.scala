package com.gravity.interests.jobs.intelligence.operations

/*
*     __         __
*  /"  "\     /"  "\
* (  (\  )___(  /)  )
*  \               /
*  /               \
* /    () ___ ()    \  erik 5/7/15
* |      (   )      |
*  \      \_/      /
*    \...__!__.../
*/

import akka.actor.ActorSystem
import com.googlecode.concurrentlinkedhashmap.{ConcurrentLinkedHashMap, EvictionListener}
import com.gravity.interests.jobs.intelligence.{ArticleKey, ArticleRow}
import com.gravity.service.grvroles
import com.gravity.utilities.components.FailureResult
import com.gravity.utilities.grvakka.Configuration._

import scala.collection.JavaConversions._
import scala.collection._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scalaz.Scalaz._
import scalaz._

object ArticleDataLiteCache {
 import com.gravity.logging.Logging._
  import com.gravity.utilities.Counters._

  val system = ActorSystem("ArticleDataLiteCache", defaultConf)

  val counterCategory: String = "Article Data Lite LRU Cache"

  val hitRatioCounter : HitRatioCounter = getOrMakeHitRatioCounter(counterCategory, "Hit Ratio", shouldLog = true)

  val listener = new EvictionListener[ArticleKey, ArticleRow]() {
    override def onEviction(key: ArticleKey, value: ArticleRow): Unit = {
      trace("LruArticleDataLiteCache evicted: " + key)
      countPerSecond(counterCategory, "Evicted")
    }
  }

//  val memoryMeter = new MemoryMeter()
//
//  override def weightOf(key: ArticleKey, value: ArticleRow) : Int = {
//    val bytes = memoryMeter.measure(key) + memoryMeter.measure(value)
//    val ret = math.min(bytes, Integer.MAX_VALUE).toInt
//    setAverageCount(counterCategory, "Entry size", ret)
//    ret
//  }

  val entrySize = 635l
  val maxItems = if(grvroles.isInRecoGenerationRole || grvroles.isInApiRole) 100000 else 50000

  setAverageCount(counterCategory, "Capacity", maxItems)
  val lruCache = new ConcurrentLinkedHashMap.Builder[ArticleKey, ArticleRow]()
      .maximumWeightedCapacity(maxItems)
      .listener(listener)
      .build()

  val refreshIntervalSeconds = 600
  val refreshBatchSize = 1000

  system.scheduler.scheduleOnce(refreshIntervalSeconds.seconds)({
    refresh()
  })

  def getMulti(keys: Set[ArticleKey]): ValidationNel[FailureResult, Map[ArticleKey, ArticleRow]] = {
    var missKeys = mutable.ArrayBuffer[ArticleKey]()

    val cacheHits = keys.map(k => {
      val article = lruCache.get(k)
      if (article == null) {
        missKeys += k
      }

      k -> article
    }).toMap


    if (missKeys.nonEmpty) {
      val numberOfMisses = missKeys.size
      val numberOfHits = keys.size - numberOfMisses
      hitRatioCounter.incrementHitsAndMisses(numberOfHits, numberOfMisses)

      ArticleDataLiteService.getFromHbase(missKeys.toSet, skipCache = true) match {
        case Success(hbaseHits: Map[ArticleKey, ArticleRow]) =>
          lruCache.putAll(hbaseHits)
          (cacheHits ++ hbaseHits).successNel
        case Failure(failures) =>
          countPerSecond(counterCategory, "Unable to fetch from Hbase", numberOfMisses)
          failures.failure
      }
    }
    else {
      hitRatioCounter.incrementHits(keys.size)
      cacheHits.successNel
    }
  }

  def refresh() {
    try {
      countPerSecond(counterCategory, "Refreshes")

      val keysCopy: immutable.Set[ArticleKey] = lruCache.keySet().toSet

      if (keysCopy.nonEmpty) {
        info("Refreshing " + keysCopy.size + " article data lite keys in batches of " + refreshBatchSize)

        val groupedKeys = keysCopy.grouped(refreshBatchSize).toList

        groupedKeys.foreach(keySet => {
          ArticleDataLiteService.getFromHbase(keySet, skipCache = true) match {
            case Success(hits: Map[ArticleKey, ArticleRow]) =>
              hits.foreach(row => {
                lruCache.putQuietly(row._1, row._2)
              })
              countPerSecond(counterCategory, "Refresh batches")
              countPerSecond(counterCategory, "Refreshed keys", hits.size)
            case Failure(failures) =>
              countPerSecond(counterCategory, "Refresh batch errors")
              countPerSecond(counterCategory, "Unable to fetch from Hbase in refresh", keySet.size)
              warn(failures, "Unable to fetch Article Data Lite from Hbase")
          }
        })

        info("Done refreshing " + keysCopy.size + " article data lite keys in batches of " + refreshBatchSize)
      }

      setAverageCount(counterCategory, "Weighted Size", lruCache.weightedSize())
    }
    catch {
      case e:Exception =>
        countPerSecond(counterCategory, "Refresh Exceptions")
        warn(e, "Exception refreshing article data lite cache")
    }
    finally {
      system.scheduler.scheduleOnce(refreshIntervalSeconds.seconds)({
        refresh()
      })
    }
  }
}

