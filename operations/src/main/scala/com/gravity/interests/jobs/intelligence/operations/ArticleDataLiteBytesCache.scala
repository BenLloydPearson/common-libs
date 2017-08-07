package com.gravity.interests.jobs.intelligence.operations

/*
*     __         __
*  /"  "\     /"  "\
* (  (\  )___(  /)  )
*  \               /
*  /               \
* /    () ___ ()    \  erik 10/7/15
* |      (   )      |
*  \      \_/      /
*    \...__!__.../
*/

import akka.actor.ActorSystem
import com.googlecode.concurrentlinkedhashmap.{ConcurrentLinkedHashMap, EvictionListener}
import com.gravity.interests.jobs.intelligence._
import com.gravity.service.grvroles
import com.gravity.utilities.grvakka.Configuration._
import com.gravity.utilities.components.FailureResult
import scala.collection.JavaConversions._
import scala.collection._
import scalaz._, Scalaz._
import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits.global


object ArticleDataLiteBytesCache {
 import com.gravity.logging.Logging._
  val system = ActorSystem("ArticleDataLiteBytesCache", defaultConf)
  private val cacher = Schema.Articles.cache.asInstanceOf[IntelligenceSchemaCacher[ArticlesTable, ArticleKey, ArticleRow]]
  val counterCategory: String = "Article Data Lite Bytes LRU Cache"
  import com.gravity.utilities.Counters._

  val hitRatioCounter = getOrMakeHitRatioCounter("Hit Ratio", counterCategory, shouldLog = true)

  val listener = new EvictionListener[ArticleKey, Array[Byte]]() {
    override def onEviction(key: ArticleKey, value: Array[Byte]): Unit = {
      trace("LruArticleDataLiteBytesCache evicted: " + key)
      countPerSecond(counterCategory, "Evicted")
    }
  }

  val entrySize = 635l
  val maximumCachedBytes =
    if(grvroles.isInRole(grvroles.REMOTE_RECOS))
      8l * 1024l * 1024l * 1024l //8 gigs
   else
      3l * 1024l * 1024l * 1024l //3 gigs

  val maxItems = maximumCachedBytes / entrySize
  setAverageCount(counterCategory, "Capacity", maxItems)
  val lruCache = new ConcurrentLinkedHashMap.Builder[ArticleKey, Array[Byte]]()
      .maximumWeightedCapacity(maxItems)
      .listener(listener)
      .build()

  val refreshIntervalSeconds = 600
  val refreshBatchSize = 1000

  system.scheduler.scheduleOnce(refreshIntervalSeconds.seconds)(refresh())

  def getMulti(keys: Set[ArticleKey]): ValidationNel[FailureResult, Map[ArticleKey, Array[Byte]]] = {
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
          val hitBytes = hbaseHits.flatMap{ case (key, value) =>
            cacher.toBytesTransformer(value) match {
              case Success(rowBytes) =>
                Some(key -> rowBytes)
              case Failure(fails) =>
                warn("Failed to serialize row " + key.articleId + ": " + fails)
                None
            }
          }
          lruCache.putAll(hitBytes)
          (cacheHits ++ hitBytes).successNel
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
                cacher.toBytesTransformer(row._2) match {
                  case Success(rowBytes) =>
                    lruCache.putQuietly(row._1, rowBytes)
                  case Failure(fails) =>
                    warn("Failed to serialize row during refresh " + row._1.articleId + ": " + fails)
                    None
                }

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
      system.scheduler.scheduleOnce(refreshIntervalSeconds.seconds)(refresh())
    }
  }
}

