package com.gravity.utilities.cache

import akka.actor.ActorSystem
import com.googlecode.concurrentlinkedhashmap.{ConcurrentLinkedHashMap, EvictionListener}
import com.gravity.utilities.components.FailureResult
import com.gravity.utilities.grvakka.Configuration._

import scala.collection.JavaConversions._
import scala.collection.mutable
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scalaz.syntax.validation._
import scalaz.{Failure, Success, ValidationNel}

/**
  * Created by robbie on 02/25/2016.
  *            _ 
  *          /  \
  *         / ..|\
  *        (_\  |_)
  *        /  \@'
  *       /     \
  *   _  /  \   |
  *  \\/  \  | _\
  *   \   /_ || \\_
  *    \____)|_) \_)
  *
  * Provides the basis for a refreshable LRU cache of key `K` and value `V`
  */
trait LruRefreshedCache[K, V] {
 import com.gravity.logging.Logging._
  import com.gravity.utilities.Counters._

  val counterCategory: String = name + " LRU Cache"

  /**
    * The name that counters and logging will be decorated with
    * @return This cache's specified name
    */
  def name: String

  /**
    * The maximum items to hold in this LRU cache. Older items will be dropped at capacity
    * @return This cache's specified maximum number of items
    */
  def maxItems: Int

  /**
    * The number of seconds between refreshes of this cache's items.
    * @return This cache's specified refresh interval in seconds
    */
  def refreshIntervalSeconds: Int

  /**
    * The maximum number of keys to refresh at a time from [[getFromOrigin()]]
    * @return This cache's specified refresh batch size
    */
  def refreshBatchSize: Int

  /**
    * The method used internally to refresh as well as fallback for cache misses in the [[getMulti()]] call
    * @param keys The keys to retrieve a value map from this cache's data origin
    * @return A validation of either failures or the successfully retrieved value map from origin
    */
  def getFromOrigin(keys: Set[K]): ValidationNel[FailureResult, Map[K, V]]

  /**
    * The primary exposed purpose of this cache: Get multiple values for the specified `keys` from cache and fallback to origin for any misses.
    * @param keys of the desired values
    * @return A validation of either failures or the successfully retrieved value map
    */
  def getMulti(keys: Set[K]): ValidationNel[FailureResult, Map[K, V]] = {
    val missKeys = mutable.ArrayBuffer[K]()

    val cacheHits = keys.flatMap(key => {
      Option(lruCache.get(key)) match {
        case Some(hit) =>
          Some(key -> hit)
        case None =>
          missKeys += key
          None
      }
    }).toMap

    if (missKeys.nonEmpty) {
      val numberOfMisses = missKeys.size
      val numberOfHits = keys.size - numberOfMisses
      hitRatioCounter.incrementHitsAndMisses(numberOfHits, numberOfMisses)

      getFromOrigin(missKeys.toSet) match {
        case Success(originResults) =>
          lruCache.putAll(originResults)
          (cacheHits ++ originResults).successNel
        case Failure(failures) =>
          countPerSecond(counterCategory, "Unable to fetch from origin", numberOfMisses)
          failures.failure
      }
    }
    else {
      hitRatioCounter.incrementHits(keys.size)
      cacheHits.successNel
    }
  }

  private val system = ActorSystem(name + "_System", defaultConf)

  private val hitRatioCounter = new HitRatioCounter("Hit Ratio", counterCategory, true)

  private val listener = new EvictionListener[K, V]() {
    def onEviction(key: K, value: V): Unit = {
      trace("{0} evicted: {1}", name, key)
      countPerSecond(counterCategory, "Evicted")
    }
  }

  // It's lazy so that we don't create the lruCache during trait-construction time; allow it to be constructed after the subclass exists.
  private lazy val lruCache = {
    val cache = new ConcurrentLinkedHashMap.Builder[K, V]()
      .maximumWeightedCapacity(maxItems)
      .listener(listener)
      .build()

    // Don't do the first schedule until the cache has been created.
    system.scheduler.scheduleOnce(refreshIntervalSeconds.seconds)(refresh())

    cache
  }

  // Force the cache to be emptied.  Mostly expected to be used for unit tests.
  def clear(): Unit = this.synchronized {
    lruCache.clear()
  }

  // synchronized so that any call to clear() will wait for this to finish before clearing the cache.
  private def refresh(): Unit = this.synchronized {
    try {
      countPerSecond(counterCategory, "Refreshes")

      val keysCopy = lruCache.keySet().toSet

      if (keysCopy.nonEmpty) {
        info(s"Refreshing $name LRU cache's ${keysCopy.size} keys in batches of $refreshBatchSize")

        keysCopy.grouped(refreshBatchSize).toList.foreach {
          keySet => getFromOrigin(keySet) match {
            case Success(batchResults) =>
              countPerSecond(counterCategory, "Refresh Batch Successes")
              batchResults.foreach {
                case (key, value) =>
                  countPerSecond(counterCategory, "Refreshed keys")
                  lruCache.putQuietly(key, value)
              }
            case Failure(fails) =>
              countPerSecond(counterCategory, "Refresh Batch Failures")
              warn(fails, s"Failed to refresh batch of keys for $name LRU cache!")
          }
        }
      }

      setAverageCount(counterCategory, "Weighted Size", lruCache.weightedSize())
    }
    catch {
      case e: Exception =>
        countPerSecond(counterCategory, "Refresh Exceptions")
        warn(e, "Exception refreshing {0} LRU cache", name)
    }
    finally {
      system.scheduler.scheduleOnce(refreshIntervalSeconds.seconds)(refresh())
    }
  }
}
