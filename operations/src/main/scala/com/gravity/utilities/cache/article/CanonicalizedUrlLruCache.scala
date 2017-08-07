package com.gravity.utilities.cache.article

import com.googlecode.concurrentlinkedhashmap.{ConcurrentLinkedHashMap, EvictionListener}
import com.gravity.interests.jobs.intelligence.ArticleKey
import com.gravity.service
import com.gravity.service.grvroles

/*

         ▒▒
         ██
       ██████
     ██████████
     ██░░██░░██
     ██████████
       ██████
       ░░░░░░      It's gonna be fun on the bun!
       ██████

*/

object CanonicalizedUrlLruCache {
  import com.gravity.logging.Logging._
  import com.gravity.utilities.Counters._

  val counterCategory = "Lru Canonicalized Url Cache"

  //not sure if this gets called on clear()...
  val listener = new EvictionListener[ArticleKey, String]() {
    override def onEviction(key: ArticleKey, value: String): Unit = {
      ifTrace(trace("CanonicalizedUrlLruCache.evicted: " + key))
      countPerSecond(counterCategory, "CanonicalizedUrlLruCache.evicted")
    }
  }

  val MAX_ITEMS: Int = if (service.grvroles.isInOneOfRoles(grvroles.DATAFEEDS)) 50000 else 1500

  val lruCache = new ConcurrentLinkedHashMap.Builder[ArticleKey, String]()
    .maximumWeightedCapacity(MAX_ITEMS)
    .listener(listener)
    .build()

  def put(key: ArticleKey, value: String): String = {
    countPerSecond(counterCategory, "CanonicalizedUrlLruCache.put total")
    countPerSecond(counterCategory, "CanonicalizedUrlLruCache.put")
    lruCache.put(key,value)
  }

  def get(key: ArticleKey): Option[String] = {
    countPerSecond(counterCategory, "CanonicalizedUrlLruCache.get")
    val ret = Option(lruCache.get(key))
    ret match {
      case None => countPerSecond(counterCategory, "CanonicalizedUrlLruCache.miss")
      case Some(_) => countPerSecond(counterCategory, "CanonicalizedUrlLruCache.hit")
    }
    ret
  }

  def clear(): Unit = {
    countPerSecond(counterCategory, "CanonicalizedUrlLruCache.clear")
    lruCache.clear()
    clearCountPerSecond(counterCategory, "CanonicalizedUrlLruCache.put")
  }


  // debug stuff
  def getSize: Int = {

    lruCache.size()
  }

}
