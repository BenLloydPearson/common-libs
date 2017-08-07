package com.gravity.utilities.cache.throttle.impl

import com.googlecode.concurrentlinkedhashmap.{ConcurrentLinkedHashMap, EvictionListener}
import com.gravity.utilities.cache.throttle.Throttle

/**
 * Created by agrealish14 on 12/15/16.
 */
class LruThrottleImpl[K,V](name:String, maxItems:Int) extends Throttle[K,V] {
  import com.gravity.logging.Logging._
  import com.gravity.utilities.Counters._

  val counterCategory = name

  val itemCount : MaxCounter = getOrMakeMaxCounter(counterCategory, "Item Count")

  val listener = new EvictionListener[K, V]() {
    override def onEviction(key: K, value: V): Unit = {
      ifTrace(trace("evicted: " + key))
      countPerSecond(counterCategory, "evicted")
      itemCount.decrement
    }
  }

  info(s"Setting $name capacity to $maxItems")

  val lruCache = new ConcurrentLinkedHashMap.Builder[K, V]()
    .maximumWeightedCapacity(maxItems)
    .listener(listener)
    .build()

  override def get(key: K): Option[V] = {

    val user = lruCache.get(key)

    if (user != null) {

      countPerSecond(counterCategory, "hit")

      Some(user)

    } else {

      countPerSecond(counterCategory, "miss")

      None
    }
  }

  override def put(key: K, value: V): V = {
    itemCount.increment
    lruCache.put(key, value)
  }

  override def clear(): Unit = {
    countPerSecond(counterCategory, "clear")
    lruCache.clear()
    itemCount.set(0)
  }
}