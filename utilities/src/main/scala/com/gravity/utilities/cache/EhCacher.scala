package com.gravity.utilities.cache

import java.io.Serializable
import java.lang.management.ManagementFactory

import net.sf.ehcache.config.{CacheConfiguration, PersistenceConfiguration}
import net.sf.ehcache.management.ManagementService
import net.sf.ehcache.store.MemoryStoreEvictionPolicy
import net.sf.ehcache.{CacheException, CacheManager, Element, Cache => EhCache}
import com.gravity.grvlogging._

/**
 * Created by IntelliJ IDEA.
 * Author: Robbie Coleman
 * Date: 7/6/11
 * Time: 11:31 AM
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
 */
object EhCacher {
 import com.gravity.logging.Logging._
  val TTL_DEFAULT = 120

  private val mgr = CacheManager.getInstance()

  def shutdown(): Unit = {
    mgr.shutdown()
  }

  def clear(): Unit = {
    mgr.clearAll()
  }

  private val noPersistence = new PersistenceConfiguration().strategy(PersistenceConfiguration.Strategy.NONE)
  private val diskPersistence = new PersistenceConfiguration().strategy(PersistenceConfiguration.Strategy.LOCALTEMPSWAP)
//  private val sizeOfPolicy = new SizeOfPolicyConfiguration().maxDepthExceededBehavior(SizeOfPolicyConfiguration.MaxDepthExceededBehavior.ABORT)

  private val defaultConfig = new CacheConfiguration("default", 50000)
                    .memoryStoreEvictionPolicy(MemoryStoreEvictionPolicy.LRU)
                    .eternal(false)
                    .timeToLiveSeconds(TTL_DEFAULT)
                    .timeToIdleSeconds(TTL_DEFAULT)
                    .diskExpiryThreadIntervalSeconds(TTL_DEFAULT)
                    .persistence(noPersistence)
                    .overflowToOffHeap(false)
//                    .sizeOfPolicy(sizeOfPolicy)

//  private val offHeapConfig = new CacheConfiguration("offheap", 50000)
//    .memoryStoreEvictionPolicy(MemoryStoreEvictionPolicy.LRU)
//    .eternal(false)
//    .timeToLiveSeconds(TTL_DEFAULT)
//    .timeToIdleSeconds(TTL_DEFAULT)
//    .diskExpiryThreadIntervalSeconds(TTL_DEFAULT)
//    .persistence(noPersistence)
//    .overflowToOffHeap(true)
//  //                    .sizeOfPolicy(sizeOfPolicy)


  private def getOrCreateBaseCache(name: String, ttlSeconds: Int = TTL_DEFAULT, persistToDisk: Boolean = false, maxItemsInMemory: Int = 50000, maxLocalBytes: Long = 0): EhCache = {
    mgr.getCache(name) match {
      case cache: EhCache => cache
      case null =>
        val config = if (persistToDisk) {
          defaultConfig.clone().persistence(diskPersistence)
        }
        else {
          defaultConfig.clone()
        }

        config.setName(name)
        config.setDiskExpiryThreadIntervalSeconds(ttlSeconds)

        if (maxItemsInMemory > 0) {
          config.setMaxEntriesLocalHeap(maxItemsInMemory)
        }
        else if (maxLocalBytes > 0) {
          config.setMaxEntriesLocalHeap(0)
          if(persistToDisk) config.setMaxBytesLocalDisk(maxLocalBytes)
          config.setMaxBytesLocalHeap(maxLocalBytes)
        }

        config.setTimeToLiveSeconds(ttlSeconds)
        config.setTimeToIdleSeconds(ttlSeconds)
        val cache = new EhCache(config)
        trace("Created on heap eh cache named " + name + " with " + maxLocalBytes + " bytes capacity.")
        mgr.addCache(cache)
        cache
    }
  }

//  private def getOrCreateOffHeapBaseCache(name: String, ttlSeconds: Int = TTL_DEFAULT, persistToDisk: Boolean = false, maxLocalBytes: Long): EhCache = {
//    mgr.getCache(name) match {
//      case cache: EhCache => cache
//      case null =>
//        val config = if (persistToDisk) {
//          offHeapConfig.clone().persistence(diskPersistence)
//        }
//        else {
//          offHeapConfig.clone()
//        }
//
//        config.setName(name)
//        config.setDiskExpiryThreadIntervalSeconds(ttlSeconds)
//
//        if (maxLocalBytes > 0) {
//          config.setMaxEntriesLocalHeap(1) //0 means unlimited. 1 means none. because obviously.
//          if(persistToDisk) config.setMaxBytesLocalDisk(maxLocalBytes)
//          config.setMaxBytesLocalOffHeap(maxLocalBytes)
//        }
//
//        config.setTimeToLiveSeconds(ttlSeconds)
//        config.setTimeToIdleSeconds(ttlSeconds)
//        val cache = new EhCache(config)
//        info("Created off heap eh cache named " + name + " with " + maxLocalBytes + " bytes capacity.")
//        mgr.addCache(cache)
//        cache
//    }
//  }

  def getOrCreateRefCache[K <: Any, T <: Any](name: String, ttlSeconds: Int = TTL_DEFAULT, persistToDisk: Boolean = false, maxItemsInMemory: Int = 50000): RefCacheInstance[K, T] = {
    RefCacheInstance[K, T](getOrCreateBaseCache(name, ttlSeconds, persistToDisk, maxItemsInMemory))
  }

  /** Preferred simplified way of creating a cache instance for most usages.
   * @param name How this cache instance will be uniquely identified
   * @param ttlSeconds the number of seconds each item should remain present within this cache
   * @tparam K the cache key type. Must be Serializable
   * @tparam T the cache value type. Must be Serializable
   * @return A fully initialized CacheInstance that you can use to put and get items
   */
  def getOrCreateCache[K <: Serializable, T <: Serializable](name: String, ttlSeconds: Int = TTL_DEFAULT, persistToDisk: Boolean = false, maxItemsInMemory: Int = 50000, maxLocalBytes: Long = 0): CacheInstance[K, T] = {
    CacheInstance[K, T](getOrCreateBaseCache(name, ttlSeconds, persistToDisk, maxItemsInMemory, maxLocalBytes))
  }

  /*
  net.sf.ehcache.CacheException: Cache EhCacherTest cannot be configured because the enterprise features manager could not be found. You must use an enterprise version of Ehcache to successfully enable overflowToOffHeap.
   */
//  def getOrCreateOffHeapCache[K <: Serializable, T <: Serializable](name: String, ttlSeconds: Int = TTL_DEFAULT, persistToDisk: Boolean = false, maxLocalBytes: Long = 0): CacheInstance[K, T] = {
//    CacheInstance[K, T](getOrCreateOffHeapBaseCache(name, ttlSeconds, persistToDisk, maxLocalBytes))
//  }

  try {
    ManagementService.registerMBeans(mgr, ManagementFactory.getPlatformMBeanServer, true, true, true, true)
  } catch {
    case ex:Exception => warn(ex, "Failed to register EhCache MBeans")
  }
}

class CacheInstanceBase[K, T](cache: EhCache) {
  def contains(key: K): Boolean = cache.isKeyInCache(key)

  private def getElement(key: K): Option[Element] = Option(cache.get(key))

  def valueExistForKey(key: K): Boolean = Option(cache.getQuiet(key)).isDefined

  /** Retrieves an item from this cache instance as an Option to inspect
   * @param key the identifying key for the item to retrieve
   * @return Some(T) if present in this cache otherwise None
   */
  def getItem(key: K): Option[T] = {
    getElement(key).map(_.getObjectValue.asInstanceOf[T])
  }

  /**
   * @param modTimeInSeconds Known modification time of subject data in seconds.
   *
   * @return The cached item only if it is available in cache and created on or after the given modTimeInSeconds.
   */
  def getItemIfNotModified(key: K, modTimeInSeconds: Long): Option[T] = {
    val elemOpt = getElement(key)

    // Item was in cache
    elemOpt match {
      case Some(elem) if elem.getCreationTime / 1000 >= modTimeInSeconds =>
        Some(elem.getObjectValue.asInstanceOf[T])
      case Some(elem) =>
        removeItem(key)
        None
      case _ =>
        None
    }
  }

  /**
   * Retrieves an item from the cache.  If it is not there, the supplied function should create the item, at which time it will be put into the cache and returned
   */
  def getItemOrUpdate(key: K, op: => T): T = {
    getElement(key) match {
      case Some(elem) => elem.getObjectValue.asInstanceOf[T]
      case None =>
        val value = op
        putItem(key, value)
        value
    }
  }

  def getItemOrUpdateWithTTL(key: K, op: => T, ttlSeconds: Int): T = {
    getElement(key) match {
      case Some(elem) => elem.getObjectValue.asInstanceOf[T]
      case None =>
        val value = op
        putItem(key, value, ttlSeconds)
        value
    }
  }

  /** Put an item into this cache instance
   * @param key the identifying key for the item to put into this cache
   * @param item the item to cache
   */
  def putItem(key: K, item: T): Unit = { cache.put(new Element(key, item)) }

  def putItem(key: K, item: T, ttl: Int): Unit = {
    val element = new Element(key, item)
    element.setTimeToLive(ttl)
    cache.put(element)
  }

  def putItem(key: K, item: T, ttl: Int, tti: Int): Unit = {
    val element = new Element(key, item)
    element.setTimeToLive(ttl)
    element.setTimeToIdle(tti)
    cache.put(element)
  }

  def removeItem(key: K): Boolean = try {
    cache.remove(key)
  }
  catch {
    case ce: CacheException =>
      cache.flush()
      warn(ce, "Failed to remove key: {0} from cache: {1}. Entire cache has been flushed to clear bad data!", key, cache.getName)
      false
  }

}

case class RefCacheInstance[K <: Any, T <: Any](cache: EhCache) extends CacheInstanceBase[K, T](cache)

case class CacheInstance[K <: Serializable, T <: Serializable](cache: EhCache) extends CacheInstanceBase[K, T](cache)
