package com.gravity.utilities.cache

import com.gravity.utilities.Settings

import scala.collection.JavaConverters._

/*             )\._.,--....,'``.
.b--.        /;   _.. \   _\  (`._ ,.
`=,-,-'~~~   `----(,_..'--(,_..'`-.;.'  */

/**
 * When an exclusive access temporary cache is required.
 * @tparam V
 */
trait SingletonCache[V <: AnyRef] {
  def cacheName: String

  val max: Int = if (Settings.getHeapSizeMB > 10000) 20000 else 15000
  val cache: RefCacheInstance[String, V] = EhCacher.getOrCreateRefCache[String,V](cacheName, maxItemsInMemory = max)

  def getOrUpdate[T <: V](key: String, factory: => T, ttlSeconds: Int) : T = {
    cache.getItemOrUpdateWithTTL(key, factory, ttlSeconds).asInstanceOf[T]
  }

  def getOrUpdate[T <: V](key:String, ttlSeconds: Int)(factory : => T) : T = {
    cache.getItemOrUpdateWithTTL(key, factory, ttlSeconds).asInstanceOf[T]
  }

  def getKeysNoDuplicateCheck: Seq[String] = {
    val res = cache.cache.getKeysNoDuplicateCheck.asScala
    res.toSeq.collect {
      case itm: String => itm
    }
  }

}

/**
  * When an exclusive access temporary cache is required. This trait uses Longs for the key
  * @tparam V
  */
trait LongKeySingletonCache[V <: AnyRef] {
  def cacheName: String

  val max: Int = if (Settings.getHeapSizeMB > 10000) 20000 else 15000
  val cache: RefCacheInstance[Long, V] = EhCacher.getOrCreateRefCache[Long, V](cacheName, maxItemsInMemory = max)

  def getOrUpdate[T <: V](key: Long, factory: => T, ttlSeconds: Int): T = {
    cache.getItemOrUpdateWithTTL(key, factory, ttlSeconds).asInstanceOf[T]
  }

  def getOrUpdate[T <: V](key: Long, ttlSeconds: Int)(factory: => T): T = {
    cache.getItemOrUpdateWithTTL(key, factory, ttlSeconds).asInstanceOf[T]
  }

  def getKeysNoDuplicateCheck: Seq[Long] = {
    val res = cache.cache.getKeysNoDuplicateCheck.asScala
    res.toSeq.collect {
      case itm: Long => itm
    }
  }
}

/**
 * When an exclusive access (with atomic updates) temporary cache is required.
 * NOTE:  This was originally [[SingletonCache]], but @chris and I (@robbie) couldn't
 *        truly identify the reasoning behind this additional locking, so I made the original
 *        without locking in order to have a version without it for simplicity.
 * @tparam V
 */
trait SingletonLockingCache[V <: AnyRef] extends SingletonCache[V] with Locking {

  override def getOrUpdate[T <: V](key: String, factory : => T, ttlSeconds: Int) : T = withLock(key) {
    super.getOrUpdate(key, factory, ttlSeconds)
  }

  override def getOrUpdate[T <: V](key: String, ttlSeconds: Int)(factory : => T) : T = withLock(key) {
    super.getOrUpdate(key, factory, ttlSeconds)
  }

}

object GrvMapJsonCacher extends SingletonCache[String] {
  override def cacheName = "gen-grvmap-json"
}

/**Simple generic cache for items that should not be reloaded in the background, and instead should naturally leave.
  *
  */
object TempCacher extends SingletonLockingCache[AnyRef] {
  override def cacheName = "gen-tmp"
}

/*
 * Simple boxing of a boolean value for ref caching
 * @param isTrue the boolean value we're wrapping (boxing)
 */
case class BoolWrap(isTrue: Boolean = false)

object BooleanCacher {
  val cache: RefCacheInstance[String, BoolWrap] = EhCacher.getOrCreateRefCache[String, BoolWrap]("bool-tmp", maxItemsInMemory = 50000)

  def getOrUpdate(key:String, factory : => Boolean, ttlSeconds: Int) : Boolean = {
      cache.getItemOrUpdateWithTTL(key, BoolWrap(factory), ttlSeconds).isTrue
    }
}