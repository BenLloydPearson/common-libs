package com.gravity.utilities.grvcache

import com.gravity.utilities.cache.{EhCacher, RefCacheInstance}
import com.gravity.utilities.components._

import scala.concurrent.duration.FiniteDuration
import scalaz.ValidationNel
import scalaz.syntax.validation._

/*
 *    __   _         __
 *   / /  (_)__  ___/ /__  ____
 *  / _ \/ / _ \/ _  / _ \/ _  /
 * /_//_/_/_//_/\_,_/\___/\_, /
 *                       /___/
 *
 */

class EhCacheReferenceCache[K, V](val name: String, val maxItems: Int = 50000, val defaultTimeout: FiniteDuration = Cache.defaultTimeout, val defaultTtl: FiniteDuration = Cache.defaultTtl) extends Cache[K, V] {
  val ehCache: RefCacheInstance[K, V] = EhCacher.getOrCreateRefCache[K, V](name, defaultTtl.toSeconds.toInt, false, maxItems)

  override def internalGet(key: K, timeout: FiniteDuration = defaultTimeout): ValidationNel[FailureResult, CacheResult[V]] = execute(timeout) {
    ehCache.getItem(key).fold(NotFound: CacheResult[V])(v => Found(v)).successNel
  }

  override def internalPut(key: K, value: V, timeout: FiniteDuration = defaultTimeout, ttl: FiniteDuration = defaultTtl): ValidationNel[FailureResult, CacheResult[V]] = execute(timeout) {
    ehCache.putItem(key, value, ttl.toSeconds.toInt)
    ValueSet(value).successNel
  }

  override protected[this] def internalRemove(key: K, timeout: FiniteDuration): ValidationNel[FailureResult, CacheResult[V]] = execute(timeout) {
    ehCache.removeItem(key)
    ValueRemoved.successNel
  }
}
