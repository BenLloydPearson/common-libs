package com.gravity.utilities.grvcache

import java.util.concurrent._

import com.google.common.cache.CacheBuilder
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

class MemoryCache[K, V](val name: String, val maxItems: Int = 50000, val defaultTimeout: FiniteDuration = Cache.defaultTimeout, val defaultTtl: FiniteDuration = Cache.defaultTtl, val concurrencyLevel: Int = 8) extends Cache[K, V] {
  private val cache = CacheBuilder.newBuilder()
                                  .concurrencyLevel(concurrencyLevel)
                                  .expireAfterAccess(defaultTtl.toSeconds, TimeUnit.SECONDS)
                                  .maximumSize(maxItems)
                                  .build[AnyRef, AnyRef]().asMap().asInstanceOf[java.util.Map[K, V]]

  override def internalGet(key: K, timeout: FiniteDuration = defaultTimeout): ValidationNel[FailureResult, CacheResult[V]] = execute(timeout) {
    val res = (if (cache.containsKey(key)) { Found[V](cache.get(key)) } else { NotFound: CacheResult[V] }).successNel
    res
  }

  override def internalPut(key: K, value: V, timeout: FiniteDuration = defaultTimeout, ttl: FiniteDuration = defaultTtl): ValidationNel[FailureResult, CacheResult[V]] = execute(timeout) {
    cache.put(key, value)
    ValueSet(value).successNel
  }

  override protected[this] def internalRemove(key: K, timeout: FiniteDuration = defaultTimeout): ValidationNel[FailureResult, CacheResult[V]] = execute(timeout) {
    cache.remove(key)
    ValueRemoved.successNel
  }
}
