package com.gravity.utilities.grvcache

import com.gravity.utilities.components._

import scala.concurrent.duration.FiniteDuration
import scalaz.syntax.validation._
import scalaz.{Failure, Success, ValidationNel}

/*
 *    __   _         __
 *   / /  (_)__  ___/ /__  ____
 *  / _ \/ / _ \/ _  / _ \/ _  /
 * /_//_/_/_//_/\_,_/\___/\_, /
 *                       /___/
 *
 */

abstract class CacheResultCache[K, V](val cache: Cache[K, V], val maxItems: Int = 100000, val defaultTimeout: FiniteDuration = Cache.defaultTimeout, val defaultTtl: FiniteDuration = Cache.defaultTtl)
extends Cache[K, V] {
 import com.gravity.logging.Logging._
  protected val resultCache: Cache[K, ValidationNel[FailureResult, CacheResult[V]]] = new MemoryCache[K, ValidationNel[FailureResult, CacheResult[V]]]("result cache", maxItems = maxItems, defaultTtl = defaultTtl)

  override def internalPut(key: K, value: V, timeout: FiniteDuration = defaultTimeout, ttl: FiniteDuration = defaultTtl): ValidationNel[FailureResult, CacheResult[V]] = {
    // nothing to set on this cache, just forward to delegate cache
    cache.put(key, value, timeout, ttl)
  }

  override def internalGet(key: K, timeout: FiniteDuration = defaultTimeout): ValidationNel[FailureResult, CacheResult[V]] = {
    resultCache.get(key, timeout) match {
      case Success(Found(result)) => result // we found a cached result
      case Failure(fails) => fails.failure[CacheResult[V]] // failure while looking up cached result
      case _ => {
        val result = cache.get(key) // invoke delegate
        if (isCacheableResult(result)) { // check if cacheable
          resultCache.put(key, result, timeout, defaultTtl) // cache result for next lookup
        }
        result
      }
    }
  }

  override protected[this] def internalRemove(key: K, timeout: FiniteDuration): ValidationNel[FailureResult, CacheResult[V]] = resultCache.remove(key, timeout) match {
    case Success(ValueRemoved) => ValueRemoved.successNel[FailureResult]
    case Success(res) => UnexpectedState(res).failureNel[CacheResult[V]]
    case Failure(fails) => fails.failure[CacheResult[V]]
  }

  val isCacheableResult: PartialFunction[ValidationNel[FailureResult, CacheResult[V]], Boolean]
}
