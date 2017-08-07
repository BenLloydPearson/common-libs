package com.gravity.utilities.grvcache

import com.gravity.utilities.components._
import com.gravity.utilities.grvfunc._

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

// Improvement: figure out best way to handle timeout/ttl across primary/secondary caches
class DelegatingCache[K, V](primary: Cache[K, V], secondary: Cache[K, V], propagateSet: => Boolean = true, propagateGet: => Boolean = true, logPrimaryFailures: Boolean = true) extends Cache[K, V] {
 import com.gravity.logging.Logging._

  override val maxItems: Int = Math.max(primary.maxItems, secondary.maxItems)
  override val name: String = primary.name + " delgating to " + secondary.name
  override val defaultTtl: FiniteDuration = primary.defaultTtl.max(secondary.defaultTtl)
  override val defaultTimeout: FiniteDuration = primary.defaultTimeout.max(secondary.defaultTimeout)

  override def internalGet(key: K, timeout: FiniteDuration = defaultTimeout): ValidationNel[FailureResult, CacheResult[V]] = execute(timeout) {
    primary.get(key).ifThen(propagateGet) {
      case Success(NotFound) => secondary.get(key, timeout) // not found in primary, fallback to secondary
      case Success(res) => UnexpectedState(res).failureNel
      case Failure(fails) => {
        // error with primary, fallback to secondary
        if (logPrimaryFailures) {
          warn(fails, s"Primary cache failure when getting key: $key, falling back to secondary...")
        }
        secondary.get(key)
      }
    }
  }

  override def internalPut(key: K, value: V, timeout: FiniteDuration = defaultTimeout, ttl: FiniteDuration = defaultTtl): ValidationNel[FailureResult, CacheResult[V]] = execute(timeout) {
    primary.put(key, value, timeout, ttl).ifThen(propagateSet) {
      case Success(ValueSet(v)) => secondary.put(key, v, timeout, ttl) // propagate set to secondary
      case Success(res) => UnexpectedState(res).failureNel
      case Failure(fails) => {
        // error with primary, continue to propagate set to secondary
        if (logPrimaryFailures) {
          warn(fails, s"Primary cache failure when setting key: $key, falling back to secondary...")
        }
        secondary.put(key, value, timeout, ttl)
      }
    }
  }

  override protected[this] def internalRemove(key: K, timeout: FiniteDuration): ValidationNel[FailureResult, CacheResult[V]] = execute(timeout) {
    primary.remove(key, timeout).ifThen(propagateSet) {
      case Success(ValueRemoved) => secondary.remove(key, timeout)
      case Success(res) => UnexpectedState(res).failureNel
      case Failure(fails) => {
        // error with primary, continue to propagate set to secondary
        if (logPrimaryFailures) {
          warn(fails, s"Primary cache failure when setting key: $key, falling back to secondary...")
        }
        secondary.remove(key, timeout)
      }
    }
  }
}
