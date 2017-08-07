package com.gravity.utilities.grvcache

import java.util.concurrent.atomic.AtomicReference

import com.gravity.utilities.components._
import com.gravity.utilities.grvtime
import org.joda.time.DateTime
import com.gravity.utilities.grvz._

import scala.actors.threadpool.AtomicInteger
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


case class CacheStats(
  ttl: FiniteDuration,
  hits: AtomicInteger = new AtomicInteger(),
  misses: AtomicInteger = new AtomicInteger(),

  getTimeouts: AtomicInteger = new AtomicInteger(),
  putTimeouts: AtomicInteger = new AtomicInteger(),
  removeTimeouts: AtomicInteger = new AtomicInteger(),

  getFailures: AtomicInteger = new AtomicInteger(),
  putFailures: AtomicInteger = new AtomicInteger(),
  removeFailures: AtomicInteger = new AtomicInteger(),

  getSuccesses: AtomicInteger = new AtomicInteger(),
  putSuccesses: AtomicInteger = new AtomicInteger(),
  removeSuccesses: AtomicInteger = new AtomicInteger(),

  lastAccessed: AtomicReference[DateTime] = new AtomicReference[DateTime],
  lastUpdated: AtomicReference[DateTime] = new AtomicReference[DateTime](grvtime.currentTime),
  created: AtomicReference[DateTime] = new AtomicReference[DateTime](grvtime.currentTime),

  unexpectedError: AtomicInteger = new AtomicInteger()
) {

  def totalFailures: Int = getFailures.get() + putFailures.get() + removeFailures.get()
  def totalTimeouts: Int = getTimeouts.get() + putTimeouts.get() + removeTimeouts.get()
  def totalSuccesses: Int = getSuccesses.get() + putSuccesses.get() + removeSuccesses.get()

}

class CacheWithStatistics[K, V](val name: String, cache: Cache[K, V]) extends Cache[K, V] {
  override val maxItems: Int = cache.maxItems
  override val defaultTtl: FiniteDuration = cache.defaultTtl
  override val defaultTimeout: FiniteDuration = cache.defaultTimeout

  protected val statsCache: Cache[K, CacheStats] = new MemoryCache[K, CacheStats](name + " (stats)", maxItems = cache.maxItems)
  protected val cacheStats = new CacheStats(ttl = defaultTtl)

  def getStats(): CacheStats = cacheStats
  def getKeyStats(key: K): ValidationNel[FailureResult, CacheResult[CacheStats]] = statsCache.get(key)

  //
  private def withStatUpdater[T](key: K, timeout: FiniteDuration)(thunk: Updater => ValidationNel[FailureResult, T]): ValidationNel[FailureResult, T] = {
    (for {
      keyStats <- statsCache.getOrRegister(key)(new CacheStats(ttl = timeout).successNel[FailureResult])
    } yield {
      new Updater(keyStats)
    }).flatMap(u => thunk(u))
  }

  class Updater(val keyStats: CacheStats) {
    def update(f: CacheStats => Unit): Unit = {
      f(keyStats)
      f(cacheStats)
    }
  }

  override protected[this] def internalGet(key: K, timeout: FiniteDuration): ValidationNel[FailureResult, CacheResult[V]] = {
    withStatUpdater(key, timeout) {
      updater => {
        val result = cache.get(key, timeout)

        result match {
          case Failure(fails) => {
            updater.update(_.getFailures.incrementAndGet())
            fails.list.collect{ case CacheTimeout(_) => updater.update(_.getTimeouts.incrementAndGet()) }
          }
          case Success(cresult) => {
            updater.update(_.getSuccesses.incrementAndGet())
            cresult match {
              case Found(v) => updater.update(_.hits.incrementAndGet())
              case NotFound => updater.update(_.misses.incrementAndGet())
              case res => updater.update(_.unexpectedError.incrementAndGet())
            }
          }
        }

        updater.update(_.lastAccessed.set(grvtime.currentTime))

        result
      }
    }
  }

  override protected[this] def internalPut(key: K, value: V, timeout: FiniteDuration, ttl: FiniteDuration): ValidationNel[FailureResult, CacheResult[V]] = {
    withStatUpdater(key, timeout) {
      updater => {
        val result = cache.put(key, value, timeout, ttl)

        result match {
          case Failure(fails) => {
            updater.update(_.putFailures.incrementAndGet())
            fails.list.collect { case CacheTimeout(_) => updater.update(_.putTimeouts.incrementAndGet()) }
          }
          case Success(_) => updater.update(_.putSuccesses.incrementAndGet())
        }

        updater.update(_.lastUpdated.set(grvtime.currentTime))

        result
      }
    }
  }

  override protected[this] def internalRemove(key: K, timeout: FiniteDuration): ValidationNel[FailureResult, CacheResult[V]] = {
    withStatUpdater(key, timeout) {
      updater => {
        val result = cache.remove(key)

        result match {
          case Failure(fails) => {
            updater.update(_.removeFailures.incrementAndGet())
            fails.list.collect { case CacheTimeout(_) => updater.update(_.removeTimeouts.incrementAndGet())}
          }
          case Success(cresult) => updater.update(_.removeSuccesses.incrementAndGet())
        }

        updater.update(_.lastUpdated.set(grvtime.currentTime))

        result
      }
    }
  }
}
