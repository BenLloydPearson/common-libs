package com.gravity.utilities.grvcache

import com.gravity.hbase.schema.ByteConverter
import com.gravity.utilities.components.FailureResult
import com.gravity.utilities.grvz._

import scala.collection._
import scala.concurrent._
import scala.concurrent.duration._
import scala.language.postfixOps
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

sealed trait CacheResult[+T]

case class ValueSet[+T](value: T) extends CacheResult[T]
case class Found[+T](value: T) extends CacheResult[T]
case object NotFound extends CacheResult[Nothing]
case object ValueRemoved extends CacheResult[Nothing]

case object NoFactoryRegistered extends FailureResult(s"Default factory not registered")
case class ResultMismatch(expected: Int, actual: Int) extends FailureResult(s"Expected $expected results, only got $actual")
case class CacheTimeout(timeout: Duration) extends FailureResult(s"Cache timeout ($timeout)")
case class UnexpectedState[T](res: CacheResult[T], ex: Throwable = new Throwable()) extends FailureResult(s"Unexpected cache state: " + res, Some(ex))

trait CacheListener[K, V] {

  def onBeforePut(key: K, value: V)
  def onAfterPut(key: K, value: CacheResult[V])

  def onBeforeGet(key: K)
  def onAfterGet(key: K, value: CacheResult[V])

  def onBeforeRemove(key: K)
  def onAfterRemove(key: K)

  def onFailure(key: K, failure: FailureResult)

}

trait Cache[K, V] {

  val defaultTtl: FiniteDuration
  val defaultTimeout: FiniteDuration
  val maxItems: Int
  val name: String

  val listeners: mutable.HashSet[CacheListener[K, V]] = new mutable.HashSet[CacheListener[K, V]]

  protected[this] def internalGet(key: K, timeout: FiniteDuration = defaultTimeout): ValidationNel[FailureResult, CacheResult[V]]

  protected[this] def internalPut(key: K, value: V, timeout: FiniteDuration = defaultTimeout, ttl: FiniteDuration = defaultTtl): ValidationNel[FailureResult, CacheResult[V]]

  protected[this] def internalRemove(key: K, timeout: FiniteDuration = defaultTimeout): ValidationNel[FailureResult, CacheResult[V]]

  protected[this] def execute[X](timeout: FiniteDuration)(thunk: => ValidationNel[FailureResult, X])(implicit ec: ExecutionContext = scala.concurrent.ExecutionContext.global): ValidationNel[FailureResult, X] = {
//    val f = future { thunk }
//    try {
//      Await.result(f, timeout)
//    } catch {
//      case ex: TimeoutException => CacheTimeout(timeout).failureNel
//    }
    thunk
  }

  def get(key: K, timeout: FiniteDuration = defaultTimeout): ValidationNel[FailureResult, CacheResult[V]] = execute(timeout) {
    listeners.foreach(_.onBeforeGet(key))
    val r =  internalGet(key, timeout)
    r.map(res => { listeners.foreach(_.onAfterGet(key, res)); res } )
  }

  def put(key: K, value: V, timeout: FiniteDuration = defaultTimeout, ttl: FiniteDuration = defaultTtl): ValidationNel[FailureResult, CacheResult[V]] = execute(timeout) {
    listeners.foreach(_.onBeforePut(key, value))
    internalPut(key, value, timeout, ttl).map(res => { listeners.foreach(_.onAfterPut(key, res)); res })
  }

  def remove(key: K, timeout: FiniteDuration = defaultTimeout): ValidationNel[FailureResult, CacheResult[V]] = execute(timeout) {
    listeners.foreach(_.onBeforeRemove(key))
    internalRemove(key).map(res => { listeners.foreach(_.onAfterRemove(key)); res })
  }

  // override this method to enable the getOrRegisterDefault() methods
  def factoryOp: (Set[K]) => ValidationNel[FailureResult, Map[K, V]] = (keys: Set[K]) => NoFactoryRegistered.failureNel

  def getOrRegisterDefault(key: K, timeout: FiniteDuration = defaultTimeout, ttl: FiniteDuration = defaultTtl): ValidationNel[FailureResult, V] = execute(timeout) {
    getOrRegister(key, timeout, ttl)(factoryOp(Set(key)).flatMap(_.values.headOption.toValidationNel(ResultMismatch(1, 0))))
  }

  def getOrRegisterMultiDefault(keys: Set[K], timeout: FiniteDuration = defaultTimeout, ttl: FiniteDuration = defaultTtl): ValidationNel[FailureResult, Map[K, V]] = execute(timeout) {
    getOrRegisterMulti(keys, timeout, ttl)(factoryOp)
  }

  def getOrRegister(key: K, timeout: FiniteDuration = defaultTimeout, ttl: FiniteDuration = defaultTtl)(factoryOp: => ValidationNel[FailureResult, V]): ValidationNel[FailureResult, V] = execute(timeout) {
    getOrRegisterMulti(Set(key), timeout, ttl)((keys: Set[K]) => { keys.headOption.toValidationNel(ResultMismatch(1, 0)).flatMap(_ => factoryOp).map(v => Map(key -> v)) } ).flatMap(res => res.values.headOption.toValidationNel(ResultMismatch(1, 0)))
  }

  /*
    This method will perform a multi-get, sourcing from the cache whatever keys it can.
    It will then issue a multi-get for the keys not in cache, and cache the results

    Currently, we will fail-fast throughout or if we don't read all of the keys expected
  */
  def getOrRegisterMulti(keys: Set[K], timeout: FiniteDuration = defaultTimeout, ttl: FiniteDuration = defaultTtl)(factoryOp: Set[K] => ValidationNel[FailureResult, Map[K, V]]): ValidationNel[FailureResult, Map[K, V]] = {

    execute(timeout) {

      val found: mutable.Map[K, V] = new mutable.HashMap[K, V]()
      val notFound = new mutable.ListBuffer[K]

      // for look for keys in the cache, keeping track of what we find/don't find
      keys.foreach(key => {
        get(key) match {
          case Success(Found(v)) => found += key -> v
          case Success(NotFound) => notFound += key
          case Failure(fails) => return fails.failure
          case _ =>
        }
      })

      // for keys not already in cache, lets go fetch the data and put them into the cache
      if (notFound.nonEmpty) {
        factoryOp(notFound.toSet).map(res => {
          res.foreach { case (k, v) => {
            put(k, v) match {
              case Success(ValueSet(value)) => found += k -> value
              case Failure(fails) => return fails.failure
              case _ =>
            }
          }}
        })
      }

      found.success[FailureResult].ensure(ResultMismatch(keys.size, found.size))(_.size == keys.size).toNel
    }
  }

}























object Cache {

  val defaultTtl: FiniteDuration = 10 minutes
  val defaultTimeout: FiniteDuration = 5 seconds

  implicit class CacheExtensions[K, V](cache: Cache[K, V]) {

    def withStatistics: CacheWithStatistics[K, V] = {
      new CacheWithStatistics[K, V](cache.name, cache)
    }

    def withDefault(factory: (Set[K]) => ValidationNel[FailureResult, Map[K, V]]): Cache[K, V] = {
      new Cache[K, V] {
        override val maxItems: Int = cache.maxItems
        override val name: String = cache.name
        override val defaultTtl: FiniteDuration = cache.defaultTtl
        override val defaultTimeout: FiniteDuration = cache.defaultTimeout

        override protected[this] def internalPut(key: K, value: V, timeout: FiniteDuration = defaultTimeout, ttl: FiniteDuration = defaultTtl): ValidationNel[FailureResult, CacheResult[V]] = cache.put(key, value, timeout, ttl)

        override protected[this] def internalGet(key: K, timeout: FiniteDuration = defaultTimeout): ValidationNel[FailureResult, CacheResult[V]] = cache.get(key, timeout)

        override protected[this] def internalRemove(key: K, timeout: FiniteDuration): ValidationNel[FailureResult, CacheResult[V]] = cache.remove(key, timeout)

        // override this method to enable the getOrRegisterDefault() methods
        override def factoryOp: (Set[K]) => ValidationNel[FailureResult, Map[K, V]] = factory
      }
    }

    def delegateTo(delegate: Cache[K, V]): Cache[K, V] = {
      new DelegatingCache[K, V](cache, delegate)
    }

  }
}


/*
  Example usage

 */

object Example extends App {

  def buildL1L2Cache[K, V : ByteConverter](name: String, keyToString: K => String, maxItems: Int = 10000, ttlInSeconds: Int = 60 * 5): CacheResultCache[K, V] {val isCacheableResult: PartialFunction[ValidationNel[FailureResult, CacheResult[V]], Boolean]; val name: String} = {

    // memcached serves as the L2
    val l2Cache = new MemcachedCache[K, V]("l2Cache", v => s"$name-${keyToString(v)}", defaultTimeout = 5 seconds, defaultTtl = ttlInSeconds seconds)

    // EHCache serves as the L1
    val l1Cache = new EhCacheReferenceCache[K, V](name, maxItems = maxItems)

    // delegate l1 to l2
    // See "DelegatingCache".. it defaults to propagating Sets (on primary set) and Gets (on primary miss)
    val l1l2Cache = l1Cache delegateTo l2Cache

    // front l1/l2 cache with an in-memory result cache.  We will intercept and cache certain failures (ie: RowNotFound)
    // so that downstream caches won't receive another lookup until this the ttl expires
    new CacheResultCache[K, V](l1l2Cache, maxItems, defaultTtl = ttlInSeconds seconds) {
      override val name: String = l1l2Cache.name
      override val isCacheableResult: PartialFunction[ValidationNel[FailureResult, CacheResult[V]], Boolean] = {
        case Failure(failures) => failures.list.collect {
          // list Failure types that will be cached here
          case v:FailureResult => v
          //case v@RowNotFound(_) => v
        }.nonEmpty
        case _ => false // cache all other results
      }
    }

  }

  val intStringCache: CacheResultCache[Int, String] {val isCacheableResult: PartialFunction[ValidationNel[FailureResult, CacheResult[String]], Boolean]; val name: String} = buildL1L2Cache[Int, String]("intStringCache", _.toString)
  val intDoubleCache: CacheResultCache[Int, Double] {val isCacheableResult: PartialFunction[ValidationNel[FailureResult, CacheResult[Double]], Boolean]; val name: String} = buildL1L2Cache[Int, Double]("intDoubleCache", _.toString)

}