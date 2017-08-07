package com.gravity.utilities.grvcache

import com.gravity.hbase.schema.ByteConverter
import com.gravity.utilities
import com.gravity.utilities.components._
import com.gravity.utilities.grvmemcachedClient

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


// Memcached only supports String keys
class MemcachedCache[K, V : ByteConverter](val name: String, keyToString: K => String, val maxItems: Int = 50000, val defaultTimeout: FiniteDuration = Cache.defaultTimeout, val defaultTtl: FiniteDuration = Cache.defaultTtl) extends Cache[K, V] {

  val client: grvmemcachedClient = utilities.grvmemcached.getClient()

  val valueConverter: ByteConverter[V] = implicitly[ByteConverter[V]]

  override def internalGet(key: K, timeout: FiniteDuration = defaultTimeout): ValidationNel[FailureResult, CacheResult[V]] = execute(timeout) {
    Option(client.get(keyToString(key), timeout.toSeconds.toInt)).fold(NotFound: CacheResult[V])(bytes => Found(valueConverter.fromBytes(bytes))).successNel
  }

  override def internalPut(key: K, value: V, timeout: FiniteDuration = defaultTimeout, ttl: FiniteDuration = defaultTtl): ValidationNel[FailureResult, CacheResult[V]] = execute(timeout) {
    client.set(keyToString(key), ttl.toSeconds.toInt, valueConverter.toBytes(value))
    ValueSet(value).successNel
  }

  override protected[this] def internalRemove(key: K, timeout: FiniteDuration): ValidationNel[FailureResult, CacheResult[V]] = execute(timeout) {
    client.delete(keyToString(key))
    ValueRemoved.successNel
  }
}
