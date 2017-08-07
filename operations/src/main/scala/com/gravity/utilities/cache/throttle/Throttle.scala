package com.gravity.utilities.cache.throttle

/**
 * Created by agrealish14 on 2/7/17.
 */
trait Throttle[K,V] {

  def get(key: K): Option[V]

  def put(key: K, value: V): V

  def clear(): Unit
}