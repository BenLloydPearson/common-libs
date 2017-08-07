//package com.gravity.utilities
//
//import scalaz._
//import Scalaz._
//import java.util.concurrent.ConcurrentHashMap
//import scala.collection.mutable
//import scala.collection.JavaConversions._
///**
// * Created with IntelliJ IDEA.
// * User: ahiniker
// * Date: 7/2/13
// */
//package object grvcache extends Memos {
//
//  def concurrentHashMapMemo[K, V]: Memo[K, V] = {
//    val map: mutable.ConcurrentMap[K, V] = new ConcurrentHashMap[K, V]
//    memo[K, V](f => key => map.getOrElseUpdate(key, f(key)))
//  }
//
//  def functionMemo[K, V]: Memo[K, V] = {
//    memo[K, V](f => key => f(key))
//  }
//}
