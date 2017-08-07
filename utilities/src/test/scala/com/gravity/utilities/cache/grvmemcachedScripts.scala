package com.gravity.utilities.cache

/**
* Created with IntelliJ IDEA.
* User: erik
* Date: 9/13/12
* Time: 3:28 PM
* To change this template use File | Settings | File Templates.
*/

import com.gravity.hbase.schema.{CacheRequestResult, Error, Found, FoundEmpty, NotFound}
import com.gravity.utilities.grvmemcached
import org.junit.Assert._

import scala.collection._
import scala.collection.immutable.IndexedSeq
import scalaz.Validation
import scalaz.syntax.validation._

object emptyItemTest extends App {
  val keyClass = "byte[]"

  implicit val transformer: (Array[Byte]) => Validation[Nothing, Array[Byte]] = (bytes:Array[Byte]) => {
    bytes.success
  }

  grvmemcached.setSync[Array[Byte]]("empty data test", keyClass, 10, None)
  grvmemcached.get[Array[Byte]]("empty data test", keyClass) match {
    case Found(item) => fail("got empty item back as found")
    case FoundEmpty => println("got empty item back!")
    case NotFound => fail("did not get empty object wrapper back")
    case Error(message, eo) => fail(message)
  }
  grvmemcached.delete("empty data test", keyClass)
}

object emptyItemsTest extends App {
  val keyClass = "byte[]"

  implicit val transformer: (Array[Byte]) => Validation[Nothing, Array[Byte]] = (bytes:Array[Byte]) => {
    bytes.success
  }

  val testItems: Map[String, Option[Array[Byte]]] = Map("empty test 1" -> None, "empty test 2" -> Some(Array(1, 2, 3, 4, 5, 6)), "empty test 3" -> Some(Array(6, 5, 4, 3, 2, 1)), "empty test 4" -> None)
  testItems.foreach(item => grvmemcached.setSync(item._1, keyClass, 10, item._2))
  val returnItems: collection.Map[String, CacheRequestResult[Array[Byte]]] = grvmemcached.getMulti(testItems.keySet, keyClass)

  println(testItems)
  println(returnItems)
  testItems.keySet.foreach {
    key =>
      assertTrue(returnItems(key).hasValue)
      testItems(key) match {
        case Some(testValue) =>
          assertArrayEquals(testValue, returnItems(key).get)
        case None =>
          assertTrue(returnItems(key).isEmptyValue)
      }
  }
}

object getSetDeleteTest extends App {
  val keyClass = "byte[]"

  implicit val transformer: (Array[Byte]) => Validation[Nothing, Array[Byte]] = (bytes:Array[Byte]) => {
    bytes.success
  }

  grvmemcached.get[Array[Byte]]("unsettestvalue", keyClass) match {
    case NotFound => println("1st unset not found")
    case _ => fail("not unset value back")
  }

//  val data0 : Array[Byte] = Array(1, 2, 3, 4, 5, 6)
  val data0: Array[Byte] = Array(1, 2, 3, 4, 5, 6)
  val key0 = "unittestsyncsetvalue"
  grvmemcached.get[Array[Byte]](key0, keyClass) match {
    case NotFound => println("unset not found as expected")
    case _ => fail("something other than not found for unset value")
  }


  grvmemcached.setSync(key0, keyClass, 100, Some(data0))
  grvmemcached.get[Array[Byte]](key0, keyClass) match {
    case Found(ret) => assertArrayEquals(data0, ret)
    case _ => fail("didn't get data 0 back")
  }

  grvmemcached.delete[Array[Byte]](key0, keyClass)
  assertEquals("delete didn't work", NotFound, grvmemcached.get[Array[Byte]](key0, keyClass))

  val data1 : Array[Byte] = Array(2, 3, 4, 5, 6)
  val key1 = "unittestvalue"
  assertEquals("key 1 existed before test", NotFound, grvmemcached.get[Array[Byte]](key1, keyClass))

  grvmemcached.setAsync(key1, keyClass, 100, Some(data1))
  grvmemcached.get[Array[Byte]](key1, keyClass) match {
    case Found(ret) => assertArrayEquals(data1, ret)
    case _ => fail("didn't get data 1 back")
  }

  grvmemcached.delete(key1, keyClass)
  assertEquals("delete didn't work", NotFound, grvmemcached.get[Array[Byte]](key1, keyClass))

  val data2: Array[Byte] = "I am a thing what is data".getBytes
  val key2 = "unittestrefvalue"
  assertEquals("key 2 existed before test", NotFound, grvmemcached.get[Array[Byte]](key2, keyClass))
  grvmemcached.setAsync(key2, keyClass, 100, Some(data2))
  grvmemcached.get[Array[Byte]](key2, keyClass) match {
    case Found(ret) => assertArrayEquals(data2, ret)
    case _ => fail("didn't get data 2 back")
  }

  grvmemcached.delete(key2, keyClass)
  assertEquals("delete didn't work", NotFound, grvmemcached.get[Array[Byte]](key2, keyClass))

  val data3 : Array[Byte] = Array(3,4,5,6,7)
  val key3 = "bulkgettest"
  val data4 : Array[Byte] = Array(7,6,5,4,3,2,1)
  val key4 = "bulkgettest2"
  grvmemcached.setAsync(key3, keyClass, 100, Some(data3))
  grvmemcached.setAsync(key4, keyClass, 100, Some(data4))
  val multiget: collection.Map[String, CacheRequestResult[Array[Byte]]] = grvmemcached.getMulti[Array[Byte]](Set(key3, key4), keyClass)

  assertArrayEquals(data3, multiget(key3).get)
  assertArrayEquals(data4, multiget(key4).get)
  grvmemcached.delete(key3, keyClass)
  grvmemcached.delete(key4, keyClass)




  val key5 = "thiskeydoesnotexist"
  assertEquals("non existent key did not return empty map", Map.empty[String, Array[Byte]], grvmemcached.getMulti[Array[Byte]](Set(key5), keyClass))

  assertEquals("empty key request did not return empty map", Map.empty[String, Array[Byte]], grvmemcached.getMulti[Array[Byte]](Set.empty[String], keyClass))
}

object getMultiBatchedTest extends App {
  val keyClass = "byte[]"

  implicit val transformer: (Array[Byte]) => Validation[Nothing, Array[Byte]] = (bytes:Array[Byte]) => {
    bytes.success
  }

  val keys: IndexedSeq[String] = for(i <- 0 until 765) yield "testkey " + i
  keys.foreach(key => {
    println("setting key " + key)
    grvmemcached.setSync(key, keyClass, 1000, Some(key.getBytes))})

  val multiget: collection.Map[String, CacheRequestResult[Array[Byte]]] = grvmemcached.getMulti[Array[Byte]](keys.toSet, keyClass,  1000, 112)

  println(multiget.keys.toArray.sorted.mkString("\n"))
  val returnedKeys: Set[String] = multiget.keys.toSet
  val missing: IndexedSeq[String] = keys.filter(key => !returnedKeys.contains(key))
  println("missing: ")
  println(missing.mkString("\n"))
  assertEquals(keys.size, multiget.keys.size)
}

object grvmemcachedIT {
  val keyClass = "byte[]"
}