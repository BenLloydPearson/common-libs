package com.gravity.utilities.cache

/**
 * Created by IntelliJ IDEA.
 * Author: Robbie Coleman
 * Date: 7/6/11
 * Time: 9:12 PM
 */
import org.junit.Assert._
import org.junit._

class EhCacherTest {

//  @Test def testOffHeapCaching() {
//    val cache = EhCacher.getOrCreateOffHeapCache[String,String]("EhCacherTest", maxLocalBytes = 500000)
//    val key = "mykey"
//    val value = "myvalue"
//
//    cache.putItem(key, value)
//    assertTrue(cache.cache.isElementOffHeap(key))
//    cache.getItem("mykey") match {
//      case Some(cachedVal) => {
//        println("Successfully retrieved value from cache. Value = " + cachedVal)
//        assertEquals("Cached value should equal original value!", value, cachedVal)
//      }
//      case None => fail("Failed to get value from cache!")
//    }
//  }

  @Test def testBasicCaching() {
    val cache = EhCacher.getOrCreateCache[String,String]("EhCacherTest")
    val key = "mykey"
    val value = "myvalue"

    cache.putItem(key, value)

    cache.getItem("mykey") match {
      case Some(cachedVal) => {
        println("Successfully retrieved value from cache. Value = " + cachedVal)
        assertEquals("Cached value should equal original value!", value, cachedVal)
      }
      case None => fail("Failed to get value from cache!")
    }
  }

  @Ignore @Test def testByteArrayCaching() {
    val cache = EhCacher.getOrCreateCache[Array[Byte], Array[Byte]]("EhCacherTest")
    val key = "mykey"
    val value = "myvalue"

    val kBytes = key.getBytes("UTF-8")
    val vBytes = value.getBytes("UTF-8")

    cache.putItem(kBytes, vBytes)

    cache.getItem(key.getBytes("UTF-8")) match {
      case Some(cachedBytes) => {
        val cachedVal = new String(cachedBytes, "UTF-8")
        println("Successfully retrieved value from cache. Value = " + cachedVal)
        assertEquals("Cached value should equal original value!", value, cachedVal)
      }
      case None => fail("Failed to get value from cache!")
    }
  }

  @Test def testGetItemIfNotModified() {
    val key = "mykey"
    val value = "a"
    val valueOpt = Some(value)
    val ttlSeconds = 3600
    val cache = EhCacher.getOrCreateCache[String, String](key, ttlSeconds, persistToDisk = false)

    // Cached value is now "a"
    cache.putItem(key, value)
    assertEquals(valueOpt, cache.getItem(key))

    // No cached value if mtime > creation time
    assertEquals(None, cache.getItemIfNotModified(key, System.currentTimeMillis() / 1000 + 60))
  }
}