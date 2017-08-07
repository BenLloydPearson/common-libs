//package com.gravity.utilities.cache
//
//import java.util.concurrent.atomic.AtomicInteger
//
//import com.gravity.test.utilitiesTesting
//import org.mockito.invocation.InvocationOnMock
//import org.mockito.stubbing.Answer
//
//import scala.actors.threadpool.{Executors, ExecutorService}
//import scala.collection._
//import scala.concurrent.duration._
//import scalaz._
//import Scalaz._
//import com.gravity.grvlogging._
//import com.gravity.utilities.grvstrings._
//import com.gravity.utilities.grvz._
//import com.gravity.utilities.components._
//import com.gravity.utilities.grvjson._
//import com.gravity.utilities.grvtime._
//import com.gravity.utilities.{BaseScalaTest, Logging}
//import org.mockito.Mockito._
//
//import com.gravity.utilities.grvannotation._
//import com.gravity.utilities.grvfunc._
//
//import com.gravity.valueclasses.ValueClassesForUtilities._
//
//
///*
// *    __   _         __
// *   / /  (_)__  ___/ /__  ____
// *  / _ \/ / _ \/ _  / _ \/ _  /
// * /_//_/_/_//_/\_,_/\___/\_, /
// *                       /___/
// */
//class CacheTest extends BaseScalaTest with utilitiesTesting {
//
//  test("perma cache test") {
//    val mockCache = mock[Cache[String, Int]]
//
//    val cache = new PermaCache[String, Int]("test", PermaCache.defaultScheduler, 1, mockCache).withStatistics
//
//    val threadPool = Executors.newFixedThreadPool(10)
//
//    val counter = new AtomicInteger()
//
//    for (i <- 0 until 25) {
//      assertResult(42) {
//        cache.getOrRegister("someKey", 5 seconds, 5 seconds) {
//          counter.incrementAndGet().successNel
//        }
//      }
//    }
//
//    println(cache.getStats())
//  }
//
//}
