package com.gravity.utilities

import org.junit.Assert._
import org.junit._
import scala.Some
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.ConcurrentHashMap
import scala.collection.JavaConverters._

/*
*     __         __
*  /"  "\     /"  "\
* (  (\  )___(  /)  )
*  \               /
*  /               \
* /    () ___ ()    \  erik 4/17/14
* |      (   )      |
*  \      \_/      /
*    \...__!__.../
*/

object TestClass {
  var instantiateCount = 0

  def reset() {
    instantiateCount = 0
  }

  def apply(int: Int): TestClass = {
    instantiateCount += 1
    new TestClass(int)
  }
}

class TestClass(val int: Int)

class ConcurrentMapTest {

  @Test def getOrElseUpdateTest() {
    TestClass.reset()
    val map = new GrvConcurrentMap[Int, TestClass]()
    val ret1 = map.getOrElseUpdate(1, TestClass(1))
    val ret2 = map.getOrElseUpdate(1, TestClass(2))
    assertEquals(1, ret1.int)
    assertEquals(1, ret2.int)
    assertEquals(1, TestClass.instantiateCount)
  }

  @Test def replaceTest() {
    TestClass.reset()
    val map = new GrvConcurrentMap[Int, TestClass]()

    map.replace(1, TestClass(1)).map(_ => fail())
    map.put(1, TestClass(1))
    map.replace(1, TestClass(2)) match {
      case Some(ret) => assertEquals(1, ret.int)
      case None => fail()
    }
  }

  @Test def conditionalReplaceTest() {
    TestClass.reset()
    val map = new GrvConcurrentMap[Int, TestClass]()

    val val1 = TestClass(1)
    val val2 = TestClass(2)
    assertFalse(map.replace(1, val2, val1))
    map.put(1, val1)
    assertTrue(map.replace(1, val1, val2))
    assertTrue(map.replace(1, val2, new TestClass(2)))
  }

  @Test def removeTest() {
    TestClass.reset()
    val map = new GrvConcurrentMap[Int, TestClass]()

    map.remove(1).map(_ => fail())
    map.put(1, new TestClass(1))
    assertTrue(map.remove(1).isDefined)
  }

  @Test def putIfAbsentTest() {
    TestClass.reset()
    val map = new GrvConcurrentMap[Int, TestClass]()

    map.putIfAbsent(1, new TestClass(1)).map(_ => fail())
    map.putIfAbsent(1, new TestClass(2)) match {
      case Some(ret) => assertEquals(1, ret.int)
      case None => fail()
    }
  }

  @Test def minusEqualsTest() {
    TestClass.reset()
    val map = new GrvConcurrentMap[Int, TestClass]()

    map.put(1, new TestClass(1))
    map -= 1
    map.get(1).map(_ => fail())
  }

  @Test def plusEqualsTest() {
    TestClass.reset()
    val map = new GrvConcurrentMap[Int, TestClass]()

    map += Tuple2(1, new TestClass(1))
    assertTrue(map.contains(1))
  }

  @Test def iteratorTest() {
    TestClass.reset()
    val map = new GrvConcurrentMap[Int, TestClass]()

    map.put(1, new TestClass(1))
    map.put(2, new TestClass(2))

    val map2 = new GrvConcurrentMap[Int, TestClass]()

    for((k, v) <- map)
      map2.put(k, v)

    assertTrue(map2.contains(1))
    assertTrue(map2.contains(2))
  }

  @Test def getTest() {
    TestClass.reset()
    val map = new GrvConcurrentMap[Int, TestClass]()

    map.put(1, new TestClass(1))
    map.get(1) match {
      case Some(ret) => assertEquals(1, ret.int)
      case None => fail()
    }
  }

  @Test def multiThreadGetOrElseUpdateTestTest() {
    TestClass.reset()
    val map = new ConcurrentHashMap[String, Int]().asScala
    val counter = new AtomicInteger(0)
    multiTheadMapExerciser(map, counter)
    assertTrue(map.contains("blah"))
    println("got " + map("blah"))
    assertTrue(counter.get > 1)
  }

  @Test def multiThreadGetOrElseUpdateTest() {
    TestClass.reset()
    val map = new GrvConcurrentMap[String, Int]()
    val counter = new AtomicInteger(0)
    multiTheadMapExerciser(map, counter)
    assertEquals(1,map("blah"))
    assertEquals(1, counter.get)
  }

  def multiTheadMapExerciser(map: scala.collection.concurrent.Map[String, Int], counter: AtomicInteger) {
    TestClass.reset()
    val threads = for (i <- 0 to 8) yield {
      new Thread {
        override def run {
          map.getOrElseUpdate("blah", {Thread.sleep(100); counter.incrementAndGet})
        }
      }
    }
    threads.foreach(_.start)
    threads.foreach(_.join)
  }
}
