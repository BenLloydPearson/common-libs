package com.gravity.utilities

import java.util.concurrent.CountDownLatch
import java.util.concurrent.atomic.AtomicLong

import org.junit.Assert._
import org.junit._

import scala.util.Random

class CountersTest extends BaseScalaTest {
  test("Average Counter Test") {
    val avg = Counters.getOrMakeAverageCounter("test", "test 1")
    val testValues : List[Long] = List(10, 20, 30, 40, 50)
    val testAvg = testValues.sum.toDouble / testValues.length.toDouble
    avg.set(10)
    avg.set(20)
    avg.set(30)
    avg.set(40)
    avg.set(50)
    assertEquals("average was wrong", testAvg, avg.average,0)
    avg.reset()
    assertEquals("did not reset", 0, avg.get, 0)
    for(i <- 0 until 10) {
      avg.increment
      //println(avg.get + " : " + avg.average)
    }
    //1 + 2 + 3 + 4 + 5 + 6 + 7 + 8 + 9 + 10 = 55
    //55 / 10 = 5.5
    assertEquals("average from incrementing was wrong", 5.5, avg.average, 0)
  }

  test("Per Second Counter Test") {
    Counters.setTickIntervalSeconds(1)
    val ps = Counters.getOrMakePerSecondCounter("test", "test 1")
    for(i <- 0 until 10) {
      ps.increment
      Thread.sleep(1000)
      println(ps.get + ": " + ps.getInterval)
    }
    assertEquals("per second was wrong", 1.0, ps.getInterval, 0.1)
  }

  test("Average Counters Crement Test") {
    val avg = Counters.getOrMakeAverageCounter("test", "test 2")

    for(i <- 0 until 100)
      println(avg.increment)

    assertEquals(100, avg.get)

    for(i <- 0 until 200)
      println(avg.decrement)

    assertEquals(-100, avg.get)

    avg.set(0)

    assertEquals(0, avg.get)

  }

  test("Average counter multi-threaded crement test") {
    import MultithreadedCounterTestSupport._
    val threads = for (i <- 0 until numThreads)
      yield {
        new Thread(new Runnable {
          def run() {
            for (i <- 0 until iterations) {
              val goUp = rand.nextBoolean()
              if (goUp) {
                check.incrementAndGet()
                counter.increment
              }
              else {
                check.decrementAndGet()
                counter.decrement
              }
            }
            cdl.countDown()
          }
        })
      }

    threads.foreach(_.start())
    cdl.await()
    assertEquals(check.get(), counter.get)
  }

}

object MultithreadedCounterTestSupport {
  val iterations = 100
  val numThreads = 25
  val rand = new Random()
  val check = new AtomicLong()
  val counter = Counters.getOrMakeAverageCounter("test", "multithreaded avg")
  val cdl = new CountDownLatch(numThreads)
}

class CounterTest {

  @Test def deltaCounterSmokeTest(): Unit = {
    Counters.countPerSecondWithDelta("test", "test", 1, 1, java.util.concurrent.TimeUnit.HOURS, "1-hour")
    Counters.getAll.foreach(println)
  }

  @Test def testHitRatioCounter() {
    val testCounter = Counters.getOrMakeHitRatioCounter("test counter", "testing")
    assertTrue(0F == testCounter.getInterval)
    assertTrue(0L == testCounter.get)
    testCounter.incrementMiss()
    assertTrue(0F == testCounter.getInterval)
    assertTrue(1L == testCounter.get)
    testCounter.incrementHit()
    println(testCounter.getInterval)
    assertTrue(50F == testCounter.getInterval)
    assertTrue(2L == testCounter.get)
    //    Thread.sleep(60000)
    //    assertTrue(0F == testCounter.getPerInterval)
    //    assertTrue(0L == testCounter.getTotal)
    val testCounter2 = Counters.getOrMakeHitRatioCounter("test counter 2", "testing", shouldLog = false)
    testCounter2.incrementHitsAndMisses(9999, 1)
    assertTrue(99.99F == testCounter2.getInterval)
  }

//  @Test def testMovingAverageCounter(): Unit = {
//    val testCounter = new MovingAverageCounter("test counter", "testing", 3, false)
//    assertTrue(0F == testCounter.getPerInterval)
//    assertTrue(0L == testCounter.getTotal)
//    Thread.sleep(950)
//    // sample 2
//    testCounter.sample(2)
//    assertEquals(2F, testCounter.getPerInterval, 0.001)
//    assertEquals(2L, testCounter.getTotal)
//    Thread.sleep(950)
//    // sample 4
//    testCounter.sample(4)
//    assertEquals(3F, testCounter.getPerInterval, 0.001)
//    assertEquals(3, testCounter.getTotal)
//    Thread.sleep(950)
//    // sample 8
//    testCounter.sample(8)
//    assertEquals(4.666F, testCounter.getPerInterval, 0.001)
//    assertEquals(4L, testCounter.getTotal)
//    Thread.sleep(950)
//    // sample 16
//    testCounter.sample(16)
//    assertEquals(9.333F, testCounter.getPerInterval, 0.001) // moving average only includes last 3 samples (ie: (4+8+16)/3)
//    assertEquals(7L, testCounter.getTotal) // total average is all-time and lower at this point because it includes all samples (ie: (2+4+8+16)/4)
//    Thread.sleep(950)
//    // sample -8
//    testCounter.sample(-8)
//    assertEquals(5.333F, testCounter.getPerInterval, 0.001) // moving average only includes last 3 samples (ie: (8+16-8)/3)
//    assertEquals(4L, testCounter.getTotal) // total average is all-time and lower at this point because it includes all samples (ie: (2+4+8+16-8)/5)
//
//  }
}

//class CounterScalaTest extends BaseScalaTest {
//  ignore("Counters average as expected"){
//    val testCounter = new Counter("test", "test", false, CounterType.AVERAGE)
//
//    testCounter.setAverage(4)
//    testCounter.setAverage(5)
//    testCounter.setAverage(6)
//
//    testCounter.getPerInterval should equal (5)
//  }
//
//  ignore("TrackAverage average as expected"){
//
//    val testCounter = TrackAverage("test", "test")
//
//    testCounter.addMeasurement(4)
//    testCounter.addMeasurement(5)
//    testCounter.addMeasurement(6)
//
//    val r = testCounter.average
//
//    threeDecimalPlacesOf(r) should equal (threeDecimalPlacesOf(5.0))
//  }
//
//  def nextSync[T](it: Iterator[T]): T = synchronized {
//    it.next()
//  }
//
//  ignore("TrackAverage isn't obviously thread unsafe during addMeasurement") {
//    val numthreads = 24
//    val numCalls = 1000
//
//    val threads = (1 to numthreads).par
//    threads.tasksupport = new ForkJoinTaskSupport(new ForkJoinPool(numthreads))
//
//    val groups = Iterator.continually("group1" :: "group2" :: Nil).flatten
//    val names = Iterator.continually("ctr1" :: "ctr2" :: "ctr3" :: Nil).flatten
//
//    val task = threads.map(i => {
//      val randList = (1 to numCalls).map(i => {
//        val tracker = TrackAverage(nextSync(names), nextSync(groups))
//        val rand = util.Random.nextInt(11)
//        tracker.addMeasurement(rand)
//        (tracker, rand)
//      })
//      randList
//    })
//
//    val trackerMap = task.seq.flatten.groupBy(_._1)
//    val mapped = trackerMap.mapValues(s=>s.map(_._2))
//
////    println(s"\nTask: $task\n\n")
//
//    mapped.foreach {
//      case(tracker, ints) => {
////        println(s"tracker.count ${tracker.count} ints.size ${ints.size}")
//        tracker.count should equal(ints.size)
//        tracker.total should equal (ints.sum.toFloat)
//        threeDecimalPlacesOf(tracker.average) should equal(threeDecimalPlacesOf(ints.sum.toFloat / ints.size))
////        println(tracker)
//      }
//    }
//  }
//
//  def threeDecimalPlacesOf(d: Double): Long = math.round(d * math.pow(10, 3))
//
//  ignore("RunningStats calculates as expected"){
//
//    val testCounter = RunningStats("test", "test")
//
//    val data = Seq(1, 2, -2, 4, -3).map(_.toDouble)
//    data.foreach(testCounter.addSample)
//
//    threeDecimalPlacesOf(testCounter.mean) should equal (threeDecimalPlacesOf(0.4))
//    threeDecimalPlacesOf(testCounter.stdDev) should equal (threeDecimalPlacesOf(2.8809720581775866))
//    testCounter.total should equal (2)
//    testCounter.count should equal (5)
//    testCounter.max should equal (4)
//    testCounter.min should equal (-3)
//  }
//
//
//  ignore("RunningStats isn't obviously thread unsafe during addMeasurement") {
//    val numthreads = 24
//    val numCalls = 1000
//
//    val threads = (1 to numthreads).par
//    threads.tasksupport = new ForkJoinTaskSupport(new ForkJoinPool(numthreads))
//
//    val groups = Iterator.continually("group1" :: "group2" :: Nil).flatten
//    val names = Iterator.continually("ctr1" :: "ctr2" :: "ctr3" :: Nil).flatten
//
//    val task = threads.map(i => {
//      val randList = (1 to numCalls).map(i => {
//        val tracker = RunningStats(nextSync(names), nextSync(groups))
//        val rand = util.Random.nextInt(11)
//        tracker.addSample(rand)
//        (tracker, rand)
//      })
//      randList
//    })
//
//    val trackerMap = task.seq.flatten.groupBy(_._1)
//    val mapped = trackerMap.mapValues(s=>s.map(_._2))
//
////    println(s"\nTask: $task\n\n")
//
//    mapped.foreach {
//      case(tracker, ints) => {
////        println(s"tracker.count ${tracker.count} ints.size ${ints.size}")
//        tracker.count should equal(ints.size)
//        threeDecimalPlacesOf(tracker.total) should equal (threeDecimalPlacesOf(ints.sum.toFloat))
//        threeDecimalPlacesOf(tracker.mean) should equal (threeDecimalPlacesOf(ints.sum.toFloat / ints.size))
////        println(tracker)
//      }
//    }
//  }
//
//}