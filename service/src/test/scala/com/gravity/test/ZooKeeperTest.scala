package com.gravity.test

import java.util.concurrent.CountDownLatch

import akka.actor.ActorSystem
import com.gravity.service.ZooCommon
import com.gravity.utilities.BaseScalaTest
import com.gravity.utilities.grvakka.Configuration._
import org.joda.time.DateTime
import scalaz._, Scalaz._
import scala.concurrent.duration._
/*
*     __         __
*  /"  "\     /"  "\
* (  (\  )___(  /)  )
*  \               /
*  /               \
* /    () ___ ()    \  erik 1/21/15
* |      (   )      |
*  \      \_/      /
*    \...__!__.../
*/

class ZooKeeperTest extends BaseScalaTest {
  val system: ActorSystem = ActorSystem("ZooKeeperTest", defaultConf)

  test("Test node operations") {
    val testPath = "/testing/test" + new DateTime().getMillis
    val testBytes : Array[Byte]= Array(1.toByte, 2.toByte, 3.toByte, 4.toByte, 5.toByte, 6.toByte)
    assertResult(None, "Node existed before test") { ZooCommon.getNodeIfExists(ZooCommon.devClient, testPath) }
    assertResult(testPath.success, "Node was not created") { ZooCommon.createNode(ZooCommon.devClient,testPath, testBytes) }
    assertResult(testBytes, "Node was not fetched correctly") { ZooCommon.getNodeIfExists(ZooCommon.devClient,testPath).get }
    assertResult(true, "Node was not deleted") { ZooCommon.deleteNodeIfExists(ZooCommon.devClient,testPath) }
    assertResult(None, "Node exited after test") { ZooCommon.getNodeIfExists(ZooCommon.devClient,testPath) }
  }

  test("test locking") {
    val testPath = "/testing/locks/" + new DateTime().getMillis
    import system.dispatcher
    val numThreads = 3
    val latch = new CountDownLatch(3)
    for(i <- 0 until numThreads) {
      system.scheduler.scheduleOnce(0.seconds)(ZooKeeperLockTestHelper.doStuff(testPath, latch))
    }

    println("Waiting for " + numThreads + " threads to do stuff")
    latch.await()
    println("Done waiting")
    assertResult(0) { ZooKeeperLockTestHelper.thingsInLock }
  }
}

object ZooKeeperLockTestHelper {

  var thingsInLock = 0

  def doStuff(path: String, latch: CountDownLatch): Unit = {
    val threadName = Thread.currentThread().getName
    def say(thing: String): Unit = {
      println(threadName + ": " + thing)
    }

    say("Building lock")
    val lock = ZooCommon.buildLock(ZooCommon.devClient,path)
    say("Aquiring lock")
    try {
      lock.acquire()
      say("Lock acquired, things in lock currently " + thingsInLock)
      thingsInLock += 1
      Thread.sleep(500)
    }
    finally {
      say("Releasing lock, things in lock currently " + thingsInLock)
      thingsInLock -= 1
      lock.release()
      latch.countDown()
    }

  }
}