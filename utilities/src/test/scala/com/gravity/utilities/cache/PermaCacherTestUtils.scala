package com.gravity.utilities.cache

import java.util.concurrent.locks.{Condition, Lock, ReentrantLock}
import java.util.concurrent.{CountDownLatch, Semaphore}

import akka.actor.ActorSystem
import akka.testkit.{ImplicitSender, TestKit}
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach, MustMatchers, WordSpecLike}

import scala.collection.JavaConversions._
import scala.collection._


/**
 * Created with IntelliJ IDEA.
 * User: asquassabia
 * Date: 10/22/13
 * Time: 9:36 AM
 * To change this template use File | Settings | File Templates.
 */

object PermaCacherTestUtils  {
  val cardinality = 3
}

class PermaCacherConcurrentTest(_system: ActorSystem)
  extends TestKit(_system)
  with ImplicitSender
  with WordSpecLike
  with MustMatchers
  with BeforeAndAfterAll
  with BeforeAndAfterEach {

  import PermaCacher._
  import PermaCacherAgent._
  import PermaCacherPsmTest._

  val myKey = "KEY_PermaCacherConcurrentTest"
  val myValue = "VALUE_PermaCacherConcurrentTest"
  val myRewrittenValue = "REWRITTEN_VALUE_PermaCacherConcurrentTest"
  val myOKey = "OKEY_PermaCacherConcurrentTest"
  val myOValue = "OVALUE_PermaCacherConcurrentTest"
  val myORewrittenValue = "OREWRITTEN_VALUE_PermaCacherConcurrentTest"
  val isNotEvictable = false

  def this() = this(ActorSystem(PermaCacherAgent.refresherActorSystemName))

  override def beforeEach {
    restart()
    listeners.clear()
    assert(0===listeners.size)
    CountDownListener.oracleClear()
    assert(0===CountDownListener.oracleSize())
    //
    // ** this is now mandatory after the evictable refactoring **
    //
    addOneWithOption(myKey, myValue, isNotEvictable)
    addOneWithOption(myOKey, myOValue, isNotEvictable)
    addOneWithOption(myKey+myOKey, myValue+myOValue, isNotEvictable)
  }

  override def afterEach {
    restart()
    listeners.clear()
    assert(0===listeners.size)
    CountDownListener.oracleClear()
    assert(0===CountDownListener.oracleSize())
  }



  "Actors, under concurrent execution" must {

    "rewrite the cache (condition)" in {

      val lock: Lock = new ReentrantLock()
      val completion: Condition = lock.newCondition()
      val conditionListener = new ConditionListener(lock, completion, myKey, myValue, myRewrittenValue, true)
      val rop = ReloadOp(myKey, () => myRewrittenValue, 1)
      val router = defaultRefresher.balancer

      listeners += conditionListener
      // not the best sync semantic, but it works in this case,
      // so I'm leaving it in here, should it be inspirational
      // for a similar problem at some hypothetical future time
      lock.lock()
      try {
        println("firing 1 message from from thread " + Thread.currentThread().getId)
        router ! rop
        completion.await()
      } finally {
        lock.unlock()
        listeners -= conditionListener
        assert(0 === listeners.size)
      }
    }

    "rewrite the cache (latch)" in {

      val numberOfMessages = 1
      assert(numberOfMessages <= refresherConcurrentActors,
        "too many messages (i.e. more messages than actors), unreliable unit test!")

      // better sync, not sure if optimal
      val latch = new CountDownLatch(numberOfMessages)
      val probe = new StateProbe()
      val countingListener = CountDownListener(latch, probe)
      listeners += countingListener

      CountDownListener.forOracleMap(myKey, myRewrittenValue)
      val rop = ReloadOp(myKey, () => myRewrittenValue, 1)
      val router = defaultRefresher.balancer

      try {
        println("firing 1 message from from thread " + Thread.currentThread().getId)
        router ! rop
        latch.await()
        probe.threadState.foreach(record => println("used:" + record._2))
        probe.errorState.foreach(record => println("Error in thread:" + record._1 + "\n" + record._2.getStackTrace.mkString("", "\n", "\n")))
        probe.asserts.foreach(asrt => assert(asrt(true)))
        if (!probe.errorState.isEmpty) {
          fail("at least one actor has thrown")
        }
      } finally {
        listeners -= countingListener
        assert(0 === listeners.size)
      }
    }

    "use multiple threads (ReloadOp) [not asserted, happens unreliably; check visually...]" in {

      val numberOfMessages = 2
      assert(numberOfMessages <= refresherConcurrentActors,
        "too many messages (i.e. more messages than actors), unreliable unit test!")

      val latch = new CountDownLatch(numberOfMessages)
      val probe = new StateProbe()
      val countingListener = CountDownListener(latch, probe)
      listeners += countingListener

      CountDownListener.forOracleMap(myKey, myRewrittenValue)
      val rop1 = ReloadOp(myKey, () => myRewrittenValue, 1)

      CountDownListener.forOracleMap(myOKey, myORewrittenValue)
      val rop2 = ReloadOp(myOKey, () => myORewrittenValue, 1)

      // protected [cache] case class ReloadOptionally(key: String, reloadOp: () => Option[Any], reloadInSeconds: Long)
      val router = defaultRefresher.balancer
      assert(numberOfMessages === CountDownListener.oracleSize())

      try {
        println("firing 2 messages from thread " + Thread.currentThread().getId)
        router ! rop1
        router ! rop2
        latch.await()
        probe.threadState.foreach(record => println("used:" + record._2))
        probe.errorState.foreach(record => println("Error in thread:" + record._1 + "\n" + record._2.getStackTrace.mkString("", "\n", "\n")))
        probe.asserts.foreach(asrt => assert(asrt(true)))
        if (!probe.errorState.isEmpty) {
          fail("at least one actor has thrown")
        }
      } finally {
        listeners -= countingListener
        assert(0 === listeners.size)
      }
    }

    "use multiple threads (ReloadOptionally) [not asserted, happens unreliably; check visually...]" in {

      val numberOfMessages = 3
      assert(numberOfMessages <= refresherConcurrentActors,
        "too many messages (i.e. more messages than actors), unreliable unit test!")

      val latch = new CountDownLatch(numberOfMessages)
      val probe = new StateProbe()
      val countingListener = CountDownListener(latch, probe)
      listeners += countingListener

      CountDownListener.forOracleMap(myKey, Some(myRewrittenValue))
      val ropt1 = ReloadOptionally(myKey, () => Some(myRewrittenValue), 1)

      CountDownListener.forOracleMap(myOKey, Some(myORewrittenValue))
      val ropt2 = ReloadOptionally(myOKey, () => Some(myORewrittenValue), 1)

      CountDownListener.forOracleMap(myKey + myOKey, Some(None))
      val ropt3 = ReloadOptionally(myKey + myOKey, () => Some(None), 1)

      // protected [cache] case class ReloadOptionally(key: String, reloadOp: () => Option[Any], reloadInSeconds: Long)
      val router = defaultRefresher.balancer
      assert(numberOfMessages === CountDownListener.oracleSize())

      try {
        println("firing 3 messages from thread " + Thread.currentThread().getId)
        router ! ropt1
        router ! ropt2
        router ! ropt3
        latch.await()
        probe.threadState.foreach(record => println("used:" + record._2))
        probe.errorState.foreach(record => println("Error in thread:" + record._1 + "\n" + record._2.getStackTrace.mkString("", "\n", "\n")))
        probe.asserts.foreach(asrt => assert(asrt(true)))
        if (!probe.errorState.isEmpty) {
          fail("at least one actor has thrown")
        }
      } finally {
        listeners -= countingListener
        assert(0 === listeners.size)
      }
    }
  } // An Actor must

  override protected def afterAll(): Unit = {
    system.shutdown()
  }
}


//
// LISTENERS ==================
//

class ConditionListener(lock : Lock, condition: Condition, key : String, priorContent: String, content: String, assertSize : Boolean = false) extends PermaCacherListener {

  override def onBeforeKeyUpdated(key: String): Unit = {
    import com.gravity.utilities.cache.PermaCacher._
    lock.lock()
    try {
      println("onBeforeKeyUpdated fired (condition), thread "+Thread.currentThread().getId)
      if (assertSize) assert(PermaCacherTestUtils.cardinality==cache.size,
        "cache size, expected: "+PermaCacherTestUtils.cardinality+", found:"+cache.size)
      val actual = PermaCacher.get(key)
      assert(actual.contains(Some(priorContent)),"cache content, expected:"+priorContent+", found:"+actual)
      if (assertSize) assert(PermaCacherTestUtils.cardinality==reloadOps.size,
        "reloadOps size, expected: "+PermaCacherTestUtils.cardinality+", found:"+reloadOps.size)
    } finally {
      condition.signal()
      lock.unlock()
    }
  }

  override def onAfterKeyUpdated[T](key: String, value: Option[T]): Unit = {
    import com.gravity.utilities.cache.PermaCacher._
    lock.lock()
    try {
      println("onAfterKeyUpdated fired (condition), thread "+Thread.currentThread().getId)
      if (assertSize) assert(PermaCacherTestUtils.cardinality==cache.size,
        "cache size, expected: "+PermaCacherTestUtils.cardinality+", found:"+cache.size)
      val actual = PermaCacher.get(key)
      assert(actual.contains(content),"cache content, expected:"+content+", found:"+actual)
      if (assertSize) assert(PermaCacherTestUtils.cardinality==reloadOps.size,
        "reloadOps size, expected: "+PermaCacherTestUtils.cardinality+", found:"+reloadOps.size)
    } finally {
      condition.signal()
      lock.unlock()
    }
  }

  override def onError(key: String, complaint: Throwable): Unit = {
    lock.lock()
    try {
      println("unexpected major trouble")
      condition.signal()
    } finally {
      lock.unlock()
    }
  }

}

class StateProbe {

  def conditionalAssertBefore (asrt : () => Boolean, msg : String)(doCheck : Boolean) : Boolean = {
    val asrtBefore : () => Boolean = asrt
    val asrtBeforeMsg = msg
    if (doCheck) {
      if (asrtBefore()) true
      else {
        println (asrtBeforeMsg)
        false
      }
    }
    else true
  }
  def conditionalAssertAfter (asrt:() => Boolean, msg : String)(doCheck : Boolean) : Boolean = {
    val asrtAfter : () => Boolean = asrt
    val asrtAfterMsg = msg
    if (doCheck) {
      if (asrtAfter()) true
      else {
        println (asrtAfterMsg)
        false
      }
    }
    else true
  }

  val asserts :java.util.concurrent.LinkedBlockingDeque[(Boolean) => Boolean] =
    new java.util.concurrent.LinkedBlockingDeque[(Boolean) => Boolean]()

  val threadState : java.util.concurrent.LinkedBlockingDeque[(Long, String)] =
    new java.util.concurrent.LinkedBlockingDeque[(Long, String)]()

  val errorState : java.util.concurrent.LinkedBlockingDeque[(Long, Throwable)] =
    new java.util.concurrent.LinkedBlockingDeque[(Long, Throwable)]()
}

object CountDownListener {

  private val oracle = mutable.HashMap[String,Option[Any]]()
  private val oracleSem = new Semaphore(1)

  def apply(lock : CountDownLatch, key : String, content: String, probe : StateProbe): CountDownListener = {
    forOracleMap(key,content)
    new CountDownListener(lock, oracle, oracleSem, probe)
  }

  def apply(lock : CountDownLatch, probe : StateProbe = new StateProbe()): CountDownListener = {
    new CountDownListener(lock, oracle, oracleSem, probe)
  }

  def forOracleMap( key : String, content: String ) = {
    oracleSem.acquire()
    try {
      oracle += (key -> Some(content))
    } finally {
      oracleSem.release()
    }
  }

  def forOracleMap( key : String, content: Option[Any] ) = {
    oracleSem.acquire()
    try {
      oracle += (key -> Some(content))
    } finally {
      oracleSem.release()
    }
  }

  def oracleSize() : Int = {
    oracleSem.acquire()
    try {
      oracle.size
    } finally {
      oracleSem.release()
    }
  }

  def oracleClear(): Unit = {
    oracleSem.acquire()
    try {
      oracle.clear
    } finally {
      oracleSem.release()
    }
  }
}

class CountDownListener(lock : CountDownLatch, oracle : Map[String,Option[Any]], mutex : Semaphore, reporter:StateProbe) extends PermaCacherListener {

  val v = false
  override def onBeforeKeyUpdated(key: String): Unit = {

    mutex.acquire()
    val isFound = try { oracle.contains(key) } finally { mutex.release() }

    if (v) println("onBeforeKeyUpdated fired (latch),"+key+", thread (#id:"+Thread.currentThread().getId+") "+Thread.currentThread().getName)
    reporter.threadState.push((Thread.currentThread().getId, Thread.currentThread().getName))
    reporter.asserts.push(reporter.conditionalAssertBefore(() => { isFound },"key "+key+" not found (out of order execution?  incomplete oracle?)"))
  }

  override def onAfterKeyUpdated[T](key: String, value: Option[T]): Unit = {
    try {
      if (v) println("onAfterKeyUpdated fired (latch), thread (#id:"+Thread.currentThread().getId+") "+Thread.currentThread().getName)
      val actual = PermaCacher.get(key)//.getOrElse("(None!)")

      mutex.acquire()
      val expected = try { oracle.get(key).getOrElse(Some("(None!)")) } finally { mutex.release() }

      //println("actual:"+actual+", expected:"+expected)
      reporter.asserts.push(reporter.conditionalAssertAfter(() => {
        expected == actual
      }, "cache content for key: " + key + ", expected:" + expected + ", found:" + actual))
    } catch {
      case ex:Throwable =>
        reporter.errorState.push((Thread.currentThread().getId, ex))
    } finally {
      lock.countDown()
    }
  }

  override def onError(key: String, complaint: Throwable): Unit = {
    try {
      println("unexpected major trouble: "+complaint.getMessage)
      reporter.errorState.push((Thread.currentThread().getId, complaint))
    } finally {
      lock.countDown()
    }
  }

}
