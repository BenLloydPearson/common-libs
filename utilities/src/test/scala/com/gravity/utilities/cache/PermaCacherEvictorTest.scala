package com.gravity.utilities.cache

import akka.actor.ActorSystem
import akka.testkit.{ImplicitSender, TestActorRef, TestKit}
import com.gravity.utilities.cache.PermaCacher._
import com.gravity.utilities.cache.PermaCacherEvictor._
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach, MustMatchers, WordSpecLike}

import scala.reflect.ClassTag

/**
 * Created by alsq on 1/15/14.
 */
class PermaCacherEvictorTest (_system: ActorSystem) extends TestKit(_system)
with ImplicitSender
with WordSpecLike
with MustMatchers
with BeforeAndAfterEach
with BeforeAndAfterAll {

  import PermaCacherPsmTest._

  val myKey = "EVICTABLEKEY_PermaCacherEvictorTest"
  val myValue = "EVICTABLEVALUE_PermaCacherEvictorTest"
  val myoKey = "UNEVICTABLEKEY_PermaCacherEvictorTest"
  val myoValue = "UNEVICTABLEVALUE_PermaCacherEvictorTest"
  val reloadInterval = 2
  val isEvictable = true
  val isNotEvictable = false
  val guaranteedEvictionTtl: Int = -10
  val guaranteedNotEvictionTtl: Long = Long.MaxValue

  def this() = this(ActorSystem(evictorActorSystemName, evictorSystemConf))

  override def beforeEach() {
    restart()
    addOneWithOption(myKey,myValue,isEvictable)
    addOneWithOption(myoKey,myoValue,isNotEvictable)
  }

  override def afterEach() {
    restart()
  }


  "PermaCacherEvictor when receiving PceTimeToLiveCheckOp" must {

    "evict when appropriate (never used)" in {
      val evictCheckOp = PceTimeToLiveCheckOp(cacheLastAccess,guaranteedEvictionTtl)

      val unusedBefore = unusedCounter.get
      val runBefore = runCounter.get
      val aut = TestActorRef[PermaCacherEvictor](ClassTag[PermaCacherEvictor](classOf[PermaCacherEvictor]), evictorSystem)
      aut.receive(evictCheckOp)

      // make sure aut actually removed evictable...
      val result1 = get[String](myKey)
      assert(None===result1)
      assert((unusedBefore+1)===unusedCounter.get)
      assert((runBefore+1)===runCounter.get)
      // and left the unevictable alone
      val result2 = get[String](myoKey)
      assert(Some(Some(myoValue))===result2)
    }

    "not evict when appropriate (not yet used)" in {
      val evictCheckOp = PceTimeToLiveCheckOp(cacheLastAccess,guaranteedNotEvictionTtl)

      val unusedYetBefore = unusedYetCounter.get
      val runBefore = runCounter.get
      val aut = TestActorRef[PermaCacherEvictor](ClassTag[PermaCacherEvictor](classOf[PermaCacherEvictor]), evictorSystem)
      aut.receive(evictCheckOp)

      // make sure aut actually left evictable...
      val result1 = get[String](myKey)
      assert(Some(Some(myValue))===result1)
      assert((unusedYetBefore+1)===unusedYetCounter.get)
      assert((runBefore+1)===runCounter.get)
      // and left the unevictable alone
      val result2 = get[String](myoKey)
      assert(Some(Some(myoValue))===result2)
    }

    "evict when appropriate (used)" in {

      val tran2use = get[String](myKey)
      assert(Some(Some(myValue))===tran2use)
      val evictCheckOp = PceTimeToLiveCheckOp(cacheLastAccess,guaranteedEvictionTtl)

      val unusedBefore = unusedCounter.get
      val runBefore = runCounter.get
      val aut = TestActorRef[PermaCacherEvictor](ClassTag[PermaCacherEvictor](classOf[PermaCacherEvictor]), evictorSystem)
      aut.receive(evictCheckOp)

      // make sure aut actually removed evictable...
      val result1 = get[String](myKey)
      assert(None===result1)
      assert(unusedBefore===unusedCounter.get)
      assert((runBefore+1)===runCounter.get)
      // and left the unevictable alone
      val result2 = get[String](myoKey)
      assert(Some(Some(myoValue))===result2)
    }

    "not evict when appropriate (used)" in {

      val tran2use = get[String](myKey)
      assert(Some(Some(myValue))===tran2use)
      val evictCheckOp = PceTimeToLiveCheckOp(cacheLastAccess,guaranteedNotEvictionTtl)

      val unusedYetBefore = unusedYetCounter.get
      val runBefore = runCounter.get
      val aut = TestActorRef[PermaCacherEvictor](ClassTag[PermaCacherEvictor](classOf[PermaCacherEvictor]), evictorSystem)
      aut.receive(evictCheckOp)

      // make sure aut actually left evictable...
      val result1 = get[String](myKey)
      assert(Some(Some(myValue))===result1)
      assert(unusedYetBefore===unusedYetCounter.get)
      assert((runBefore+1)===runCounter.get)
      // and left the unevictable alone
      val result2 = get[String](myoKey)
      assert(Some(Some(myoValue))===result2)
    }
  }

  override protected def afterAll(): Unit = {
    system.shutdown()
  }
}
