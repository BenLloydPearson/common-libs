package com.gravity.utilities.cache

import akka.actor.{ActorSystem, Props}
import akka.testkit.{ImplicitSender, TestActorRef, TestKit}
import com.gravity.utilities.cache.PermaCacher._
import com.gravity.utilities.cache.PermaCacherAgent._
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach, MustMatchers, WordSpecLike}

import scala.Predef._
import scala.reflect.ClassTag


/**
* Created with IntelliJ IDEA.
* User: asquassabia
* Date: 10/22/13
* Time: 2:33 PM
* To change this template use File | Settings | File Templates.
*/
class PermaCacherAgentTest(_system: ActorSystem) extends TestKit(_system)
with ImplicitSender
with WordSpecLike
with MustMatchers
with BeforeAndAfterEach
with BeforeAndAfterAll {

  //  ignore("the underlying harness is Log4j") {
  //    assert(LoggingTest.isLog4j)
  //  }

  import PermaCacherPsmTest._

  val myKey = "KEY_PermaCacherAgentTest"
  val myValue = "VALUE_PermaCacherAgentTest"
  val myNonmatchingvalue: String = myValue + "nomatch"
  val myValueRel1: String = myValue + "_rel1"
  val myoKey = "OKEY_PermaCacherAgentTest"
  val myoValue = "OVALUE_PermaCacherAgentTest"
  val myEvictedKey = "KEY_EvictedPermaCacherAgentTest"
  val reloadInterval = 2
  val isEvictable = true
  val isNotEvictable = false

  def this() = this(ActorSystem(refresherActorSystemName, PermaCacherAgent.getSystemConf("")))

  override def beforeEach() {
    restart()
    addOneStorageResult(myKey, myValue, isEvictable)
    addOneStorageResult(myoKey, myoValue, isNotEvictable)
  }

  override def afterEach() {
    restart()
  }


  override protected def afterAll(): Unit = {
    system.shutdown()
  }

  def assertNoKeyTs(keyName : String ): Unit = assert(PermaCacher.cacheLastAccess.get(keyName) match {case Some(ts) => false case None => true},"found key "+keyName+" where none expected")
  def assertYesKeyTs(keyName : String ): Unit = assert(PermaCacher.cacheLastAccess.get(keyName) match {case Some(ts) => true case None => false},"key "+keyName+" not found when expected")


  "PermaCacherAgent when receiving ReloadOpConditional" must {

    "refresh when appropriate" in {
      def dummyReloadFactory = dummyConditionFactory(myValue, myValueRel1) _
      val dummyReloadOpConditional = ReloadOpConditional(myKey, dummyReloadFactory, 1)

      val aut = TestActorRef[PermaCacherAgent](ClassTag[PermaCacherAgent](classOf[PermaCacherAgent]), defaultRefresher.system)
      aut.receive(dummyReloadOpConditional)

      // make sure aut actually fired...
      val result1 = get[String](myKey)
      assert(Some(myValueRel1) === result1)
      val result2 = getOrRegisterConditional(myKey, -1, false)(dummyReloadFactory)
      assert(Some(myValueRel1) === result2)
    }

    "opine not to refresh if necessary" in {
      def dummyReloadFactory = dummyConditionFactory(myNonmatchingvalue, myValueRel1) _
      val dummyReloadOpConditional = ReloadOpConditional(myKey, dummyReloadFactory, 1)

      val aut = TestActorRef[PermaCacherAgent](ClassTag[PermaCacherAgent](classOf[PermaCacherAgent]), defaultRefresher.system)
      aut.receive(dummyReloadOpConditional)

      // make sure aut actually fired...
      val result1 = get[String](myKey)
      assert(Some(myValue) === result1)
      val result2 = getOrRegisterConditional(myKey, -1, false)(dummyReloadFactory)
      assert(Some(myValue) === result2)
    }

    "handle factory errors with poise" in {
      val throwOnBadMatch = true
      def dummyReloadFactory = dummyConditionFactory(myNonmatchingvalue, myValueRel1, throwOnBadMatch) _
      val dummyReloadOpConditional = ReloadOpConditional(myKey, dummyReloadFactory, 1)

      val aut = TestActorRef[PermaCacherAgent](ClassTag[PermaCacherAgent](classOf[PermaCacherAgent]), defaultRefresher.system)
      aut.receive(dummyReloadOpConditional)

      // make sure aut actually fired...
      val result1 = get[String](myKey)
      assert(Some(myValue) === result1)
      val result2 = getOrRegisterConditional(myKey, -1, false)(dummyReloadFactory)
      assert(Some(myValue) === result2)
    }
  }

  "PermaCacherKeyAgent when receiving ReloadOpConditional" must {

    "refresh when appropriate" in {
      def dummyReloadFactory = dummyConditionFactory(myValue, myValueRel1) _
      val dummyReloadOpConditional = ReloadOpConditional(myKey, dummyReloadFactory, 1)

      val aut = TestActorRef(Props(new PermaCacherKeyAgent(List())))
      aut.receive(dummyReloadOpConditional)

      // make sure aut actually fired...
      val result1 = get[String](myKey)
      assert(Some(myValueRel1) === result1)
      val result2 = getOrRegisterConditional(myKey, -1, false)(dummyReloadFactory)
      assert(Some(myValueRel1) === result2)
    }

    "opine not to refresh if necessary" in {
      def dummyReloadFactory = dummyConditionFactory(myNonmatchingvalue, myValueRel1) _
      val dummyReloadOpConditional = ReloadOpConditional(myKey, dummyReloadFactory, 1)

      val aut = TestActorRef(Props(new PermaCacherKeyAgent(List())))
      aut.receive(dummyReloadOpConditional)

      // make sure aut actually fired...
      val result1 = get[String](myKey)
      assert(Some(myValue) === result1)
      val result2 = getOrRegisterConditional(myKey, -1, false)(dummyReloadFactory)
      assert(Some(myValue) === result2)
    }

    "handle factory errors with poise" in {
      val throwOnBadMatch = true
      def dummyReloadFactory = dummyConditionFactory(myNonmatchingvalue, myValueRel1, throwOnBadMatch) _
      val dummyReloadOpConditional = ReloadOpConditional(myKey, dummyReloadFactory, 1)

      val aut = TestActorRef(Props(new PermaCacherKeyAgent(List())))

      aut.receive(dummyReloadOpConditional)

      // make sure aut actually fired...
      val result1 = get[String](myKey)
      assert(Some(myValue) === result1)
      val result2 = getOrRegisterConditional(myKey, -1, false)(dummyReloadFactory)
      assert(Some(myValue) === result2)
    }

    "handle keys unsuitable as actor names gracefully" in {
      try {
        // good URI, bad name ( / not allowed in simple name)
        val n1 = "/user/gravity/reports/sitePlacementReport/byDay/2014_176"
        val newActor1 = PermaCacherKeySystem.createKeyActor(n1, List(), "")
        val r1 = newActor1.toString()
        assert(r1.contains("_NU_"))
        println(newActor1.toString())
        // good URI, good name (RFC3986 example)
        val n2 = "urn:oasis:names:specification:docbook:dtd:xml:4.1.2"
        val newActor2 = PermaCacherKeySystem.createKeyActor(n2, List(), "")
        val r2 = newActor2.toString()
        assert(r2.contains(n2))
        println(newActor2.toString())
        // bad URI ( _ not allowed in scheme)
        val n3 = "bad_scheme://urn:oasis:names:specification:docbook:dtd:xml:4.1.2"
        val newActor3 = PermaCacherKeySystem.createKeyActor(n3, List(), "")
        val r3 = newActor3.toString()
        assert(r3.contains("keyIsBadURI"))
        println(newActor3.toString())
      } catch {
        case ex: Exception => fail(ex)
        case s: Throwable => fail("unknown reason: " + s.toString)
      }
    }

  }

  "PermaCacherKeyAgent when receiving PceTimeToLiveKeyCheckOp" must {

    "log inconceivable when irregularity" in {
      val key = "not_There"
      assertNoKeyTs(key)
      val msgPceTimeToLiveKeyCheckOp0 = PceTimeToLiveKeyCheckOp(key)

      val aut = TestActorRef(Props(new PermaCacherKeyAgent(List())))
      aut.receive(msgPceTimeToLiveKeyCheckOp0)
      assertNoKeyTs(key)
    }

    "evict if appropriate" in {
      assertYesKeyTs(myKey)

      val msgPceTimeToLiveKeyCheckOp1 = PceTimeToLiveKeyCheckOp(myKey)

      // set timestamp to eviction
      val cheat = PermaCacher.cacheLastAccess(myKey)
      val ts = cheat.get()
      cheat.set(ts-PermaCacherKeyAgent.msTimeToLiveDefault);
      PermaCacher.cacheLastAccess.put(myKey,cheat)

      val aut = TestActorRef(Props(new PermaCacherKeyAgent(List())))
      aut.receive(msgPceTimeToLiveKeyCheckOp1)

      assertNoKeyTs(myKey)
    }

    "do nothing if appropriate" in {
      assertYesKeyTs(myKey)

      val msgPceTimeToLiveKeyCheckOp1 = PceTimeToLiveKeyCheckOp(myKey)

      val aut = TestActorRef(Props(new PermaCacherKeyAgent(List())))
      aut.receive(msgPceTimeToLiveKeyCheckOp1)

      assertYesKeyTs(myKey)
    }
  }

}
