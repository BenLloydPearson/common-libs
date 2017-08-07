package com.gravity.utilities.cache

import akka.testkit.TestActorRef
import com.gravity.utilities.BaseScalaTest
import com.gravity.utilities.cache.PermaCacher._
import com.gravity.utilities.cache.PermaCacherAgent._
import org.mockito.Mockito.{mock => MOCK}
import org.scalatest.BeforeAndAfterEach
import scala.Some
import scala.reflect.ClassTag

/**
 * Created by alsq on 1/9/14.
 */
object PermaCacherPsmTest {

  import PermaCacherPsm._

  val priming = "PRIMING--THIS MUST NEVER APPEAR"
  val myKey = "KEY_PermaCacherPsmTest"
  val myValue = "VALUE_PermaCacherPsmTest"
  val myEvictedKey = "KEY_Evicted"
  val reloadInterval = 200
  val isEvictable = true
  val isNotEvictable = false
  val assertEnforce: Boolean = PermaCacher.isEnforceAssertions


  val factoryOutput: AnyRef = new AnyRef()
  def anyFactory(): AnyRef = factoryOutput
  def anyoFactory(): Some[AnyRef] = Some(factoryOutput)
  def noneFactory(): None.type = None

  def dummyConditionFactory(prevVal: String, newVal : String, isThrowing : Boolean = false, isPriming : Boolean = false)
                           (previous : Option[String]) : StorageResult[String] = previous match {
    case Some(pv) if (prevVal==pv) => StorageRefresh(newVal)
    case Some(bad) => {
      if (isThrowing) throw new RuntimeException("unmatched previous -- USUALLY EXPECTED WHEN TESTING")
      else StorageNOP
    }
    case None => {
      if (isPriming) StorageRefresh(newVal)
      else StorageNOP
    }
  }

  def sameAs(underTest : Any, oracle : String = myValue) : Boolean = {
    underTest match {
      case None => false
      case Some(None) => false
      case Some(Some(str)) => oracle.equals(str.toString)
      case Some(str) => oracle.equals(str.toString)
      case str => oracle.equals(str.toString)
    }
  }

  def addOneStorageResult(eKey : String, eValue : String, isEvictable : Boolean): Unit = {

    def dummyFactory = dummyConditionFactory(priming, eValue, true, true) _
    val result = getOrRegisterConditional(eKey,dummyFactory,reloadInterval,isEvictable)

    assert(Some(eValue)==result)
    if (isEvictable) {
      assert(KeyInitEvictablePsmState(eKey).id==getPsmState(eKey).id)
    } else {
      assert(KeyPermanentPsmState(eKey).id==getPsmState(eKey).id)
    }
  }

  def addOne(eKey : String, eValue : String, isEvictable : Boolean): Unit = {
    val result = if (0.5 < Math.random()) {
      def dummyFactory = Some(eValue)
      getOrRegisterFactoryWithOption(eKey,dummyFactory,reloadInterval,isEvictable, "test1")
    } else {
      def dummyFactory = eValue
      getOrRegister(eKey,dummyFactory,reloadInterval,isEvictable, "test2")
    }

    assert(sameAs(result, eValue))
    if (isEvictable) {
      assert(KeyInitEvictablePsmState(eKey).id==getPsmState(eKey).id)
    } else {
      assert(KeyPermanentPsmState(eKey).id==getPsmState(eKey).id)
    }
  }

  def addOneWithOption(eKey : String, eValue : String, isEvictable : Boolean): Unit = {
    def dummyFactory = Some(eValue)
    val result = getOrRegisterFactoryWithOption(eKey, reloadInterval,isEvictable)(dummyFactory)

    assert(sameAs(result, eValue))
    if (isEvictable) {
      assert(KeyInitEvictablePsmState(eKey).id==getPsmState(eKey).id)
    } else {
      assert(KeyPermanentPsmState(eKey).id==getPsmState(eKey).id)
    }
  }

}

// ##################################################
//
// EMPTY
//
// ##################################################

class PermaCacherEmptyTest
  extends BaseScalaTest with BeforeAndAfterEach {

  // 1|<--get->|
  // 2    =--getOrR-->
  // 3    =--getOrR------------->
  // H|<restart>|
  // K|<-evict>|

  import PermaCacherPsm._
  import PermaCacherPsmTest._

  override def beforeEach() {
    restart()
    assert(isPmsStateEmpty)
  }

  override def afterEach() {
    restart()
    assert(isPmsStateEmpty)
  }


  test("1|<--get->|") {

    val key = "1|<--get->|"

    val stateBefore = {
      val (isConsistent, state) = isConsistentBeforeGet(key)
      assert(isConsistent,"inconsistent before get")
      state
    }

    val result = get(key)

    assert(None===result)
    assert(EmptyPsmState(key).id===getPsmState(key).id)
    assert(isConsistentAfterGet(key,stateBefore))

  }

  test("2    =--getOrR-->") {

    val key = "2    =--getOrR-->"

    val stateBefore = {
      val (isConsistent, state) = isConsistentBeforeGetOrRegister(key)
      assert(isConsistent,"inconsistent before getOrRegister")
      state
    }

    val result = getOrRegisterFactoryWithOption(key,200,isEvictable)(anyoFactory)

    assert(Some(factoryOutput)===result)
    assert(KeyInitEvictablePsmState(key).id===getPsmState(key).id)
    assert(isConsistentAfterGetOrRegister(key,stateBefore))

  }

  test("3    =--getOrR------------->") {

    val key = "3    =--getOrR------------->"

    val stateBefore = {
      val (isConsistent, state) = isConsistentBeforeGetOrRegister(key)
      assert(isConsistent,"inconsistent before getOrRegister")
      state
    }

    val result = getOrRegister(key,noneFactory,200,isNotEvictable)

    assert(None===result)
    assert(KeyPermanentPsmState(key).id===getPsmState(key).id)
    assert(isConsistentAfterGetOrRegister(key,stateBefore))

  }

  test("H|<restart>|") {

    val key = "H|<restart>|"

    assert(isConsistentBeforeRestart,"inconsistent before restart")

    restart

    assert(EmptyPsmState(key).id===getPsmState(key).id)
    assert(isConsistentAfterRestart)

  }

  test("K|<-evict>|") {

    val key = "K|<-evict>|"

    val stateBefore = {
      val (isConsistent, state) = isConsistentBeforeEvict(key)
      assert(isConsistent,"inconsistent before evict")
      state
    }

    val result = evict(key)

    assert(result)
    assert(EmptyPsmState(key).id===getPsmState(key).id)
    assert(isConsistentAfterEvict(key,stateBefore))

  }

}

// ##################################################
//
// KEY-INIT-E
//
// ##################################################

class PermaCacherKeyInitEvictableTest
  extends BaseScalaTest with BeforeAndAfterEach {

  // 4               =--getOrR------------->
  // 5               =--get---------------->
  // D           |<rfrsh->|
  // G    <----------=--restart--=---------=----------=       (1 transition)
  // I               =---evict---=---------=---------->       (1 transition)
  // J    <----------=---evict---=---------=                  (1 transition)

  import PermaCacherPsm._
  import PermaCacherPsmTest._

  override def beforeEach() {
    restart()
    assert(isPmsStateEmpty)
    addOne(myKey,myValue,isEvictable)
  }

  override def afterEach() {
    restart()
    assert(isPmsStateEmpty)
  }


  test("4               =--getOrR------------->") {

    val stateBefore = {
      val (isConsistent, state) = isConsistentBeforeGetOrRegister(myKey)
      assert(isConsistent,"inconsistent before getOrRegister")
      state
    }

    val result = getOrRegisterFactoryWithOption(myKey,reloadInterval,isEvictable)(anyoFactory)

    /*
        val foo = result match {
          case None => false
          case Some(None) => false
          case Some(Some(str)) => myValue.equals(str.toString)
          case Some(str) => myValue.equals(str.toString)
          case str => myValue.equals(str.toString)
        }
    */

    assert(sameAs(result))
    assert(KeyEvictablePsmState(myKey).id===getPsmState(myKey).id)
    assert(isConsistentAfterGetOrRegister(myKey,stateBefore))

  }

  test("5               =--get---------------->") {

    val stateBefore = {
      val (isConsistent, state) = isConsistentBeforeGet(myKey)
      assert(isConsistent,"inconsistent before get")
      state
    }

    val result = get(myKey)

    assert(sameAs(result))
    assert(KeyEvictablePsmState(myKey).id===getPsmState(myKey).id)
    assert(isConsistentAfterGet(myKey,stateBefore))

  }

  test("D           |<rfrsh->|") {

    val stateBefore = {
      val (isConsistent, state) = isConsistentBeforeRefresh(myKey)
      assert(isConsistent,"inconsistent before Refresh")
      state
    }

    val pca = TestActorRef[PermaCacherAgent](ClassTag[PermaCacherAgent](classOf[PermaCacherAgent]), defaultRefresher.system)
    val asReloaded = myValue+"Reloaded"
    def reloaded() : Option[String] = Some(asReloaded)
    val op = new ReloadOptionally(myKey,reloaded,reloadInterval)
    pca.receive(op)

    assert(KeyInitEvictablePsmState(myKey).id===getPsmState(myKey).id)
    assert(isConsistentAfterRefresh(myKey,stateBefore))

    // make sure pca actually fired...
    val result = get(myKey)
    assert(sameAs(result,asReloaded))

  }

  test("G    <----------*--restart--=---------=----------=       (1 transition)") {

    assert(isConsistentBeforeRestart,"inconsistent before restart")

    restart

    assert(EmptyPsmState(myKey).id===getPsmState(myKey).id)
    assert(isConsistentAfterRestart)

  }

  test("I               *---evict---=---------=---------->       (1 transition)") {

    addOne("lestCacheBecomes","emptyAfterEviction", isEvictable)

    val stateBefore = {
      val (isConsistent, state) = isConsistentBeforeEvict(myKey)
      assert(isConsistent,"inconsistent before evict")
      state
    }

    val result = evict(myKey)

    assert(result)
    assert(RemovedPsmState(myKey).id===getPsmState(myKey).id)
    assert(isConsistentAfterEvict(myKey,stateBefore))

  }

  test("J    <----------*---evict---=---------=                  (1 transition)") {

    val stateBefore = {
      val (isConsistent, state) = isConsistentBeforeEvict(myKey)
      assert(isConsistent,"inconsistent before evict")
      state
    }

    val result = evict(myKey)

    assert(result)
    assert(EmptyPsmState(myKey).id===getPsmState(myKey).id)
    assert(isConsistentAfterEvict(myKey,stateBefore))

  }
}

// ##################################################
//
// KEY-P
//
// ##################################################

class PermaCacherKeyPermanentTest
  extends BaseScalaTest with BeforeAndAfterEach {

  // 6                      |<--get->|
  // 7                      |<getOrR>|
  // E                      |<rfrsh->|
  // G    <----------=--restart--*---------=----------=       (1 transition)
  // I               =---evict---*---------=---------->       (1 transition)
  // J    <----------=---evict---*---------=                  (1 transition)

  import PermaCacherPsm._
  import PermaCacherPsmTest._

  override def beforeEach() {
    restart()
    assert(isPmsStateEmpty)
    addOne(myKey,myValue,isNotEvictable)
  }

  override def afterEach() {
    restart()
    assert(isPmsStateEmpty)
  }

  test("6                      |<--get->|") {

    val stateBefore = {
      val (isConsistent, state) = isConsistentBeforeGet(myKey)
      assert(isConsistent,"inconsistent before get")
      state
    }

    val result = get(myKey)

    assert(sameAs(result))
    assert(KeyPermanentPsmState(myKey).id===getPsmState(myKey).id)
    assert(isConsistentAfterGet(myKey,stateBefore))

  }

  test("7                      |<getOrR>|") {

    val stateBefore = {
      val (isConsistent, state) = isConsistentBeforeGetOrRegister(myKey)
      assert(isConsistent,"inconsistent before getOrRegister")
      state
    }

    val result = getOrRegisterFactoryWithOption(myKey,reloadInterval,isNotEvictable)(anyoFactory)

    assert(sameAs(result))
    assert(KeyPermanentPsmState(myKey).id===getPsmState(myKey).id)
    assert(isConsistentAfterGetOrRegister(myKey,stateBefore))

  }

  test("E                      |<rfrsh->|") {

    val stateBefore = {
      val (isConsistent, state) = isConsistentBeforeRefresh(myKey)
      assert(isConsistent,"inconsistent before Refresh")
      state
    }

    val pca = TestActorRef[PermaCacherAgent](ClassTag[PermaCacherAgent](classOf[PermaCacherAgent]), defaultRefresher.system)
    def reloaded() : Option[String] = None
    val op = new ReloadOptionally(myKey,reloaded,reloadInterval)
    pca.receive(op)

    assert(KeyPermanentPsmState(myKey).id===getPsmState(myKey).id)
    assert(isConsistentAfterRefresh(myKey,stateBefore))

    // make sure pca actually fired...
    val result = get(myKey)
    assert(Some(None)===result)

  }

  test("G    <----------=--restart--*---------=----------=       (1 transition)") {

    assert(isConsistentBeforeRestart,"inconsistent before restart")

    restart

    assert(EmptyPsmState(myKey).id===getPsmState(myKey).id)
    assert(isConsistentAfterRestart)

  }

  test("I               =---evict---*---------=---------->       (1 transition)") {

    addOne("lestCacheBecomes","emptyAfterEviction",isNotEvictable)

    val stateBefore = {
      val (isConsistent, state) = isConsistentBeforeEvict(myKey)
      assert(isConsistent,"inconsistent before evict")
      state
    }

    val result = evict(myKey)

    assert(result)
    assert(RemovedPsmState(myKey).id===getPsmState(myKey).id)
    assert(isConsistentAfterEvict(myKey,stateBefore))

  }

  test("J    <----------=---evict---*---------=                  (1 transition)") {

    val stateBefore = {
      val (isConsistent, state) = isConsistentBeforeEvict(myKey)
      assert(isConsistent,"inconsistent before evict")
      state
    }

    val result = evict(myKey)

    assert(result)
    assert(EmptyPsmState(myKey).id===getPsmState(myKey).id)
    assert(isConsistentAfterEvict(myKey,stateBefore))

  }
}

// ##################################################
//
// KEY-E
//
// ##################################################

class PermaCacherKeyEvictableTest
  extends BaseScalaTest with BeforeAndAfterEach {

  // 8                                 |<--get->|
  // 9                                 |<getOrR>|
  // F                                 |<rfrsh->|
  // G    <----------=--restart--=---------*----------=       (1 transition)
  // I               =---evict---=---------*---------->       (1 transition)
  // J    <----------=---evict---=---------*                  (1 transition)

  import PermaCacherPsm._
  import PermaCacherPsmTest._

  override def beforeEach() {
    restart()
    assert(isPmsStateEmpty)
    // create evictable-init
    addOne(myKey,myValue,isEvictable)
    // transition to generic
    get(myKey)
    assert(KeyEvictablePsmState(myKey).id === getPsmState(myKey).id)
  }

  override def afterEach() {
    restart()
    assert(isPmsStateEmpty)
  }

  test("8                                 |<--get->|") {

    val stateBefore = {
      val (isConsistent, state) = isConsistentBeforeGet(myKey)
      assert(isConsistent,"inconsistent before get")
      state
    }

    val result = get(myKey)

    assert(sameAs(result))
    assert(KeyEvictablePsmState(myKey).id===getPsmState(myKey).id)
    assert(isConsistentAfterGet(myKey,stateBefore))

  }

  test("9                                 |<getOrR>|") {

    val stateBefore = {
      val (isConsistent, state) = isConsistentBeforeGetOrRegister(myKey)
      assert(isConsistent,"inconsistent before getOrRegister")
      state
    }

    val result = getOrRegister(myKey,anyFactory,reloadInterval,isEvictable) match {
      case Some(zz) => zz
      case None => None
      case v => v
    }

    assert(sameAs(result))
    assert(KeyEvictablePsmState(myKey).id===getPsmState(myKey).id)
    assert(isConsistentAfterGetOrRegister(myKey,stateBefore))

  }

  test("F                                 |<rfrsh->|") {

    val stateBefore = {
      val (isConsistent, state) = isConsistentBeforeRefresh(myKey)
      assert(isConsistent,"inconsistent before Refresh")
      state
    }

    val pca = TestActorRef[PermaCacherAgent](ClassTag[PermaCacherAgent](classOf[PermaCacherAgent]), defaultRefresher.system)
    val asReloaded = myValue+"Reloaded"
    def reloaded() : String = asReloaded
    val op = new ReloadOp(myKey,reloaded,reloadInterval)
    pca.receive(op)

    assert(KeyEvictablePsmState(myKey).id===getPsmState(myKey).id)
    assert(isConsistentAfterRefresh(myKey,stateBefore))

    // make sure pca actually fired...
    val result = get(myKey)
    assert(sameAs(result,asReloaded))

  }

  test("G    <----------=--restart--=---------*----------=       (1 transition)") {

    assert(isConsistentBeforeRestart,"inconsistent before restart")

    restart

    assert(EmptyPsmState(myKey).id===getPsmState(myKey).id)
    assert(isConsistentAfterRestart)

  }

  test("I               =---evict---=---------*---------->       (1 transition)") {

    addOne("lestCacheBecomes","emptyAfterEviction",isEvictable)

    val stateBefore = {
      val (isConsistent, state) = isConsistentBeforeEvict(myKey)
      assert(isConsistent,"inconsistent before evict")
      state
    }

    val result = evict(myKey)

    assert(result)
    assert(RemovedPsmState(myKey).id===getPsmState(myKey).id)
    assert(isConsistentAfterEvict(myKey,stateBefore))

  }

  test("J    <----------=---evict---=---------*                  (1 transition)") {

    val stateBefore = {
      val (isConsistent, state) = isConsistentBeforeEvict(myKey)
      assert(isConsistent,"inconsistent before evict")
      state
    }

    val result = evict(myKey)

    assert(result)
    assert(EmptyPsmState(myKey).id===getPsmState(myKey).id)
    assert(isConsistentAfterEvict(myKey,stateBefore))

  }
}

// ##################################################
//
// REMOVED
//
// ##################################################

class PermaCacherKeyRemovedTest
  extends BaseScalaTest with BeforeAndAfterEach {

  // A                                            |<--get->|
  // B                          <-----------getOrR----=
  // C               <----------------------getOrR----=
  // G    <----------=--restart--=---------=----------*       (1 transition)
  // L                                            |<-evict>|

  import PermaCacherPsm._
  import PermaCacherPsmTest._


  override def beforeEach() {
    restart()
    assert(isPmsStateEmpty)
    addOne(myKey,myValue,isEvictable)
    assert(RemovedPsmState(myEvictedKey).id===getPsmState(myEvictedKey).id)
  }

  override def afterEach() {
    restart()
    assert(isPmsStateEmpty)
  }

  test("A                                            |<--get->|") {

    val stateBefore = {
      val (isConsistent, state) = isConsistentBeforeGet(myEvictedKey)
      assert(isConsistent,"inconsistent before get")
      state
    }

    val result = get(myEvictedKey)

    assert(None===result)
    assert(RemovedPsmState(myEvictedKey).id===getPsmState(myEvictedKey).id)
    assert(isConsistentAfterGet(myEvictedKey,stateBefore))

  }

  test("B                          <-----------getOrR----=") {

    val myEvictedValue = "VALUE_"+myEvictedKey
    def dummyFactory = myEvictedValue

    val stateBefore = {
      val (isConsistent, state) = isConsistentBeforeGetOrRegister(myEvictedKey)
      assert(isConsistent,"inconsistent before getOrRegister")
      state
    }

    val result = getOrRegister(myEvictedKey,dummyFactory,reloadInterval,isNotEvictable)

    assert(myEvictedValue===result)
    assert(KeyPermanentPsmState(myEvictedKey).id===getPsmState(myEvictedKey).id)
    assert(isConsistentAfterGetOrRegister(myEvictedKey,stateBefore))

  }

  test("C               <----------------------getOrR----=") {

    val myEvictedValue = "VALUE_"+myEvictedKey
    def dummyFactory = Some(myEvictedValue)

    val stateBefore = {
      val (isConsistent, state) = isConsistentBeforeGetOrRegister(myEvictedKey)
      assert(isConsistent,"inconsistent before getOrRegister")
      state
    }

    val result = getOrRegisterFactoryWithOption(myEvictedKey,reloadInterval,isEvictable)(dummyFactory)

    assert(Some(myEvictedValue)===result)
    assert(KeyInitEvictablePsmState(myEvictedKey).id===getPsmState(myEvictedKey).id)
    assert(isConsistentAfterGetOrRegister(myEvictedKey,stateBefore))

  }

  test("G    <----------=--restart--=---------=----------*       (1 transition)") {

    assert(isConsistentBeforeRestart,"inconsistent before restart")

    restart

    assert(EmptyPsmState(myEvictedKey).id===getPsmState(myEvictedKey).id)
    assert(isConsistentAfterRestart)

  }

  test("L                                            |<-evict>|") {

    val stateBefore = {
      val (isConsistent, state) = isConsistentBeforeEvict(myEvictedKey)
      assert(isConsistent,"inconsistent before evict")
      state
    }

    val result = evict(myEvictedKey)

    assert(result)
    assert(RemovedPsmState(myEvictedKey).id===getPsmState(myEvictedKey).id)
    assert(isConsistentAfterEvict(myEvictedKey,stateBefore))

  }
}