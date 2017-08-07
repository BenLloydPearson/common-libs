//package com.gravity.utilities.cache
//
//import com.gravity.utilities.cache.PermaCacherPsmTest.assertEnforce
//import com.gravity.utilities.logging.SpyLogAppender
//import com.gravity.utilities.{BaseScalaTest, grvtime}
//import org.mockito.Matchers.{any, same}
//import org.mockito.Mockito.{never, times, verify, mock => MOCK}
//import org.scalatest.BeforeAndAfterEach
//
//
///**
// * Created with IntelliJ IDEA.
// * User: Unger
// * Date: 9/18/13
// * Time: 3:25 PM
// */
//// class PermaCacherTest {
//// this "ignore" seems to interfere in some
//// magical way with Mockito imports, go figure
////
////  ignore.getOrRegister("test", 1){
////      Thread.sleep(1500)
////    }
////
////    Thread.sleep(2600)
////
////    val counter = CounterBundle.getOrMakeCounter(PermaCacherMeta.counterCategory, PermaCacherMeta.delayCounterLabel)
////    val count = counter.get
////
////    println("Number of delays: " + count)
////
////    assert(count > 0)
////  }
////}
//
//class PermaCacherMetaTest
//  extends BaseScalaTest with BeforeAndAfterEach {
//
//  import PermaCacher._
//  import PermaCacherPsm._
//
//  override def afterEach() {
//    PermaCacher.restart()
//    assert(isPmsStateEmpty)
//  }
//
//  override def beforeEach() {
//    PermaCacher.restart()
//    assert(isPmsStateEmpty)
//  }
//
//  // it is actually org.slf4j.impl.JDK14LoggerFactory
//  //  test("the underlying harness is Log4j") {
//  //    assert(LoggingTest.isLog4j,(new LoggingTestHarness).underlying)
//  //  }
//
//  test("don't  blow on arg 0") {
//    if (assertEnforce) assert(PermaCacher.isEnforceAssertions)
//    PermaCacherMeta(0)
//  }
//
//  test("blow on bad negative arg ") {
//    if (assertEnforce) assert(PermaCacher.isEnforceAssertions)
//    val thrown2 = intercept[IllegalArgumentException] {
//      PermaCacherMeta(-1)
//    }
//    assert("requirement failed"===thrown2.getMessage)
//  }
//
//  test("meta or default with reloadInSeconds") {
//    if (assertEnforce) assert(PermaCacher.isEnforceAssertions)
//    val start = grvtime.currentMillis
//    val itemNotFound = "dfasdfasfvsdfvw453453ef34rff"
//    val defaultIntervalInSec = 1
//    val aut = metaOrDefault(itemNotFound, defaultIntervalInSec)
//    assert(defaultIntervalInSec === aut.reloadInSeconds)
//    assert(start <= aut.timestamps.head)
//    assert(!cache.contains(itemNotFound))
//  }
//
//  test("meta withCurrentTimestamp") {
//    if (assertEnforce) assert(PermaCacher.isEnforceAssertions)
//    val aut = metaOrDefault("dummy", 1)
//    Thread.sleep(10)
//    val aut1 = aut.withCurrentTimestamp
//    Thread.sleep(10)
//    val aut2 = aut1.withCurrentTimestamp
//    assert(3===aut2.timestamps.size)
//    //cache.put("dummy",("test",aut2))
//    //display.foreach(println)
//  }
//
////  test("cache get remove") {
////    assert(PermaCacher.isEnforceAssertions)
////    val aux1 = "diuadcpjnasd"
////    val aux2 = metaOrDefault("dummy", 1)
////    val content = (aux1,aux2)
////    val key = "adcasdflkasdvjnasdverpoirf"
////    val dummy = MOCK(classOf[Cancellable])
////    PermaCacher.restart()
////    cache.put(key,content)
////    cacheKeyMutex.put(key,new AnyRef())
////    reloadOps.put(key,dummy)
////    primeLastAccess(key,true)
////    assert(1==cache.size)
////    assert(1==cacheLastAccess.size)
////    val aut = PermaCacher.get(key)
////    assert(aut==Some(aux1))
////    PermaCacher.clearResultFromCache(key)
////    assert(0==cache.size)
////    assert(0==cacheLastAccess.size)
////  }
//}
//
//class PermaCacherGetOrRegisterTest
//  extends BaseScalaTest with BeforeAndAfterEach {
//
//  import PermaCacher._
//  import PermaCacherPsm._
//
//  override def beforeEach() {
//    PermaCacher.restart()
//    assert(isPmsStateEmpty)
//  }
//
//  override def afterEach() {
//    PermaCacher.restart()
//    assert(isPmsStateEmpty)
//  }
//
//// it is actually org.slf4j.impl.JDK14LoggerFactory
////  test("the underlying harness is Log4j") {
////    assert(LoggingTest.isLog4j,(new LoggingTestHarness).underlying)
////  }
//
//  test("simple value register/get") {
//
//    if (assertEnforce) assert(PermaCacher.isEnforceAssertions)
//    val mockListener1 = MOCK(classOf[PermaCacherListener])
//    PermaCacher.listeners += mockListener1
//
//    val value = "bar0"
//    val key = "foo0"
//    def dummyFactory = value
//
//    val result = getOrRegister(key,2, mayBeEvicted = false)(dummyFactory)
//
//    assert(value===result)
//
//    cacheLastAccess.get(key) match {
//      case Some(ts) => assert(ts.get() === cacheAccessTimeInfiniteTTL )
//      case None => fail()
//    }
//
//    verify(mockListener1, times(1)).onBeforeKeyUpdated(key)
//    verify(mockListener1, times(1)).onAfterKeyUpdated(same(key), any())
//    verify(mockListener1, never()).onError(same(key), any())
//
//    PermaCacher.listeners -= mockListener1
//  }
//
//  test("uncached simple value register/get") {
//
//    if (assertEnforce) assert(PermaCacher.isEnforceAssertions)
//    val mockListener1 = MOCK(classOf[PermaCacherListener])
//    PermaCacher.listeners += mockListener1
//
//    val value = "bar0u"
//    val key = "foo0u"
//    val uncached = 0
//    def dummyFactory = value
//
//    val result = getOrRegister(key, uncached, false)(dummyFactory)
//
//    assert(value===result)
//
//    cache.get(key) match {
//      case Some(ts) => fail()
//      case None =>
//    }
//
//    verify(mockListener1, times(1)).onBeforeKeyUpdated(key)
//    verify(mockListener1, times(1)).onAfterKeyUpdated(same(key), any())
//    verify(mockListener1, never()).onError(same(key), any())
//
//    PermaCacher.listeners -= mockListener1
//  }
//
//  test("simple UTF-8 value register/get") {
//
//    if (assertEnforce) assert(PermaCacher.isEnforceAssertions)
//    val mockListener1 = MOCK(classOf[PermaCacherListener])
//    PermaCacher.listeners += mockListener1
//
//    val value = "http://www.bbook.com/wp-content/uploads/2013/12/BlackBook-3-Minutes-Marina-Abramović-and-William-Basinski.jpg"
//    val key = "foo0"
//    def dummyFactory = value
//
//    val result = getOrRegister(key, 2, false)(dummyFactory)
//
//    assert(value===result)
//
//    cacheLastAccess.get(key) match {
//      case Some(ts) => assert(ts.get() === cacheAccessTimeInfiniteTTL )
//      case None => fail()
//    }
//
//    verify(mockListener1, times(1)).onBeforeKeyUpdated(key)
//    verify(mockListener1, times(1)).onAfterKeyUpdated(same(key), any())
//    verify(mockListener1, never()).onError(same(key), any())
//
//    PermaCacher.listeners -= mockListener1
//  }
//
//  test("evictable simple value register/get") {
//
//    if (assertEnforce) assert(PermaCacher.isEnforceAssertions)
//    val mockListener1 = MOCK(classOf[PermaCacherListener])
//    PermaCacher.listeners += mockListener1
//
//    val value = "bar0e"
//    val key = "foo0e"
//    def dummyFactory = value
//
//    val result = getOrRegister(key, 2, true)(dummyFactory)
//
//    assert(value===result)
//
//    cacheLastAccess.get(key) match {
//      case Some(ts) => assert(ts.get() === cacheAccessTimeNeverYet )
//      case None => fail()
//    }
//
//    verify(mockListener1, times(1)).onBeforeKeyUpdated(key)
//    verify(mockListener1, times(1)).onAfterKeyUpdated(same(key), any())
//    verify(mockListener1, never()).onError(same(key), any())
//
//    val gotten = getOrRegister(key, 2, false)(dummyFactory)
//
//    assert(value===gotten)
//
//    cacheLastAccess.get(key) match {
//      case Some(ts1) => assert(ts1.get() > 0 && ts1.get() != cacheAccessTimeNeverYet )
//      case None => fail()
//    }
//
//    val lapTime = cacheLastAccess.get(key).get.get()
//    Thread.sleep(200)
//    val oneMore = get[String](key)
//
//    assert(value===oneMore.get)
//    cache.foreach( println )
//    // assert(1===cache.size)
//    // assert(1===cacheLastAccess.size)
//    // assert(1===reloadOps.size)
//    // assert(1==loaderLockPool.getNumIdle(key))
//
//    cacheLastAccess.get(key) match {
//      case Some(ts2) => {
//        assert(ts2.get() > lapTime && ts2.get() != cacheAccessTimeNeverYet )
//      }
//      case None => fail()
//    }
//
//    PermaCacher.listeners -= mockListener1
//  }
//
//  test("value register/get that throws") {
//
//    // only to inhibit a stack trace to console
//    val spyAppender = new SpyLogAppender()
//    val (restoreMe, consoles) = logAllLevelsToSpyNoConsole(spyAppender)
//
//    if (assertEnforce) assert(PermaCacher.isEnforceAssertions)
//    val mockListener1 = MOCK(classOf[PermaCacherListener])
//    PermaCacher.listeners += mockListener1
//
//    val value = "bar1"
//    val key = "foo1"
//    val exMsg = "ohps!1"
//    val throwMe = new RuntimeException(exMsg)
//    def bombFactory = {throw throwMe; value}
//
//    val thrown1 = intercept[RuntimeException] {
//      getOrRegister(key, 2, false)(bombFactory)
//    }
//    assert(assertableUnlog(spyAppender, restoreMe, consoles))
//
//    assert(exMsg===thrown1.getMessage)
//
//    verify(mockListener1, times(1)).onBeforeKeyUpdated(key)
//    verify(mockListener1, never()).onAfterKeyUpdated(same(key), any())
//    verify(mockListener1, times(1)).onError(same(key), any())
//
//    PermaCacher.listeners -= mockListener1
//  }
//
//}
//
//class PermaCacherGetOrRegisterWithHealingTest
//  extends BaseScalaTest with BeforeAndAfterEach {
//
//  import PermaCacher._
//  import PermaCacherPsm._
//  val psmValue = "bar00"
//  val psmKey = "foo00"
//  def dummyFactory: String = psmValue
//
//  def corrupt_psmValue {
//    val result = getOrRegister(psmKey, 2, false)(dummyFactory)
//    cacheLastAccess.remove(psmKey)
//    reloadOps.remove(psmKey)
//    cacheKeyMutex.remove(psmKey)
//  }
//
//  override def beforeEach() {
//    PermaCacher.restart()
//    assert(isPmsStateEmpty)
//    corrupt_psmValue
//  }
//
//  override def afterEach() {
//    PermaCacher.restart()
//    assert(isPmsStateEmpty)
//  }
//
//  // it is actually org.slf4j.impl.JDK14LoggerFactory
//  //  test("the underlying harness is Log4j") {
//  //    assert(LoggingTest.isLog4j,(new LoggingTestHarness).underlying)
//  //  }
//
//  test("simple value register/get with healing") {
//
//    if (!assertEnforce) {
//      val mockListener1 = MOCK(classOf[PermaCacherListener])
//      PermaCacher.listeners += mockListener1
//
//      val result = getOrRegister(psmKey, 2, false)(dummyFactory)
//
//      assert(psmValue === result)
//
//      cacheLastAccess.get(psmKey) match {
//        case Some(ts) => assert(ts.get() === cacheAccessTimeInfiniteTTL)
//        case None => fail()
//      }
//
//      verify(mockListener1, times(1)).onBeforeKeyUpdated(psmKey)
//      verify(mockListener1, times(1)).onAfterKeyUpdated(same(psmKey), any())
//      verify(mockListener1, never()).onError(same(psmKey), any())
//
//      PermaCacher.listeners -= mockListener1
//    } else {
//      println("must skip test when psm state enforced")
//    }
//  }
//
//  test("uncached simple value register/get with healing") {
//
//    if (!assertEnforce) {
//      val mockListener1 = MOCK(classOf[PermaCacherListener])
//      PermaCacher.listeners += mockListener1
//
//      val uncached = 0
//
//      val result = getOrRegister(psmKey, uncached, false)(dummyFactory)
//
//      assert(psmValue === result)
//
//      cache.get(psmKey) match {
//        case Some(ts) => fail()
//        case None =>
//      }
//
//      verify(mockListener1, times(1)).onBeforeKeyUpdated(psmKey)
//      verify(mockListener1, times(1)).onAfterKeyUpdated(same(psmKey), any())
//      verify(mockListener1, never()).onError(same(psmKey), any())
//
//      PermaCacher.listeners -= mockListener1
//    } else {
//      println("must skip test when psm state enforced")
//    }
//  }
//
//}
//
//class PermaCacherGetOrRegisterLoggingTest
//  extends BaseScalaTest with BeforeAndAfterEach {
//
//  import PermaCacher._
//  import PermaCacherPsm._
//
//  override def beforeEach() {
//    PermaCacher.restart()
//    assert(isPmsStateEmpty)
//  }
//
//  override def afterEach() {
//    PermaCacher.restart()
//    assert(isPmsStateEmpty)
//  }
//
//  ignore("value register/get that throws (logging)") {
//
//    if (assertEnforce) assert(PermaCacher.isEnforceAssertions)
//    val spyAppender = new SpyLogAppender()
//    val (restoreMe, consoles) = logAllLevelsToSpy(spyAppender)
//
//    val value = "bar3"
//    val key = "foo3"
//    val exMsg = "ohps!3"
//    val throwMe = new RuntimeException(exMsg)
//    def bombFactory = {throw throwMe; value}
//
//    val thrown1 = intercept[RuntimeException] {
//      getOrRegister(key, 2, false)(bombFactory)
//    }
//
//    assert(exMsg===thrown1.getMessage)
//
//    //val autLog = spyAppender.spy()
//    //println(autLog)
//    // now check logs
//    val autLog = spyAppender.spyByToken()
//    assert(autLog(1).trim === errorToken)
//    val aux = autLog(2).split("\n")
//    val actual = aux(0).trim
//    val expectedStart = unableGetOrRegisterIntro.replace("{0}",key).replace("{1}","")
//    val expectedEnd = "java.lang.RuntimeException: "+exMsg
//    val errMsg = "expected:"+expectedStart+".*"+expectedEnd+", found:"+actual
//    assert(actual.startsWith(expectedStart),errMsg)
//    assert(actual.endsWith(expectedEnd),errMsg)
//    assert(assertableUnlog(spyAppender, restoreMe, consoles))
//  }
//
//}
//
//class PermaCacherGetOrRegisterWithOptionTest
//  extends BaseScalaTest with BeforeAndAfterEach {
//
//  import PermaCacher._
//  import PermaCacherPsm._
//
//  override def beforeEach() {
//    PermaCacher.restart()
//    assert(isPmsStateEmpty)
//  }
//
//  override def afterEach() {
//    PermaCacher.restart()
//    assert(isPmsStateEmpty)
//  }
//
//  test("simple Option Some register/get") {
//
//    if (assertEnforce) assert(PermaCacher.isEnforceAssertions)
//    val mockListener1 = MOCK(classOf[PermaCacherListener])
//    PermaCacher.listeners += mockListener1
//
//    val value = "bar00"
//    val key = "foo00"
//    def dummyFactory = Some(value)
//
//    val result = getOrRegisterFactoryWithOption(key,2)(dummyFactory)
//
//    assert(Some(value)===result)
//
//    cacheLastAccess.get(key) match {
//      case Some(ts) => assert(ts.get() === cacheAccessTimeInfiniteTTL )
//      case None => fail()
//    }
//
//    verify(mockListener1, times(1)).onBeforeKeyUpdated(key)
//    verify(mockListener1, times(1)).onAfterKeyUpdated(same(key), any())
//    verify(mockListener1, never()).onError(same(key), any())
//
//    PermaCacher.listeners -= mockListener1
//  }
//
//  test("uncached Option Some register/get") {
//
//    if (assertEnforce) assert(PermaCacher.isEnforceAssertions)
//    val mockListener1 = MOCK(classOf[PermaCacherListener])
//    PermaCacher.listeners += mockListener1
//
//    val value = "bar00Opu"
//    val key = "foo00Opu"
//    val uncached = 0
//    def dummyFactory = Some(value)
//
//    val result = getOrRegisterFactoryWithOption(key,uncached)(dummyFactory)
//
//    assert(Some(value)===result)
//
//    cache.get(key) match {
//      case Some(ts) => fail()
//      case None =>
//    }
//
//    verify(mockListener1, times(1)).onBeforeKeyUpdated(key)
//    verify(mockListener1, times(1)).onAfterKeyUpdated(same(key), any())
//    verify(mockListener1, never()).onError(same(key), any())
//
//    PermaCacher.listeners -= mockListener1
//  }
//
//  test("evictable simple Option Some register/get") {
//
//    if (assertEnforce) assert(PermaCacher.isEnforceAssertions)
//    val mockListener1 = MOCK(classOf[PermaCacherListener])
//    PermaCacher.listeners += mockListener1
//
//    val value = "bar00e"
//    val key = "foo00e"
//    def dummyFactory = Some(value)
//
//    val result = getOrRegisterFactoryWithOption(key, 2, true)(dummyFactory)
//
//    assert(Some(value)===result)
//
//    cacheLastAccess.get(key) match {
//      case Some(ts) => assert(ts.get() === cacheAccessTimeNeverYet )
//      case None => fail()
//    }
//
//    verify(mockListener1, times(1)).onBeforeKeyUpdated(key)
//    verify(mockListener1, times(1)).onAfterKeyUpdated(same(key), any())
//    verify(mockListener1, never()).onError(same(key), any())
//
//    val gotten = getOrRegisterFactoryWithOption(key, 2, false)(dummyFactory)
//
//    assert(Some(value)===gotten)
//
//    cacheLastAccess.get(key) match {
//      case Some(ts) => assert(ts.get() > 0 && ts.get() != cacheAccessTimeNeverYet )
//      case None => fail()
//    }
//
//    PermaCacher.listeners -= mockListener1
//  }
//
//  test("simple Option None register/get") {
//
//    if (assertEnforce) assert(PermaCacher.isEnforceAssertions)
//    val mockListener1 = MOCK(classOf[PermaCacherListener])
//    PermaCacher.listeners += mockListener1
//
//    val key = "foo02"
//    def dummyFactory = None
//
//    val result = getOrRegisterFactoryWithOption(key,2)(dummyFactory)
//
//    assert(None===result)
//
//    cacheLastAccess.get(key) match {
//      case Some(ts) => assert(ts.get() === cacheAccessTimeInfiniteTTL )
//      case None => fail()
//    }
//
//    verify(mockListener1, times(1)).onBeforeKeyUpdated(key)
//    verify(mockListener1, times(1)).onAfterKeyUpdated(same(key), any())
//    verify(mockListener1, never()).onError(same(key), any())
//
//    PermaCacher.listeners -= mockListener1
//  }
//
//  test("uncached Option None register/get") {
//
//    if (assertEnforce) assert(PermaCacher.isEnforceAssertions)
//    val mockListener1 = MOCK(classOf[PermaCacherListener])
//    PermaCacher.listeners += mockListener1
//
//    val key = "foo02Nu"
//    val uncached = 0
//    def dummyFactory = None
//
//    val result = getOrRegisterFactoryWithOption(key,uncached)(dummyFactory)
//
//    assert(None===result)
//
//    cache.get(key) match {
//      case Some(ts) => fail()
//      case None =>
//    }
//
//    verify(mockListener1, times(1)).onBeforeKeyUpdated(key)
//    verify(mockListener1, times(1)).onAfterKeyUpdated(same(key), any())
//    verify(mockListener1, never()).onError(same(key), any())
//
//    PermaCacher.listeners -= mockListener1
//  }
//
//  test("evictable simple Option None register/get") {
//
//    if (assertEnforce) assert(PermaCacher.isEnforceAssertions)
//    val mockListener1 = MOCK(classOf[PermaCacherListener])
//    PermaCacher.listeners += mockListener1
//
//    val key = "foo02e"
//    def dummyFactory = None
//
//    val result = getOrRegisterFactoryWithOption(key, 2, true)(dummyFactory)
//
//    assert(None===result)
//
//    cacheLastAccess.get(key) match {
//      case Some(ts) => assert(ts.get() === cacheAccessTimeNeverYet )
//      case None => fail()
//    }
//
//    verify(mockListener1, times(1)).onBeforeKeyUpdated(key)
//    verify(mockListener1, times(1)).onAfterKeyUpdated(same(key), any())
//    verify(mockListener1, never()).onError(same(key), any())
//
//    val gotten = getOrRegisterFactoryWithOption(key,2)(dummyFactory)
//
//    assert(None===gotten)
//
//    cacheLastAccess.get(key) match {
//      case Some(ts) => assert(ts.get() > 0 && ts.get() != cacheAccessTimeNeverYet )
//      case None => fail()
//    }
//
//    PermaCacher.listeners -= mockListener1
//  }
//
//  test("Option register/get that throws") {
//
//    // only to inhibit a stack trace to stdout
//    val spyAppender = new SpyLogAppender()
//    val (restoreMe, consoles) = logAllLevelsToSpyNoConsole(spyAppender)
//
//    if (assertEnforce) assert(PermaCacher.isEnforceAssertions)
//    val mockListener1 = MOCK(classOf[PermaCacherListener])
//    PermaCacher.listeners += mockListener1
//
//    val value = "bar01"
//    val key = "foo01"
//    val exMsg = "ohps!01"
//    val throwMe = new RuntimeException(exMsg)
//    def bombFactory = {throw throwMe; Some(value)}
//
//    val thrown1 = intercept[RuntimeException] {
//      getOrRegisterFactoryWithOption(key,2)(bombFactory)
//    }
//    assert(assertableUnlog(spyAppender, restoreMe, consoles))
//
//    assert(exMsg===thrown1.getMessage)
//
//    verify(mockListener1, times(1)).onBeforeKeyUpdated(key)
//    verify(mockListener1, never()).onAfterKeyUpdated(same(key), any())
//    verify(mockListener1, times(1)).onError(same(key), any())
//
//    PermaCacher.listeners -= mockListener1
//  }
//}
//
///* */
//class PermaCacherGetOrRegisterWithOptionWithHealingTest
//  extends BaseScalaTest with BeforeAndAfterEach {
//
//  import PermaCacher._
//  import PermaCacherPsm._
//  val psmValue = "bar00"
//  val psmKey = "foo00"
//  def dummyFactory: Some[String] = Some(psmValue)
//
//  def corrupt_psmValue {
//    val result = getOrRegisterFactoryWithOption(psmKey,2)(dummyFactory)
//    cacheLastAccess.remove(psmKey)
//    reloadOps.remove(psmKey)
//    cacheKeyMutex.remove(psmKey)
//  }
//
//  override def beforeEach() {
//    PermaCacher.restart()
//    assert(isPmsStateEmpty)
//    corrupt_psmValue
//  }
//
//  override def afterEach() {
//    PermaCacher.restart()
//    assert(isPmsStateEmpty)
//  }
//
//  test("simple Option Some register/get with healing") {
//
//    if (!assertEnforce) {
//      val mockListener1 = MOCK(classOf[PermaCacherListener])
//      PermaCacher.listeners += mockListener1
//
//      val result = getOrRegisterFactoryWithOption(psmKey, 2)(dummyFactory)
//
//      assert(Some(psmValue) === result)
//
//      cacheLastAccess.get(psmKey) match {
//        case Some(ts) => assert(ts.get() === cacheAccessTimeInfiniteTTL)
//        case None => fail()
//      }
//
//      verify(mockListener1, times(1)).onBeforeKeyUpdated(psmKey)
//      verify(mockListener1, times(1)).onAfterKeyUpdated(same(psmKey), any())
//      verify(mockListener1, never()).onError(same(psmKey), any())
//
//      PermaCacher.listeners -= mockListener1
//    } else {
//      println("must skip test when psm state enforced")
//    }
//  }
//
//  test("uncached Option Some register/get with healing") {
//
//    if (!assertEnforce) {
//      val mockListener1 = MOCK(classOf[PermaCacherListener])
//      PermaCacher.listeners += mockListener1
//
//      val uncached = 0
//
//      val result = getOrRegisterFactoryWithOption(psmKey, uncached)(dummyFactory)
//
//      assert(Some(psmValue) === result)
//
//      cache.get(psmKey) match {
//        case Some(ts) => fail()
//        case None =>
//      }
//
//      verify(mockListener1, times(1)).onBeforeKeyUpdated(psmKey)
//      verify(mockListener1, times(1)).onAfterKeyUpdated(same(psmKey), any())
//      verify(mockListener1, never()).onError(same(psmKey), any())
//
//      PermaCacher.listeners -= mockListener1
//    } else {
//      println("must skip test when psm state enforced")
//    }
//  }
//
//}
//
//class PermaCacherGetOrRegisterWithOptionLoggingTest
//  extends BaseScalaTest with BeforeAndAfterEach {
//
//  import PermaCacher._
//  import PermaCacherPsm._
//
//  override def beforeEach() {
//    PermaCacher.restart()
//    assert(isPmsStateEmpty)
//  }
//
//  override def afterEach() {
//    PermaCacher.restart()
//    assert(isPmsStateEmpty)
//  }
//
//  ignore("simple Option Some register/get (logging)") {
//
//    if (assertEnforce) assert(PermaCacher.isEnforceAssertions)
//    val spyAppender = new SpyLogAppender()
//    val (restoreMe, consoles) = logAllLevelsToSpy(spyAppender)
//
//    val value = "bar10"
//    val key = "foo10"
//    val interval = 2
//    def dummyFactory = Some(value)
//
//    val result = getOrRegisterFactoryWithOption(key,interval)(dummyFactory)
//
//    assert(Some(value)===result)
//    // val autLog = spyAppender.spy()
//    // println(autLog)
//    // now check logs
//    val autLog = spyAppender.spyByToken()
//    assert(autLog(1).trim === infoToken)
//    assert(autLog(2).trim === getOrRegisterWithOptionSome.replace("{0}",key).replace("{1}",""+interval))
//    assert(assertableUnlog(spyAppender, restoreMe, consoles))
//  }
//
//  ignore("simple Option None register/get (logging)") {
//
//    if (assertEnforce) assert(PermaCacher.isEnforceAssertions)
//    val spyAppender = new SpyLogAppender()
//    val (restoreMe, consoles) = logAllLevelsToSpy(spyAppender)
//
//    val key = "foo20"
//    val interval = 3
//    def dummyFactory = None
//
//    val result = getOrRegisterFactoryWithOption(key,interval)(dummyFactory)
//
//    assert(None===result)
//    // val autLog = spyAppender.spy()
//    // println(autLog)
//    // now check logs
//    val autLog = spyAppender.spyByToken()
//    assert(autLog(1).trim === infoToken)
//    assert(autLog(2).trim === getOrRegisterWithOptionNone.replace("{0}",key).replace("{1}",""+interval))
//    assert(assertableUnlog(spyAppender, restoreMe, consoles))
//  }
//
//  ignore("Option register/get that throws (logging)") {
//
//    if (assertEnforce) assert(PermaCacher.isEnforceAssertions)
//    val spyAppender = new SpyLogAppender()
//    val (restoreMe, consoles) = logAllLevelsToSpy(spyAppender)
//
//    val value = "bar11"
//    val key = "foo11"
//    val exMsg = "ohps!11"
//    val throwMe = new RuntimeException(exMsg)
//    def bombFactory = {throw throwMe; Some(value)}
//
//    val thrown1 = intercept[RuntimeException] {
//      getOrRegisterFactoryWithOption(key,2)(bombFactory)
//    }
//
//    assert(exMsg===thrown1.getMessage)
//
//    //val autLog = spyAppender.spy()
//    //println(autLog)
//    // now check logs
//    val autLog = spyAppender.spyByToken()
//    assert(autLog(1).trim === errorToken)
//    val aux = autLog(2).split("\n")
//
//    val actual = aux(0).trim
//    val expectedStart = unableGetOrRegisterWithOptionIntro.replace("{0}",key).replace("{1}","")
//    val expectedEnd = "java.lang.RuntimeException: "+exMsg
//    val errMsg = "expected:"+expectedStart+".*"+expectedEnd+", found:"+actual
//    assert(actual.startsWith(expectedStart),errMsg)
//    assert(actual.endsWith(expectedEnd),errMsg)
//    assert(assertableUnlog(spyAppender, restoreMe, consoles))
//  }
//}
//
//class PermaCacherGetOrRegisterConditionalTest
//  extends BaseScalaTest with BeforeAndAfterEach {
//
//  import PermaCacher._
//  import PermaCacherPsm._
//
//  override def beforeEach() {
//    PermaCacher.restart()
//    assert(isPmsStateEmpty)
//  }
//
//  override def afterEach() {
//    PermaCacher.restart()
//    assert(isPmsStateEmpty)
//  }
//
//  test("simple Conditional register/get") {
//
//    if (assertEnforce) assert(PermaCacher.isEnforceAssertions)
//    val mockListener1 = MOCK(classOf[PermaCacherListener])
//    PermaCacher.listeners += mockListener1
//
//    val value = "bar00Cond"
//    val key = "foo00Cond"
//    def dummyFactory(sss : Option[String]) = StorageRefresh(value)
//
//    val result = getOrRegisterConditional(key, 2, false)(dummyFactory)
//
//    assert(Some(value)===result)
//
//    cacheLastAccess.get(key) match {
//      case Some(ts) => assert(ts.get() === cacheAccessTimeInfiniteTTL )
//      case None => fail()
//    }
//
//    verify(mockListener1, times(1)).onBeforeKeyUpdated(key)
//    verify(mockListener1, times(1)).onAfterKeyUpdated(same(key), any())
//    verify(mockListener1, never()).onError(same(key), any())
//
//    PermaCacher.listeners -= mockListener1
//  }
//
//  test("uncached Conditional register/get") {
//
//    if (assertEnforce) assert(PermaCacher.isEnforceAssertions)
//    val mockListener1 = MOCK(classOf[PermaCacherListener])
//    PermaCacher.listeners += mockListener1
//
//    val value = "bar00CondU"
//    val key = "foo00CondU"
//    val uncached = 0
//    def dummyFactory(sss : Option[String]) = StorageRefresh(value)
//
//    val result = getOrRegisterConditional(key, uncached, false)(dummyFactory)
//
//    assert(Some(value)===result)
//
//    cache.get(key) match {
//      case Some(ts) => fail()
//      case None =>
//    }
//
//    verify(mockListener1, times(1)).onBeforeKeyUpdated(key)
//    verify(mockListener1, times(1)).onAfterKeyUpdated(same(key), any())
//    verify(mockListener1, never()).onError(same(key), any())
//
//    PermaCacher.listeners -= mockListener1
//  }
//
//  test("evictable simple Conditional register/get") {
//
//    if (assertEnforce) assert(PermaCacher.isEnforceAssertions)
//    val mockListener1 = MOCK(classOf[PermaCacherListener])
//    PermaCacher.listeners += mockListener1
//
//    val value = "bar00eCond"
//    val key = "foo00eCond"
//    def dummyFactory(sss : Option[String]) = StorageRefresh(value)
//
//    val result = getOrRegisterConditional(key,dummyFactory,2,true)
//
//    assert(Some(value)===result)
//
//    cacheLastAccess.get(key) match {
//      case Some(ts) => assert(ts.get() === cacheAccessTimeNeverYet )
//      case None => fail()
//    }
//
//    verify(mockListener1, times(1)).onBeforeKeyUpdated(key)
//    verify(mockListener1, times(1)).onAfterKeyUpdated(same(key), any())
//    verify(mockListener1, never()).onError(same(key), any())
//
//    val gotten = getOrRegisterConditional(key, 2, false)(dummyFactory)
//
//    assert(Some(value)===gotten)
//
//    cacheLastAccess.get(key) match {
//      case Some(ts) => assert(ts.get() > 0 && ts.get() != cacheAccessTimeNeverYet )
//      case None => fail()
//    }
//
//    PermaCacher.listeners -= mockListener1
//  }
//
//  test("evictable or permanent NOP Conditional register/get") {
//
//    if (assertEnforce) assert(PermaCacher.isEnforceAssertions)
//    val mockListener1 = MOCK(classOf[PermaCacherListener])
//    PermaCacher.listeners += mockListener1
//
//    val value = "bar00eCond"
//    val key = "foo00eCond"
//    def dummyFactory(sss : Option[String]) : StorageResult[String] = StorageNOP
//
//    val result = getOrRegisterConditional(key, 2, true)(dummyFactory)
//
//    assert(None===result)
//    assert(isPmsStateEmpty)
//
//    verify(mockListener1, times(1)).onBeforeKeyUpdated(key)
//    verify(mockListener1, times(1)).onAfterKeyUpdated(same(key), any())
//    verify(mockListener1, never()).onError(same(key), any())
//
//    val gotten = getOrRegisterConditional(key, 2, false)(dummyFactory)
//
//    assert(None===gotten)
//    assert(isPmsStateEmpty)
//
//    PermaCacher.listeners -= mockListener1
//  }
//
//  test("uncached Conditional NOP register/get") {
//
//    if (assertEnforce) assert(PermaCacher.isEnforceAssertions)
//    val mockListener1 = MOCK(classOf[PermaCacherListener])
//    PermaCacher.listeners += mockListener1
//
//    val value = "bar00CondNopU"
//    val key = "foo00CondNopU"
//    val uncached = 0
//    def dummyFactory(sss : Option[String]) : StorageResult[String]= StorageNOP
//
//    val result = getOrRegisterConditional(key, uncached, false)(dummyFactory)
//
//    assert(None===result)
//
//    cache.get(key) match {
//      case Some(ts) => fail()
//      case None =>
//    }
//
//    verify(mockListener1, times(1)).onBeforeKeyUpdated(key)
//    verify(mockListener1, times(1)).onAfterKeyUpdated(same(key), any())
//    verify(mockListener1, never()).onError(same(key), any())
//
//    PermaCacher.listeners -= mockListener1
//  }
//
//  test("Conditional register/get that throws") {
//
//    // only to inhibit a stack trace to stdout
//    val spyAppender = new SpyLogAppender()
//    val (restoreMe, consoles) = logAllLevelsToSpyNoConsole(spyAppender)
//
//    if (assertEnforce) assert(PermaCacher.isEnforceAssertions)
//    val mockListener1 = MOCK(classOf[PermaCacherListener])
//    PermaCacher.listeners += mockListener1
//
//    // add another item to the cache...
//    val ovalue = "bar00Cond"
//    val okey = "foo00Cond"
//    def dummyFactory(sss : Option[String]) = StorageRefresh(ovalue)
//    val result = getOrRegisterConditional(okey, 2, false)(dummyFactory)
//    assert(Some(ovalue)===result)
//
//    // proceed
//    val value = "bar01Cond"
//    val key = "foo01Cond"
//    val exMsg = "ohps!01Cond"
//    val throwMe = new RuntimeException(exMsg)
//    def bombFactory(sss : Option[String]) = { throw throwMe; StorageRefresh(value) }
//
//    val stateBefore = {
//      val (isConsistent, state) = isConsistentBeforeGetOrRegister(okey)
//      assert(isConsistent,"inconsistent before getOrRegister with bomb")
//      state
//    }
//
//    val thrown1 = intercept[RuntimeException] {
//      getOrRegisterConditional(key, 2, false)(bombFactory)
//    }
//    assert(assertableUnlog(spyAppender, restoreMe, consoles))
//
//    assert(exMsg===thrown1.getMessage)
//
//    verify(mockListener1, times(1)).onBeforeKeyUpdated(key)
//    verify(mockListener1, never()).onAfterKeyUpdated(same(key), any())
//    verify(mockListener1, times(1)).onError(same(key), any())
//
//    // if not another item in the cashe this will fail on a corner case
//    assert(isConsistentAfterGetOrRegister(okey,stateBefore),"bad cleanup after bomb")
//
//    PermaCacher.listeners -= mockListener1
//  }
//}
//
//class PermaCacherGetOrRegisterConditionalWithHealingTest
//  extends BaseScalaTest with BeforeAndAfterEach {
//
//  import PermaCacher._
//  import PermaCacherPsm._
//  val psmValue = "bar00"
//  val psmKey = "foo00"
//  def dummyFactory(sss : Option[String]): StorageRefresh[String] = StorageRefresh(psmValue)
//
//  def corrupt_psmValue {
//    val result = getOrRegisterConditional(psmKey, 2, false)(dummyFactory)
//    cacheLastAccess.remove(psmKey)
//    reloadOps.remove(psmKey)
//    cacheKeyMutex.remove(psmKey)
//  }
//
//  override def beforeEach() {
//    PermaCacher.restart()
//    assert(isPmsStateEmpty)
//    corrupt_psmValue
//  }
//
//  override def afterEach() {
//    PermaCacher.restart()
//    assert(isPmsStateEmpty)
//  }
//
//  test("simple Conditional register/get with healing") {
//
//    if (!assertEnforce) {
//      val mockListener1 = MOCK(classOf[PermaCacherListener])
//      PermaCacher.listeners += mockListener1
//
//      val result = getOrRegisterConditional(psmKey, 2, false)(dummyFactory)
//
//      assert(Some(psmValue) === result)
//
//      cacheLastAccess.get(psmKey) match {
//        case Some(ts) => assert(ts.get() === cacheAccessTimeInfiniteTTL)
//        case None => fail()
//      }
//
//      verify(mockListener1, times(1)).onBeforeKeyUpdated(psmKey)
//      verify(mockListener1, times(1)).onAfterKeyUpdated(same(psmKey), any())
//      verify(mockListener1, never()).onError(same(psmKey), any())
//
//      PermaCacher.listeners -= mockListener1
//    } else {
//      println("must skip test when psm state enforced")
//    }
//  }
//
//  test("uncached Conditional register/get with healing") {
//
//    if (!assertEnforce) {
//      val mockListener1 = MOCK(classOf[PermaCacherListener])
//      PermaCacher.listeners += mockListener1
//
//      val uncached = 0
//
//      val result = getOrRegisterConditional(psmKey, uncached, false)(dummyFactory)
//
//      assert(Some(psmValue)===result)
//
//      cache.get(psmKey) match {
//        case Some(ts) => fail()
//        case None =>
//      }
//
//      verify(mockListener1, times(1)).onBeforeKeyUpdated(psmKey)
//      verify(mockListener1, times(1)).onAfterKeyUpdated(same(psmKey), any())
//      verify(mockListener1, never()).onError(same(psmKey), any())
//
//      PermaCacher.listeners -= mockListener1
//    } else {
//      println("must skip test when psm state enforced")
//    }
//  }
//
//}
