//package com.gravity.utilities.logging
//
//import com.gravity.logging.Logstashable
//import com.gravity.utilities.components.FailureResult
//import com.gravity.utilities.{BaseScalaTest, Logging}
//import org.apache.log4j.{Logger => LOGGER4J, _}
//import org.slf4j.impl.StaticLoggerBinder
//
//import scala.collection.JavaConversions._
//import scalaz.NonEmptyList
//
////
////
//import scalaz.std.list._
//
//object LoggingTest {
//
//  val prefix = "Logstash:"
//  val replaceme = "%%%"
//  val delay = 50
//  val repeats = 2
//
//  val errorToken = "ERROR"
//  val warnToken = "WARN"
//  val infoToken = "INFO"
//  val traceToken = "TRACE"
//
//  // true iff the underlying harness is log4j
//  val isLog4j: Boolean = (new LoggingTestHarness).underlying.contains("Log4j")
//
//  def logAllLevelsToSpy(appender: WriterAppender, isRemoveConsoles : Boolean = true) : (Level,List[Appender]) = {
//    if (isRemoveConsoles) logAllLevelsToSpyNoConsole(appender)
//    else {
//      if (!LOGGER4J.getRootLogger.getAllAppenders.hasMoreElements) {
//        println("log4j not initialized at tospy")
//      }
//      val previous = LOGGER4J.getRootLogger.getLevel()
//      LOGGER4J.getRootLogger.setLevel(Level.ALL)
//      LOGGER4J.getRootLogger.addAppender(appender)
//      (previous, Nil)
//    }
//  }
//
//  def logAllLevelsToSpyNoConsole(appender: WriterAppender) : (Level,List[Appender]) = {
//
//    def seekConsoleAppenders : List[Appender] = {
//      // the following needed so Scala can do its Enumeration.asScala on-the-fly
//      val javaEnumeration = (LOGGER4J.getRootLogger.getAllAppenders)
//      val found = for (app <- javaEnumeration ) yield {
//        app match {
//          case s:ConsoleAppender => Some(s)
//          case _ => None
//        }
//      }
//      if (!found.hasNext) Nil
//      else found.toList.flatten
//    }
//
//    val consoles = if (!LOGGER4J.getRootLogger.getAllAppenders.hasMoreElements) {
//      println("log4j not initialized at consoles")
//      Nil
//    } else seekConsoleAppenders
//    val previousLvl = LOGGER4J.getRootLogger.getLevel()
//    LOGGER4J.getRootLogger.setLevel(Level.ALL)
//    LOGGER4J.getRootLogger.addAppender(appender)
//    if (consoles.isEmpty) (previousLvl,consoles)
//    else {
//      // remove console appenders
//      consoles.foreach(LOGGER4J.getRootLogger.removeAppender(_))
//      (previousLvl,consoles)
//    }
//  }
//
//  def assertableUnlog(appender: WriterAppender, previousLevel: Level, previousAppender : List[Appender]) : Boolean = {
//
//    def seekAmongAppenders (target : Appender) : Boolean = {
//      // the following needed so Scala can do its Enumeration.asScala on-the-fly
//      val javaEnumeration = (LOGGER4J.getRootLogger.getAllAppenders)
//      val found :Iterator[Boolean] = for (app <- javaEnumeration if app.equals(target)) yield true
//      if (found.isEmpty) false else true
//    }
//
//    if (!LOGGER4J.getRootLogger.getAllAppenders.hasMoreElements) {
//      // we are unlogging, so we need some appender to start with...
//      throw new IllegalStateException("log4j not initialized at unlog")
//    } else {
//      if (!previousAppender.isEmpty) {
//        previousAppender.foreach(LOGGER4J.getRootLogger.addAppender(_))
//      }
//      if (seekAmongAppenders(appender)) {
//        // target found, so now remove and restore Level
//        LOGGER4J.getRootLogger.removeAppender(appender)
//        LOGGER4J.getRootLogger.setLevel(previousLevel)
//
//
//
//        true
//      } else {
//        // target not found
//        false
//      }
//    }
//  }
//
//}
//
///**
// * Created with IntelliJ IDEA.
// * User: asquassabia
// * Date: 10/14/13
// * Time: 11:44 AM
// * To change this template use File | Settings | File Templates.
// */
//class LoggingTest extends BaseScalaTest {
//
//  import LoggingTest._
//
//  test("trace lazily allocates the message") {
//    val logging = new Logging {}
//    logging.logger.isTraceEnabled should be(false)
//    logging.trace({ fail("this should not be eval'd") })
//    logging.trace({ fail("this should not be eval'd") }, 1, 2, 3, 4, 5)
//
//    logging.trace(
//      { fail("this should not be eval'd"); new Exception },
//      { fail("this should not be eval'd") },
//      1, 2, 3, 4, 5
//    )
//
//    logging.trace(
//      { fail("this should not be eval'd"); NonEmptyList(FailureResult("")) },
//      { fail("this should not be eval'd") }
//    )
//
//    logging.trace({
//      fail("this should not be eval'd")
//
//      class Foo extends Logstashable {
//        override def getKVs: Seq[(String, String)] = Seq.empty
//      }
//
//      new Foo
//    })
//  }
//
//  /*
//
//     aut means artifact under test
//     Aut AUT aUt... mean the same :)
//
//   */
//
//  def assert2tokensForAutWhenUsing (f: => String, levelToken : String): Unit = {
//    val spyAppender = new SpyLogAppender();
//    val (restoreMe, consoles) = logAllLevelsToSpy(spyAppender)
//    val oracle = f
//    val aut  = spyAppender.spyByToken()
//    //  println(aut(0)+" "+aut(1))
//    assert(aut(1).trim===levelToken)
//    assert(aut(2).trim===oracle)
//    assert(assertableUnlog(spyAppender,restoreMe, consoles))
//  }
//
//  def assertTokensForAutWhenLogstashableUsing (f: => String, prefix : String, levelToken : String): Unit = {
//    val spyAppender = new SpyLogAppender();
//    val (restoreMe, consoles) = logAllLevelsToSpy(spyAppender)
//    val oracle = f
//    val aut = spyAppender.spyByToken()
//    assert(aut(1).trim===levelToken)
//    assert(aut(2).trim===oracle.trim)
//    assert(oracle.startsWith(prefix))
//    assert(assertableUnlog(spyAppender,restoreMe, consoles))
//  }
//
//  def assertWithThrowableForAutUsing (f: => (String, String), levelToken: String): Unit = {
//    val spyAppender = new SpyLogAppender();
//    val (restoreMe, consoles) = logAllLevelsToSpy(spyAppender)
//    val (oracle, excMsg) = f
//    val aut  = spyAppender.spyByToken()
//    //  println(aut(0)+" "+aut(1))
//    assert(aut(1).trim===levelToken)
//    assert(aut(2).trim===oracle)
//    assert(aut(3).trim.startsWith(excMsg))
//    // println("==>>> "+aut(2))
//    assert(assertableUnlog(spyAppender,restoreMe, consoles))
//  }
//
//  def assertWithThrowableForAutWhenLogstashableUsing (f: => (String,String), prefix : String, levelToken : String): Unit = {
//    val spyAppender = new SpyLogAppender();
//    val (restoreMe, consoles) = logAllLevelsToSpy(spyAppender)
//    val (oracle,excMsg) = f
//    val aut = spyAppender.spyByToken()
//    assert(aut(1).trim===levelToken)
//    //println("==>>> "+aut(1))
//    val cont = aut(2).split("\\n")
//    //println("==>>> "+cont(0)) // header
//    //println("==>>> "+cont(1)) // 1st line of stack trace
//    // yes, all on the same line, with no breaks...
//    assert(cont(0).trim===(oracle.trim+excMsg))
//    assert(assertableUnlog(spyAppender,restoreMe, consoles))
//  }
//
//  def assertForAutWhenFoldedUsing (f: => (String,String), prefix : String, levelToken : String): Unit = {
//    val spyAppender = new SpyLogAppender();
//    val (restoreMe, consoles) = logAllLevelsToSpy(spyAppender)
//    val (first,second) = f
//    val aut = spyAppender.spyByToken()
//    // each element in the folded collection issues its own log call,
//    // so there are as many log entries as there are elements, each
//    // prepended with levelToken.
//    // aut.foreach(q => println("aut==>>"+q))
//    assert(aut(1).trim===levelToken)
//    // may not use === since never did .split("\\n")
//    assert(aut(2).trim.startsWith(first),"expected:'"+(first)+"' found:'"+aut(2).trim.substring(0,aut(2).trim.indexOf('\n'))+"'")
//    assert(aut(3).trim===levelToken)
//    assert(aut(4).trim.startsWith(second),"expected:'"+(second)+"' found:'"+aut(4).trim.substring(0,aut(4).trim.indexOf('\n'))+"'")
//    assert(5===aut.length)
//    assert(assertableUnlog(spyAppender,restoreMe, consoles))
//  }
//
//  def assertForTraceSpecialAut (f: => String, levelToken : String): Unit = {
//    val spyAppender = new SpyLogAppender();
//    val (restoreMe, consoles) = logAllLevelsToSpy(spyAppender)
//    val (oracle) = f
//    val aut = spyAppender.spyByToken()
//    assert(aut(1).trim===levelToken)
//    // println("==TS==>>>"+aut(1))
//    if (!aut(2).trim.startsWith(replaceme)) // i.e. coming from testNone or testEmpty or testWithTrace
//      assert(aut(2)===oracle)
//    else if (aut(2).trim.startsWith(replaceme+replaceme)) { // i.e. coming from testIfTraceBenchmarkWithRepeats
//    // extract the timed runtime YYY from "%%%%%% (ran X times with a total of YYY ms for an average of ZZZ.Z ms)" where YYY should be a number
//    val l : Long = java.lang.Long.parseLong(aut(2).subSequence(aut(2).indexOf("of ")+3,aut(2).indexOf(" ms ")).toString)
//      // just a smoke test here (should not be smaller, should it?)
//      assert(l >= delay*repeats)
//    } else if (aut(2).trim.startsWith(replaceme)) { // i.e. coming from testIfTraceBenchmark
//      // extract the timed runtime XXX from "%%% (timed at XXX ms)" where XXX should be a number
//      val l : Long = java.lang.Long.parseLong(aut(2).subSequence(aut(2).indexOf("at ")+3,aut(2).indexOf(" ms")).toString)
//      assert(l >= delay)
//    } else {
//      throw new RuntimeException("missing clause")
//    }
//    assert(assertableUnlog(spyAppender,restoreMe, consoles))
//  }
//
//  ignore("underlying slf4j is Log4j") {
//    // if not, our spy games are doomed
//    assert(isLog4j)
//  }
//
//  test("loggerName") {
//    // make trivially sure identification works as expected
//    val holder = new LoggingTestHarness
//    if (holder.getClass.getName.contains("$")) throw new RuntimeException("bad holder")
//    // should rhyme with "com.gravity.utilities.logging.ContrivedTestHarness"
//    else assert((new LoggingTestHarness).loggerName === holder.getClass.getName)
//  }
//
//  //=======================================
//  // traditional logging
//  //=======================================
//
//  ignore("forCritical1") {
//    assert2tokensForAutWhenUsing((new LoggingTestHarness).forCritical1, errorToken)
//  }
//
//  ignore("forCritical2") {
//    assert2tokensForAutWhenUsing((new LoggingTestHarness).forCritical2, errorToken)
//  }
//
//  ignore("forCritical3") {
//    assert2tokensForAutWhenUsing((new LoggingTestHarness).forCritical3, errorToken)
//  }
//
//  ignore("forCritical4") {
//    assert2tokensForAutWhenUsing((new LoggingTestHarness).forCritical4, errorToken)
//  }
//
//  ignore("forCritical5") {
//    assert2tokensForAutWhenUsing((new LoggingTestHarness).forCritical5, errorToken)
//  }
//
//  ignore("forCriticalWithThrowable") {
//    assertWithThrowableForAutUsing((new LoggingTestHarness).forCriticalWithThrowable, errorToken)
//  }
//
//  ignore("forError1") {
//    assert2tokensForAutWhenUsing((new LoggingTestHarness).forError1, errorToken)
//  }
//
//  ignore("forError2") {
//    assert2tokensForAutWhenUsing((new LoggingTestHarness).forError2, errorToken)
//  }
//
//  ignore("forError3") {
//    assert2tokensForAutWhenUsing((new LoggingTestHarness).forError3, errorToken)
//  }
//
//  ignore("forError4") {
//    assert2tokensForAutWhenUsing((new LoggingTestHarness).forError4, errorToken)
//  }
//
//  ignore("forError5") {
//    assert2tokensForAutWhenUsing((new LoggingTestHarness).forError5, errorToken)
//  }
//
//  ignore("forErrorWithThrowable") {
//    assertWithThrowableForAutUsing((new LoggingTestHarness).forErrorWithThrowable, errorToken)
//  }
//
//  ignore("forWarn1") {
//    assert2tokensForAutWhenUsing((new LoggingTestHarness).forWarn1, warnToken)
//  }
//
//  ignore("forWarn2") {
//    assert2tokensForAutWhenUsing((new LoggingTestHarness).forWarn2, warnToken)
//  }
//
//  ignore("forWarn3") {
//    assert2tokensForAutWhenUsing((new LoggingTestHarness).forWarn3, warnToken)
//  }
//
//  ignore("forWarn4") {
//    assert2tokensForAutWhenUsing((new LoggingTestHarness).forWarn4, warnToken)
//  }
//
//  ignore("forWarn5") {
//    assert2tokensForAutWhenUsing((new LoggingTestHarness).forWarn5, warnToken)
//  }
//
//  ignore("forWarnWithThrowable") {
//    assertWithThrowableForAutUsing((new LoggingTestHarness).forWarnWithThrowable, warnToken)
//  }
//
//  ignore("forInfo1") {
//    assert2tokensForAutWhenUsing((new LoggingTestHarness).forInfo1, infoToken)
//  }
//
//  ignore("forInfo2") {
//    assert2tokensForAutWhenUsing((new LoggingTestHarness).forInfo2, infoToken)
//  }
//
//  ignore("forInfo3") {
//    assert2tokensForAutWhenUsing((new LoggingTestHarness).forInfo3, infoToken)
//  }
//
//  ignore("forInfo4") {
//    assert2tokensForAutWhenUsing((new LoggingTestHarness).forInfo4, infoToken)
//  }
//
//  ignore("forInfo5") {
//    assert2tokensForAutWhenUsing((new LoggingTestHarness).forInfo5, infoToken)
//  }
//
//  ignore("forInfoWithThrowable") {
//    assertWithThrowableForAutUsing((new LoggingTestHarness).forInfoWithThrowable, infoToken)
//  }
//
//  ignore("forTrace1") {
//    assert2tokensForAutWhenUsing((new LoggingTestHarness).forTrace1, traceToken)
//  }
//
//  ignore("forTrace2") {
//    assert2tokensForAutWhenUsing((new LoggingTestHarness).forTrace2, traceToken)
//  }
//
//  ignore("forTrace3") {
//    assert2tokensForAutWhenUsing((new LoggingTestHarness).forTrace3, traceToken)
//  }
//
//  ignore("forTrace4") {
//    assert2tokensForAutWhenUsing((new LoggingTestHarness).forTrace4, traceToken)
//  }
//
//  ignore("forTrace5") {
//    assert2tokensForAutWhenUsing((new LoggingTestHarness).forTrace5, traceToken)
//  }
//
//  ignore("forTraceWithThrowable") {
//    assertWithThrowableForAutUsing((new LoggingTestHarness).forTraceWithThrowable, traceToken)
//  }
//
//  //=======================================
//  // LogStash formatted logging
//  //=======================================
//
//  ignore("forCriticalWithLogstashFailure") {
//    assertTokensForAutWhenLogstashableUsing(
//      (new LoggingTestHarness).forCriticalWithLogstashFailure, prefix, errorToken)
//  }
//
//  ignore("forCriticalWithLogstashFailureAndThrowable") {
//    assertWithThrowableForAutWhenLogstashableUsing(
//      (new LoggingTestHarness).forCriticalWithLogstashFailureAndThrowable, prefix, errorToken)
//  }
//
//  ignore("forErrorWithLogstashFailure") {
//    assertTokensForAutWhenLogstashableUsing(
//      (new LoggingTestHarness).forErrorWithLogstashFailure, prefix, errorToken)
//  }
//
//  ignore("forErrorWithLogstashFailureAndThrowable") {
//    assertWithThrowableForAutWhenLogstashableUsing(
//      (new LoggingTestHarness).forErrorWithLogstashFailureAndThrowable, prefix, errorToken)
//  }
//
//  ignore("forWarnWithLogstashFailure") {
//    assertTokensForAutWhenLogstashableUsing(
//      (new LoggingTestHarness).forWarnWithLogstashFailure, prefix, warnToken)
//  }
//
//  ignore("forWarnWithLogstashFailureAndThrowable") {
//    assertWithThrowableForAutWhenLogstashableUsing(
//      (new LoggingTestHarness).forWarnWithLogstashFailureAndThrowable, prefix, warnToken)
//  }
//
//  ignore("forInfoWithLogstashFailure") {
//    assertTokensForAutWhenLogstashableUsing(
//      (new LoggingTestHarness).forInfoWithLogstashFailure, prefix, infoToken)
//  }
//
//  ignore("forInfoWithLogstashFailureAndThrowable") {
//    assertWithThrowableForAutWhenLogstashableUsing(
//      (new LoggingTestHarness).forInfoWithLogstashFailureAndThrowable, prefix, infoToken)
//  }
//
//  ignore("forTraceWithLogstashFailure") {
//    assertTokensForAutWhenLogstashableUsing(
//      (new LoggingTestHarness).forTraceWithLogstashFailure, prefix, traceToken)
//  }
//
//  ignore("forTraceWithLogstashFailureAndThrowable") {
//    assertWithThrowableForAutWhenLogstashableUsing(
//      (new LoggingTestHarness).forTraceWithLogstashFailureAndThrowable, prefix, traceToken)
//  }
//
//  ignore("forCriticalWithLogstashCustom") {
//    assertTokensForAutWhenLogstashableUsing(
//      (new LoggingTestHarness).forCriticalWithLogstashCustom, prefix, errorToken)
//  }
//
//  ignore("forCriticalWithLogstashCustomAndThrowable") {
//    assertWithThrowableForAutWhenLogstashableUsing(
//      (new LoggingTestHarness).forCriticalWithLogstashCustomAndThrowable, prefix, errorToken)
//  }
//
//  ignore("forErrorWithLogstashCustom") {
//    assertTokensForAutWhenLogstashableUsing(
//      (new LoggingTestHarness).forErrorWithLogstashCustom, prefix, errorToken)
//  }
//
//  ignore("forErrorWithLogstashCustomAndThrowable") {
//    assertWithThrowableForAutWhenLogstashableUsing(
//      (new LoggingTestHarness).forErrorWithLogstashCustomAndThrowable, prefix, errorToken)
//  }
//
//  ignore("forWarnWithLogstashCustom") {
//    assertTokensForAutWhenLogstashableUsing(
//      (new LoggingTestHarness).forWarnWithLogstashCustom, prefix, warnToken)
//  }
//
//  ignore("forWarnWithLogstashCustomAndThrowable") {
//    assertWithThrowableForAutWhenLogstashableUsing(
//      (new LoggingTestHarness).forWarnWithLogstashCustomAndThrowable, prefix, warnToken)
//  }
//
//  ignore("forInfoWithLogstashCustom") {
//    assertTokensForAutWhenLogstashableUsing(
//      (new LoggingTestHarness).forInfoWithLogstashCustom, prefix, infoToken)
//  }
//
//  ignore("forInfoWithLogstashCustomAndThrowable") {
//    assertWithThrowableForAutWhenLogstashableUsing(
//      (new LoggingTestHarness).forInfoWithLogstashCustomAndThrowable, prefix, infoToken)
//  }
//
//  ignore("forTraceWithLogstashCustom") {
//    assertTokensForAutWhenLogstashableUsing(
//      (new LoggingTestHarness).forTraceWithLogstashCustom, prefix, traceToken)
//  }
//
//  ignore("forTraceWithLogstashCustomAndThrowable") {
//    assertWithThrowableForAutWhenLogstashableUsing(
//      (new LoggingTestHarness).forTraceWithLogstashCustomAndThrowable, prefix, traceToken)
//  }
//
//  //=======================================
//  // LogStash (foldable collections)
//  //=======================================
//
//  // XXXQQQXXX
//  ignore("forCriticalWithFoldableFailure") {
//    assertForAutWhenFoldedUsing(
//      (new LoggingTestHarness).forCriticalWithFoldableFailure, prefix, errorToken)
//  }
//
//  val fmtMsg: (String) => (String) => (String) => String = (mVal: String) => (tVal: String) => (sVal: String) => s"Logstash: message=$mVal^type=$tVal^superType=$sVal"
//
//  ignore("critical with NonEmptyList[FailureResult] has correct return"){
//    val logger = new Logging{}
//    val msg1 = "failure result message1"
//    val msg2 = "message2 for failure result"
//    val actual = logger.critical(NonEmptyList(FailureResult(msg1), FailureResult(msg2)))
//    println(s"ret: $actual")
//    val expected = List(msg1, msg2).map(fmtMsg(_)("FailureResult")("FailureResult")).mkString("\n")
//    println(s"expected: $expected")
//    assert(expected == actual)
//  }
//
//  ignore("logError with List[Logstashable] has correct return"){
//    val logger = new Logging{}
//    val msg1 = "failure result message1"
//    val msg2 = "message2 for failure result"
//    val tVal = "tvalllll"
//    val actual = logger.error(List(new CustomLogstashable(List(("m", msg1), ("t",tVal)), None), new CustomLogstashable(List(("m", msg2), ("t", tVal)), None)))
//    val expected = List(msg1, msg2).map(fmtMsg(_)(tVal)).mkString("\n")
//    println(s"actual: $actual")
//    println(s"expected: $expected")
//    assert(expected == actual)
//  }
//
//  // XXXQQQXXX
//  ignore("forCriticalWithFoldableLogstashable") {
//    assertForAutWhenFoldedUsing(
//      (new LoggingTestHarness).forCriticalWithFoldableLogstashable, prefix, errorToken)
//  }
//
//  // XXXQQQXXX
//  ignore("forErrorWithFoldableFailure") {
//    assertForAutWhenFoldedUsing(
//      (new LoggingTestHarness).forErrorWithFoldableFailure, prefix, errorToken)
//  }
//
//  // XXXQQQXXX
//  ignore("forErrorWithFoldableLogstashable") {
//    assertForAutWhenFoldedUsing(
//      (new LoggingTestHarness).forErrorWithFoldableLogstashable, prefix, errorToken)
//  }
//
//  ignore("forWarnWithFoldableFailure") {
//    assertForAutWhenFoldedUsing(
//      (new LoggingTestHarness).forWarnWithFoldableFailure, prefix, warnToken)
//  }
//
//  ignore("forWarnWithFoldableLogstashable") {
//    assertForAutWhenFoldedUsing(
//      (new LoggingTestHarness).forWarnWithFoldableLogstashable, prefix, warnToken)
//  }
//
//  ignore("forInfoWithFoldableFailure") {
//    assertForAutWhenFoldedUsing(
//      (new LoggingTestHarness).forInfoWithFoldableFailure, prefix, infoToken)
//  }
//
//  ignore("forInfoWithFoldableLogstashable") {
//    assertForAutWhenFoldedUsing(
//      (new LoggingTestHarness).forInfoWithFoldableLogstashable, prefix, infoToken)
//  }
//
//  ignore("forTraceWithFoldableFailure") {
//    assertForAutWhenFoldedUsing(
//      (new LoggingTestHarness).forTraceWithFoldableFailure, prefix, traceToken)
//  }
//
//  ignore("forTraceWithFoldableLogstashable") {
//    assertForAutWhenFoldedUsing(
//      (new LoggingTestHarness).forTraceWithFoldableLogstashable, prefix, traceToken)
//  }
//
//  // trace specials
//
//  ignore("testTraceNone") {
//    assertForTraceSpecialAut((new LoggingTestHarness).testTraceNone, traceToken)
//  }
//
//  ignore("testTraceEmpty") {
//    assertForTraceSpecialAut((new LoggingTestHarness).testTraceEmpty, traceToken)
//  }
//
//  ignore("testIfTraceBench") {
//    assertForTraceSpecialAut((new LoggingTestHarness).testIfTraceBench, traceToken)
//  }
//
//  ignore("testIfTraceBenchWithRepeats") {
//    assertForTraceSpecialAut((new LoggingTestHarness).testIfTraceBenchWithRepeats, traceToken)
//  }
//
//  test("testIfTraceBenchWithRepeatsBombs") {
//    val thrown = intercept[IllegalArgumentException] {
//      (new LoggingTestHarness).testIfTraceBenchWithRepeatsBombs
//    }
//    val msgExpected = "requirement failed: The number of `times` to repeat MUST be greater than 0 (zero)!"
//    assert(thrown.getMessage === msgExpected)
//  }
//
//  ignore("testWithTrace") {
//    assertForTraceSpecialAut((new LoggingTestHarness).testWithTrace, traceToken)
//  }
//
//  ignore("testWithTraceVarArgs") {
//    assertForTraceSpecialAut((new LoggingTestHarness).testWithTraceVarArgs, traceToken)
//  }
//
//}
//
//class LoggingTestHarness {
//  import com.gravity.logging.Logging._
//  import LoggingTest._
//
//  def underlying: String = {
//    val binder : StaticLoggerBinder = StaticLoggerBinder.getSingleton
//    binder.getLoggerFactoryClassStr();
//  }
//
//  def loggerName: String = {
//    logger.getName()
//  }
//
//  def forCritical1 : String = {
//    // this always seems to return String
//    levelFor1(critical)
//  }
//
//  def forCritical2 : String = {
//    // the compiler gets confused with multiple ambiguous
//    // overloading, if it is not guided along the right path
//    def f(a:String,b:Any,c:Any*) : Option[Any] = Some(critical(a,b,c:_*))
//    // NOTE 1: without the _* hint on c there will be failures
//    // NOTE 2: some levels return String, others return Unit, no
//    // obvious rhyme or reason for the choice: this accommodates
//    // both possibilities BUT does NOT enforce any; a better test
//    // should probably do just that, and then again, maybe not...
//    levelFor3a(f)
//  }
//
//  def forCritical3 : String = {
//    def f(a:String,b:Any,c:Any*) : Option[Any] = Some(critical(a,b,c:_*))
//    levelFor3b(f)
//  }
//
//  def forCritical4 : String = {
//    def f(a:String,b:Any,c:Any*) : Option[Any] = Some(critical(a,b,c:_*))
//    levelFor3c(f)
//  }
//
//  def forCritical5 : String = {
//    def f(a:String,b:Any,c:Any*) : Option[Any] = Some(critical(a,b,c:_*))
//    levelFor3d(f)
//  }
//
//  def forCriticalWithThrowable : (String, String) = {
//    def f(a:Throwable,b:String,c:Any*) : Option[Any] = Some(critical(a,b,c:_*))
//    levelWithThrowable(f)
//  }
//
//  def forError1 : String = {
//    levelFor1(error)
//  }
//
//  def forError2 : String = {
//    def f(a:String,b:Any,c:Any*) : Option[Any] = Some(error(a,b,c:_*))
//    levelFor3a(f)
//  }
//
//  def forError3 : String = {
//    def f(a:String,b:Any,c:Any*) : Option[Any] = Some(error(a,b,c:_*))
//    levelFor3b(f)
//  }
//
//  def forError4 : String = {
//    def f(a:String,b:Any,c:Any*) : Option[Any] = Some(error(a,b,c:_*))
//    levelFor3c(f)
//  }
//
//  def forError5 : String = {
//    def f(a:String,b:Any,c:Any*) : Option[Any] = Some(error(a,b,c:_*))
//    levelFor3d(f)
//  }
//
//  def forErrorWithThrowable : (String, String) = {
//    def f(a:Throwable,b:String,c:Any*) : Option[Any] = Some(error(a,b,c:_*))
//    levelWithThrowable(f)
//  }
//
//  def forWarn1 : String = {
//    levelFor1(warn)
//  }
//
//  def forWarn2 : String = {
//    def f(a:String,b:Any,c:Any*) : Option[Any] = Some(warn(a,b,c:_*))
//    levelFor3a(f)
//  }
//
//  def forWarn3 : String = {
//    def f(a:String,b:Any,c:Any*) : Option[Any] = Some(warn(a,b,c:_*))
//    levelFor3b(f)
//  }
//
//  def forWarn4 : String = {
//    def f(a:String,b:Any,c:Any*) : Option[Any] = Some(warn(a,b,c:_*))
//    levelFor3c(f)
//  }
//
//  def forWarn5 : String = {
//    def f(a:String,b:Any,c:Any*) : Option[Any] = Some(warn(a,b,c:_*))
//    levelFor3d(f)
//  }
//
//  def forWarnWithThrowable : (String, String) = {
//    def f(a:Throwable,b:String,c:Any*) : Option[Any] = Some(warn(a,b,c:_*))
//    levelWithThrowable(f)
//  }
//
//  def forInfo1 : String = {
//    levelFor1(info)
//  }
//
//  def forInfo2 : String = {
//    def f(a:String,b:Any,c:Any*) : Option[Any] = Some(info(a,b,c:_*))
//    levelFor3a(f)
//  }
//
//  def forInfo3 : String = {
//    def f(a:String,b:Any,c:Any*) : Option[Any] = Some(info(a,b,c:_*))
//    levelFor3b(f)
//  }
//
//  def forInfo4 : String = {
//    def f(a:String,b:Any,c:Any*) : Option[Any] = Some(info(a,b,c:_*))
//    levelFor3c(f)
//  }
//
//  def forInfo5 : String = {
//    def f(a:String,b:Any,c:Any*) : Option[Any] = Some(info(a,b,c:_*))
//    levelFor3d(f)
//  }
//
//  def forInfoWithThrowable : (String, String) = {
//    def f(a:Throwable,b:String,c:Any*) : Option[Any] = Some(info(a,b,c:_*))
//    levelWithThrowable(f)
//  }
//
//  def forTrace1 : String = {
//    levelFor1(trace(_))
//  }
//
//  def forTrace2 : String = {
//    def f(a:String,b:Any,c:Any*) : Option[Any] = Some(trace(a,b,c:_*))
//    levelFor3a(f)
//  }
//
//  def forTrace3 : String = {
//    def f(a:String,b:Any,c:Any*) : Option[Any] = Some(trace(a,b,c:_*))
//    levelFor3b(f)
//  }
//
//  def forTrace4 : String = {
//    def f(a:String,b:Any,c:Any*) : Option[Any] = Some(trace(a,b,c:_*))
//    levelFor3c(f)
//  }
//
//  def forTrace5 : String = {
//    def f(a:String,b:Any,c:Any*) : Option[Any] = Some(trace(a,b,c:_*))
//    levelFor3d(f)
//  }
//
//  def forTraceWithThrowable : (String, String) = {
//    def f(a:Throwable,b:String,c:Any*) : Option[Any] = Some(trace(a,b,c:_*))
//    levelWithThrowable(f)
//  }
//
//  // Logstash, individual items (vs.  foldable collections)
//  //
//  // NOTE: there was an interesting issue arising from the, say, logError signature in Logging.scala
//  // since that call ends up being, when passed as a parameter, a curried function with an argument
//  // coming from an anonymous class (e.g. the CanLogstash.FailureResultCanLogstash type below).  Could
//  // not figure what the syntax for a First-order function argument with that signature is, grrr.
//  // hence the workaround patterned in the following calls.  Sorry!
//  // PS: the workaround works well enough that it may not be worth refactoring, but the issue is
//  // worthwhile in principle.
//
//  def forCriticalWithLogstashFailure : String = {
//    // workaround, see note above
//    def f (ls : FailureResult) : Option[Any] = Some(critical(ls))
//    level4LogstashFailure(f)
//  }
//
//  def forCriticalWithLogstashFailureAndThrowable : (String,String) = {
//    def f (ls : FailureResult) : Option[Any] = Some(critical(ls))
//    level4LogstashFailureWithThrowable(f)
//  }
//
//  def forErrorWithLogstashFailure : String = {
//    def f (ls : FailureResult) : Option[Any] = Some(error(ls))
//    level4LogstashFailure(f)
//  }
//
//  def forErrorWithLogstashFailureAndThrowable : (String,String) = {
//    def f (ls : FailureResult) : Option[Any] = Some(error(ls))
//    level4LogstashFailureWithThrowable(f)
//  }
//
//  def forWarnWithLogstashFailure : String = {
//    def f (ls : FailureResult) : Option[Any] = Some(warn(ls))
//    level4LogstashFailure(f)
//  }
//
//  def forWarnWithLogstashFailureAndThrowable : (String,String) = {
//    def f (ls : FailureResult) : Option[Any] = Some(warn(ls))
//    level4LogstashFailureWithThrowable(f)
//  }
//
//  def forInfoWithLogstashFailure : String = {
//    def f (ls : FailureResult) : Option[Any] = Some(info(ls))
//    level4LogstashFailure(f)
//  }
//
//  def forInfoWithLogstashFailureAndThrowable : (String,String) = {
//    def f (ls : FailureResult) : Option[Any] = Some(info(ls))
//    level4LogstashFailureWithThrowable(f)
//  }
//
//  def forTraceWithLogstashFailure : String = {
//    def f (ls : FailureResult) : Option[Any] = Some(trace(ls))
//    level4LogstashFailure(f)
//  }
//
//  def forTraceWithLogstashFailureAndThrowable : (String,String) = {
//    def f (ls : FailureResult) : Option[Any] = Some(trace(ls))
//    level4LogstashFailureWithThrowable(f)
//  }
//
//  def forCriticalWithLogstashCustom : String = {
//    def f (ls : Logstashable) : Option[Any] = Some(critical(ls))
//    levelForLogstashCustom(f)
//  }
//
//  def forCriticalWithLogstashCustomAndThrowable : (String,String) = {
//    def f (ls : Logstashable) : Option[Any] = Some(critical(ls))
//    levelForLogstashCustomWithThrowable(f)
//  }
//
//  def forErrorWithLogstashCustom : String = {
//    def f (ls : Logstashable) : Option[Any] = Some(error(ls))
//    levelForLogstashCustom(f)
//  }
//
//  def forErrorWithLogstashCustomAndThrowable : (String,String) = {
//    def f (ls : Logstashable) : Option[Any] = Some(error(ls))
//    levelForLogstashCustomWithThrowable(f)
//  }
//
//  def forWarnWithLogstashCustom : String = {
//    def f (ls : Logstashable) : Option[Any] = Some(warn(ls))
//    levelForLogstashCustom(f)
//  }
//
//  def forWarnWithLogstashCustomAndThrowable : (String,String) = {
//    def f (ls : Logstashable) : Option[Any] = Some(warn(ls))
//    levelForLogstashCustomWithThrowable(f)
//  }
//
//  def forInfoWithLogstashCustom : String = {
//    def f (ls : Logstashable) : Option[Any] = Some(info(ls))
//    levelForLogstashCustom(f)
//  }
//
//  def forInfoWithLogstashCustomAndThrowable : (String,String) = {
//    def f (ls : Logstashable) : Option[Any] = Some(info(ls))
//    levelForLogstashCustomWithThrowable(f)
//  }
//
//  def forTraceWithLogstashCustom : String = {
//    def f (ls : Logstashable) : Option[Any] = Some(trace(ls))
//    levelForLogstashCustom(f)
//  }
//
//  def forTraceWithLogstashCustomAndThrowable : (String,String) = {
//    def f (ls : Logstashable) : Option[Any] = Some(trace(ls))
//    levelForLogstashCustomWithThrowable(f)
//  }
//
//  // logstash foldable collections: experimental, I'm still figuring this out...
//
//  // See note way above for my inability of finding a signature for passing THAT curried function.
//  // This is the same, only worse, so I'm using another version of the same workaround, this time
//  // inspired by warnFailNel around line 255 in Logging.scala.  Note that any foldable collections
//  // of CanLogstash MUST be homogeneous (i.e. all content must be of the same type) because there
//  // is no common parent to Logstashable and FailureResult meeting the [T : CanLogstash] constraint.
//  // (It's found in the signature of the caller in Logging.scala). It is therefore imho legitimate
//  // to parametrize List with either FailureResult or Logstashable.  Hmm.  Is it?  It works.
//
//  def forCriticalWithFoldableFailure : (String,String) = {
//    def f (l : List[FailureResult]) : String = {
//      critical[FailureResult, List](l)
//    }
//    levelForLogstashFoldableFailure(f)
//  }
//
//  def forCriticalWithFoldableLogstashable : (String,String) = {
//    def f (l : List[Logstashable]) : String = {
//      critical[Logstashable, List](l)
//    }
//    levelForLogstashFoldableCustom(f)
//  }
//
//  def forErrorWithFoldableFailure : (String,String) = {
//    def f (l : List[FailureResult]) : String = {
//      error[FailureResult, List](l)
//    }
//    levelForLogstashFoldableFailure(f)
//  }
//
//  def forErrorWithFoldableLogstashable : (String,String) = {
//    def f (l : List[Logstashable]) : String = {
//      error[Logstashable, List](l)
//    }
//    levelForLogstashFoldableCustom(f)
//  }
//
//  def forWarnWithFoldableFailure : (String,String) = {
//    def f (l : List[FailureResult]) : String = {
//      warn[FailureResult, List](l)
//    }
//    levelForLogstashFoldableFailure(f)
//  }
//
//  def forWarnWithFoldableLogstashable : (String,String) = {
//    def f (l : List[Logstashable]) : String = {
//      warn[Logstashable, List](l)
//    }
//    levelForLogstashFoldableCustom(f)
//  }
//
//  def forInfoWithFoldableFailure : (String,String) = {
//    def f (l : List[FailureResult]) : String = {
//      info[FailureResult, List](l)
//    }
//    levelForLogstashFoldableFailure(f)
//  }
//
//  def forInfoWithFoldableLogstashable : (String,String) = {
//    def f (l : List[Logstashable]) : String = {
//      info[Logstashable, List](l)
//    }
//    levelForLogstashFoldableCustom(f)
//  }
//
//  def forTraceWithFoldableFailure : (String,String) = {
//    def f (l : List[FailureResult]) : String = {
//      trace[FailureResult, List](l)
//    }
//    levelForLogstashFoldableFailure(f)
//  }
//
//  def forTraceWithFoldableLogstashable : (String,String) = {
//    def f (l : List[Logstashable]) : String = {
//      trace[Logstashable, List](l)
//    }
//    levelForLogstashFoldableCustom(f)
//  }
//
//  // trace specials
//
//  def testTraceNone : (String) = {
//    val returnsThisOption = "returns This Option"
//    val logsTheMessage = "logs The Message"
//    def iAmNone : Option[String] = None
//    def iAmSome : Option[String] = Some(returnsThisOption)
//    val returned = traceNone[Option[String]]("unlogged")(iAmSome) match {
//      case None => throw new RuntimeException("broken!")
//      case Some(x) => x // returnThisOption, i.e. the content of iAmSome
//    }
//    println("either returns "+checkReturn(Some(returned),returnsThisOption)+" (not None), or")
//    traceNone[Option[String]](logsTheMessage)(iAmNone) match {
//      case None => logsTheMessage
//      case Some(x) => throw new RuntimeException("broken!")
//    }
//  }
//
//  def testTraceEmpty : (String) = {
//    val returnsThisMessage = "returns This Message"
//    val logsTheMessage = "logs The Message"
//    def iAmEmpty : List[String] = Nil
//    def iAmFull : List[String] = returnsThisMessage :: Nil
//    val returned = traceEmpty[List[String]]("unlogged")(iAmFull) match {
//      case Nil => throw new RuntimeException("broken!")
//      case x :: Nil => x // returnThisOption, i.e. the content of iAmSome
//      case _ => throw new RuntimeException("broken (in a new way)!")
//    }
//    println("either returns "+checkReturn(Some(returned),returnsThisMessage)+" (not empty), or")
//    traceEmpty[List[String]](logsTheMessage)(iAmEmpty) match {
//      case Nil => logsTheMessage
//      case _ => throw new RuntimeException("broken!")
//    }
//  }
//
//  def testIfTraceBench : (String) = {
//    def idler : Long = {
//      val start = (new java.util.Date()).getTime()
//      Thread.sleep(delay)
//      (new java.util.Date()).getTime() - start
//    }
//    ""+ifTraceBench[Long](replaceme)(idler)
//  }
//
//  def testIfTraceBenchWithRepeats : (String) = {
//    def idler : Long = {
//      val start = (new java.util.Date()).getTime()
//      Thread.sleep(delay)
//      (new java.util.Date()).getTime() - start
//    }
//    ""+ifTraceBenchWithRepeats[Long](repeats, replaceme+replaceme)(idler)
//  }
//
//  def testIfTraceBenchWithRepeatsBombs : (String) = {
//    def idler : Long = {
//      val start = (new java.util.Date()).getTime()
//      Thread.sleep(delay)
//      (new java.util.Date()).getTime() - start
//    }
//    ""+ifTraceBenchWithRepeats[Long](0, replaceme+replaceme)(idler)
//  }
//
//  def testWithTrace : (String) = {
//    val logMsg = "with Trace also logs this"
//    val msg = "dckasdcadc"
//    def simple : String = msg
//    val returned = withTrace[String](logMsg)(simple)
//    println("also returns "+checkReturn(Some(returned),msg)+" (besides logging)")
//    logMsg
//  }
//
//  def testWithTraceVarArgs : (String) = {
//    val logMsg = "with Trace also logs this, with optional substitution"
//    val msg = "vfdfvsdvsdfv"
//    def simple : String = msg
//    val returned = withTrace[String](logMsg,Nil,Nil)(simple)
//    println("also returns "+checkReturn(Some(returned),msg)+" (besides logging)")
//    logMsg
//  }
//
//  // common functionality
//
//  def checkReturn(o: Option[Any], oracle : String) : String = {
//    val whatIsReturned = o match {
//      case None => throw new RuntimeException("broken harness")
//      case Some(v) => v match {
//        case aut:String => {
//          // i.e. if (normal || (withThrowable) || (logstashableThrowable) )
//          // where all of normal, but not all of the Throwable(s) is tallied
//          if (oracle.equals(aut) ||
//             (aut.startsWith(oracle) && aut.startsWith(SpyLogAppender.SEP,oracle.length)) ||
//             (oracle.startsWith(prefix) && aut.startsWith(oracle) && aut.startsWith("\\n",oracle.length))
//          ) "String"
//          else throw new RuntimeException("expected:'"+oracle+"' found:'"+aut+"'")
//        }
//        case zzz:Unit => "Unit"
//        case _ => throw new RuntimeException("only String or Unit expected")
//      }
//    }
//    whatIsReturned
//  }
//
//  def levelFor1 (f : String => String) : String = {
//    val msg = "Help1"
//    val fromTheHorsesMouth = f(msg)
//    if (!msg.equals(fromTheHorsesMouth)) throw new RuntimeException("broken")
//    else msg
//  }
//
//  def levelFor3a ( f: (String, Any, Any*) => Option[Any]) : String = {
//    val msg = "Help3a"
//    val whatIsReturned = f(msg,List(),List())
//    println("aut returns "+checkReturn(whatIsReturned, msg))
//    msg
//  }
//
//  def levelFor3b ( f: (String, Any, Any*) => Option[Any]) : String = {
//    val msg = "Help3b {0}"
//    val oracle = "Help3b extra"
//    val whatIsReturned = f(msg,"extra",List())
//    println("aut returns "+checkReturn(whatIsReturned, oracle))
//    oracle
//  }
//
//  def levelFor3c ( f: (String, Any, Any*) => Option[Any]) : String = {
//    val msg = "Help3c {0} {1}"
//    val oracle = "Help3c extra more"
//    val whatIsReturned = f(msg,"extra","more")
//    println("aut returns "+checkReturn(whatIsReturned, oracle))
//    oracle
//  }
//
//  def levelFor3d ( f: (String, Any, Any*) => Option[Any]) : String = {
//    val msg = "Help3d {0} {1} {2} {3} {4}"
//    val oracle = "Help3d extra more plus 2 {4}"
//    val whatIsReturned = f(msg,"extra","more","plus",2)
//    println("aut returns "+checkReturn(whatIsReturned, oracle))
//    oracle
//  }
//
//  def levelWithThrowable ( f: (Throwable, String, Any*) => Option[Any]) : (String, String) = {
//    // SEP used to tokenize the exception output and separate it from oracle
//    val msg = "HelpThrow {0} {1} {2} {3} {4}"+SpyLogAppender.SEP
//    val numeric = 134214543
//    val oracle = "HelpThrow extra more plus 2 134,214,543"
//    val ex = "abra"
//    val excMsg = "java.lang.RuntimeException: "+ex
//    val throwable = new RuntimeException(ex)
//    val whatIsReturned = f(throwable,msg,"extra","more","plus",2,numeric)
//    println("aut returns "+checkReturn(whatIsReturned, oracle))
//    (oracle, excMsg)
//  }
//
//  //  def levelForLogstashFailure[N <% CanLogstash[FailureResult]] ( f: (FailureResult) => N => Unit ) : String = {
//  //    val ex = new RuntimeException("rexMsg")
//  //    val bad : FailureResult = FailureResult("failureMessage", Some(ex))
//  //    f(bad)(CanLogstash.FailureResultCanLogstash)
//  //    ""
//  //  }
//
//
//  //  def levelForLogstashFailure : String = {
//  //    val ex = new RuntimeException("rexMsg")
//  //    val bad : FailureResult = FailureResult("failureMessage", Some(ex))
//  //    error(bad)(CanLogstash.FailureResultCanLogstash)
//  //    ""
//  //  }
//
//  def level4LogstashFailure ( f: (FailureResult) =>  Option[Any] ,
//                              a: Option[Throwable] = None ,
//                              b: String = "failureMessageWithoutThrowable",
//                              c: String = "m="+replaceme+"^t=anon$1" ) : String = {
//    val bad : FailureResult = FailureResult(b, a)
//    val whatIsReturned = f(bad)
//    val ctx = c.replace(replaceme,b)
//    val oracle = prefix + " " + ctx
//    println("aut returns "+checkReturn(whatIsReturned, oracle))
//    oracle
//  }
//
//  def level4LogstashFailureWithThrowable ( f: (FailureResult) => Option[Any] ) : (String, String) = {
//    val exm = "rexMsgExtraordinaire"
//    val ex = new RuntimeException(exm)
//    val exMsg = "java.lang.RuntimeException: "+exm
//    val msg = "failureMessageWithThrowable"
//    val oracle = level4LogstashFailure(f,Some(ex),msg)
//    (oracle, exMsg)
//  }
//
//  def levelForLogstashCustom ( f: (Logstashable) =>  Option[Any] ,
//                               a: Option[Throwable] = None ,
//                               b: List[(String,String)] = List(("haveNoStack1","trace1"),("haveNoStack2","trace2")),
//                               c: String = "haveNoStack1=trace1^haveNoStack2=trace2"): String = {
//    val bad : CustomLogstashable = new CustomLogstashable(b,a)
//    val oracle = prefix + " " + c
//    val whatIsReturned = f(bad)
//    println("aut returns "+checkReturn(whatIsReturned, oracle))
//    oracle
//  }
//
//  def levelForLogstashCustomWithThrowable ( f: (Logstashable) =>  Option[Any] ): (String,String) = {
//    val exm = "CustomIaexMsg_Special"
//    val ex = new IllegalArgumentException(exm)
//    val exMsg = "java.lang.IllegalArgumentException: "+exm
//    val oracle = levelForLogstashCustom(f,Some(ex),
//      List(("withStack1","v1"),("withStack2","v2"),("withStack3","v3")),
//      "withStack1=v1^withStack2=v2^withStack3=v3")
//    (oracle, exMsg)
//  }
//
//  // INCOMPLETE
//
//  def levelForLogstashFoldableFailure (f : List[FailureResult] => String ) : (String,String) = {
//    val exm1 = "rexMsgForFailure"
//    val ex1 = new RuntimeException(exm1)
//    val msg1 = "aDumbFailureResultWithStackTraces"
//    val bad1 : FailureResult = FailureResult(msg1, Some(ex1))
//    val exm2 = "CustomIaexMsgForFailure"
//    val ex2 = new IllegalArgumentException(exm2)
//    val msg2 = "anotherFailureResultWithStackTraces"
//    val bad2 : FailureResult = FailureResult(msg2, Some(ex2))
//    val badder = List(bad1,bad2)
//    val whatIsReturned = Some(f(badder))
//    val c = prefix + " " + "m="+replaceme+"^t=anon$1"
//    val partialOracle1 = c.replace(replaceme,msg1)
//    val partialExcMsg1 = "java.lang.RuntimeException: "+exm1
//    val partialOracle2 = c.replace(replaceme,msg2)
//    val partialExcMsg2 = "java.lang.IllegalArgumentException: "+exm2
//    println("aut returns "+checkReturn(whatIsReturned, partialOracle1+"\n"+partialOracle2))
//    (partialOracle1+partialExcMsg1,partialOracle2+partialExcMsg2)
//  }
//
//  def levelForLogstashFoldableCustom (f : List[Logstashable] => String ) : (String,String) = {
//    val exm1 = "rexMsgForCustomLogstashable"
//    val ex1 = new RuntimeException(exm1)
//    val arg1 = List(("foo1","bar1"),("foo2","bar2"))
//    val bad1 = new CustomLogstashable(arg1,Some(ex1))
//    val exm2 = "CustomIaexMsgForLogstashable"
//    val ex2 = new IllegalArgumentException(exm2)
//    val arg2 = List(("key1","val1"),("key2","val2"),("key3","val3"))
//    val bad2 = new CustomLogstashable(arg2,Some(ex2))
//    val badder = List(bad1,bad2)
//    val whatIsReturned = Some(f(badder))
//    val partialOracle1 = prefix + " " + "foo1=bar1^foo2=bar2"
//    val partialExcMsg1 = "java.lang.RuntimeException: "+exm1
//    val partialOracle2 = prefix + " " + "key1=val1^key2=val2^key3=val3"
//    val partialExcMsg2 = "java.lang.IllegalArgumentException: "+exm2
//    println("aut returns "+checkReturn(whatIsReturned, partialOracle1+"\n"+partialOracle2))
//    (partialOracle1+partialExcMsg1,partialOracle2+partialExcMsg2)
//  }
//
//}
//
//class CustomLogstashable(cfKeyVal : List[(String, String)], ex:Option[Throwable]) extends Logstashable {
//  private def fromCfKeyVal(src : List[(String,String)]) : Seq[(String,String)] = {
//    val aux = src.map { t:(String,String) => Seq((t._1,t._2))}
//    aux.flatten
//  }
//  def getKVs: Seq[(String, String)] = fromCfKeyVal(cfKeyVal)
//  override def exceptionOption: Option[Throwable] = ex
//}
