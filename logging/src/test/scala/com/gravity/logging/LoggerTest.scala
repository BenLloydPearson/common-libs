package com.gravity.logging

import java.util.concurrent.atomic.AtomicInteger

import org.apache.log4j.spi.LoggingEvent
import org.apache.log4j._
import org.scalatest.FunSuite

import scala.collection._
import scala.collection.JavaConverters._
import com.gravity.grvlogging._
import com.gravity.grvlogging.{info => logInfo}
/*
 *    __   _         __         
 *   / /  (_)__  ___/ /__  ____
 *  / _ \/ / _ \/ _  / _ \/ _  /
 * /_//_/_/_//_/\_,_/\___/\_, / 
 *                       /___/
 */

protected[logging] trait TestTrait {
	def log = info("this is a log message")
}

object TestFoo {
	def log = info("this is a log message")
	object TestFooNested {
		def log = info("this is a log message")
		class TestFooNested2 extends TestTrait {

		}
	}
}

class LoggerTest extends FunSuite {

	case class LogCapture(events: mutable.ListBuffer[LoggingEvent] = mutable.ListBuffer.empty) {
		def getByLevel(l: Level) = events.filter(_.getLevel == l)
		def countByLevel(l: Level) = getByLevel(l).size
	}

	def capture(level: String)(work: => Unit): LogCapture = {
		val root = Logger.getRootLogger
		val existinglevel = root.getLevel
		val appenders = root.getAllAppenders
		val counter = new AtomicInteger()
		val captured = LogCapture()

		try {
			root.removeAllAppenders()
			root.setLevel(Level.toLevel(level))
			root.addAppender(new AppenderSkeleton() {
				setThreshold(Level.toLevel(level))
				override def append(loggingEvent: LoggingEvent): Unit = {
					println("[" + loggingEvent.getLevel + "] " + loggingEvent.getLogger.getName + " - " + loggingEvent.getMessage)
					captured.events += loggingEvent
				}
				override def requiresLayout(): Boolean = false
				override def close(): Unit = {}
			})
			work
			captured
		} finally {
			root.setLevel(existinglevel)
			appenders.asScala.foreach(a => root.addAppender(a.asInstanceOf[Appender]))
		}
	}
//
//	test("log levels") {
//		Seq("ERROR" -> 1, "WARN" -> 2, "INFO" -> 3, "DEBUG" -> 4, "TRACE" -> 5, "ALL" -> 5).foreach{ case (level, count) => {
//			val counter = new AtomicInteger()
//			capture(level) {
//				error(counter.incrementAndGet().toString)
//				warn(counter.incrementAndGet().toString)
//				logInfo(counter.incrementAndGet().toString)
//				debug(counter.incrementAndGet().toString)
//				trace(counter.incrementAndGet().toString)
//			}
//			assertResult(count)(counter.get())
//		}}
//	}

	test("lazy evaluation") {
		val counter = new AtomicInteger()
		val logs = capture("INFO") {
			error(counter.incrementAndGet().toString)
			logInfo(counter.incrementAndGet().toString)
			debug(counter.incrementAndGet().toString)
			trace(counter.incrementAndGet().toString)
		}

		assertResult(2)(counter.get())
		assertResult(1)(logs.countByLevel(Level.ERROR))
		assertResult(1)(logs.countByLevel(Level.INFO))
		assertResult(0)(logs.countByLevel(Level.DEBUG))
		assertResult(0)(logs.countByLevel(Level.TRACE))
	}


	test("class name") {
		val counter = new AtomicInteger()
		val logs = capture("INFO") {
			error(counter.incrementAndGet().toString)
			logInfo(counter.incrementAndGet().toString)
			debug(counter.incrementAndGet().toString)
			trace(counter.incrementAndGet().toString)
		}

		logs.events.foreach(l => println(l.getLogger.getName))
		assertResult(true)(logs.events.forall(_.getLogger.getName == "com.gravity.logging.LoggerTest"))

		assertResult("com.gravity.logging.TestFoo")(capture("INFO") {
			TestFoo.log
		}.events.head.getLogger.getName)

		assertResult("com.gravity.logging.TestFoo.TestFooNested")(capture("INFO") {
			TestFoo.TestFooNested.log
		}.events.head.getLogger.getName)

		assertResult("com.gravity.logging.TestTrait")(capture("INFO") {
			new TestFoo.TestFooNested.TestFooNested2().log
		}.events.head.getLogger.getName)

	}

}
