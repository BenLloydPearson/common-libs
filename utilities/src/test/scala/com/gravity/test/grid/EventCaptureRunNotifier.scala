package com.gravity.test.grid

import java.io._
import java.net.InetAddress

import com.gravity.logging.Logging._
import com.gravity.test.grid.TestProtocol._
import com.gravity.utilities.{GrvSimpleFormatter, Settings}
import org.apache.log4j.{AppenderSkeleton, Level, Logger}
import org.apache.log4j.spi.LoggingEvent
import org.junit.runner.{Description, Result}
import org.junit.runner.notification.{Failure, RunNotifier}

import scala.collection._
import scala.util.Try
import scalaz.std.effect.outputStream

/*
 *    __   _         __         
 *   / /  (_)__  ___/ /__  ____
 *  / _ \/ / _ \/ _  / _ \/ _  /
 * /_//_/_/_//_/\_,_/\___/\_, / 
 *                       /___/
 */
class EventCaptureRunNotifier(suiteClass: Class[_]) extends RunNotifier with Serializable {

	import EventCaptureRunNotifier._

	// initialize logging framework
	val logger = Settings.APPLICATION_ROLE


	val hostname = InetAddress.getLocalHost.getHostName
	var currentTest: Option[EventLog] = None
	val testRunEvents = mutable.ListBuffer[Any]()

	val appender = new AppenderSkeleton {
		override def append(loggingEvent: LoggingEvent): Unit = currentTest.foreach(_.write(TestLog(loggingEvent.getLevel, loggingEvent.getRenderedMessage, loggingEvent.timeStamp, hostname)))
		override def requiresLayout(): Boolean = false
		override def close(): Unit = {
			currentTest.foreach(_.close)
			Logger.getRootLogger.removeAppender(this)
		}
	}

	Logger.getRootLogger.addAppender(appender)

	private def printBanner(name: String, description: Description) = {
		currentTest.foreach(t => {
			t.write(TestLog(Level.INFO, s"------------------------------------------------------------------------------"))
			t.write(TestLog(Level.INFO, s"  ${name.toUpperCase} FOR TEST: ${description.getDisplayName}"))
			t.write(TestLog(Level.INFO, s"------------------------------------------------------------------------------"))
		})
	}

	override def fireTestRunStarted(description: Description): Unit = currentTest.foreach(_.write(TestRunStarted(description)))
	override def fireTestRunFinished(result: Result): Unit = currentTest.foreach(_.write(TestRunFinished(result)))

	override def fireTestStarted(description: Description): Unit = {
		testRunEvents += TestStarted(description)
		currentTest = Some(EventLog(suiteClass, description.getMethodName))
		printBanner("START LOG", description)
	}

	override def fireTestFinished(description: Description): Unit = {
		currentTest.foreach(_.close)
		testRunEvents += TestFinished(description)
		currentTest = None
	}

	override def fireTestAssumptionFailed(failure: Failure): Unit = {
		currentTest.foreach(t => {
			t.write(TestLog(Level.ERROR, s"\nTest Failed: ${failure.getTestHeader}\nFailure Message:\t${failure.getMessage}\nException:\t${failure.getTrace}"))
		})
		printBanner("END LOG", failure.getDescription)
		currentTest.foreach(_.close)
		testRunEvents ++= currentTest.toSeq.flatMap(_.events)
		testRunEvents += TestAssumptionFailure(failure)
	}

	override def fireTestFailure(failure: Failure): Unit = {
		currentTest.foreach(t => {
			t.write(TestLog(Level.ERROR, s"\nTest Failed: ${failure.getTestHeader}\nFailure Message:\t${failure.getMessage}\nException:\t${failure.getTrace}"))
		})
		printBanner("END LOG", failure.getDescription)
		currentTest.foreach(_.close)
		testRunEvents ++= currentTest.toSeq.flatMap(_.events)
		testRunEvents += TestFailure(failure)
	}

	override def fireTestIgnored(description: Description): Unit = {
		testRunEvents += TestIgnored(description)
	}
}

object EventCaptureRunNotifier {
	protected object EOF extends Serializable

	case class EventLog(suiteClass: Class[_], testName: String) {
		private lazy val file = File.createTempFile(suiteClass.getName + "." + testName, ".eventLog")
		private lazy val outputStream: ObjectOutputStream = new ObjectOutputStream(new FileOutputStream(file))
		private var closed = false

		def write(event: Any) = outputStream.writeObject(event)

		def close = synchronized {
			if (!closed) {
				outputStream.writeObject(EOF)
				outputStream.flush()
				outputStream.close()
				closed = true
			}
		}

		def flush = outputStream.flush()
		def events: Iterable[Any] = {
			val input = new ObjectInputStream(new FileInputStream(file))
			Stream.continually(Try(input.readObject()).toOption.getOrElse(EOF)).takeWhile(_ != EOF)
		}
	}

}