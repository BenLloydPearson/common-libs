package com.gravity.test.grid

import java.io._
import java.net.InetAddress

import org.apache.log4j.Level
import org.junit.runner.notification.Failure
import org.junit.runner.{Description, Result}

/*
 *    __   _         __         
 *   / /  (_)__  ___/ /__  ____
 *  / _ \/ / _ \/ _  / _ \/ _  /
 * /_//_/_/_//_/\_,_/\___/\_, / 
 *                       /___/
 */
object TestProtocol {
	case class RunSuite(runnerClass: Class[_], suiteClass: Class[_])
	case class SuiteFinished(suiteClass: Class[_])

	case class TestStarted(description: Description) extends Serializable
	case class TestFinished(description: Description) extends Serializable
	case class TestFailure(description: Failure) extends Serializable
	case class TestIgnored(description: Description) extends Serializable
	case class TestRunStarted(description: Description) extends Serializable
	case class TestRunFinished(description: Result) extends Serializable
	case class TestAssumptionFailure(description: Failure) extends Serializable
	case class TestLog(level: Level, msg: String, timestamp: Long = System.currentTimeMillis(), hostname: String = InetAddress.getLocalHost.getHostName) extends Serializable {
	  def write(writer: PrintStream): Unit = writer.println(s"[$hostname] ${level.toString} - $msg")
	}
	case object Shutdown extends Serializable
	case object Disconnect extends Serializable
}

