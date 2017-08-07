package com.gravity.test.grid

import java.lang.reflect.Modifier
import java.util

import com.gravity.logging.Logging._
import com.gravity.test.grid.TestProtocol._
import com.gravity.utilities.grvgrid.{GridConfig, GridExecutor}
import org.junit.runner.notification.RunNotifier
import org.junit.runner.{Description, RunWith, Runner}
import org.junit.runners.ParentRunner
import org.junit.runners.model.Statement
import org.reflections.Reflections
import org.reflections.scanners.{MethodAnnotationsScanner, SubTypesScanner, TypeAnnotationsScanner}
import org.reflections.util.{ClasspathHelper, ConfigurationBuilder}

import scala.collection.JavaConversions._
import scala.collection.{Seq, mutable}
import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.util.Try
/*
 *    __   _         __
 *   / /  (_)__  ___/ /__  ____
 *  / _ \/ / _ \/ _  / _ \/ _  /
 * /_//_/_/_//_/\_,_/\___/\_, /
 *                       /___/
 */
@RunWith(classOf[GridTestRunner])
class GridTestAll {

}

class GridTestRunner(clazz: Class[_]) extends ParentRunner[Runner](clazz) {

	val gridExecutor: GridExecutor = GridExecutor(GridConfig.jenkinsConfig.withStdOutEcho(false).withStdErrEcho(false))
	val finished = mutable.ListBuffer[TestFinished]()
	val failed = mutable.ListBuffer[TestFailure]()
	val ignored = mutable.ListBuffer[TestIgnored]()
	lazy val totalTestCount = testCount()

	override def describeChild(child: Runner): Description = child.getDescription

	override def childrenInvoker(notifier: RunNotifier): Statement = new Statement {
		override def evaluate(): Unit = {
			implicit val ec = ExecutionContext.fromExecutorService(gridExecutor)

			val totalTestCount = testCount

			val testRuns = getChildren.map(child => {
				val runnerClass = child.getClass
				val suiteClass = child.getDescription.getTestClass

				Future {
					val runner = runnerClass.getConstructor(classOf[Class[_]]).newInstance(suiteClass)
					val runNotifier = new EventCaptureRunNotifier(suiteClass)

					// run the test suite
					runner.run(runNotifier)

					// return recorded events
					runNotifier.testRunEvents.toArray
				}.andThen(recordTestResult(notifier))
			})

			Await.result(Future.sequence(testRuns), 60 minutes)
			ec.shutdown()
		}
	}

	override def runChild(child: Runner, notifier: RunNotifier): Unit = ???

	override def getChildren: util.List[Runner] = {

		if (System.getProperty("grid.test") == "true") {
			// Build a classpath scanner
			val reflections = new Reflections(
				new ConfigurationBuilder()
					.setUrls(ClasspathHelper.forPackage("com.gravity") ++
								 ClasspathHelper.forPackage("org.scalatest") ++
								 ClasspathHelper.forPackage("org.junit"))
					.setScanners(new MethodAnnotationsScanner, new SubTypesScanner(false), new TypeAnnotationsScanner)
			)

			def scan[T](f: Reflections => List[Class[_ <: T]]): Seq[Class[_ <: T]] = {
				f(reflections).filter(cls => cls.getName.startsWith("com.gravity") && !Modifier.isAbstract(cls.getModifiers))
			}

			info("Scanning for tests to run...")
			val scalatestTests = scan[org.scalatest.Suite](_.getSubTypesOf(classOf[org.scalatest.Suite]).toList).flatMap(cls => Try(new org.scalatest.junit.JUnitRunner(cls)).toOption)
			val junitTests = scan[Any](_.getMethodsAnnotatedWith(classOf[org.junit.Test]).map(_.getDeclaringClass).toSet.toList).flatMap(cls => {
				if (cls.getAnnotation(classOf[org.junit.Ignore]) != null) {
					None
				} else {
					Some(new org.junit.runners.JUnit4(cls))
				}
			})
			val junitRunWithTests = scan[Any](_.getTypesAnnotatedWith(classOf[org.junit.runner.RunWith]).map(_.getAnnotation(classOf[org.junit.runner.RunWith]).value()).toSet.toList).filterNot(_ == classOf[GridTestRunner]).map(cls => cls.getConstructor(classOf[Class[_]]).newInstance(cls).asInstanceOf[Runner])

			val tests = (scalatestTests ++ junitTests ++ junitRunWithTests).filter(t => t.getDescription.getChildren.nonEmpty && t.getDescription.getDisplayName.endsWith("Test"))
			tests
		} else {
			// no tests
			new util.ArrayList[Runner]()
		}
	}

	def recordTestResult(notifier: RunNotifier): PartialFunction[Try[Array[Any]], Unit] = {
		case scala.util.Success(events) => {
			events.foreach {
				case TestRunStarted(description) => {
					notifier.fireTestRunStarted(description)
				}
				case TestRunFinished(description) => notifier.fireTestRunFinished(description)
				case e@TestStarted(description) => {
					notifier.fireTestStarted(description)
				}
				case e@TestFinished(description) => {
					finished += e
					notifier.fireTestFinished(description)
					printStatus()
				}
				case e@TestAssumptionFailure(failure) => {
					notifier.fireTestAssumptionFailed(failure)
				}
				case e@TestFailure(failure) => {
					failed += e
					notifier.fireTestFailure(failure)
				}
				case e@TestIgnored(description) => {
					ignored += e
					notifier.fireTestIgnored(description)
				}
				case log: TestLog => log.write(System.out)
				case event => warn("Unhandled test event: " + event.getClass)
			}
		}
		case scala.util.Failure(ex) => throw ex
	}

	def printStatus() = {
		info(s"Completed ${finished.size} of $totalTestCount tests (failed: ${failed.size}, ignored: ${ignored.size})")
	}

}

