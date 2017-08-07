package com.gravity.utilities

import com.gravity.grvlogging._
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.mock.MockitoSugar
import org.scalatest.{BeforeAndAfter, BeforeAndAfterAll, FunSuite, Matchers}
import org.scalatest.Tag

object UnitTest extends Tag("UnitTest")

@RunWith(classOf[JUnitRunner])
abstract class BaseScalaTest extends FunSuite with Matchers with MockitoSugar with BeforeAndAfter with BeforeAndAfterAll with BeforeAndAfterListeners {
	def info(msg: String) = com.gravity.logging.Logging.info(msg)
	def info(msg: String, args: Any*) = com.gravity.logging.Logging.info(msg, args: _*)
}

