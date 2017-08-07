package com.gravity.utilities

/**
 * Created by IntelliJ IDEA.
 * User: robbie
 * Date: 3/22/11
 * Time: 5:00 PM
 */

import com.gravity.utilities.ScalaMagic._
import com.gravity.utilities.components.FailureResult
import org.junit.Assert._
import org.junit.Test

import scala.collection.mutable.ListBuffer
import com.gravity.utilities.components.FailureResult
import com.gravity.utilities.grvz._

import scalaz.{Failure, Success, ValidationNel, Validation, NonEmptyList}
import scalaz.syntax.validation._
import scalaz.std.option._
import scalaz.syntax.apply._

class ScalaMagicTest {
  @Test def testIsNullOrEmpty(): Unit = {
    val nullString: String = null
    assertTrue(isNullOrEmpty(nullString))

    assertTrue(isNullOrEmpty(grvstrings.emptyString))

    val myString = "This is not an empty string."
    assertFalse(isNullOrEmpty(myString))

    val emptyArray: Array[Int] = new Array[Int](0)
    assertTrue(isNullOrEmpty(emptyArray))

    val myArray: Array[Int] = new Array[Int](1)
    myArray(0) = 1
    assertFalse(isNullOrEmpty(myArray))

    val emptyList: List[Int] = Nil
    assertTrue(isNullOrEmpty(emptyList))

    val myList: ListBuffer[Int] = new ListBuffer[Int]
    assertTrue(isNullOrEmpty(myList))

    myList += 1
    assertFalse(isNullOrEmpty(myList))

    val zeroInt: Int = 0
    assertTrue(isNullOrEmpty(zeroInt))

    val nonZeroInt: Int = 10
    assertFalse(isNullOrEmpty(nonZeroInt))

    val zeroLong: Long = 0
    assertTrue(isNullOrEmpty(zeroLong))

    val nonZeroLong: Long = 10
    assertFalse(isNullOrEmpty(nonZeroLong))
  }

  @Test def testRetryOnException(): Unit = {
    var attempts = 0
    def funcThatThrowsException: String = {
      attempts += 1
      throw new Exception("Here I am throwing a useless exception on attempt #" + attempts)
    }

    retryOnException(5, 125)(funcThatThrowsException) {
      case ex: Exception =>
        if (attempts == 4) {
          throw new Exception("Here I am throwing an exception within my 'onException' function.")
        }
        else {
          FailureResult("This should be failure #" + attempts, ex)
        }
    } match {
      case Success(wtf) => fail(s"We expected a failure but impossibly got a success with the string: '$wtf'")
      case Failure(fails) =>
        assertEquals("There should have been 5 FailureResults!", 5, fails.size)
        println("Failed as expected:")
        println(formatFailures(fails))
    }
  }

  @Test def testRetryOnFailure(): Unit = {
    var attempts = 0
    def funcThatFails: ValidationNel[FailureResult, String] = {
      attempts += 1
      FailureResult("Here I am returning a useless failure on attempt #" + attempts).failureNel
    }

    retryOnFailure(5, 125)(funcThatFails) {
      case msg: String => FailureResult(msg)
    } match {
      case Success(wtf) => fail("We expected a failure but impossibly got a success with the string: '$wtf'")
      case Failure(fails) =>
        assertEquals("There should have been 5 FailureResults!", 5, fails.size)
        println("Failed as expected:")
        println(formatFailures(fails))
    }
  }

  @Test def testFormatException(): Unit = {
    val ex = new Exception("Just a test exception to test ScalaMagic.formatException")
    val expectedStart = "Exception of type java.lang.Exception was thrown.\nMessage: Just a test exception to test ScalaMagic.formatException"
    val actualString = ScalaMagic.formatException(ex)
    val actualStart = actualString.substring(0, expectedStart.length)

    assertEquals("Formatted exception did is not as expected!", expectedStart, actualStart)
  }
}