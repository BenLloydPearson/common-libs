package com.gravity.utilities

import org.scalatest.Matchers

import scalaz.Equal

/**
 * Typesafe equals for ScalaTest.
 */
class ShouldScalazEquals[T](t: T, shouldMatchers: Matchers)(implicit eq: Equal[T]) {
  import shouldMatchers._

  def shouldScalazEqual(otherT: T) {
    withClue(t + " did not equal " + otherT) {
      eq.equal(t, otherT) should be (true)
    }
  }

}
