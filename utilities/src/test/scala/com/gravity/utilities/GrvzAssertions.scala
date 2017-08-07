package com.gravity.utilities

import org.scalatest.Matchers

import scalaz.Equal

/**
 * Created by runger on 3/14/14.
 */
trait GrvzAssertions {
  this: Matchers =>

  implicit def any2ShouldScalazEquals[T](t: T)(implicit eq: Equal[T]): ShouldScalazEquals[T] = new ShouldScalazEquals(t, this)
}
