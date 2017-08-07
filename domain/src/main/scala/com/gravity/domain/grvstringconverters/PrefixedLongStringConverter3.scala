package com.gravity.domain.grvstringconverters

import com.gravity.utilities.grvstrings._

/** Created by IntelliJ IDEA.
  * Author: Robbie Coleman
  * Date: 8/13/13
  * Time: 1:58 PM
  */
abstract class PrefixedLongStringConverter3[T](prefix1: String, prefix2: String, prefix3: String, typePrefix: String) extends MultiFieldStringConverter[T, Long] {

  def converterTypePrefix: String = typePrefix

  def longValues(data: T): (Long, Long, Long)

  def typedValue(input1: Long, input2: Long, input3: Long): T

  def parseValue(value: String): Option[Long] = value.tryToLong

  def compose(fields: Map[String, Long]): Option[T] = for {
    value1 <- fields.get(prefix1)
    value2 <- fields.get(prefix2)
    value3 <- fields.get(prefix3)
  } yield typedValue(value1, value2, value3)

  def decompose(data: T): Map[String, Long] = longValues(data) match {
    case (value1, value2, value3) => Map(prefix1 -> value1, prefix2 -> value2, prefix3 -> value3)
  }
}
