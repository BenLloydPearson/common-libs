package com.gravity.domain.grvstringconverters

import com.gravity.utilities.grvstrings._

/** Created by IntelliJ IDEA.
  * Author: Robbie Coleman
  * Date: 8/13/13
  * Time: 1:58 PM
  */
abstract class PrefixedLongStringConverter[T](prefix: String, typePrefix: String) extends MultiFieldStringConverter[T, Long] {

  def longValue(data: T): Long

  def typedValue(input: Long): T

  def converterTypePrefix: String = typePrefix

  def parseValue(value: String): Option[Long] = value.tryToLong

  def compose(fields: Map[String, Long]): Option[T] = fields.get(prefix).map(typedValue)

  def decompose(data: T): Map[String, Long] = Map(prefix -> longValue(data))
}
