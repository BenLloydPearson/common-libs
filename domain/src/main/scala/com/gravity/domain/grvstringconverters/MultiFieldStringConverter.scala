package com.gravity.domain.grvstringconverters

import com.gravity.utilities.grvstrings._

/** Created by IntelliJ IDEA.
  * Author: Robbie Coleman
  * Date: 8/15/13
  * Time: 2:34 PM
  */
trait MultiFieldStringConverter[T, F] extends StringConverter[T] {
  def keyValDelim: String = "_"
  def fieldDelim: String = "+"

  def compose(fields: Map[String, F]): Option[T]

  def decompose(data: T): Map[String, F]

  def parseValue(value: String): Option[F]

  def write(data: T): String = {
    val fields = decompose(data)

    if (fields.isEmpty) return emptyString

    val sb = new StringBuilder
    var pastFirst = false
    for ((key, value) <- fields) {
      if (pastFirst) sb.append(fieldDelim) else pastFirst = true
      sb.append(key).append(keyValDelim).append(value)
    }

    sb.toString()
  }

  def parse(input: String): Option[T] = {
    val fields = for {
      field <- tokenize(input, fieldDelim).toSeq
      keyVal = tokenize(field, keyValDelim)
      key <- keyVal.headOption
      valueString <- keyVal.lift(1)
      value <- parseValue(valueString)
    } yield key -> value

    if (fields.isEmpty) return None

    compose(fields.toMap)
  }
}
