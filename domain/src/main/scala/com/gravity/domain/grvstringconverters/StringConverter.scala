package com.gravity.domain.grvstringconverters

import com.gravity.domain.grvstringconverters
import com.gravity.utilities.components.FailureResult
import com.gravity.utilities.grvstrings._
import com.gravity.utilities.grvz._

import scala.collection._
import scalaz.{Failure, Success, Validation}

/**
 * Created by IntelliJ IDEA.
 * Author: Robbie Coleman
 * Date: 8/12/13
 * Time: 3:09 PM
 */
trait StringConverter[T] {

  type valueType = T

  def converterTypePrefix: String
  def write(data: T): String
  def parse(input: String): Option[T]

  def writeString(data: T): String = s"$converterTypePrefix${StringConverter.typePrefixSeparator}${write(data)}"
}

object StringConverter {
 import com.gravity.logging.Logging._
  import com.gravity.utilities.Counters._

  val counterCategory = "StringConverter"

  val typePrefixSeparator = "_._"

  private val prefixToConverterMap = mutable.HashMap[String, StringConverter[_]]()

  def registerConverter(converter: StringConverter[_]) {
    prefixToConverterMap.put(converter.converterTypePrefix, converter).foreach {
      case oldConverter =>
        throw new Exception(s"Tried to register string converter $converter to prefix ${converter.converterTypePrefix}, but $oldConverter is already using that prefix.")
    }
  }

  def validateString(input: String): Validation[FailureResult, Any] = {
    /** DO NOT DELETE. This helps ensure all serializers are registered. */
    grvstringconverters.ensureSerializersRegistered()

    for {
      prefixTokenIndex <- input.findIndex(typePrefixSeparator).toValidation(FailureResult(s"input string `$input` did not contain the typePrefixSeparator `$typePrefixSeparator`!"))
      prefix <- nullOrEmptyToNone(input.substring(0, prefixTokenIndex).trim).toValidation(FailureResult(s"No typePrefix found in input string: $input"))
      converter <- prefixToConverterMap.get(prefix).toValidation(FailureResult(s"No StringConverter implementation was registered for the specified typePrefix `$prefix` and input `$input`!"))
      remainingInput <- nullOrEmptyToNone(input.substring(prefixTokenIndex + typePrefixSeparator.length).trim).toValidation(FailureResult(s"No type data found in input `$input` after the type prefix!"))
      result <- converter.parse(remainingInput).toValidation(FailureResult(s"StringConverter of type `${converter.getClass.getCanonicalName}` failed to parse input string: $input"))
    } yield result
  }

  def parseRaw(input: String): Option[Any] = validateString(input).toOption

  def parse(input: String): Any = validateString(input) match {
    case Success(output) =>
      countPerSecond(counterCategory, "Successfully parsed strings")
      output
    case Failure(failed) =>
      countPerSecond(counterCategory, "Failed parse attempts")
      warn(failed)
      throw new StringConverterParsingException(input, failed)
  }

  def parseOrDefault(input: String, default: Any): Any = validateString(input) match {
    case Success(output) =>
      countPerSecond(counterCategory, "Successfully parsed strings")
      output
    case Failure(failed) =>
      countPerSecond(counterCategory, "Failed parse attempts")
      warn(failed)
      default
  }

  def parseAs[T : Manifest](input: String): Option[T] = parseRaw(input) match {
    case Some(t: T) => Some(t)
    case Some(_) => None
    case None => None
  }

  def writeString[T](data: T)(implicit c: StringConverter[T]): String = c.writeString(data)
}
