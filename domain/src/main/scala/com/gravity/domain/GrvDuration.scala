package com.gravity.domain

import com.gravity.utilities.grvstrings._
import com.gravity.utilities.swagger.adapter.{DefaultValueWriter, ParameterType}
import com.gravity.utilities.web.{ApiParam, ApiRequest, ValidationCategory}
import org.joda.time.DateTime
import play.api.libs.json._
import scala.collection._
import com.gravity.utilities.components.FailureResult
import com.gravity.utilities.grvz._

import scalaz.{Failure, Success, ValidationNel, Validation, NonEmptyList}
import scalaz.syntax.validation._
import scalaz.std.option._
import scalaz.syntax.apply._

/**
 * Created by robbie on 06/15/2015.
 *            _ 
 *          /  \
 *         / ..|\
 *        (_\  |_)
 *        /  \@'
 *       /     \
 *   _  /  \   |
 *  \\/  \  | _\
 *   \   /_ || \\_
 *    \____)|_) \_)
 *
 */
case class GrvDuration(weeks: Int, days: Int, hours: Int) {
  require(weeks > -1, "weeks must be positive!")
  require(days > -1, "days must be positive!")
  require(hours > -1, "hours must be positive!")

  def isEmpty: Boolean = weeks == 0 && days == 0 && hours == 0

  def nonEmpty: Boolean = !isEmpty

  def fromTime(from: DateTime): DateTime = {
    if (isEmpty) return from

    val mutableDateTime = from.toMutableDateTime

    if (weeks > 0)
      mutableDateTime.addWeeks(weeks)

    if (days > 0)
      mutableDateTime.addDays(days)

    if (hours > 0)
      mutableDateTime.addHours(hours)

    mutableDateTime.toDateTime
  }

  override def toString: String = {
    if (isEmpty) return emptyString

    val sb = new StringBuilder

    if (weeks > 0) {
      sb.append(weeks).append(WeekDurationCode.code)
    }

    if (days > 0) {
      if (sb.nonEmpty) sb.append(GrvDuration.partDelimiter)
      sb.append(days).append(DayDurationCode.code)
    }

    if (hours > 0) {
      if (sb.nonEmpty) sb.append(GrvDuration.partDelimiter)
      sb.append(hours).append(HourDurationCode.code)
    }

    sb.toString()
  }
}

object GrvDuration {
  val partDelimiter: String = "|"

  val empty: GrvDuration = GrvDuration(0, 0, 0)

  def parse(input: String): ValidationNel[FailureResult, GrvDuration] = {
    if (input.length < 2) return FailureResult(s"input must contain at least one pairing of number to duration code {w, d, h}. Input received: '$input'").failureNel

    val parts = input.splitBetter(partDelimiter)

    val partPairs = parts.map {
      case part: String => part.splitAt(part.length - 1)
    }

    val partDurations = partPairs.map {
      case (value: String, code: String) => value.tryToInt tuple DurationCode.parse(code) match {
        case Some((intVal, _)) if intVal < 0 => return FailureResult(s"Durations must be positive (greater than -1): (value: $value, code: $code) found in input: '$input'").failureNel
        case Some((intVal, dc)) => dc -> intVal
        case None => return FailureResult(s"Invalid duration pairing: (value: $value, code: $code) found in input: '$input'").failureNel
      }
    }

    val durationMap = mutable.Map[DurationCode, Int]()

    partDurations.foreach {
      case (code: DurationCode, value: Int) => durationMap.get(code) match {
        case Some(duplicate) => return FailureResult(s"Duplicate duration code '${code.code}' (${code.name}) was present in input: '$input'").failureNel
        case None => durationMap += code -> value
      }
    }

    GrvDuration(
      durationMap.getOrElse(WeekDurationCode, 0),
      durationMap.getOrElse(DayDurationCode, 0),
      durationMap.getOrElse(HourDurationCode, 0)
    ).successNel
  }

  implicit val jsonFormat: Format[GrvDuration] = Format(Reads[GrvDuration] {
    case JsString(input) => parse(input).toJsResult
    case x => JsError(s"Could not convert $x to GrvDuration")
  }, Writes[GrvDuration](d => JsString(d.toString)))

  implicit val defaultValueWriter: DefaultValueWriter[GrvDuration] = new DefaultValueWriter[GrvDuration] {
    override def serialize(t: GrvDuration): String = t.toString
  }
}

sealed trait DurationCode {
  def code: Char
  def name: String
  def asSome: Option[DurationCode]
  override def toString: String = s"$name:$code"
}

case object WeekDurationCode extends DurationCode {
  val code: Char = 'w'
  val name: String = "Week"
  val asSome: Option[DurationCode] = Some(this)
}

case object DayDurationCode extends DurationCode {
  val code: Char = 'd'
  val name: String = "Day"
  val asSome: Option[DurationCode] = Some(this)
}

case object HourDurationCode extends DurationCode {
  val code: Char = 'h'
  val name: String = "Hour"
  val asSome: Option[DurationCode] = Some(this)
}

object DurationCode {
  def parse(input: String): Option[DurationCode] = {
    input.headOption.flatMap {
      case c: Char => c match {
        case wc if wc == WeekDurationCode.code => WeekDurationCode.asSome
        case dc if dc == DayDurationCode.code => DayDurationCode.asSome
        case hc if hc == HourDurationCode.code => HourDurationCode.asSome
        case _ => None
      }
    }
  }
}

case class ApiDurationParam(key: String, required: Boolean, description: String,
                            defaultValue: GrvDuration = GrvDuration.empty, exampleValue: String = "1w|2d|3h")
  extends ApiParam[GrvDuration](key, required, description, defaultValue, exampleValue,
                                typeDescription = "A duration of time defined as a string") {

  override val doFormatValidation: Boolean = false

  override def apply(implicit request: ApiRequest): Option[GrvDuration] = {
    request.get(key).flatMap(input => GrvDuration.parse(input).toOption)
  }

  /**
   * @return If validation success, None.
   *         If validation fails, ("error message", ValidationCategory).
   */
  override def validate(request: ApiRequest, paramTypeDisplayName: String): Option[(String, ValidationCategory)] = {
    super.validate(request, paramTypeDisplayName) match {
      case errorResult @ Some(_) => errorResult

      // Passed prelim validations
      case _ =>
        request.get(key).flatMap {
          case input: String =>
            // if not required, then emptyString is valid
            if (!required && input.isEmpty) return None

            GrvDuration.parse(input) match {
              case Success(_) => None
              case Failure(fails) => Some(fails.list.map(_.messageWithExceptionInfo).mkString(" AND ") -> ValidationCategory.ParseOther)
            }
        }
    }
  }

  override def swaggerType: ParameterType = ParameterType.string
}
