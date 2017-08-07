package com.gravity.interests.jobs.intelligence

import com.gravity.hbase.schema.{ComplexByteConverter, PrimitiveInputStream, PrimitiveOutputStream}
import com.gravity.interests.jobs.intelligence.SchemaTypes._
import com.gravity.interests.jobs.intelligence.schemas.byteconverters.UnsupportedVersionExcepion
import com.gravity.utilities.components.FailureResult
import com.gravity.utilities.grvenum.GrvEnum
import com.gravity.utilities.grvenum.GrvEnum._
import com.gravity.utilities.{ScalaMagic, grvstrings, grvtime}
import com.gravity.utilities.grvtime._
import play.api.libs.functional.syntax._
import play.api.libs.json._

import scala.reflect.Manifest
import scalaz.Validation

/*

         ▒▒
         ██
       ██████
     ██████████
     ██░░██░░██
     ██████████
       ██████
       ░░░░░░      It's gonna be fun on the bun!
       ██████

*/

object ExchangeThrottleTypes extends GrvEnum[Short] {

  case class Type(i: Short, n: String) extends ValueTypeBase(i, n)

  override def mkValue(id: Short, name: String): Type = Type(id, name)

  //this default value will throw an exception if you try and read or write it to HBase
  def defaultValue: Type = UNKNOWN_DO_NOT_USE

  val UNKNOWN_DO_NOT_USE: Type = Value(0, "unknown_do_not_use")
  val NO_THROTTLE: Type = Value(1, "no_throttle")
  val MONTHLY_MAX_RECEIVE_SEND_RATIO: Type = Value(2, "monthly_max_receive_send_ratio")
  val MONTHLY_SEND_RECEIVE_BUFFER: Type = Value(3, "monthly_send_receive_buffer")
  //When adding more types above, make sure to add them to ExchangeThrottleByteConverter and JsValueToExchangeThrottleValidations

  implicit val byteConverter: ComplexByteConverter[Type] = shortEnumByteConverter(this)

  implicit val jsonFormat: Format[Type] = makeJsonFormat[Type]
}

//These converters are those related to the base ExchangeThrottle so it's easier to see where to add ExchangeThrottleTypes enumerations,
//other converters belong in ExchangeByteConverters
object ExchangeThrottleConverters {

  implicit object ExchangeThrottleByteConverter extends ComplexByteConverter[ExchangeThrottle] {
    val version = 1 //only needs to be bumped for breaking read/write changes

    def write(data: ExchangeThrottle, output: PrimitiveOutputStream) {
      output.writeShort(version)
      output.writeObj(data.throttleType)
      data.writeToOutput(output)
    }

    def read(input: PrimitiveInputStream): ExchangeThrottle = {
      val readVersion = input.readShort() //For the future

      if (readVersion != version)
        throw new RuntimeException(UnsupportedVersionExcepion.buildMessage("ExchangeThrottle", version, version, version))


      val throttleType = input.readObj[ExchangeThrottleTypes.Type]

      throttleType match {
        case ExchangeThrottleTypes.UNKNOWN_DO_NOT_USE => throw new RuntimeException("Invalid throttle type " + throttleType)
        case ExchangeThrottleTypes.NO_THROTTLE => new NoExchangeThrottle()
        case ExchangeThrottleTypes.MONTHLY_MAX_RECEIVE_SEND_RATIO => input.readObj[MonthlyMaxReceiveSendRatioThrottle]
        case ExchangeThrottleTypes.MONTHLY_SEND_RECEIVE_BUFFER => input.readObj[MonthlySendReceiveBufferThrottle]
        case unknown => throw new RuntimeException("Unable to parse throttle type " + unknown)
      }
    }
  }

  implicit val fmtExchangeThrottle = Format(Reads[ExchangeThrottle] {
    json => (json \ "throttleType").validate[ExchangeThrottleTypes.Type].flatMap(json.validateThrottleConfigFromType)
  }, Writes[ExchangeThrottle] {
    case throttle: NoExchangeThrottle => asThrottleJsObject(throttle)
    case throttle: MonthlyMaxReceiveSendRatioThrottle => asThrottleJsObject(throttle)
    case throttle: MonthlySendReceiveBufferThrottle => asThrottleJsObject(throttle)
  })

  def asThrottleJsObject[T <: ExchangeThrottle](throttle: T)(implicit tjs: Writes[T]): JsValue = { Json.obj("throttleType" -> Json.toJson(throttle.throttleType)) ++ Json.toJson(throttle)(tjs).as[JsObject] }

  implicit class JsValueToExchangeThrottleValidations(val underlying: JsValue) extends AnyVal {
    def validateThrottleConfigFromType(throttle_type: ExchangeThrottleTypes.Type): JsResult[ExchangeThrottle] = throttle_type match {
      case ExchangeThrottleTypes.UNKNOWN_DO_NOT_USE => JsError(s"throttle_type `${throttle_type.toString}` should not be used")
      case ExchangeThrottleTypes.NO_THROTTLE => underlying match { case _ : JsUndefined => JsSuccess(new NoExchangeThrottle()); case other => other.validate[NoExchangeThrottle] }
      case ExchangeThrottleTypes.MONTHLY_MAX_RECEIVE_SEND_RATIO => underlying.validate[MonthlyMaxReceiveSendRatioThrottle]
      case ExchangeThrottleTypes.MONTHLY_SEND_RECEIVE_BUFFER => underlying.validate[MonthlySendReceiveBufferThrottle]
      case _ => JsError(s"Internal Error. Unable to validate throttle_type `${throttle_type.toString}`")
    }
  }

}

//sealed to require pattern matching consider all subclasses
sealed abstract class ExchangeThrottle {
  val throttleType: ExchangeThrottleTypes.Type
  def writeToOutput(output: PrimitiveOutputStream): Unit
  def throttledSitesFromMetrics(metrics: Seq[ExchangeMetric]): Set[SiteKey]
  def throttleParamsStr: String

  override def toString = {
    val subclassParams = throttleParamsStr
    val delimiter = if (subclassParams.isEmpty) "" else ", "
    "throttleType: " + throttleType.toString + delimiter + subclassParams
  }

  def typedThrottle[T <: ExchangeThrottle]: T = this.asInstanceOf[T]
  def tryToTypedThrottle[T <: ExchangeThrottle : Manifest] : Validation[FailureResult, T] = ScalaMagic.tryCast[T](this)
}

class NoExchangeThrottle() extends ExchangeThrottle {
  val throttleType = ExchangeThrottleTypes.NO_THROTTLE

  def writeToOutput(output: PrimitiveOutputStream) = output.writeObj(this)

  def throttledSitesFromMetrics(metrics: Seq[ExchangeMetric]): Set[SiteKey] = Set()

  def throttleParamsStr: String = grvstrings.argNamesAndValues

  //any two NoExchangeThrottle are equal, needed for test
  override def equals(that: Any): Boolean = that.isInstanceOf[NoExchangeThrottle]
}
object NoExchangeThrottle {
  implicit val fmtNoExchangeThrottle: Format[NoExchangeThrottle] = Format(Reads[NoExchangeThrottle] {
    case JsObject(Seq(("throttleType", throttleType))) => throttleType.validate[ExchangeThrottleTypes.Type].withFilter(_ == ExchangeThrottleTypes.NO_THROTTLE).map(_ => new NoExchangeThrottle())
    case x => JsError(s"Could not convert $x to ${ExchangeThrottleTypes.NO_THROTTLE.toString}")
  }, Writes[NoExchangeThrottle](throttle => Json.obj("throttleType" -> throttle.throttleType)))
}

case class MonthlyMaxReceiveSendRatioThrottle(ratio: Double) extends ExchangeThrottle {
  val throttleType = ExchangeThrottleTypes.MONTHLY_MAX_RECEIVE_SEND_RATIO

  def writeToOutput(output: PrimitiveOutputStream) = output.writeObj(this)

  def throttledSitesFromMetrics(metrics: Seq[ExchangeMetric]): Set[SiteKey] = {
    val currentMonthMillis = grvtime.currentTime.toDateMonth.getMillis

    val siteKeySendAndReceived = for {
      month <- metrics.collect{ case month: ExchangeMonthMetric if month.key.dateTimeMs == currentMonthMillis => month }
      metric <- (month.source, month.value, 0L) :: (month.destination, 0L, month.value) :: List()
    } yield metric

    val siteToSendAndReceived = siteKeySendAndReceived.groupBy(_._1).mapValues(_.aggregate((0L,0L))( {
      case ((txSoFar, rxSoFar), (_,tx,rx)) => (txSoFar + tx, rxSoFar + rx)
    }, {
      case ((txA, rxA), (txB, rxB)) => (txA + txB, rxA + rxB)
    }))

    siteToSendAndReceived.filter { case (_, (sent, received)) => received.toDouble/Math.max(1l, sent) > ratio }.keySet
  }

  def throttleParamsStr: String = grvstrings.argNamesAndValues
}
object MonthlyMaxReceiveSendRatioThrottle {
  implicit val fmtMonthlyMaxReceiveSendRatioThrottle: Format[MonthlyMaxReceiveSendRatioThrottle] = (
    (__ \ "throttleType").format[ExchangeThrottleTypes.Type] and
      (__ \ "ratio").format[Double]
    )(
    {
      case (throttleType, ratio) if throttleType == ExchangeThrottleTypes.MONTHLY_MAX_RECEIVE_SEND_RATIO => MonthlyMaxReceiveSendRatioThrottle(ratio)
    },
    throttle => (throttle.throttleType, throttle.ratio)
  )
}

case class MonthlySendReceiveBufferThrottle(buffer: Long) extends ExchangeThrottle {
  val throttleType = ExchangeThrottleTypes.MONTHLY_SEND_RECEIVE_BUFFER

  def writeToOutput(output: PrimitiveOutputStream) = output.writeObj(this)

  def throttledSitesFromMetrics(metrics: Seq[ExchangeMetric]): Set[SiteKey] = {
    val currentMonthMillis = grvtime.currentTime.toDateMonth.getMillis

    val siteKeySendAndReceived = for {
      month <- metrics.collect{ case month: ExchangeMonthMetric if month.key.dateTimeMs == currentMonthMillis => month }
      metric <- (month.source, month.value, 0L) :: (month.destination, 0L, month.value) :: List()
    } yield metric

    val siteToSendAndReceived = siteKeySendAndReceived.groupBy(_._1).mapValues(_.aggregate((0L,0L))( {
      case ((txSoFar, rxSoFar), (_,tx,rx)) => (txSoFar + tx, rxSoFar + rx)
    }, {
      case ((txA, rxA), (txB, rxB)) => (txA + txB, rxA + rxB)
    }))

    siteToSendAndReceived.filter { case (_, (sent, received)) => received - buffer > sent }.keySet
  }

  def throttleParamsStr: String = grvstrings.argNamesAndValues
}
object MonthlySendReceiveBufferThrottle {
  implicit val fmtMonthlySendReceiveBufferThrottle: Format[MonthlySendReceiveBufferThrottle] = (
    (__ \ "throttleType").format[ExchangeThrottleTypes.Type] and
      (__ \ "buffer").format[Long]
    )(
    {
      case (throttleType, buffer) if throttleType == ExchangeThrottleTypes.MONTHLY_SEND_RECEIVE_BUFFER => MonthlySendReceiveBufferThrottle(buffer)
    },
    throttle => (throttle.throttleType, throttle.buffer)
  )
}