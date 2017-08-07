package com.gravity.interests.jobs.intelligence

import com.gravity.hbase.schema.{ComplexByteConverter, PrimitiveInputStream, PrimitiveOutputStream}
import com.gravity.interests.jobs.intelligence.SchemaTypes._
import com.gravity.interests.jobs.intelligence.hbase.ScopedMetricsKey
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

object ExchangeGoalTypes extends GrvEnum[Short] {
  case class Type(i: Short, n: String) extends ValueTypeBase(i, n)
  override def mkValue(id: Short, name: String): Type = Type(id, name)

  //this default value will throw an exception if you try and read or write it to HBase
  def defaultValue: Type = UNKNOWN_DO_NOT_USE

  val UNKNOWN_DO_NOT_USE: Type = Value(0, "unknown_do_not_use")
  val CLICKS_UNLIMITED: Type = Value(1, "clicks_unlimited")
  val CLICKS_RECEIVED_MONTHLY: Type = Value(2, "clicks_received_monthly")
  //When adding more types above, make sure to add them to ExchangeGoalByteConverter, JsValueToExchangeGoalValidations and exchange-edit.scaml

  implicit val byteConverter: ComplexByteConverter[Type] = shortEnumByteConverter(this)

  implicit val jsonFormat: Format[Type] = makeJsonFormat[Type]
}

//These converters are those related to the base ExchangeGoal so it's easier to see where to add ExchangeGoalTypes enumerations,
//other converters belong in ExchangeByteConverters
object ExchangeGoalConverters {

  implicit object ExchangeGoalByteConverter extends ComplexByteConverter[ExchangeGoal] {
    val version = 1 //only needs to be bumped for breaking read/write changes

    def write(data: ExchangeGoal, output: PrimitiveOutputStream) {
      output.writeShort(version)
      output.writeObj(data.goalType)
      data.writeToOutput(output)
    }

    def read(input: PrimitiveInputStream): ExchangeGoal = {
      val readVersion = input.readShort() //For the future

      if (readVersion != version)
        throw new RuntimeException(UnsupportedVersionExcepion.buildMessage("ExchangeGoal", version, version, version))

      val goalType = input.readObj[ExchangeGoalTypes.Type]

      goalType match {
        case ExchangeGoalTypes.UNKNOWN_DO_NOT_USE => throw new RuntimeException("Invalid goal type " + goalType)
        case ExchangeGoalTypes.CLICKS_UNLIMITED => input.readObj[ClicksUnlimitedGoal]
        case ExchangeGoalTypes.CLICKS_RECEIVED_MONTHLY => input.readObj[ClicksReceivedMonthlyGoal]
        case unknown => throw new RuntimeException("Unknown goal type " + goalType)
      }
    }
  }

  implicit val fmtExchangeGoal = Format(Reads[ExchangeGoal] {
    json => (json \ "goalType").validate[ExchangeGoalTypes.Type].flatMap(json.validateGoalConfigFromType)
  }, Writes[ExchangeGoal] {
    case goal: ClicksUnlimitedGoal => asGoalJsObject(goal)
    case goal: ClicksReceivedMonthlyGoal => asGoalJsObject(goal)
  })

  def asGoalJsObject[G <: ExchangeGoal](goal: G)(implicit tjs: Writes[G]): JsValue = { Json.obj("goalType" -> Json.toJson(goal.goalType)) ++ Json.toJson(goal)(tjs).as[JsObject] }

  implicit class JsValueToExchangeGoalValidations(val underlying: JsValue) extends AnyVal {
    def validateGoalConfigFromType(goal_type: ExchangeGoalTypes.Type): JsResult[ExchangeGoal] = goal_type match {
      case ExchangeGoalTypes.UNKNOWN_DO_NOT_USE => JsError(s"goal_type `${goal_type.toString}` should not be used")
      case ExchangeGoalTypes.CLICKS_UNLIMITED => underlying.validate[ClicksUnlimitedGoal]
      case ExchangeGoalTypes.CLICKS_RECEIVED_MONTHLY => underlying.validate[ClicksReceivedMonthlyGoal]
      case _ => JsError(s"Internal Error. Unable to validate goal_type `${goal_type.toString}`")
    }
  }

}

abstract class ExchangeMetric{
  val source: SiteKey
  val destination: SiteKey
}
case class ExchangeMonthMetric(source: SiteKey, destination: SiteKey, key: ScopedMetricsKey, value: Long) extends ExchangeMetric

//sealed to require pattern matching consider all subclasses
sealed abstract class ExchangeGoal {
  val goalType: ExchangeGoalTypes.Type
  def writeToOutput(output: PrimitiveOutputStream): Unit
  def goalCompletedSitesFromMetrics(metrics: Seq[ExchangeMetric]): Set[SiteKey]
  def goalParamsStr: String

  override def toString = {
    val subclassParams = goalParamsStr
    val separator = if (subclassParams.isEmpty) "" else ", "
    "goalType: " + goalType.toString + separator + subclassParams
  }

  def typedGoal[T <: ExchangeGoal]: T = this.asInstanceOf[T]
  def tryToTypedGoal[T <: ExchangeGoal : Manifest] : Validation[FailureResult, T] = ScalaMagic.tryCast[T](this)
}

class ClicksUnlimitedGoal() extends ExchangeGoal {
  val goalType = ExchangeGoalTypes.CLICKS_UNLIMITED

  def writeToOutput(output: PrimitiveOutputStream) = output.writeObj(this)

  def goalParamsStr = grvstrings.argNamesAndValues

  def goalCompletedSitesFromMetrics(metrics: Seq[ExchangeMetric]): Set[SiteKey] = Set()

  //any two ClicksUnlimitedGoal are equal, needed for test
  override def equals(that: Any): Boolean = that.isInstanceOf[ClicksUnlimitedGoal]
}
object ClicksUnlimitedGoal {
  implicit val fmtClicksUnlimitedGoal: Format[ClicksUnlimitedGoal] = Format(Reads[ClicksUnlimitedGoal] {
    case JsObject(Seq(("goalType",goalType))) => goalType.validate[ExchangeGoalTypes.Type].withFilter(_ == ExchangeGoalTypes.CLICKS_UNLIMITED).map(_ => new ClicksUnlimitedGoal())
    case x => JsError(s"Could not convert $x to ${ExchangeGoalTypes.CLICKS_UNLIMITED.toString}")
  }, Writes[ClicksUnlimitedGoal](goal => Json.obj("goalType" -> goal.goalType)))
}

case class ClicksReceivedMonthlyGoal(clickGoal: Long) extends ExchangeGoal {
  val goalType = ExchangeGoalTypes.CLICKS_RECEIVED_MONTHLY

  def writeToOutput(output: PrimitiveOutputStream) = output.writeObj(this)

  def goalParamsStr = grvstrings.argNamesAndValues

  def goalCompletedSitesFromMetrics(metrics: Seq[ExchangeMetric]): Set[SiteKey] = {
    val currentMonthMillis = grvtime.currentTime.toDateMonth.getMillis

    val siteReceivedClicks = metrics.collect{ case month: ExchangeMonthMetric if month.key.dateTimeMs == currentMonthMillis => month }.groupBy(_.destination).mapValues(_.map(_.value).sum)
    siteReceivedClicks.filter(_._2 >= clickGoal).keySet
  }

}
object ClicksReceivedMonthlyGoal {
  implicit val fmtClicksReceivedMonthlyGoal: Format[ClicksReceivedMonthlyGoal] = (
      (__ \ "goalType").format[ExchangeGoalTypes.Type] and
      (__ \ "clickGoal").format[Long]
    )(
    {
      case (goalType, clickGoal) if goalType == ExchangeGoalTypes.CLICKS_RECEIVED_MONTHLY => ClicksReceivedMonthlyGoal(clickGoal)
    },
    goal => (goal.goalType, goal.clickGoal)
  )
}
