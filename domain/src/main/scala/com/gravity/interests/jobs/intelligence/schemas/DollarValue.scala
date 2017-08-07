package com.gravity.interests.jobs.intelligence.schemas

import com.gravity.interests.jobs.intelligence.schemas.byteconverters.SchemaTypeHelpers
import com.gravity.utilities.grvjson._
import com.gravity.utilities.grvstrings._
import net.liftweb.json._
import play.api.libs.json._

import scalaz.{Order, Ordering}

object DollarValue {
  object JsonSerializer extends Serializer[DollarValue] {
    override def deserialize(implicit format: Formats): PartialFunction[(TypeInfo, JValue), DollarValue] = {
      case (_, JInt(pennies)) => DollarValue(pennies.toLong)
      case (_, JObject(fields)) if fields.exists(_.name == "pennies") =>
        DollarValue(fields.find(_.name == "pennies").flatMap({
          case JField(_, JString(str)) => str.tryToLong
          case JField(_, JInt(num)) => Some(num.toLong)
        }).getOrElse(throw new MappingException(s"Couldn't convert $fields to DollarValue.")))
    }

    override def serialize(implicit format: Formats): PartialFunction[Any, JValue] = {
      case DollarValue(pennies) => JObject(List(JField("pennies", JInt(pennies))))
    }
  }

  implicit val jsonFormat: Format[DollarValue] = longValueClassFormat[DollarValue]("pennies", DollarValue.apply, _.pennies)

  val jsonWritesAsDecimalDollars: Writes[DollarValue] = Writes((dv: DollarValue) => JsNumber(dv.toDecimal))

  val penniesPerDollar: BigDecimal = BigDecimal(100)

  val zero: DollarValue = DollarValue(0l)
  val minValue: DollarValue = DollarValue(SchemaTypeHelpers.longLowerBounds)
  val maxValue: DollarValue = DollarValue(SchemaTypeHelpers.longUpperBounds)
  val infinite: DollarValue = DollarValue(Long.MaxValue)
  def fromDollars(dollars: Double): DollarValue = DollarValue(math.round(dollars * 100))

  implicit val order: Order[DollarValue] = new scalaz.Order[DollarValue] {
    override def order(x: DollarValue, y: DollarValue): Ordering = {
      val result = x.pennies.compareTo(y.pennies)
      if(result > 0) Ordering.GT
      else if(result < 0) Ordering.LT
      else Ordering.EQ
    }
  }

  implicit val scalaOrder: scala.Ordering[DollarValue] = order.toScalaOrdering
}

@SerialVersionUID(3371816286405894142l)
case class DollarValue(pennies: Long) {
  def dollars: Double = pennies * 0.01d

  def +(that: DollarValue): DollarValue = DollarValue(pennies + that.pennies)

  def +(that: Long): DollarValue = DollarValue(pennies + that)

  def *(that: DollarValue): DollarValue = DollarValue(pennies * that.pennies)

  def *(that: Long): DollarValue = DollarValue(pennies * that)

  def >(that: DollarValue): Boolean = this.pennies > that.pennies

  def <(that: DollarValue): Boolean = this.pennies < that.pennies

  override def toString: String = f"$$$dollars%,.2f"

  def isEmpty: Boolean = pennies == 0L

  def nonEmpty: Boolean = !isEmpty

  def toDecimal: BigDecimal = BigDecimal(pennies) / DollarValue.penniesPerDollar
}