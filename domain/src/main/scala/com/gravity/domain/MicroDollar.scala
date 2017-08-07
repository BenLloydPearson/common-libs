package com.gravity.domain

import com.gravity.interests.jobs.intelligence.schemas.DollarValue
import com.gravity.utilities.grvstrings._
import play.api.data.validation.ValidationError
import play.api.libs.json._

/**
 * Created by IntelliJ IDEA.
 * Author: Robbie Coleman
 * Date: 3/6/14
 * Time: 9:50 AM
 *            _ 
 *          /  \
 *         / ..|\
 *        (_\  |_)
 *        /  \@'
 *       /     \
 *   _  /  \   |
 * \\/  \  | _\
 *   \   /_ || \\_
 *    \____)|_) \_)
 *
 */

//One millionth of a dollar, AKA one ten-thousandth of a penny
case class MicroDollar(tenThousandthPennies: Long) extends AnyVal {
  def dollars: Double = tenThousandthPennies * 0.000001
  def pennies: Long = math.round(tenThousandthPennies * 0.0001)
  def dollarValue: DollarValue = DollarValue(pennies)

  def +(that: MicroDollar): MicroDollar = MicroDollar(that.tenThousandthPennies + tenThousandthPennies)
  def -(that: MicroDollar): MicroDollar = MicroDollar(that.tenThousandthPennies - tenThousandthPennies)
  def *(that: MicroDollar): MicroDollar = MicroDollar(math.round(that.tenThousandthPennies * tenThousandthPennies.toDouble))
  def /(that: MicroDollar): MicroDollar = MicroDollar(math.round(that.tenThousandthPennies / tenThousandthPennies.toDouble))

  def *(times: Double): MicroDollar = MicroDollar(math.round(tenThousandthPennies * times))
  def /(dividedBy: Double): MicroDollar = MicroDollar(math.round(tenThousandthPennies / dividedBy))

  def *(times: Int): MicroDollar = this * times.toDouble
  def /(dividedBy: Int): MicroDollar = this / dividedBy.toDouble

  override def toString: String = MicroDollar.asString(this)
}

object MicroDollar {
  val stringSuffix = "MD"
  val bdMillion: BigDecimal = BigDecimal(1000 * 1000)

  def asString(microDollar: MicroDollar): String = s"${microDollar.tenThousandthPennies}$stringSuffix"

  def fromDollarValue(dollarValue: DollarValue): MicroDollar = MicroDollar(dollarValue.pennies * 10000)

  def fromPennies(pennies: Long): MicroDollar = MicroDollar(pennies * 10000)

  def fromDollarBD(dollars: BigDecimal): MicroDollar = {
    val micro = dollars * bdMillion
    MicroDollar(micro.toLong)
  }

  def parse(input: String): Option[MicroDollar] = {
    if (input.endsWith(stringSuffix)) {
      input.stripSuffix(stringSuffix).tryToLong.map(tenThousandthPennies => MicroDollar(tenThousandthPennies))
    } else {
      input.tryToLong.map(pennies => fromDollarValue(DollarValue(pennies)))
    }
  }

  def parseJson(jsonStr: String): Option[MicroDollar] = {
    Json.parse(jsonStr).asOpt[JsNumber].map(n => MicroDollar((n.value * bdMillion).toLong))
  }

    implicit val mdFmt: Format[MicroDollar] = Format(
      Reads{
        case jNum: JsNumber => if(jNum.value.isValidLong) JsSuccess(MicroDollar(jNum.value.longValue()))
          else JsError(ValidationError("Not a valid microdollar value"))
        case _ => JsError()
      },
      Writes(
        (md: MicroDollar) => {
          JsNumber(BigDecimal(md.tenThousandthPennies))
        }
      )
    )
}
