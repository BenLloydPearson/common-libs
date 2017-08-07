package com.gravity.interests.jobs.intelligence

import com.gravity.utilities.grvenum.GrvEnum
import play.api.libs.json.Format

@SerialVersionUID(1L)
object ExchangeType extends GrvEnum[Byte] {

  case class Type(i: Byte, n: String) extends ValueTypeBase(i, n)
  override def mkValue(id: Byte, name: String): Type = Type(id, name)

  val oneWayTrafficExchange: Type = Value(1, "oneWayTrafficExchange")
  val twoWayTrafficExchange: Type = Value(2, "twoWayTrafficExchange")

  val defaultValue: Type = twoWayTrafficExchange

  implicit val jsonFormat: Format[Type] = makeJsonFormat[Type]
  implicit val defaultValueWriter = makeDefaultValueWriter[Type]

}