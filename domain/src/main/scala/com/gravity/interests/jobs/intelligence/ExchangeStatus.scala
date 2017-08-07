package com.gravity.interests.jobs.intelligence

import com.gravity.utilities.grvenum.GrvEnum
import play.api.libs.json.Format


@SerialVersionUID(1l)
object ExchangeStatus extends GrvEnum[Byte] {
  case class Type(i: Byte, n: String) extends ValueTypeBase(i, n)
  override def mkValue(id: Byte, name: String): Type = Type(id, name)

  val active: Type = Value(0, "active")
  val setup: Type = Value(1, "setup")
  val awaitingStart: Type = Value(2, "awaitingStart")
  val paused: Type = Value(3, "paused")
  val completed: Type = Value(4, "completed")

  val defaultValue: Type = setup

  implicit val jsonFormat: Format[Type] = makeJsonFormat[Type]
  implicit val defaultValueWriter = makeDefaultValueWriter[Type]

}