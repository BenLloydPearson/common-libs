package com.gravity.interests.jobs.intelligence

import com.gravity.utilities.grvenum.GrvEnum
import play.api.libs.json.Format

@SerialVersionUID(255414934630829772l)
object CampaignStatus extends GrvEnum[Byte] {
  case class Type(i: Byte, n: String) extends ValueTypeBase(i, n)
  override def mkValue(id: Byte, name: String): Type = Type(id, name)

  val pending: Type = Value(0, "pending")
  val approved: Type = Value(1, "approved")
  val active: Type = Value(2, "active")
  val paused: Type = Value(3, "paused")
  val completed: Type = Value(4, "completed")
  val dailyCapped: Type = Value(5, "dailyCapped")
  val weeklyCapped: Type = Value(6, "weeklyCapped")
  val monthlyCapped: Type = Value(7, "monthlyCapped")

  val defaultValue: Type = pending

  val cappedStatuses: Set[Type] = Set(dailyCapped, weeklyCapped, monthlyCapped)

  implicit val jsonFormat: Format[Type] = makeJsonFormat[Type]

}