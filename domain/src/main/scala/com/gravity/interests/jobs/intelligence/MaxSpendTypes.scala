package com.gravity.interests.jobs.intelligence

import com.gravity.utilities.grvenum.GrvEnum
import com.gravity.utilities.grvjson.EnumNameSerializer
import play.api.libs.json.Format

@SerialVersionUID(-8402000720296620173l)
object MaxSpendTypes extends GrvEnum[Byte] {
  case class Type(i: Byte, n: String) extends ValueTypeBase(i, n)
  override def mkValue(id: Byte, name: String): Type = Type(id, name)

  val daily: Type = Value(0, "daily")
  val total: Type = Value(1, "total")
  val weekly: Type = Value(2, "weekly") // This is being deprecated
  val monthly: Type = Value(3, "monthly")

  val defaultValue: Type = daily

  val JsonSerializer: EnumNameSerializer[MaxSpendTypes.type] = new EnumNameSerializer(this)
  implicit val jsonFormat: Format[Type] = makeJsonFormat[Type]
}
