package com.gravity.interests.jobs.intelligence.operations

import com.gravity.utilities.grvenum.GrvEnum
import com.gravity.utilities.grvjson.EnumNameSerializer
import play.api.libs.json.Format

@SerialVersionUID(2l)
object ScrubberEnum extends GrvEnum[Byte] {
  case class Type(i: Byte, n: String) extends ValueTypeBase(i, n)
  override def mkValue(id: Byte, name: String): Type = Type(id, name)

  val noOp: Type = Value(0, "no-op")
  val normalize: Type = Value(1, "normalize")
  val canonicalize: Type = Value(2, "canonicalize")
  val abTesting: Type = Value(3, "abtesting")

  val defaultValue: Type = noOp

  val JsonSerializer: EnumNameSerializer[ScrubberEnum.type] = new EnumNameSerializer(this)
  implicit val jsonFormat: Format[Type] = makeJsonFormat[Type]
  implicit val defaultValueWriter = makeDefaultValueWriter[Type]
}

