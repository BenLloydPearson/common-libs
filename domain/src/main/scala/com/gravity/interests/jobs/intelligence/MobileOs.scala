package com.gravity.interests.jobs.intelligence

import com.gravity.hbase.schema.ComplexByteConverter
import com.gravity.utilities.grvenum.GrvEnum
import com.gravity.utilities.grvenum.GrvEnum._
import play.api.libs.json.Format

@SerialVersionUID(1l)
object MobileOs extends GrvEnum[Byte] {
  case class Type(i: Byte, n: String) extends ValueTypeBase(i, n)
  override def mkValue(id: Byte, name: String): Type = Type(id, name)

  val unknown: Type = Value(0, "unknown")
  val ios: Type = Value(1, "ios")
  val android: Type = Value(2, "android")

  val defaultValue: Type = unknown

  @transient implicit val byteConverter: ComplexByteConverter[Type] = byteEnumByteConverter(this)
  implicit val jsonFormat: Format[Type] = makeJsonFormat[Type]
}