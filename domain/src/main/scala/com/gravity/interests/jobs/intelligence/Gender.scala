package com.gravity.interests.jobs.intelligence

import com.gravity.domain.articles.ContentGroupStatus._
import com.gravity.hbase.schema.ComplexByteConverter
import com.gravity.utilities.grvenum.GrvEnum
import com.gravity.utilities.grvenum.GrvEnum._
import com.gravity.utilities.grvjson.EnumNameSerializer
import play.api.libs.json.Format

/**
 * Created by jengelman14 on 8/13/15.
 */
@SerialVersionUID(1l)
object Gender extends GrvEnum[Byte] {
  case class Type(i: Byte, n: String) extends ValueTypeBase(i, n)
  override def mkValue(id: Byte, name: String): Type = Type(id, name)

  val unknown: Type = Value(0, "unknown")
  val male: Type = Value(1, "male")
  val female: Type = Value(2, "female")

  val defaultValue: Type = unknown

  @transient implicit val byteConverter: ComplexByteConverter[Type] = byteEnumByteConverter(this)

  implicit val enumJsonSerializer: EnumNameSerializer[Gender.type] = new EnumNameSerializer(this)

  implicit val jsonFormat: Format[Type] = makeJsonFormat[Type]

}
