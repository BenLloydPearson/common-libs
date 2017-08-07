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
object AgeGroup extends GrvEnum[Byte] {
  case class Type(i: Byte, n: String) extends ValueTypeBase(i, n)
  override def mkValue(id: Byte, name: String): Type = Type(id, name)

  val unknown: Type = Value(0, "unknown")
  val under18: Type = Value(1, "under18")
  val age18to24: Type = Value(2, "age18to24")
  val age25to34: Type = Value(3, "age25to34")
  val age35to44: Type = Value(4, "age35to44")
  val age45to54: Type = Value(5, "age45to54")
  val age55to64: Type = Value(6, "age55to64")
  val over64: Type = Value(7, "over64")

  val defaultValue: Type = unknown

  @transient implicit val byteConverter: ComplexByteConverter[Type] = byteEnumByteConverter(this)

  implicit val enumJsonSerializer: EnumNameSerializer[AgeGroup.type] = new EnumNameSerializer(this)

  implicit val jsonFormat: Format[Type] = makeJsonFormat[Type]

}
