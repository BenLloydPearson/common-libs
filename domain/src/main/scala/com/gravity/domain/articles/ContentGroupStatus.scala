package com.gravity.domain.articles

import com.gravity.hbase.schema.ComplexByteConverter
import com.gravity.utilities.grvenum.GrvEnum
import com.gravity.utilities.grvenum.GrvEnum._
import play.api.libs.json.Format

/** Created by IntelliJ IDEA.
  * Author: Robbie Coleman
  * Date: 10/2/13
  * Time: 11:00 AM
  */
@SerialVersionUID(2l)
object ContentGroupStatus extends GrvEnum[Byte] {
  type ContentGroupStatus = Type
  case class Type(i: Byte, n: String) extends ValueTypeBase(i, n)
  override def mkValue(id: Byte, name: String): Type = Type(id, name)

  val inactive: Type = Value(0, "inactive")
  val active: Type = Value(1, "active")

  def defaultValue: Type = inactive

  @transient implicit val byteConverter: ComplexByteConverter[Type] = byteEnumByteConverter(this)

  implicit val jsonFormat: Format[Type] = makeJsonFormat[Type]
}
