package com.gravity.interests.jobs.intelligence

import com.gravity.utilities.grvenum.GrvEnum
import com.gravity.utilities.grvjson.{EnumSetNameSerializer, EnumNameSerializer}
import play.api.libs.json.Format

@SerialVersionUID(2l)
object CampaignArticleStatus extends GrvEnum[Byte] {
  case class Type(i: Byte, n: String) extends ValueTypeBase(i, n)
  override def mkValue(id: Byte, name: String): Type = Type(id, name)

  val inactive: Type = Value(0, "inactive")
  val active: Type = Value(1, "active")

  def defaultValue: Type = inactive

  val JsonSerializer: EnumNameSerializer[CampaignArticleStatus.type] = new EnumNameSerializer(this)
  val SetJsonSerializer: EnumSetNameSerializer[CampaignArticleStatus.type] = new EnumSetNameSerializer(this)

  implicit val jsonFormat: Format[Type] = makeJsonFormat[Type]
}
