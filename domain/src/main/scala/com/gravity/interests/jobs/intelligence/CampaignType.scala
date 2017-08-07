package com.gravity.interests.jobs.intelligence

import com.gravity.utilities.grvenum.GrvEnum
import play.api.libs.json.Format

@SerialVersionUID(-2410981305913269272l)
object CampaignType extends GrvEnum[Byte] {
  case class Type(i: Byte, n: String) extends ValueTypeBase(i, n)
  override def mkValue(id: Byte, name: String): Type = Type(id, name)

  val organic: Type = Value(0, "organic")
  val sponsored: Type = Value(1, "sponsored")

  val defaultValue: Type = organic

  val sponsoredTypes: Set[Type] = Set(sponsored)

  // list of campaign types that would be considered sponsored
  def isSponsoredType(campaignType: CampaignType.Type): Boolean = sponsoredTypes.contains(campaignType)

  // list of campaign types that would be considered organic
  def isOrganicType(campaignType: CampaignType.Type): Boolean = !isSponsoredType(campaignType)

  implicit val jsonFormat: Format[Type] = makeJsonFormat[Type]
}