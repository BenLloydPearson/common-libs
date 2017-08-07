package com.gravity.interests.jobs.intelligence.operations.recommendations

import com.gravity.interests.jobs.intelligence._
import com.gravity.interests.jobs.intelligence.schemas.DollarValue
import com.gravity.utilities.grvjson._
import com.gravity.valueclasses.ValueClassesForDomain._
import play.api.libs.json._

trait HasCampaignKey {
  def campaignKey: CampaignKey
}

trait HasCampaign extends HasCampaignKey {
  val campaign: CampaignRecommendationData
  val campaignKey: CampaignKey
}

@SerialVersionUID(1L)
case class CampaignRecommendationData(key: CampaignKey, cpc: DollarValue = DollarValue.zero, campaignType: CampaignType.Type = CampaignType.sponsored, settings: Option[CampaignArticleSettings] = None) {

  def isSponsored: Boolean = CampaignType.isSponsoredType(campaignType)

  def effectiveImage(articleImageUrl: ImageUrl): ImageUrl = settings.map(_.effectiveImage(articleImageUrl)).getOrElse(articleImageUrl)

  def effectiveTitle(articleTitle: Title): Title = settings.map(_.effectiveTitle(articleTitle)).getOrElse(articleTitle)

}

object CampaignRecommendationData {

  val empty: CampaignRecommendationData = CampaignRecommendationData(CampaignKey.empty)

  implicit val campaignKeyJsonFormat: Format[CampaignKey] = ScopedKeyJsonFormats.campaignKeyJsonFormat

  private val jsonFormatBase = Json.format[CampaignRecommendationData]
  implicit val jsonFormat: Format[CampaignRecommendationData] = Format(
    jsonFormatBase.compose(withDefault("campaignType", CampaignType.sponsored)).compose(withDefault[Option[CampaignArticleSettings]]("settings", None)),
    jsonFormatBase
  )
}