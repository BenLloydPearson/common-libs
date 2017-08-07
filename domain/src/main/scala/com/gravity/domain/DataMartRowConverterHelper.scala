package com.gravity.domain

import com.gravity.utilities.grvtime
import org.joda.time.DateTime

object DataMartRowConverterHelper {

  val ArticleDmIndexPrefix = "article-dm" // YYYY-MM-dd
  val UnitDmIndexPrefix = "unit-dm" // YYYY-MM-dd
  val CampaignAttributesDmIndexPrefix = "campaign-dm" // YYYY-MM-dd

  val DmIndexType = "day"

  def getDatamartIndexBaseName(prefix: String, date: DateTime): String = {
    val dateStr = grvtime.elasticSearchDataMartFormat.print(date)
    s"$prefix-$dateStr"
  }

  def getDatamartIndexAliasName(prefix: String, date: DateTime): String = {
    val baseName = getDatamartIndexBaseName(prefix, date)
    s"$baseName-final"
  }

  val LogDate = "logDate"
  val LogHour = "logHour"
  val PublisherGuid = "publisherGuid"
  val PublisherName = "publisherName"
  val AdvertiserGuid = "advertiserGuid"
  val AdvertiserName = "advertiserName"
  val CampaignId = "campaignId"
  val CampaignName = "campaignName"
  val IsOrganic = "isOrganic"
  val SitePlacementId = "sitePlacementId"
  val SitePlacementName = "sitePlacementName"
  val ArticleId = "articleId"
  val ArticleTitle = "articleTitle"
  val ArticleUrl = "articleUrl"
  val ServedOnDomain = "servedOnDomain"
  val Cpc = "cpc"
  val AdjustedCpc = "adjustedCpc"
  val DisplayIndex = "displayIndex"
  val UnitImpressionsServedClean = "unitImpressionsServedClean"
  val UnitImpressionsServedDiscarded = "unitImpressionsServedDiscarded"
  val UnitImpressionsViewedClean = "unitImpressionsViewedClean"
  val UnitImpressionsViewedDiscarded = "unitImpressionsViewedDiscarded"
  val ArticleImpressionsServedClean = "articleImpressionsServedClean"
  val ArticleImpressionsServedDiscarded = "articleImpressionsServedDiscarded"
  val ArticleImpressionsViewedClean = "articleImpressionsViewedClean"
  val ArticleImpressionsViewedDiscarded = "articleImpressionsViewedDiscarded"
  val ClicksClean = "clicksClean"
  val ClicksDiscarded = "clicksDiscarded"
  val ConversionsClean = "conversionsClean"
  val ConversionsDiscarded = "conversionsDiscarded"
  val UnitImpressionsServedCleanUsers = "unitImpressionsServedCleanUsers"
  val UnitImpressionsViewedCleanUsers = "unitImpressionsViewedCleanUsers"
  val ClicksCleanUsers = "clicksCleanUsers"
  val ConversionsCleanUsers = "conversionsCleanUsers"
  val UnitImpressionsServedDiscardedUsers = "unitImpressionsServedDiscardedUsers"
  val UnitImpressionsViewedDiscardedUsers = "unitImpressionsViewedDiscardedUsers"
  val ClicksDiscardedUsers = "clicksDiscardedUsers"
  val ConversionsDiscardedUsers = "conversionsDiscardedUsers"
  val RevShare = "revShare"
  val ContentGroupId = "contentGroupId"
  val ContentGroupName = "contentGroupName"
  val RenderType = "renderType"
  val DailyBudget = "dailyBudget"
  val WeeklyBudget = "weeklyBudget"
  val MonthlyBudget = "monthlyBudget"
  val TotalBudget = "totalBudget"
  val PartnerPlacementId = "partnerPlacementId"
  val LogMinute = "logMinute"
  val ImpressionType = "impressionType"
  val ImpressionPurposeId = "impressionPurposeId"
  val ImpressionPurposeName = "impressionPurposeName"
  val UserFeedbackVariationId = "userFeedbackVariationId"
  val UserFeedbackVariationName = "userFeedbackVariationName"
  val ChosenUserFeedbackOptionId = "chosenUserFeedbackOptionId"
  val ChosenUserFeedbackOptionName = "chosenUserFeedbackOptionName"
  val RecoBucket = "recoBucket"
  val IsControl = "isControl"
  val CountryCode = "countryCode"
  val Country = "country"
  val State = "state"
  val DmaCode = "dmaCode"
  val GeoRollup = "geoRollup"
  val DeviceType = "deviceType"
  val Browser = "browser"
  val BrowserManufacturer = "browserManufacturer"
  val BrowserRollup = "browserRollup"
  val OperatingSystem = "operatingSystem"
  val OperatingSystemManufacturer = "operatingSystemManufacturer"
  val OperatingSystemRollup = "operatingSystemRollup"
  val OrganicClicksClean = "organicClicksClean"
  val OrganicClicksDiscarded = "organicClicksDiscarded"
  val SponsoredClicksClean = "sponsoredClicksClean"
  val SponsoredClicksDiscarded = "sponsoredClicksDiscarded"
  val RevenueClean = "revenueClean"
  val RevenueDiscarded = "revenueDiscarded"
  val MinServedCpc = "minServedCpc"
  val MinClickedCpc = "minClickedCpc"
  val ReferrerDomain = "referrerDomain"
  val UserFeedbackPresentationId = "userFeedbackPresentationId"
  val UserFeedbackPresentationName = "userFeedbackPresentationName"
  val ExchangeGuid = "exchangeGuid"
  val ExchangeHostGuid = "exchangeHostGuid"
  val ExchangeGoal = "exchangeGoal"
  val ExchangeName = "exchangeName"
  val ExchangeStatus = "exchangeStatus"
  val ExchangeType = "exchangeType"
  val TimeSpentMedian = "timeSpentMedian"

}