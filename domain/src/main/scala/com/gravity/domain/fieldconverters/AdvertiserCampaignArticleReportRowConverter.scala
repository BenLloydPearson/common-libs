package com.gravity.domain.fieldconverters

import com.gravity.domain.FieldConverters
import com.gravity.domain.recommendations.{AdvertiserCampaignArticleReportRow, RdsDateCustomFieldValueConverter}
import com.gravity.utilities.eventlogging.{FieldRegistry, FieldValueRegistry}
import com.gravity.utilities.grvfields.OptionalCategoryVersionLogLineConverter
import com.gravity.utilities.grvstrings._

trait AdvertiserCampaignArticleReportRowConverter {
  this: FieldConverters.type =>

  implicit object AdvertiserCampaignArticleReportRowConverter extends OptionalCategoryVersionLogLineConverter[AdvertiserCampaignArticleReportRow] {
    val delim = "^"
    val categoryName = "AdvertiserCampaignArticleReportRow"
    val version = 0

    val fields: FieldRegistry[AdvertiserCampaignArticleReportRow] = new FieldRegistry[AdvertiserCampaignArticleReportRow](categoryName, version)
      .registerUnencodedStringField("logDate", 0, "1970-01-01", "the date this pivot represents", required = true)
      .registerUnencodedStringField("advertiserGuid", 1, emptyString, "The GUID that represents the advertiser for this pivot", required = true)
      .registerLongField("campaignId", 2, 0L, "The ID of this pivot's campaign key", required = true)
      .registerLongField("articleId", 3, 0L, "The ID of this pivot's article", required = true)
      .registerLongField("articleImpressionsClean", 4, 0L, "The number of cleansed article `impressions", required = true)
      .registerLongField("articleImpressionsDiscarded", 5, 0L, "The number of invalid article impressions discarded during cleansing", required = true)
      .registerLongField("articleImpressionsViewedClean", 6, 0L, "The number of cleansed article impressions that were viewed", required = true)
      .registerLongField("articleImpressionsViewedDiscarded", 7, 0L, "The number of invalid article impressions that were viewed but discarded during cleansing", required = true)
      .registerLongField("clicksClean", 8, 0L, "The number of cleansed clicks", required = true)
      .registerLongField("clicksDiscarded", 9, 0L, "The number of invalid clicks discarded during cleansing", required = true)
      .registerLongField("advertiserSpendClean", 10, 0L, "The cleansed total amount spent in pennies", required = true)
      .registerLongField("advertiserSpendDiscarded", 11, 0L, "The discarded total amount spent in pennies", required = true)
      .registerLongField("conversionsClean", 12, 0L, "The number of cleansed conversions", required = false)
      .registerLongField("conversionsDiscarded", 13, 0L, "The number of invalid conversions discarded during cleansing", required = false)


    def fromValueRegistry(reg: FieldValueRegistry): AdvertiserCampaignArticleReportRow = AdvertiserCampaignArticleReportRow(reg)

    def toValueRegistry(o: AdvertiserCampaignArticleReportRow): FieldValueRegistry = {
      import o._
      val reg = new FieldValueRegistry(fields)
      RdsDateCustomFieldValueConverter.registerFieldValue(0, reg, logDate)
        .registerFieldValue(1, advertiserGuid)
        .registerFieldValue(2, campaignId)
        .registerFieldValue(3, articleId)
        .registerFieldValue(4, articleImpressionsClean)
        .registerFieldValue(5, articleImpressionsDiscarded)
        .registerFieldValue(6, articleImpressionsViewedClean)
        .registerFieldValue(7, articleImpressionsViewedDiscarded)
        .registerFieldValue(8, clicksClean)
        .registerFieldValue(9, clicksDiscarded)
        .registerFieldValue(10, advertiserSpendClean.pennies)
        .registerFieldValue(11, advertiserSpendDiscarded.pennies)
        .registerFieldValue(12, conversionsClean)
        .registerFieldValue(13, conversionsDiscarded)
    }
  }
}