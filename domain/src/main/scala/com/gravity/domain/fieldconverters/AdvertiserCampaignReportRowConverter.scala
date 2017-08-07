package com.gravity.domain.fieldconverters

import com.gravity.domain.FieldConverters
import com.gravity.domain.recommendations.{AdvertiserCampaignReportRow, RdsDateCustomFieldValueConverter}
import com.gravity.utilities.eventlogging.{FieldRegistry, FieldValueRegistry}
import com.gravity.utilities.grvfields.OptionalCategoryVersionLogLineConverter
import com.gravity.utilities.grvstrings._

trait AdvertiserCampaignReportRowConverter {
  this: FieldConverters.type =>

  implicit object AdvertiserCampaignReportRowConverter extends OptionalCategoryVersionLogLineConverter[AdvertiserCampaignReportRow] {
    val delim = "^"
    val categoryName = "AdvertiserCampaignReportRow"
    val version = 0

    val fields: FieldRegistry[AdvertiserCampaignReportRow] = new FieldRegistry[AdvertiserCampaignReportRow](categoryName, version)
      .registerUnencodedStringField("dateTime", 0, "1970-01-01", "the date portion of this row's DateHour", required = true)
      .registerIntField("hour", 1, 0, "the hour portion of this row's DateHour", required = true)
      .registerUnencodedStringField("advertiserGuid", 2, emptyString, "The GUID that represents the advertiser for this pivot", required = true)
      .registerLongField("campaignId", 3, 0L, "The ID of this pivot's campaign key", required = true)
      .registerLongField("articleImpressionsClean", 4, 0L, "The number of cleansed article impressions", required = true)
      .registerLongField("articleImpressionsDiscarded", 5, 0L, "The number of invalid article impressions discarded during cleansing", required = true)
      .registerLongField("articleImpressionsViewedClean", 6, 0L, "The number of cleansed article impressions that were viewed", required = true)
      .registerLongField("articleImpressionsViewedDiscarded", 7, 0L, "The number of invalid article impressions that were viewed but discarded during cleansing", required = true)
      .registerLongField("clicksClean", 8, 0L, "The number of cleansed clicks", required = true)
      .registerLongField("clicksDiscarded", 9, 0L, "The number of invalid clicks discarded during cleansing", required = true)
      .registerLongField("advertiserSpendClean", 10, 0L, "The cleansed total amount spent in pennies", required = true)
      .registerLongField("advertiserSpendDiscarded", 11, 0L, "The discarded total amount spent in pennies", required = true)
      .registerLongField("conversionsClean", 12, 0L, "The number of cleansed conversions", required = true)
      .registerLongField("conversionsDiscarded", 13, 0L, "The number of invalid conversions discarded during cleansing", required = true)


    def fromValueRegistry(reg: FieldValueRegistry): AdvertiserCampaignReportRow = AdvertiserCampaignReportRow(reg)

    def toValueRegistry(o: AdvertiserCampaignReportRow): FieldValueRegistry = {
      import o._
      val reg = new FieldValueRegistry(fields)
      RdsDateCustomFieldValueConverter.registerFieldValue(0, reg, logHour.toDateTime)
        .registerFieldValue(1, logHour.getHourOfDay)
        .registerFieldValue(2, advertiserGuid)
        .registerFieldValue(3, campaignId)
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