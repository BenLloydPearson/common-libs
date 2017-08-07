package com.gravity.domain.fieldconverters

import com.gravity.domain.FieldConverters
import com.gravity.interests.jobs.intelligence.operations.CampaignAttributesDataMartRow
import com.gravity.utilities.eventlogging.{FieldRegistry, FieldValueRegistry}
import com.gravity.utilities.grvfields.FieldConverter
import com.gravity.utilities.grvstrings._

trait CampaignAttributesDataMartRowConverter {
  this: FieldConverters.type =>

  implicit object CampaignAttributesDataMartRowConverter extends FieldConverter[CampaignAttributesDataMartRow] {
    val fields = new FieldRegistry[CampaignAttributesDataMartRow]("CampaignAttributesDataMartRow")
      .registerStringField("logDate", 0, emptyString, "Date of event")
      .registerIntField("logHour", 1, -1, "Hour of day of event")
      .registerStringField("publisherGuid", 2, emptyString)
      .registerStringField("publisherName", 3, emptyString)
      .registerStringField("advertiserGuid", 4, emptyString)
      .registerStringField("advertiserName", 5, emptyString)
      .registerLongField("cpc", 6, -1, "Cost per click in pennies")
      .registerDoubleField("revShare", 7, -1.0, "DEPRECATED")
      .registerStringField("campaignId", 8, emptyString, "String representation of campaign key")
      .registerStringField("campaignName", 9, emptyString)
      .registerBooleanField("isOrganic", 10, true)
      .registerStringField("countryCode", 11, emptyString)
      .registerStringField("country", 12, emptyString)
      .registerStringField("state", 13, emptyString)
      .registerStringField("dmaCode", 14, emptyString)
      .registerStringField("geoRollup", 15, emptyString)
      .registerStringField("deviceType", 16, emptyString)
      .registerStringField("browser", 17, emptyString)
      .registerStringField("browserManufacturer", 18, emptyString)
      .registerStringField("browserRollup", 19, emptyString)
      .registerStringField("operatingSystem", 20, emptyString)
      .registerStringField("operatingSystemManufacturer", 21, emptyString)
      .registerStringField("operatingSystemRollup", 22, emptyString)
      .registerLongField("sitePlacementId", 23, -1)
      .registerStringField("sitePlacementName", 24, emptyString)
      .registerStringField("renderType", 25, emptyString, "Identifies api, widget, or syndication, so not really a render type")
      .registerLongField("articleImpressionsServedClean", 26, -1)
      .registerLongField("articleImpressionsServedDiscarded", 27, -1)
      .registerLongField("articleImpressionsViewedClean", 28, -1)
      .registerLongField("articleImpressionsViewedDiscarded", 29, -1)
      .registerLongField("clicksClean", 30, -1)
      .registerLongField("clicksDiscarded", 31, -1)
      .registerStringField("partnerPlacementId", 32, emptyString)
      .registerStringField("articleImpressionsServedCleanUsers", 33, emptyString)
      .registerStringField("articleImpressionsServedDiscardedUsers", 34, emptyString)
      .registerStringField("articleImpressionsViewedCleanUsers", 35, emptyString)
      .registerStringField("articleImpressionsViewedDiscardedUsers", 36, emptyString)
      .registerStringField("clicksCleanUsers", 37, emptyString)
      .registerStringField("clicksDiscardedUsers", 38, emptyString)
      .registerIntField("impressionType", 39, -1, "1 - organic, 2 - blended, 3 - sponsored")
      .registerStringField("exchangeGuid", 40, emptyString, "The exchange the article came from")
      .registerStringField("exchangeHostGuid", 41, emptyString, "The exchange host guid the article came from")
      .registerStringField("exchangeGoal", 42, emptyString, "The exchange goal of the article")
      .registerStringField("exchangeName", 43, emptyString, "The exchange name the article came from")
      .registerStringField("exchangeStatus", 44, emptyString, "The exchange status the article uses")
      .registerStringField("exchangeType", 45, emptyString, "The exchange type the article uses")


    def fromValueRegistry(reg: FieldValueRegistry) = new CampaignAttributesDataMartRow(
      reg.getValue[String](0),
      reg.getValue[Int](1),
      reg.getValue[String](2),
      reg.getValue[String](3),
      reg.getValue[String](4),
      reg.getValue[String](5),
      reg.getValue[Long](6),
      reg.getValue[Double](7),
      reg.getValue[String](8),
      reg.getValue[String](9),
      reg.getValue[Boolean](10),
      reg.getValue[String](11),
      reg.getValue[String](12),
      reg.getValue[String](13),
      reg.getValue[String](14),
      reg.getValue[String](15),
      reg.getValue[String](16),
      reg.getValue[String](17),
      reg.getValue[String](18),
      reg.getValue[String](19),
      reg.getValue[String](20),
        reg.getValue[String](21),
        reg.getValue[String](22),
        reg.getValue[Long](23),
        reg.getValue[String](24),
        reg.getValue[String](25),
        reg.getValue[Long](26),
        reg.getValue[Long](27),
        reg.getValue[Long](28),
        reg.getValue[Long](29),
        reg.getValue[Long](30),
        reg.getValue[Long](31),
        reg.getValue[String](32),
        reg.getValue[String](33),
        reg.getValue[String](34),
        reg.getValue[String](35),
        reg.getValue[String](36),
        reg.getValue[String](37),
        reg.getValue[String](38),
        reg.getValue[Int](39),
        reg.getValue[String](40),
        reg.getValue[String](41),
        reg.getValue[String](42),
        reg.getValue[String](43),
        reg.getValue[String](44),
        reg.getValue[String](45)

    )

    def toValueRegistry(o: CampaignAttributesDataMartRow) = {
      import o._
      new FieldValueRegistry(fields)
        .registerFieldValue(0, logDate)
        .registerFieldValue(1, logHour)
        .registerFieldValue(2, publisherGuid)
        .registerFieldValue(3, publisherName)
        .registerFieldValue(4, advertiserGuid)
        .registerFieldValue(5, advertiserName)
        .registerFieldValue(6, cpc)
        .registerFieldValue(7, revShare)
        .registerFieldValue(8, campaignId)
        .registerFieldValue(9, campaignName)
        .registerFieldValue(10, isOrganic)
        .registerFieldValue(11, countryCode)
        .registerFieldValue(12, country)
        .registerFieldValue(13, state)
        .registerFieldValue(14, dmaCode)
        .registerFieldValue(15, geoRollup)
        .registerFieldValue(16, deviceType)
        .registerFieldValue(17, browser)
        .registerFieldValue(18, browserManufacturer)
        .registerFieldValue(19, browserRollup)
        .registerFieldValue(20, operatingSystem)
        .registerFieldValue(21, operatingSystemManufacturer)
        .registerFieldValue(22, operatingSystemRollup)
        .registerFieldValue(23, sitePlacementId)
        .registerFieldValue(24, sitePlacementName)
        .registerFieldValue(25, renderType)
        .registerFieldValue(26, articleImpressionsServedClean)
        .registerFieldValue(27, articleImpressionsServedDiscarded)
        .registerFieldValue(28, articleImpressionsViewedClean)
        .registerFieldValue(29, articleImpressionsViewedDiscarded)
        .registerFieldValue(30, clicksClean)
        .registerFieldValue(31, clicksDiscarded)
        .registerFieldValue(32, partnerPlacementId)
        .registerFieldValue(33, articleImpressionsServedCleanUsers)
        .registerFieldValue(34, articleImpressionsServedDiscardedUsers)
        .registerFieldValue(35, articleImpressionsViewedCleanUsers)
        .registerFieldValue(36, articleImpressionsViewedDiscardedUsers)
        .registerFieldValue(37, clicksCleanUsers)
        .registerFieldValue(38, clicksDiscardedUsers)
        .registerFieldValue(39, impressionType)
    }
  }
}