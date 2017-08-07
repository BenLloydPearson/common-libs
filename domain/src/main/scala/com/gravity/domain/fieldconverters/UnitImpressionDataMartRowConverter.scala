package com.gravity.domain.fieldconverters

import com.gravity.domain.DataMartRowConverterHelper
import com.gravity.domain.FieldConverters
import com.gravity.interests.interfaces.userfeedback.{UserFeedbackPresentation, UserFeedbackVariation}
import com.gravity.interests.jobs.intelligence.operations.{ImpressionPurpose, ExtraUnitImpressionDataMartRow, ExtraUnitImpressionDataMartRow2, UnitImpressionDataMartRow}
import com.gravity.utilities.eventlogging.{FieldRegistry, FieldValueRegistry}
import com.gravity.utilities.grvfields.FieldConverter
import com.gravity.utilities.grvstrings._

trait UnitImpressionDataMartRowConverter {
  this: FieldConverters.type =>

  import DataMartRowConverterHelper._

  implicit object UnitImpressionDataMartRowConverter extends FieldConverter[UnitImpressionDataMartRow] {
    val fields = new FieldRegistry[UnitImpressionDataMartRow]("UnitImpressionDataMartRow")
      .registerStringField(LogDate, 0, emptyString, "Date of event")
      .registerIntField(LogHour, 1, -1, "Hour of day of event")
      .registerStringField(PublisherGuid, 2, emptyString)
      .registerStringField(PublisherName, 3, emptyString)
      .registerStringField(ServedOnDomain, 4, emptyString)
      .registerIntField(RecoBucket, 5, -1, "Identifies the site placement bucket")
      .registerBooleanField(IsControl, 6, false)
      .registerStringField(CountryCode, 7, emptyString)
      .registerStringField(Country, 8, emptyString)
      .registerStringField(State, 9, emptyString)
      .registerStringField(DmaCode, 10, emptyString)
      .registerStringField(GeoRollup, 11, emptyString)
      .registerStringField(DeviceType, 12, emptyString)
      .registerStringField(Browser, 13, emptyString)
      .registerStringField(BrowserManufacturer, 14, emptyString)
      .registerStringField(BrowserRollup, 15, emptyString)
      .registerStringField(OperatingSystem, 16, emptyString)
      .registerStringField(OperatingSystemManufacturer, 17, emptyString)
      .registerStringField(OperatingSystemRollup, 18, emptyString)
      .registerStringField(RenderType, 19, emptyString)
      .registerLongField(SitePlacementId, 20, -1)
      .registerStringField(SitePlacementName, 21, emptyString)
      .registerLongField(UnitImpressionsServedClean, 22, 0L)
      .registerLongField(UnitImpressionsServedDiscarded, 23, 0L)
      .registerLongField(UnitImpressionsViewedClean, 24, 0L)
      .registerLongField(UnitImpressionsViewedDiscarded, 25, 0L)
      .registerLongField(OrganicClicksClean, 26, 0L)
      .registerLongField(OrganicClicksDiscarded, 27, 0L)
      .registerLongField(SponsoredClicksClean, 28, 0L)
      .registerLongField(SponsoredClicksDiscarded, 29, 0L)
      .registerLongField(RevenueClean, 30, 0L)
      .registerLongField(RevenueDiscarded, 31, 0L)
      .registerStringField(PartnerPlacementId, 32, emptyString)
      .registerStringField(UnitImpressionsServedCleanUsers, 33, emptyString, "A comma separate list of user guids from clean impressions")
      .registerStringField(UnitImpressionsServedDiscardedUsers, 34, emptyString, "A comma separate list of user guids from discarded impressions")
      .registerStringField(UnitImpressionsViewedCleanUsers, 35, emptyString, "A comma separate list of user guids from clean views")
      .registerStringField(UnitImpressionsViewedDiscardedUsers, 36, emptyString, "A comma separate list of user guids from discarded views")
      .registerStringField(ClicksCleanUsers, 37, emptyString, "A comma separate list of user guids from clean clicks")
      .registerStringField(ClicksDiscardedUsers, 38, emptyString, "A comma separate list of user guids from discarded clicks")
      .registerLongField(MinServedCpc, 39, -1)
      .registerLongField(MinClickedCpc, 40, -1)
      .registerIntField(ImpressionType, 41, -1, "1 - organic, 2 - blended, 3 - sponsored")
      .registerStringField(ReferrerDomain, 42, emptyString)
      .registerIntField(ImpressionPurposeId, 43, ImpressionPurpose.defaultId, "See ImpressionPurpose docs")
      .registerStringField(ImpressionPurposeName, 44, ImpressionPurpose.defaultValue.name)
      .registerIntField(UserFeedbackVariationId, 45, UserFeedbackVariation.defaultValue.id)
      .registerStringField(UserFeedbackVariationName, 46, UserFeedbackVariation.defaultValue.name)
      .registerIntField(UserFeedbackPresentationId, 47, UserFeedbackPresentation.defaultValue.id)
      .registerStringField(UserFeedbackPresentationName, 48, UserFeedbackPresentation.defaultValue.name)
      .registerStringField(ExchangeGuid, 49, "Unknown Exchange Guid", "The exchange the article came from")
      .registerStringField(ExchangeHostGuid, 50, "No Exchange Host Guid Available", "The exchange host guid the article came from")
      .registerStringField(ExchangeGoal, 51, "No Exchange Goal", "The exchange goal of the article")
      .registerStringField(ExchangeName, 52, "No Exchange Name Available", "The exchange name the article came from")
      .registerStringField(ExchangeStatus, 53, "No Exchange Status Available", "The exchange status the article uses")
      .registerStringField(ExchangeType, 54, "No Exchange Type Available", "The exchange type the article uses")


    def fromValueRegistry(reg: FieldValueRegistry) = new UnitImpressionDataMartRow(
      reg.getValue[String](0),
      reg.getValue[Int](1),
      reg.getValue[String](2),
      reg.getValue[String](3),
      reg.getValue[String](4),
      reg.getValue[Int](5),
      reg.getValue[Boolean](6),
      reg.getValue[String](7),
      reg.getValue[String](8),
      reg.getValue[String](9),
      reg.getValue[String](10),
      reg.getValue[String](11),
      reg.getValue[String](12),
      reg.getValue[String](13),
      reg.getValue[String](14),
      reg.getValue[String](15),
      reg.getValue[String](16),
      reg.getValue[String](17),
      reg.getValue[String](18),
      reg.getValue[String](19),
      reg.getValue[Long](20),
      ExtraUnitImpressionDataMartRow(
        reg.getValue[String](21),
        reg.getValue[Long](22),
        reg.getValue[Long](23),
        reg.getValue[Long](24),
        reg.getValue[Long](25),
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
        reg.getValue[Long](39),
        reg.getValue[Long](40),
        reg.getValue[Int](41),
        ExtraUnitImpressionDataMartRow2(
          reg.getValue[String](42),
          reg.getValue[Int](43),
          reg.getValue[String](44),
          reg.getValue[Int](45),
          reg.getValue[String](46),
          reg.getValue[Int](47),
          reg.getValue[String](48),
          reg.getValue[String](49),
          reg.getValue[String](50),
          reg.getValue[String](51),
          reg.getValue[String](52),
          reg.getValue[String](53),
          reg.getValue[String](54)
        )
      )
    )

    def toValueRegistry(o: UnitImpressionDataMartRow) = {
      import o._
      new FieldValueRegistry(fields)
        .registerFieldValue(0, logDate)
        .registerFieldValue(1, logHour)
        .registerFieldValue(2, publisherGuid)
        .registerFieldValue(3, publisherName)
        .registerFieldValue(4, servedOnDomain)
        .registerFieldValue(5, recoBucket)
        .registerFieldValue(6, isControl)
        .registerFieldValue(7, countryCode)
        .registerFieldValue(8, country)
        .registerFieldValue(9, state)
        .registerFieldValue(10, dmaCode)
        .registerFieldValue(11, geoRollup)
        .registerFieldValue(12, deviceType)
        .registerFieldValue(13, browser)
        .registerFieldValue(14, browserManufacturer)
        .registerFieldValue(15, browserRollup)
        .registerFieldValue(16, operatingSystem)
        .registerFieldValue(17, operatingSystemManufacturer)
        .registerFieldValue(18, operatingSystemRollup)
        .registerFieldValue(19, renderType)
        .registerFieldValue(20, sitePlacementId)
        .registerFieldValue(21, more.sitePlacementName)
        .registerFieldValue(22, more.unitImpressionsServedClean)
        .registerFieldValue(23, more.unitImpressionsServedDiscarded)
        .registerFieldValue(24, more.unitImpressionsViewedClean)
        .registerFieldValue(25, more.unitImpressionsViewedDiscarded)
        .registerFieldValue(26, more.organicClicksClean)
        .registerFieldValue(27, more.organicClicksDiscarded)
        .registerFieldValue(28, more.sponsoredClicksClean)
        .registerFieldValue(29, more.sponsoredClicksDiscarded)
        .registerFieldValue(30, more.revenueClean)
        .registerFieldValue(31, more.revenueDiscarded)
        .registerFieldValue(32, more.partnerPlacementId)
        .registerFieldValue(33, more.unitImpressionsServedCleanUsers)
        .registerFieldValue(34, more.unitImpressionsServedDiscardedUsers)
        .registerFieldValue(35, more.unitImpressionsViewedCleanUsers)
        .registerFieldValue(36, more.unitImpressionsViewedDiscardedUsers)
        .registerFieldValue(37, more.clicksCleanUsers)
        .registerFieldValue(38, more.clicksDiscardedUsers)
        .registerFieldValue(39, more.minServedCpc)
        .registerFieldValue(40, more.minClickedCpc)
        .registerFieldValue(41, more.impressionType)
        .registerFieldValue(42, more.more.referrerDomain)
        .registerFieldValue(43, more.more.impressionPurposeId)
        .registerFieldValue(44, more.more.impressionPurposeName)
        .registerFieldValue(45, more.more.userFeedbackVariationId)
        .registerFieldValue(46, more.more.userFeedbackVariationName)
        .registerFieldValue(47, more.more.userFeedbackPresentationId)
        .registerFieldValue(48, more.more.userFeedbackPresentationName)
    }
  }
}