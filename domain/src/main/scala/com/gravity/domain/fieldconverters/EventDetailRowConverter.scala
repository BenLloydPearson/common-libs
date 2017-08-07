package com.gravity.domain.fieldconverters

import com.gravity.domain.DataMartRowConverterHelper._
import com.gravity.domain.FieldConverters
import com.gravity.interests.interfaces.userfeedback.{UserFeedbackOption, UserFeedbackVariation}
import com.gravity.interests.jobs.intelligence.operations._
import com.gravity.utilities.eventlogging.{FieldRegistry, FieldValueRegistry}
import com.gravity.utilities.grvfields.FieldConverter
import com.gravity.utilities.grvstrings._
import com.gravity.utilities.grvtime
import org.joda.time.DateTime

trait EventDetailAlgoSettingsDimConverter {
  this: FieldConverters.type =>

  implicit object EventDetailAlgoSettingsDimConverter extends FieldConverter[EventDetailAlgoSettingsDimRow] {
    val fields = new FieldRegistry[EventDetailAlgoSettingsDimRow]("EventDetailAlgoSettingsDimRow")
      .registerLongField("algoSettingsHash", 0, 0L, "murmur hash used to lookup the json serialized form of the algo settings data")
      .registerSeqField[AlgoSettingsData]("algoSettingsData", 1, Seq.empty[AlgoSettingsData])
      .registerStringField("algoSettingsDataString", 2, emptyString, "String representation of the algoSettings field")

    def fromValueRegistry(reg: FieldValueRegistry) = new EventDetailAlgoSettingsDimRow(
      reg.getValue[Long](0),
      reg.getValue[Seq[AlgoSettingsData]](1),
      reg.getValue[String](2)
    )

    def toValueRegistry(o: EventDetailAlgoSettingsDimRow) = {
      import o._
      new FieldValueRegistry(fields)
        .registerFieldValue(0, algoSettingsHash)
        .registerFieldValue(1, algoSettingsData)
        .registerFieldValue(2, algoSettingsDataString)
    }
  }
}

trait EventDetailRowConverter {
  this: FieldConverters.type =>

  implicit object EventDetailRowConverter extends FieldConverter[EventDetailRow] {
    val fields = new FieldRegistry[EventDetailRow]("EventDetailRow")
      .registerIntField(LogDate, 0, 0, "Date of event stored as int in format of yyyyMMdd")
      .registerIntField(LogHour, 1, -1, "Hour of day of event")
      .registerIntField(LogMinute, 2, -1, "Minute of hour of event")
      .registerLongField("publisherGuidHash", 3, 0L)
      .registerLongField("advertiserGuidHash", 4, 0L)
      .registerLongField("campaignIdHash", 5, 0L, "Hash of String representation of campaign key")
      .registerLongField(SitePlacementId, 6, -1)
      .registerLongField(ArticleId, 7, -1)
      .registerIntField(DisplayIndex, 8, -1)
      .registerLongField(Cpc, 9, -1, "Cost per click in pennies")
      .registerLongField(ContentGroupId, 10, -1)
      .registerStringField("referrer", 11, emptyString)
      .registerStringField(ReferrerDomain, 12, emptyString)
      .registerStringField("currentUrl", 13, emptyString)
      .registerStringField(ServedOnDomain, 14, emptyString)
      .registerIntField("geoLocationId", 15, 0)
      .registerLongField("userAgentHash", 16, 0L)
      .registerStringField(RenderType, 17, emptyString, "Identifies api, widget, or syndication, so not really a render type")
      .registerStringField(PartnerPlacementId, 18, emptyString)
      .registerIntField(ImpressionType, 19, -1, "1 - organic, 2 - blended, 3 - sponsored")
      .registerIntField("recoAlgo", 20, -1, "Identifier for the recommendation algorithm")
      .registerIntField(RecoBucket, 21, -1, "Identifies the site placement bucket")
      .registerBooleanField("isMobile", 22, false)
      .registerBooleanField(IsOrganic, 23, true)
      .registerBooleanField("isOptedOut", 24, false, "is the user opted out")
      .registerLongField("algoSettingsHash", 25, 0L, "murmur hash used to lookup the json serialized form of the algo settings data")
      .registerStringField("ipAddress", 26, emptyString, "ipAddress of client")
      .registerStringField("contextPath", 27, emptyString)
      .registerLongField("rawUrlHash", 28, 0L)
      .registerLongField("userGuidHash", 29, 0L)
      .registerLongField("pageViewIdHash", 30, 0L, "A hash of the page view ID values joinable to widgetDomReady and impressionViewed events")
      .registerUnencodedStringField("pageViewIdWidgetLoaderWindowUrl", 31, emptyString, "URL of the window in which widget loader was sourced")
      .registerLongField("pageViewIdRand", 32, 0L, "Random number generated on the client to increase cardinality of page view ID")
      .registerDateTimeField("pageViewIdTime", 33, grvtime.epochDateTime, "Millis time when page view ID was generated on the client")
      .registerDateTimeField("recoGenerationDate", 34, grvtime.epochDateTime)
      .registerIntField("articleSlotsIndex", 35, -1, "The slot index of the article")
      .registerLongField("recommenderId", 36, -1, "The recommender used to make the article recommendation")
      .registerLongField("clientTime", 37, 0L, "the time provided by the viewer's browser")
      .registerIntField("placementId", 38, -1)
      .registerIntField("currentAlgo", 39, -1)
      .registerIntField("currentBucket", 40, -1)
      .registerUnencodedStringField("sponseeGuid", 41, emptyString)
      .registerLongField("sourceKeyHash", 42, 0L, "hashed for dimension lookup, associated with the contentGroupId")
      .registerBooleanField("isCleanArticleImpression", 43, false)
      .registerBooleanField("isDiscardedArticleImpression", 44, false)
      .registerIntField("cleanViewCount", 45, 0)
      .registerIntField("discardedViewCount", 46, 0)
      .registerIntField("cleanClickCount", 47, 0)
      .registerIntField("discardedClickCount", 48, 0)
      .registerIntField(ImpressionPurposeId, 49, ImpressionPurpose.defaultId, "See ImpressionPurpose docs")
      .registerStringField(ImpressionPurposeName, 50, ImpressionPurpose.defaultValue.name)
      .registerIntField(UserFeedbackVariationId, 51, UserFeedbackVariation.defaultValue.id)
      .registerStringField(UserFeedbackVariationName, 52, UserFeedbackVariation.defaultValue.name)
      .registerIntField(ChosenUserFeedbackOptionId, 53, UserFeedbackOption.defaultId)
      .registerStringField(ChosenUserFeedbackOptionName, 54, UserFeedbackOption.defaultValue.name)
      .registerStringField("impressionHashHex", 55, emptyString)

    def fromValueRegistry(reg: FieldValueRegistry) = new EventDetailRow(
      reg.getValue[Int](0),
      reg.getValue[Int](1),
      reg.getValue[Int](2),
      reg.getValue[Long](3),
      reg.getValue[Long](4),
      reg.getValue[Long](5),
      reg.getValue[Long](6),
      reg.getValue[Long](7),
      reg.getValue[Int](8),
      reg.getValue[Long](9),
      reg.getValue[Long](10),
      reg.getValue[String](11),
      reg.getValue[String](12),
      reg.getValue[String](13),
      reg.getValue[String](14),
      reg.getValue[Int](15),
      reg.getValue[Long](16),
      reg.getValue[String](17),
      reg.getValue[String](18),
      reg.getValue[Int](19),
      reg.getValue[Int](20),
      ExtraEventDetailRow(
        reg.getValue[Int](21),
        reg.getValue[Boolean](22),
        reg.getValue[Boolean](23),
        reg.getValue[Boolean](24),
        reg.getValue[Long](25),
        reg.getValue[String](26),
        reg.getValue[String](27),
        reg.getValue[Long](28),
        reg.getValue[Long](29),
        reg.getValue[Long](30),
        reg.getValue[String](31),
        reg.getValue[Long](32),
        reg.getValue[DateTime](33),
        reg.getValue[DateTime](34),
        reg.getValue[Int](35),
        reg.getValue[Long](36),
        reg.getValue[Long](37),
        reg.getValue[Int](38),
        reg.getValue[Int](39),
        reg.getValue[Int](40),
        reg.getValue[String](41),
        ExtraEventDetailRow2(
          reg.getValue[Long](42),
          reg.getValue[Boolean](43),
          reg.getValue[Boolean](44),
          reg.getValue[Int](45),
          reg.getValue[Int](46),
          reg.getValue[Int](47),
          reg.getValue[Int](48),
          reg.getValue[Int](49),
          reg.getValue[String](50),
          reg.getValue[Int](51),
          reg.getValue[String](52),
          reg.getValue[Int](53),
          reg.getValue[String](54),
          reg.getValue[String](55)
        )
      )
    )

    def toValueRegistry(o: EventDetailRow) = {
      import o._
      new FieldValueRegistry(fields)
        .registerFieldValue(0, logDate)
        .registerFieldValue(1, logHour)
        .registerFieldValue(2, logMinute)
        .registerFieldValue(3, publisherGuidHash)
        .registerFieldValue(4, advertiserGuidHash)
        .registerFieldValue(5, campaignIdHash)
        .registerFieldValue(6, sitePlacementId)
        .registerFieldValue(7, articleId)
        .registerFieldValue(8, displayIndex)
        .registerFieldValue(9, cpc)
        .registerFieldValue(10, contentGroupId)
        .registerFieldValue(11, referrer)
        .registerFieldValue(12, referrerDomain)
        .registerFieldValue(13, currentUrl)
        .registerFieldValue(14, servedOnDomain)
        .registerFieldValue(15, geoLocationId)
        .registerFieldValue(16, userAgentHash)
        .registerFieldValue(17, renderType)
        .registerFieldValue(18, partnerPlacementId)
        .registerFieldValue(19, impressionType)
        .registerFieldValue(20, recoAlgo)
        .registerFieldValue(21, more.recoBucket)
        .registerFieldValue(22, more.isMobile)
        .registerFieldValue(23, more.isOrganic)
        .registerFieldValue(24, more.isOptedOut)
        .registerFieldValue(25, more.algoSettingsHash)
        .registerFieldValue(26, more.ipAddress)
        .registerFieldValue(27, more.contextPath)
        .registerFieldValue(28, more.rawUrlHash)
        .registerFieldValue(29, more.userGuidHash)
        .registerFieldValue(30, more.pageViewIdHash)
        .registerFieldValue(31, more.pageViewIdWidgetLoaderWindowUrl)
        .registerFieldValue(32, more.pageViewIdRand)
        .registerFieldValue(33, more.pageViewIdTime)
        .registerFieldValue(34, more.recoGenerationDate)
        .registerFieldValue(35, more.articleSlotsIndex)
        .registerFieldValue(36, more.recommenderId)
        .registerFieldValue(37, more.clientTime)
        .registerFieldValue(38, more.placementId)
        .registerFieldValue(39, more.currentAlgo)
        .registerFieldValue(40, more.currentBucket)
        .registerFieldValue(41, more.sponseeGuid)
        .registerFieldValue(42, more.more.sourceKeyHash)
        .registerFieldValue(43, more.more.isCleanArticleImpression)
        .registerFieldValue(44, more.more.isDiscardedArticleImpression)
        .registerFieldValue(45, more.more.cleanViewCount)
        .registerFieldValue(46, more.more.discardedViewCount)
        .registerFieldValue(47, more.more.cleanClickCount)
        .registerFieldValue(48, more.more.discardedClickCount)
        .registerFieldValue(49, more.more.impressionPurposeId)
        .registerFieldValue(50, more.more.impressionPurposeName)
        .registerFieldValue(51, more.more.userFeedbackVariationId)
        .registerFieldValue(52, more.more.userFeedbackVariationName)
        .registerFieldValue(53, more.more.chosenUserFeedbackOptionId)
        .registerFieldValue(54, more.more.chosenUserFeedbackOptionName)
        .registerFieldValue(55, more.more.impressionHashHex)
    }
  }
}
