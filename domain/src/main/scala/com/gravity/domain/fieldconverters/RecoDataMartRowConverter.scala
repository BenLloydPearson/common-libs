package com.gravity.domain.fieldconverters

import com.gravity.domain.FieldConverters
import com.gravity.interests.jobs.intelligence.operations._
import com.gravity.utilities.eventlogging.{FieldRegistry, FieldValueRegistry}
import com.gravity.utilities.grvfields.FieldConverter
import com.gravity.utilities.grvstrings._

trait RecoDataMartRowConverter {
  this: FieldConverters.type =>

  implicit object RecoDataMartRowConverter extends FieldConverter[RecoDataMartRow] {
    val fields = new FieldRegistry[RecoDataMartRow]("RecoDataMartRow")
      .registerStringField("logDate", 0, emptyString, "Date of impression event")
      .registerIntField("logHour", 1, -1, "Hour of day of impression event")
      .registerStringField("publisherGuid", 2, emptyString)
      .registerStringField("publisherName", 3, emptyString)
      .registerStringField("advertiserGuid", 4, emptyString)
      .registerStringField("advertiserName", 5, emptyString)
      .registerStringField("campaignId", 6, emptyString, "String representation of campaign key")
      .registerStringField("campaignName", 7, emptyString)
      .registerBooleanField("isOrganic", 8, true)
      .registerLongField("sitePlacementId", 9, -1)
      .registerStringField("sitePlacementName", 10, emptyString)
      .registerIntField("recoAlgo", 11, -1, "Identifier for the recommendation algorithm")
      .registerStringField("recoAlgoName", 12, emptyString)
      .registerIntField("recoBucket", 13, -1, "Identifies the site placement bucket")
      .registerLongField("cpc", 14, -1, "Cost per click in pennies")
      .registerLongField("adjustedCpc", 15, -1, "DEPRECATED")
      .registerStringField("unitImpressionsServedClean", 16, emptyString, "A comma separated list of clean impression hashes")
      .registerStringField("unitImpressionsServedDiscarded", 17, emptyString, "A comma separated list of discarded impression hashes")
      .registerStringField("unitImpressionsViewedClean", 18, emptyString, "A comma separated list of clean view hashes")
      .registerStringField("unitImpressionsViewedDiscarded", 19, emptyString, "A comma separated list of discarded view hashes")
      .registerLongField("articleImpressionsServedClean", 20, -1, "The number of clean article impressions")
      .registerLongField("articleImpressionsServedDiscarded", 21, -1, "The number of discarded article impressions")
      .registerLongField("articleImpressionsViewedClean", 22, -1, "The number of clean article views")
      .registerLongField("articleImpressionsViewedDiscarded", 23, -1, "The number of discarded article views")
      .registerLongField("clicksClean", 24, -1, "The number of clean clicks")
      .registerLongField("clicksDiscarded", 25, -1, "The number of discarded clicks")
      .registerBooleanField("isControl", 26, false, "Identifies whether part of a control group of impressions or not")
      .registerDoubleField("revShare", 27, -1.0, "DEPRECATED")
      .registerStringField("unitImpressionsServedCleanUsers", 28, emptyString, "A comma separate list of user guids from clean impressions")
      .registerStringField("unitImpressionsViewedCleanUsers", 29, emptyString, "A comma separate list of user guids from clean views")
      .registerLongField("contentGroupId", 30, -1)
      .registerStringField("contentGroupName", 31, emptyString)
      .registerIntField("logMinute", 32, -1, "Minute of hour of impression event")
      .registerStringField("clicksCleanUsers", 33, emptyString, "A comma separate list of user guids from clean clicks")
      .registerStringField("unitImpressionsServedDiscardedUsers", 34, emptyString, "A comma separate list of user guids from discarded impressions")
      .registerStringField("unitImpressionsViewedDiscardedUsers", 35, emptyString, "A comma separate list of user guids from discarded views")
      .registerStringField("clicksDiscardedUsers", 36, emptyString, "A comma separate list of user guids from discarded clicks")
      .registerStringField("renderType", 37, emptyString, "Identifies api, widget, or syndication, so not really a render type")
      .registerLongField("dailyBudget", 38, -1, "The max daily budget in a given day for a campaign")
      .registerLongField("weeklyBudget", 39, -1, "The max weekly budget in a given day for a campaign, not in use")
      .registerLongField("monthlyBudget", 40, -1, "The max monthly budget in a given day for a campaign")
      .registerLongField("totalBudget", 41, -1, "The max total budget in a given day for a campaign")
      .registerStringField("partnerPlacementId", 42, emptyString)
      .registerStringField("algoSettingsData", 43, emptyString, "The json serialized form of the algo settings data")
      .registerIntField("displayIndex", 44, -1, "The index of the article")
      .registerIntField("impressionType", 45, -1, "1 - organic, 2 - blended, 3 - sponsored")
      .registerStringField("geoRollup", 46, emptyString, "US or Intl")
      .registerStringField("deviceTypeRollup", 47, emptyString, "Computer, Mobile, or Unknown")
      .registerIntField("articleSlotsIndex", 48, -1, "The slot index of the article")
      .registerLongField("recommenderId", 49, -1, "The recommender used to make the article recommendation")
      .registerSeqField[AlgoSettingsData]("algoSettings", 50, Seq.empty[AlgoSettingsData])
      .registerSeqField[ArticleAggData]("articleAggregates", 51, Seq.empty[ArticleAggData])
      .registerDoubleField("displayCorrection", 52, 100.0)
      .registerStringField("exchangeGuid", 53, "Unknown Exchange Guid", "The exchange the article came from")
      .registerStringField("exchangeHostGuid", 54, "No Exchange Host Guid Available", "The exchange host guid the article came from")
      .registerStringField("exchangeGoal", 55, "No Exchange Goal", "The exchange goal of the article")
      .registerStringField("exchangeName", 56, "No Exchange Name Available", "The exchange name the article came from")
      .registerStringField("exchangeStatus", 57, "No Exchange Status Available", "The exchange status the article uses")
      .registerStringField("exchangeType", 58, "No Exchange Type Available", "The exchange type the article uses")

    def fromValueRegistry(reg: FieldValueRegistry) = new RecoDataMartRow(
      reg.getValue[String](0),
      reg.getValue[Int](1),
      reg.getValue[String](2),
      reg.getValue[String](3),
      reg.getValue[String](4),
      reg.getValue[String](5),
      reg.getValue[String](6),
      reg.getValue[String](7),
      reg.getValue[Boolean](8),
      reg.getValue[Long](9),
      reg.getValue[String](10),
      reg.getValue[Int](11),
      reg.getValue[String](12),
      reg.getValue[Int](13),
      reg.getValue[Long](14),
      reg.getValue[Long](15),
      reg.getValue[String](16),
      reg.getValue[String](17),
      reg.getValue[String](18),
      reg.getValue[String](19),
      reg.getValue[Long](20),
      ExtraRecoDataMartRow(
        reg.getValue[Long](21),
        reg.getValue[Long](22),
        reg.getValue[Long](23),
        reg.getValue[Long](24),
        reg.getValue[Long](25),
        reg.getValue[Boolean](26),
        reg.getValue[Double](27),
        reg.getValue[String](28),
        reg.getValue[String](29),
        reg.getValue[Long](30),
        reg.getValue[String](31),
        reg.getValue[Int](32),
        reg.getValue[String](33),
        reg.getValue[String](34),
        reg.getValue[String](35),
        reg.getValue[String](36),
        reg.getValue[String](37),
        reg.getValue[Long](38),
        reg.getValue[Long](39),
        reg.getValue[Long](40),
        reg.getValue[Long](41),
        ExtraRecoDataMartRow2(
          reg.getValue[String](42),
          reg.getValue[String](43),
          reg.getValue[Int](44),
          reg.getValue[Int](45),
          reg.getValue[String](46),
          reg.getValue[String](47),
          reg.getValue[Int](48),
          reg.getValue[Long](49),
          reg.getValue[Seq[AlgoSettingsData]](50),
          reg.getValue[Seq[ArticleAggData]](51),
          reg.getValue[Double](52),
          reg.getValue[String](53),
          reg.getValue[String](54),
          reg.getValue[String](55),
          reg.getValue[String](56),
          reg.getValue[String](57),
          reg.getValue[String](58)
        )
      )
    )

    def toValueRegistry(o: RecoDataMartRow) = {
      import o._
      new FieldValueRegistry(fields)
        .registerFieldValue(0, logDate)
        .registerFieldValue(1, logHour)
        .registerFieldValue(2, publisherGuid)
        .registerFieldValue(3, publisherName)
        .registerFieldValue(4, advertiserGuid)
        .registerFieldValue(5, advertiserName)
        .registerFieldValue(6, campaignId)
        .registerFieldValue(7, campaignName)
        .registerFieldValue(8, isOrganic)
        .registerFieldValue(9, sitePlacementId)
        .registerFieldValue(10, sitePlacementName)
        .registerFieldValue(11, recoAlgo)
        .registerFieldValue(12, recoAlgoName)
        .registerFieldValue(13, recoBucket)
        .registerFieldValue(14, cpc)
        .registerFieldValue(15, adjustedCpc)
        .registerFieldValue(16, unitImpressionsServedClean)
        .registerFieldValue(17, unitImpressionsServedDiscarded)
        .registerFieldValue(18, unitImpressionsViewedClean)
        .registerFieldValue(19, unitImpressionsViewedDiscarded)
        .registerFieldValue(20, articleImpressionsServedClean)
        .registerFieldValue(21, more.articleImpressionsServedDiscarded)
        .registerFieldValue(22, more.articleImpressionsViewedClean)
        .registerFieldValue(23, more.articleImpressionsViewedDiscarded)
        .registerFieldValue(24, more.clicksClean)
        .registerFieldValue(25, more.clicksDiscarded)
        .registerFieldValue(26, more.isControl)
        .registerFieldValue(27, more.revShare)
        .registerFieldValue(28, more.unitImpressionsServedCleanUsers)
        .registerFieldValue(29, more.unitImpressionsViewedCleanUsers)
        .registerFieldValue(30, more.contentGroupId)
        .registerFieldValue(31, more.contentGroupName)
        .registerFieldValue(32, more.logMinute)
        .registerFieldValue(33, more.clicksCleanUsers)
        .registerFieldValue(34, more.unitImpressionsServedDiscardedUsers)
        .registerFieldValue(35, more.unitImpressionsViewedDiscardedUsers)
        .registerFieldValue(36, more.clicksDiscardedUsers)
        .registerFieldValue(37, more.renderType)
        .registerFieldValue(38, more.dailyBudget)
        .registerFieldValue(39, more.weeklyBudget)
        .registerFieldValue(40, more.monthlyBudget)
        .registerFieldValue(41, more.totalBudget)
        .registerFieldValue(42, more.more.partnerPlacementId)
        .registerFieldValue(43, more.more.algoSettingsData)
        .registerFieldValue(44, more.more.displayIndex)
        .registerFieldValue(45, more.more.impressionType)
        .registerFieldValue(46, more.more.geoRollup)
        .registerFieldValue(47, more.more.deviceTypeRollup)
        .registerFieldValue(48, more.more.articleSlotsIndex)
        .registerFieldValue(49, more.more.recommenderId)
        .registerFieldValue(50, more.more.algoSettings)
        .registerFieldValue(51, more.more.articleAggregates)
        .registerFieldValue(52, more.more.displayCorrection)
        .registerFieldValue(53, more.more.exchangeGuid)
        .registerFieldValue(54, more.more.exchangeHostGuid)
        .registerFieldValue(55, more.more.exchangeGoal)
        .registerFieldValue(56, more.more.exchangeName)
        .registerFieldValue(57, more.more.exchangeStatus)
        .registerFieldValue(58, more.more.exchangeType)
    }
  }
}
