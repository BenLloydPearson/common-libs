package com.gravity.domain.fieldconverters

import com.gravity.domain.DataMartRowConverterHelper
import com.gravity.domain.FieldConverters
import com.gravity.interests.interfaces.userfeedback.{UserFeedbackOption, UserFeedbackVariation}
import com.gravity.interests.jobs.intelligence.operations.{ImpressionPurpose, ArticleDataMartRow, ExtraArticleDataMartRow, ExtraArticleDataMartRow2}
import com.gravity.utilities.eventlogging.{FieldRegistry, FieldValueRegistry}
import com.gravity.utilities.grvfields.FieldConverter
import com.gravity.utilities.grvstrings._

trait ArticleDataMartRowConverter {
  this: FieldConverters.type =>

  import DataMartRowConverterHelper._

  implicit object ArticleDataMartRowConverter extends FieldConverter[ArticleDataMartRow] {
    val fields = new FieldRegistry[ArticleDataMartRow]("ArticleDataMartRow")
      .registerStringField(LogDate, 0, emptyString, "Date of event")
      .registerIntField(LogHour, 1, -1, "Hour of day of event")
      .registerStringField(PublisherGuid, 2, emptyString)
      .registerStringField(PublisherName, 3, emptyString)
      .registerStringField(AdvertiserGuid, 4, emptyString)
      .registerStringField(AdvertiserName, 5, emptyString)
      .registerStringField(CampaignId, 6, emptyString, "String representation of campaign key")
      .registerStringField(CampaignName, 7, emptyString)
      .registerBooleanField(IsOrganic, 8, true)
      .registerLongField(SitePlacementId, 9, -1)
      .registerStringField(SitePlacementName, 10, emptyString)
      .registerLongField(ArticleId, 11, -1)
      .registerStringField(ArticleTitle, 12, emptyString)
      .registerStringField(ArticleUrl, 13, emptyString)
      .registerStringField(ServedOnDomain, 14, emptyString)
      .registerLongField(Cpc, 15, -1, "Cost per click in pennies")
      .registerLongField(AdjustedCpc, 16, -1, "DEPRECATED")
      .registerIntField(DisplayIndex, 17, -1, "The index of the article")
      .registerStringField(UnitImpressionsServedClean, 18, emptyString, "An ! delimited list of clean impression hashes")
      .registerStringField(UnitImpressionsServedDiscarded, 19, emptyString, "An ! delimited list of discarded impression hashes")
      .registerStringField(UnitImpressionsViewedClean, 20, emptyString, "An ! delimited list of clean impression view hashes")
      .registerStringField(UnitImpressionsViewedDiscarded, 21, emptyString, "An ! delimited list of discarded impression view hashes")
      .registerLongField(ArticleImpressionsServedClean, 22, -1)
      .registerLongField(ArticleImpressionsServedDiscarded, 23, -1)
      .registerLongField(ArticleImpressionsViewedClean, 24, -1)
      .registerLongField(ArticleImpressionsViewedDiscarded, 25, -1)
      .registerLongField(ClicksClean, 26, -1)
      .registerLongField(ClicksDiscarded, 27, -1)
      .registerLongField(ConversionsClean, 28, -1)
      .registerLongField(ConversionsDiscarded, 29, -1)
      .registerStringField(UnitImpressionsServedCleanUsers, 30, emptyString)
      .registerStringField(UnitImpressionsViewedCleanUsers, 31, emptyString)
      .registerStringField(ClicksCleanUsers, 32, emptyString)
      .registerStringField(ConversionsCleanUsers, 33, emptyString)
      .registerStringField(UnitImpressionsServedDiscardedUsers, 34, emptyString)
      .registerStringField(UnitImpressionsViewedDiscardedUsers, 35, emptyString)
      .registerStringField(ClicksDiscardedUsers, 36, emptyString)
      .registerStringField(ConversionsDiscardedUsers, 37, emptyString)
      .registerDoubleField(RevShare, 38, -1.0, "DEPRECATED")
      .registerLongField(ContentGroupId, 39, -1)
      .registerStringField(ContentGroupName, 40, emptyString)
      .registerStringField(RenderType, 41, emptyString, "Identifies api, widget, or syndication, so not really a render type")
      .registerLongField(DailyBudget, 42, -1, "The max daily budget in a given day for a campaign")
      .registerLongField(WeeklyBudget, 43, -1, "The max weekly budget in a given day for a campaign, not in use")
      .registerLongField(MonthlyBudget, 44, -1, "The max monthly budget in a given day for a campaign")
      .registerLongField(TotalBudget, 45, -1, "The max total budget in a given day for a campaign")
      .registerStringField(PartnerPlacementId, 46, emptyString)
      .registerIntField(LogMinute, 47, -1, "Minute of hour of day of event")
      .registerIntField(ImpressionType, 48, -1, "1 - organic, 2 - blended, 3 - sponsored")
      .registerIntField(ImpressionPurposeId, 49, ImpressionPurpose.defaultId, "See ImpressionPurpose docs")
      .registerStringField(ImpressionPurposeName, 50, ImpressionPurpose.defaultValue.name)
      .registerIntField(UserFeedbackVariationId, 51, UserFeedbackVariation.defaultValue.id)
      .registerStringField(UserFeedbackVariationName, 52, UserFeedbackVariation.defaultValue.name)
      .registerIntField(ChosenUserFeedbackOptionId, 53, UserFeedbackOption.defaultId)
      .registerStringField(ChosenUserFeedbackOptionName, 54, UserFeedbackOption.defaultValue.name)
      .registerStringField(ExchangeGuid, 55, "Unknown Exchange Guid", "The exchange the article came from")
      .registerStringField(ExchangeHostGuid, 56, "No Exchange Host Guid Available", "The exchange host guid the article came from")
      .registerStringField(ExchangeGoal, 57, "No Exchange Goal", "The exchange goal of the article")
      .registerStringField(ExchangeName, 58, "No Exchange Name Available", "The exchange name the article came from")
      .registerStringField(ExchangeStatus, 59, "No Exchange Status Available", "The exchange status the article uses")
      .registerStringField(ExchangeType, 60, "No Exchange Type Available", "The exchange type the article uses")
      .registerFloatField(TimeSpentMedian, 61, -1.0f, "Median time spent")


    def fromValueRegistry(reg: FieldValueRegistry) = new ArticleDataMartRow(
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
      reg.getValue[Long](11),
      reg.getValue[String](12),
      reg.getValue[String](13),
      reg.getValue[String](14),
      reg.getValue[Long](15),
      reg.getValue[Long](16),
      reg.getValue[Int](17),
      reg.getValue[String](18),
      reg.getValue[String](19),
      reg.getValue[String](20),
      ExtraArticleDataMartRow(
        reg.getValue[String](21),
        reg.getValue[Long](22),
        reg.getValue[Long](23),
        reg.getValue[Long](24),
        reg.getValue[Long](25),
        reg.getValue[Long](26),
        reg.getValue[Long](27),
        reg.getValue[Long](28),
        reg.getValue[Long](29),
        reg.getValue[String](30),
        reg.getValue[String](31),
        reg.getValue[String](32),
        reg.getValue[String](33),
        reg.getValue[String](34),
        reg.getValue[String](35),
        reg.getValue[String](36),
        reg.getValue[String](37),
        reg.getValue[Double](38),
        reg.getValue[Long](39),
        reg.getValue[String](40),
        reg.getValue[String](41),
        ExtraArticleDataMartRow2(
          reg.getValue[Long](42),
          reg.getValue[Long](43),
          reg.getValue[Long](44),
          reg.getValue[Long](45),
          reg.getValue[String](46),
          reg.getValue[Int](47),
          reg.getValue[Int](48),
          reg.getValue[Int](49),
          reg.getValue[String](50),
          reg.getValue[Int](51),
          reg.getValue[String](52),
          reg.getValue[Int](53),
          reg.getValue[String](54),
          reg.getValue[String](55),
          reg.getValue[String](56),
          reg.getValue[String](57),
          reg.getValue[String](58),
          reg.getValue[String](59),
          reg.getValue[String](60),
          reg.getValue[Float](61)
        )
      )
    )

    def toValueRegistry(o: ArticleDataMartRow) = {
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
        .registerFieldValue(11, articleId)
        .registerFieldValue(12, articleTitle)
        .registerFieldValue(13, articleUrl)
        .registerFieldValue(14, servedOnDomain)
        .registerFieldValue(15, cpc)
        .registerFieldValue(16, adjustedCpc)
        .registerFieldValue(17, displayIndex)
        .registerFieldValue(18, unitImpressionsServedClean)
        .registerFieldValue(19, unitImpressionsServedDiscarded)
        .registerFieldValue(20, unitImpressionsViewedClean)
        .registerFieldValue(21, more.unitImpressionsViewedDiscarded)
        .registerFieldValue(22, more.articleImpressionsServedClean)
        .registerFieldValue(23, more.articleImpressionsServedDiscarded)
        .registerFieldValue(24, more.articleImpressionsViewedClean)
        .registerFieldValue(25, more.articleImpressionsViewedDiscarded)
        .registerFieldValue(26, more.clicksClean)
        .registerFieldValue(27, more.clicksDiscarded)
        .registerFieldValue(28, more.conversionsClean)
        .registerFieldValue(29, more.conversionsDiscarded)
        .registerFieldValue(30, more.unitImpressionsServedCleanUsers)
        .registerFieldValue(31, more.unitImpressionsViewedCleanUsers)
        .registerFieldValue(32, more.clicksCleanUsers)
        .registerFieldValue(33, more.conversionsCleanUsers)
        .registerFieldValue(34, more.unitImpressionsServedDiscardedUsers)
        .registerFieldValue(35, more.unitImpressionsViewedDiscardedUsers)
        .registerFieldValue(36, more.clicksDiscardedUsers)
        .registerFieldValue(37, more.conversionsDiscardedUsers)
        .registerFieldValue(38, more.revShare)
        .registerFieldValue(39, more.contentGroupId)
        .registerFieldValue(40, more.contentGroupName)
        .registerFieldValue(41, more.renderType)
        .registerFieldValue(42, more.more.dailyBudget)
        .registerFieldValue(43, more.more.weeklyBudget)
        .registerFieldValue(44, more.more.monthlyBudget)
        .registerFieldValue(45, more.more.totalBudget)
        .registerFieldValue(46, more.more.partnerPlacementId)
        .registerFieldValue(47, more.more.logMinute)
        .registerFieldValue(48, more.more.impressionType)
        .registerFieldValue(49, more.more.impressionPurposeId)
        .registerFieldValue(50, more.more.impressionPurposeName)
        .registerFieldValue(51, more.more.userFeedbackVariationId)
        .registerFieldValue(52, more.more.userFeedbackVariationName)
        .registerFieldValue(53, more.more.chosenUserFeedbackOptionId)
        .registerFieldValue(54, more.more.chosenUserFeedbackOptionName)
        .registerFieldValue(55, more.more.exchangeGuid)
        .registerFieldValue(56, more.more.exchangeHostGuid)
        .registerFieldValue(57, more.more.exchangeGoal)
        .registerFieldValue(58, more.more.exchangeName)
        .registerFieldValue(59, more.more.exchangeStatus)
        .registerFieldValue(60, more.more.exchangeType)
        .registerFieldValue(61, more.more.timeSpentMedian)
    }
  }
}