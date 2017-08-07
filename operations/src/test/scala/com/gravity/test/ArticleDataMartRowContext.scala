package com.gravity.test

import com.gravity.interests.interfaces.userfeedback.{UserFeedbackOption, UserFeedbackVariation}
import com.gravity.interests.jobs.intelligence.CampaignKey
import com.gravity.interests.jobs.intelligence.operations.{ArticleDataMartRow, ImpressionPurpose}
import com.gravity.valueclasses.ValueClassesForDomain.{ExchangeGuid, SiteGuid}
import org.joda.time.DateTime

import scala.collection.Seq

case class ArticleDataMartRowContext(rows: Seq[ArticleDataMartRow])

case class ArticleDataMartRowGenerationContext(
  date: DateTime,
  advertiserGuid: SiteGuid,
  ck: CampaignKey,
  placementId: Long,
  articleId: Long,
  numImps: Int = 1,
  numImpDiscards: Int = 1,
  numViews: Int = 1,
  numViewDiscards: Int = 1,
  numClicks: Int = 1,
  numClickDiscards: Int = 1,
  numConversions: Int = 1,
  numConversionDiscards: Int = 1,
  publisherGuid: SiteGuid = SiteGuid.empty,
  userFeedbackVariation: UserFeedbackVariation.Type = UserFeedbackVariation.defaultValue,
  impressionPurpose: ImpressionPurpose.Type = ImpressionPurpose.userFeedback,
  userFeedbackOption: UserFeedbackOption.Type = UserFeedbackOption.defaultValue,
  exchangeGuid: String = ExchangeGuid.toString,
  exchangeHostGuid: String = SiteGuid.toString,
  exchangeGoal: String = "exchange goal",
  exchangeName: String = "exchange Name",
  exchangeStatus: String = "exchange Status",
  exchangeType: String = "exchange Type"
)

object ArticleDataMartRowGenerationContext {
  val defaultRow = ArticleDataMartRowGenerationContext(new DateTime, SiteGuid("12345678"), CampaignKey("12345678", 11L), 101L, 1001L)

  def defaultRows(count: Int) = {
    List.fill(count)(defaultRow)
  }
}