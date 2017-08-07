package com.gravity.test

import com.gravity.interests.interfaces.userfeedback.{UserFeedbackPresentation, UserFeedbackVariation}
import com.gravity.interests.jobs.intelligence.operations.{ImpressionPurpose, UnitImpressionDataMartRow}
import com.gravity.valueclasses.ValueClassesForDomain.{ExchangeGuid, SiteGuid}
import org.joda.time.DateTime

import scala.collection.Seq

case class UnitImpressionDataMartRowContext(rows: Seq[UnitImpressionDataMartRow])

case class UnitImpressionDataMartRowGenerationContext(
  date: DateTime,
  publisherGuid: SiteGuid,
  isControl: Boolean,
  countryCode: String,
  deviceType: String,
  placementId: Long,
  impressionType: Int = 1,
  numImps: Int = 1,
  numImpDiscards: Int = 1,
  numViews: Int = 1,
  numViewDiscards: Int = 1,
  numClicks: Int = 1,
  numClickDiscards: Int = 1,
  userFeedbackVariation: UserFeedbackVariation.Type = UserFeedbackVariation.defaultValue,
  impressionPurpose: Option[ImpressionPurpose.Type] = Some(ImpressionPurpose.userFeedback),
  userFeedbackPresentation: UserFeedbackPresentation.Type = UserFeedbackPresentation.defaultValue,
  exchangeGuid: String = ExchangeGuid.toString,
  exchangeHostGuid: String = SiteGuid.toString,
  exchangeGoal: String = "exchange goal",
  exchangeName: String = "exchange Name",
  exchangeStatus: String = "exchange Status",
  exchangeType: String = "exchange Type"
)

object UnitImpressionDataMartRowGenerationContext {
  val defaultRow = UnitImpressionDataMartRowGenerationContext(new DateTime, SiteGuid("12345678"), isControl = false, "US", "Desktop", 101L)

  def defaultRows(count: Int) = {
    List.fill(count)(defaultRow)
  }
}