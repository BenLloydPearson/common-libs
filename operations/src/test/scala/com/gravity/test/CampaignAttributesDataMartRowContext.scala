package com.gravity.test

import com.gravity.interests.jobs.intelligence.operations.CampaignAttributesDataMartRow
import com.gravity.valueclasses.ValueClassesForDomain.{ExchangeGuid, SiteGuid}
import org.joda.time.DateTime

case class CampaignAttributesDataMartRowContext(rows: Seq[CampaignAttributesDataMartRow])

case class CampaignAttributesDataMartRowGenerationContext(
  date: DateTime,
  publisherGuid: SiteGuid,
  placementId: Long,
  articleImps: Int = 1,
  articleImpsDiscarded: Int = 1,
  articleViews: Int = 1,
  articleViewDiscards: Int = 1,
  exchangeGuid: String = ExchangeGuid.toString,
  exchangeHostGuid: String = SiteGuid.toString,
  exchangeGoal: String = "exchange goal",
  exchangeName: String = "exchange Name",
  exchangeStatus: String = "exchange Status",
  exchangeType: String = "exchange Type"
)

object CampaignAttributesDataMartRowGenerationContext {
  val defaultRow = CampaignAttributesDataMartRowGenerationContext(new DateTime, SiteGuid("12345678"), 101L)

  def defaultRows(count: Int) = {
    List.fill(count)(defaultRow)
  }
}