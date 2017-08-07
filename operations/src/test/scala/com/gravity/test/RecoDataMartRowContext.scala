package com.gravity.test

import com.gravity.interests.jobs.intelligence.operations.RecoDataMartRow
import com.gravity.valueclasses.ValueClassesForDomain._
import org.joda.time.DateTime

case class RecoDataMartRowContext(rows: Seq[RecoDataMartRow])

case class RecoDataMartRowGenerationContext(
  date: DateTime,
  site: SiteGuid,
  placementId: Long,
  numImps: Int = 1,
  numImpDiscards: Int = 1,
  numViews: Int = 1,
  numViewDiscards: Int = 1,
  articleImps: Int = 1,
  articleImpDiscards: Int = 1,
  articleViews: Int = 1,
  articleViewDiscards: Int = 1,
  numClicks: Int = 1,
  numClickDiscards: Int = 1,
  isControl: Boolean = false,
  impressionType: Int = 1,
  countryCode: String = "US",
  deviceType: String = "Desktop",
  exchangeGuid: String = ExchangeGuid.toString,
  exchangeHostGuid: String = SiteGuid.toString,
  exchangeGoal: String = "exchange goal",
  exchangeName: String = "exchange Name",
  exchangeStatus: String = "exchange Status",
  exchangeType: String = "exchange Type"
)

object RecoDataMartRowGenerationContext {
  val defaultRow = RecoDataMartRowGenerationContext(new DateTime, SiteGuid("12345678"), 101L)

  def defaultRows(count: Int) = {
    List.fill(count)(defaultRow)
  }
}