package com.gravity.interests.jobs.intelligence.operations.recommendations

import com.gravity.interests.jobs.intelligence.operations.ExchangeService
import com.gravity.interests.jobs.intelligence.{ExchangeKey, ExchangeMisc, ExchangeRow}
import com.gravity.valueclasses.ValueClassesForDomain.ExchangeGuid
import play.api.libs.json.Json

/*

         ▒▒
         ██
       ██████
     ██████████
     ██░░██░░██
     ██████████
       ██████
       ░░░░░░      It's gonna be fun on the bun!
       ██████

*/

trait HasExchangeSettings {
  def exchangeGuid: Option[ExchangeGuid]
}

@SerialVersionUID(1L)
case class ExchangeRecommendationData(exchangeGuid: ExchangeGuid, enableOmnitureTrackingParamsForAllSites: Boolean, exchangeTrackingParamsForAllSites: Map[String, String]) {

  def exchangeKey: ExchangeKey = exchangeGuid.exchangeKey

  @transient lazy val exchangeTrackingParamsWithOmnitureParams: Map[String,String] = exchangeTrackingParamsForAllSites ++ ExchangeMisc.omnitureTrackingParams
}

object ExchangeRecommendationData {

  def fromExchangeRow(exchangeRow: ExchangeRow): ExchangeRecommendationData = ExchangeRecommendationData(
    exchangeRow.exchangeGuid,
    exchangeRow.enableOmnitureTrackingParamsForAllSites,
    exchangeRow.trackingParamsForAllSites.toMap
  )

  def fromExchangeGuid(exchangeGuid: ExchangeGuid): ExchangeRecommendationData = {
    ExchangeService.exchangeMeta(exchangeGuid.exchangeKey).map(fromExchangeRow).getOrElse(
      empty.copy(exchangeGuid = exchangeGuid)
    )
  }

  val empty = ExchangeRecommendationData(
    ExchangeGuid.empty,
    enableOmnitureTrackingParamsForAllSites = false,
    Map()
  )

  implicit val jsonFormat = Json.format[ExchangeRecommendationData]
}
