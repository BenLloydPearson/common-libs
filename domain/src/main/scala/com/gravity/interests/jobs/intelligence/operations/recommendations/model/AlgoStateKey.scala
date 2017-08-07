package com.gravity.interests.jobs.intelligence.operations.recommendations.model

import play.api.libs.json.{Json, Format}

/**
 * Describes the state of an algo with all it's configurable overrides.
 * @param algoId The algo id
 * @param settingsOverrideHash The setting overrides as seq of key = value
 * @param pdsOverrideHash The probability distribution overrides as seq of key = selected value
 */
case class AlgoStateKey(
                         algoId: Option[Int],
                         settingsOverrideHash:Option[Long],
                         pdsOverrideHash: Option[Long]
                         )

object AlgoStateKey {
  implicit val jsonFormat: Format[AlgoStateKey] = Json.format[AlgoStateKey]
}
