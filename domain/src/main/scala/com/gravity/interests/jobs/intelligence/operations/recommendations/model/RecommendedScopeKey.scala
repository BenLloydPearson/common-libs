package com.gravity.interests.jobs.intelligence.operations.recommendations.model

import com.gravity.interests.jobs.intelligence.hbase.ScopedKey
import play.api.libs.json.{Json, Format}

/**
 * Created by agrealish14 on 7/7/16.
 */
case class RecommendedScopeKey(
                                scope: ScopedKey, //What are these recommendations for?  A site?  An article?  A user?  A cluster of users?
                                candidateQualifier: CandidateSetQualifier, //What candidate set led to this?  Conversely -- where is it allowed to show up?
                                algoStateKey: AlgoStateKey // What was the state of the algo? What were the settings & probability distribution overrides?
                                ) {

  def applyToCandidateSetQualifier(deviceTypeOverride:Option[Int], geoLocationOverride:Option[Int]) = {

    RecommendedScopeKey(scope, candidateQualifier.applyToCandidateSetQualifier(deviceTypeOverride, geoLocationOverride), algoStateKey)
  }
}


object RecommendedScopeKey {
  implicit val jsonFormat: Format[RecommendedScopeKey] = Json.format[RecommendedScopeKey]
}