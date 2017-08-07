package com.gravity.interests.jobs.intelligence.operations.analytics

import com.gravity.utilities.components.FailureResult
import scalaz.{Success, Validation}
import com.gravity.interests.jobs.intelligence.InterestCounts

trait AnalyticsMockPersistence extends AnalyticsPersistence {

  def siteInterests(siteGuid: String, resolution: String, year:Int, point:Int, useOntologyName: Boolean = true) : Validation[FailureResult, SiteInterests] = {

    val interestCounts =       InterestCounts("http://uri.com/kittens",1l,1l,1l,1l,1l) ::
      Nil


    Success(SiteInterests(siteGuid, SiteRollup(100,100,100,100,100,100),interestCounts))

  }

  def interestCounts(siteGuid: String, resolution: String, year: Int, point: Int, topicUri: String): Validation[FailureResult, InterestSummary] = {
    Success(InterestSummary(InterestCounts(topicUri, 1l, 1l, 1l, 1l, 1l), SiteRollup(100,100,100,100,100,100)))
  }
}
