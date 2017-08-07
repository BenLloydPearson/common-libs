package com.gravity.interests.jobs.intelligence.operations.sites

import com.gravity.utilities.grvstrings.emptyString
import com.gravity.utilities.web.ParamValidationFailure

import scalaz.Scalaz._
import scalaz._

case class SiteGoogleAnalyticsMeta(trackOrganicClicks: Boolean, webPropertyId: String)
object SiteGoogleAnalyticsMeta {
  def applyWithValidation(trackOrganicClicks: Boolean, webPropertyId: String): ValidationNel[ParamValidationFailure, SiteGoogleAnalyticsMeta] = {
    if(trackOrganicClicks) {
      if(webPropertyId.isEmpty)
        ParamValidationFailure("webPropertyId", _.Required, "Web Property ID is required if you want to track events with Google Analytics.").failureNel
      else
        SiteGoogleAnalyticsMeta(trackOrganicClicks, webPropertyId).successNel
    }
    else
      SiteGoogleAnalyticsMeta(trackOrganicClicks, emptyString).successNel
  }
}
