package com.gravity.domain

import com.gravity.utilities.grvtime
import org.joda.time.DateTime

object ElasticSearchHelper {
  
  val placementContentDroughtIndexPrefix = "placement-content-drought-"
  val placementContentDroughtIndexPrefixWildcard = placementContentDroughtIndexPrefix + "*"
  val placementContentDroughtType = "slot"

  def placementContentDroughtIndex(dt: DateTime): String = {
    val formattedDt = grvtime.elasticSearchDataMartFormat.print(dt)
    s"$placementContentDroughtIndexPrefix$formattedDt"
  }

  def placementContentDroughtIndexAndType(dt: DateTime): String = {
    val formattedDt = grvtime.elasticSearchDataMartFormat.print(dt)
    s"$placementContentDroughtIndexPrefix$formattedDt/$placementContentDroughtType"
  }

}
