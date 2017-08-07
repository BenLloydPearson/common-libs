package com.gravity.domain.recommendations

import com.gravity.interests.jobs.intelligence.Device
import com.gravity.utilities.CountryCodeId
import com.gravity.utilities.eventlogging.FieldValueRegistry
import com.gravity.utilities.grvtime._
import com.gravity.utilities.time.DateHour

/**
 * Created by tdecamp on 8/7/15.
 * {{insert neat ascii diagram here}}
 */
case class ReportRowImpressions(
  unitImpressionsOrganicClean: Long = 0,
  unitImpressionsOrganicDiscarded: Long = 0,
  unitImpressionsSponsoredClean: Long = 0,
  unitImpressionsSponsoredDiscarded: Long = 0,
  unitImpressionsBlendClean: Long = 0,
  unitImpressionsBlendDiscarded: Long = 0,
  unitImpressionViewsOrganicClean: Long = 0,
  unitImpressionViewsOrganicDiscarded: Long = 0,
  unitImpressionViewsSponsoredClean: Long = 0,
  unitImpressionViewsSponsoredDiscarded: Long = 0,
  unitImpressionViewsBlendClean: Long = 0,
  unitImpressionViewsBlendDiscarded: Long = 0
  )