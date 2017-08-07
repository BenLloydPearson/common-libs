package com.gravity.test

import com.gravity.interests.jobs.intelligence.reporting.PlacementWithSlotAboutToRunOutOfContent
import org.joda.time.DateTime

case class PlacementContentDroughtContext(rows: Seq[PlacementContentDroughtDayContext])

case class PlacementContentDroughtDayContext(date: DateTime, rows: Seq[PlacementWithSlotAboutToRunOutOfContent])
