package com.gravity.interests.jobs.intelligence.hbase

import com.gravity.utilities.time.{DateMonth, GrvDateMidnight, DateHour}

trait MetricsKeyLite {
  def dateHour: DateHour
  def placementId: Int
}

trait DailyMetricsKeyLite {
  def dateMidnight: GrvDateMidnight
  def placementId: Int
}

trait MonthlyMetricsKeyLite {
  def dateMonth: DateMonth
  def placementId: Int
}