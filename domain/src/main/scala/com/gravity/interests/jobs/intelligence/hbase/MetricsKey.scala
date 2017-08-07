package com.gravity.interests.jobs.intelligence.hbase

import com.gravity.interests.jobs.intelligence.RecommendationMetricCountBy

trait MetricsKey extends MetricsKeyLite {
  def countBy: Byte
  def algoId: Int
  def bucketId: Int
}

trait DailyMetricsKey extends DailyMetricsKeyLite {
  def countBy: Byte
  def algoId: Int
  def bucketId: Int
}


trait MonthlyMetricsKey extends MonthlyMetricsKeyLite {
  def countBy: Byte
  def algoId: Int
  def bucketId: Int
}