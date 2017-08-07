package com.gravity.interests.jobs.intelligence

object StandardMetricType extends Enumeration {
  type Type = Value

  val empty: StandardMetricType.Value = Value(0)
  val views: StandardMetricType.Value = Value(1)
  val publishes: StandardMetricType.Value = Value(2)
  val searchReferrers: StandardMetricType.Value = Value(3)
  val socialReferrers: StandardMetricType.Value = Value(4)
  val keyPageReferrers: StandardMetricType.Value = Value(5)
}
