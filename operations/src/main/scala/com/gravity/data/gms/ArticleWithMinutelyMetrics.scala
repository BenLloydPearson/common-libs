package com.gravity.data.gms

case class MinutelyMetrics(
  minuteMillis: Long,
  impressions: Long,
  clicks: Long,
  ctr: Double)

case class ArticleWithMinutelyMetrics(
  articleId: Long,
  totalImpressions: Long,
  totalClicks: Long,
  totalCtr: Double,
  minutelyMetrics: Seq[MinutelyMetrics])
