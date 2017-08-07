package com.gravity.domain.gms

import com.gravity.domain.grvmetrics
import com.gravity.interests.jobs.intelligence.{ArticleKey, SitePlacementIdKey}
import com.gravity.interests.jobs.intelligence.hbase.ScopedFromToKey

case class GmsArticleMetricsKey(articleKey: ArticleKey, contentGroupId: Long, sitePlacementIdKey: SitePlacementIdKey) {
  lazy val scopedFromToKey: ScopedFromToKey = ScopedFromToKey(articleKey.toScopedKey, sitePlacementIdKey.toScopedKey)
  lazy val keyString: String = GmsArticleMetricsKey.buildKeyString(scopedFromToKey)
}

object GmsArticleMetricsKey {
  def buildKeyString(scopedFromToKey: ScopedFromToKey): String = {
    s"f:${scopedFromToKey.from.keyString},t:${scopedFromToKey.to.keyString}"
  }
}

case class GmsMetricsForIndex(totalClicks: Long, totalImpressions: Long, totalCtr: Double, thirtyMinuteCtr: Double)
object GmsMetricsForIndex {
  def apply(totalClicks: Long, totalImps: Long, thirtyMinuteClicks: Long, thirtyMinuteImps: Long): GmsMetricsForIndex = {
    val totalCtr = grvmetrics.safeCTR(totalClicks, totalImps)
    val thirtyMinuteCtr = grvmetrics.safeCTR(thirtyMinuteClicks, thirtyMinuteImps)

    GmsMetricsForIndex(totalClicks, totalImps, totalCtr, thirtyMinuteCtr)
  }
}

case class GmsArticleMetrics(key: GmsArticleMetricsKey, metrics: GmsMetricsForIndex)
