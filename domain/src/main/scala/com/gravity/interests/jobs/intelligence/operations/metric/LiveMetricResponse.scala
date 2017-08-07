package com.gravity.interests.jobs.intelligence.operations.metric

import com.gravity.interests.jobs.intelligence.{ArticleKey, ScopedMetrics, SiteKey, SitePlacementIdKey}
import com.gravity.interests.jobs.intelligence.hbase.ScopedFromToKey
import com.gravity.service.remoteoperations.MergeableResponse
import com.gravity.valueclasses.ValueClassesForDomain.SitePlacementId

/**
 * Created by agrealish14 on 10/11/16.
 */
case class LiveMetricResponse(metrics:Seq[LiveMetric]) extends MergeableResponse[LiveMetricResponse] {

  override def merge(withThese: Seq[LiveMetricResponse]): LiveMetricResponse = {

    val combined = withThese.flatMap(_.metrics) ++ metrics
    LiveMetricResponse(combined.toList)
  }

  override def size: Int = metrics.size

  def toMetricsMap: Map[ScopedFromToKey, ScopedMetrics] = metrics.map(m => {
    m.key -> m.metrics
  }).toMap
}

object LiveMetricResponse {

  def apply(): LiveMetricResponse = {

    LiveMetricResponse(List.empty[LiveMetric])
  }

  val testArticleKey = ArticleKey("http://somesite.com/some/article.html").toScopedKey

  val testSiteKey = SiteKey("http://somesite.com/").toScopedKey

  val testSitePlacementKey = SitePlacementIdKey(SitePlacementId(9999l)).toScopedKey

  val testLiveMetrics = List(
    LiveMetric(ScopedFromToKey(testArticleKey, testSiteKey), ScopedMetrics(10,10,10,10,10)),
    LiveMetric(ScopedFromToKey(testArticleKey, testSitePlacementKey), ScopedMetrics(10,10,10,10,10))
  )

  val testObject = LiveMetricResponse(testLiveMetrics)
}

case class LiveMetric(key:ScopedFromToKey, metrics:ScopedMetrics)