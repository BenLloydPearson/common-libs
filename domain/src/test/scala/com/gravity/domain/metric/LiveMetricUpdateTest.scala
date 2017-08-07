package com.gravity.domain.metric

import com.cloudera.org.joda.time.DateTime
import com.gravity.interests.jobs.intelligence._
import com.gravity.interests.jobs.intelligence.hbase.{ScopedFromToKey, ScopedMetricsKey}
import com.gravity.interests.jobs.intelligence.operations.metric.LiveMetricUpdate
import com.gravity.utilities.BaseScalaTest
import com.gravity.valueclasses.ValueClassesForDomain.SitePlacementId

/**
 * Created by agrealish14 on 10/13/16.
 */
class LiveMetricUpdateTest extends BaseScalaTest {

  test("Test LIve Metrics Update filter") {

    val testArticleKey = ArticleKey("http://somesite.com/some/article.html").toScopedKey

    val testSiteKey = SiteKey("http://somesite.com/").toScopedKey

    val testSitePlacementKey = SitePlacementIdKey(SitePlacementId(9999l)).toScopedKey

    val testMetrics = Map[ScopedMetricsKey, Long](
      ScopedMetricsKey(DateTime.now().getMillis, RecommendationMetricCountBy.unitImpression) -> 1L,
      ScopedMetricsKey(DateTime.now().getMillis, RecommendationMetricCountBy.click) -> 1L
    )

    val testLiveMetrics = Map[ScopedFromToKey, Map[ScopedMetricsKey, Long]](
      ScopedFromToKey(testArticleKey, testSiteKey) -> testMetrics,
      ScopedFromToKey(testArticleKey, testSitePlacementKey) -> testMetrics
    )

    val testObject: LiveMetricUpdate = LiveMetricUpdate(testLiveMetrics)

    assert(testObject.updates.size == 2)
  }

  test("Test Live Metrics Update filter out unwanted ") {

    val testArticleKey = ArticleKey("http://somesite.com/some/article.html").toScopedKey

    val testSiteKey = SiteKey("http://somesite.com/").toScopedKey

    val testSitePlacementKey = SitePlacementIdKey(SitePlacementId(9999l)).toScopedKey

    val testEntityKey = EntityScopeKey("User-12-test-key-name", testSitePlacementKey).toScopedKey

    val testMetrics = Map[ScopedMetricsKey, Long](
      ScopedMetricsKey(DateTime.now().getMillis, RecommendationMetricCountBy.unitImpression) -> 1L,
      ScopedMetricsKey(DateTime.now().getMillis, RecommendationMetricCountBy.click) -> 1L
    )

    val testLiveMetrics = Map[ScopedFromToKey, Map[ScopedMetricsKey, Long]](
      ScopedFromToKey(testArticleKey, testSiteKey) -> testMetrics,
      ScopedFromToKey(testArticleKey, testSitePlacementKey) -> testMetrics,
      ScopedFromToKey(EverythingKey.toScopedKey, EverythingKey.toScopedKey) -> testMetrics,
      ScopedFromToKey(testArticleKey, testEntityKey) -> testMetrics
    )

    val testObject: LiveMetricUpdate = LiveMetricUpdate(testLiveMetrics)

    assert(testObject.updates.size == 3)

  }
}
