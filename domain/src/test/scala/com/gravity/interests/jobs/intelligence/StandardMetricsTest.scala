package com.gravity.interests.jobs.intelligence

import com.gravity.utilities.BaseScalaTest
import com.gravity.utilities.time.GrvDateMidnight
import org.scalatest.junit.{AssertionsForJUnit, ShouldMatchersForJUnit}

class StandardMetricsTest extends BaseScalaTest with AssertionsForJUnit {
  test("sum maps") {
    val metricsType = StandardMetricType.views

    val test1 = StandardMetrics.sumMaps()
    test1.size should be(0)

    val key1 = StandardMetricsDailyKey(new GrvDateMidnight(2013, 6, 14), metricsType)
    val key2 = StandardMetricsDailyKey(new GrvDateMidnight(2013, 10, 31), metricsType)
    val test2 = StandardMetrics.sumMaps(Map(
      key1 -> 3l,
      key2 -> 5l
    ))
    test2.size should be(2)
    test2(key1) should be(3l)
    test2(key2) should be(5l)

    val key3 = StandardMetricsDailyKey(new GrvDateMidnight(2013, 1, 1), metricsType)
    val test3 = StandardMetrics.sumMaps(Map(
      key1 -> 3l,
      key2 -> 5l
    ), Map(
      key2 -> 1l,
      key3 -> -6l
    ))
    test3.size should be (3)
    test3(key1) should be(3l)
    test3(key2) should be(6l)
    test3(key3) should be(-6l)
  }
}