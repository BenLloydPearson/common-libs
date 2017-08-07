package com.gravity.interests.jobs.intelligence

import org.scalatest.Matchers
import org.scalatest.junit.AssertionsForJUnit
import com.gravity.utilities.grvtime._
import com.gravity.utilities.time._
import org.joda.time.{DateTimeUtils, DateTime}
import org.junit.{After, Before, Test}

class ViralMetricsTest extends AssertionsForJUnit with Matchers {
  val maxHour = 6

  val startTime: DateTime = new DateTime(2011, 11, 11, 11, 11, 11, 11)
  val startHour: DateHour = startTime.minusHours(maxHour).toDateHour
  val startDay: GrvDateMidnight = startHour.toDateTime.toGrvDateMidnight

  @Before def fakeTime() {
    DateTimeUtils.setCurrentMillisFixed(startTime.getMillis)
  }

  @After def unfakeTime() {
    DateTimeUtils.setCurrentMillisSystem()
  }

  def hp(hours: Int): DateHour = startHour.plusHours(hours)
  def dp(hours: Int): DateTime = hp(hours).toDateTime

  val metrics: Map[DateHour, ViralMetrics] = Map(
    hp(0) -> ViralMetrics(1,0,0,0,0,0),
    hp(1) -> ViralMetrics(1,0,0,0,0,0),
    hp(2) -> ViralMetrics(1,0,0,0,0,0),
    hp(3) -> ViralMetrics(1,0,0,0,0,0),
    hp(4) -> ViralMetrics(2,0,0,0,0,0),
    hp(5) -> ViralMetrics(4,0,0,0,0,0),
    hp(maxHour) -> ViralMetrics(8,0,0,0,0,0),

    hp(24) -> ViralMetrics(16,0,0,0,0,0), // in the future
    hp(25) -> ViralMetrics(32,0,0,0,0,0),
    hp(26) -> ViralMetrics(64,0,0,0,0,0),

    hp(48) -> ViralMetrics(128,0,0,0,0,0),
    hp(49) -> ViralMetrics(256,0,0,0,0,0),
    hp(50) -> ViralMetrics(512,0,0,0,0,0),
    hp(51) -> ViralMetrics(512,0,0,0,0,0,0,0,0,0,0,0,0,0) //test new viral metrics that has more values
  )

  val dailyMetrics: Map[GrvDateMidnight, ViralMetrics] = metrics.foldLeft(Map[GrvDateMidnight, ViralMetrics]().withDefaultValue(ViralMetrics.empty)) { case (acc, (hour, vm)) =>
    val day = hour.toDateTime.toGrvDateMidnight
    acc.updated(day, acc(day) + vm)
  }

  @Test def calculateViralVelocity() {
    AggregableMetrics.hourlyTrendingScoreBetween(metrics, dp(0), dp(3)) should be (0.0) // not enough for 2 periods
    AggregableMetrics.hourlyTrendingScoreBetween(metrics, dp(0), dp(4)) should be (0.0)
    AggregableMetrics.hourlyTrendingScoreBetween(metrics, dp(4), dp(7)) should be (0.55555 +- 0.00001)
    AggregableMetrics.hourlyTrendingScoreBetween(metrics, dp(1), dp(7)) should be (0.5 +- 0.00001)

    val duringToday = AggregableMetrics.hourlyTrendingScoreBetween(metrics, dp(0), dp(7))
    val inTheFuture = AggregableMetrics.hourlyTrendingScoreBetween(metrics, dp(0), dp(26))
    duringToday should equal (inTheFuture)

    val trendingUpTowardTheEndOfTheDay = AggregableMetrics.hourlyTrendingScoreBetweenInclusive(metrics, startDay, startDay)
    val trendingUpTowardTheEndOfTheDayInTheFuture = AggregableMetrics.hourlyTrendingScoreBetweenInclusive(metrics, startDay, startDay.plusDays(7))
    trendingUpTowardTheEndOfTheDay should be > (duringToday)
    trendingUpTowardTheEndOfTheDay should equal (trendingUpTowardTheEndOfTheDayInTheFuture)
  }

  @Test def rollingTrend() {
    val rolling = AggregableMetrics.rollingTrendBetween(dailyMetrics, startDay, startDay.plusDays(2), stepDay)
    rolling should have size (3)
    rolling(0) should equal (0.0)
    rolling(1) should be > rolling(0)
    rolling(2) should be > rolling(1)
  }
}