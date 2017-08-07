package com.gravity.interests.jobs.intelligence

/**
 * Created by IntelliJ IDEA.
 * Author: Robbie Coleman
 * Date: 2/24/14
 * Time: 11:30 AM
 *            _ 
 *          /  \
 *         / ..|\
 *        (_\  |_)
 *        /  \@'
 *       /     \
 *   _  /  \   |
 * \\/  \  | _\
 *   \   /_ || \\_
 *    \____)|_) \_)
 *
 */

import org.junit.Assert._
import org.junit.Test
import com.gravity.utilities.grvtime
import com.gravity.utilities.analytics.{TimeSliceResolution, DateMidnightRange}
import scalaz._, Scalaz._

class ReportingRangeSplitsTest {
  @Test def testLast7days() {
    val today = grvtime.currentDay
    val last7 = TimeSliceResolution.lastSevenDays.range
    val expected = ReportingRangeSplits(hdfsRange = DateMidnightRange(today.minusDays(7), today.minusDays(2)).some, hbaseRange = DateMidnightRange.forSingleDay(today.minusDays(1)).some)
    val actual = ReportingRangeSplits(last7)

    println(s"Testing last 7 days: $last7")
    println(s"Expected: $expected")
    assertEquals(expected, actual)
  }

  @Test def testTodayOnly() {
    val today = grvtime.currentDay
    val todayRange = DateMidnightRange.forSingleDay(today)
    val expected = ReportingRangeSplits(hdfsRange = None, hbaseRange = todayRange.some)
    val actual = ReportingRangeSplits(todayRange)

    println(s"Testing today only: $todayRange")
    println(s"Expected: $expected")
    assertEquals(expected, actual)
  }

  @Test def testTodayAndYesterdayOnly() {
    val today = grvtime.currentDay
    val range = DateMidnightRange(today.minusDays(1), today)
    val expected = ReportingRangeSplits(hdfsRange = None, hbaseRange = range.some)
    val actual = ReportingRangeSplits(range)

    println(s"Testing today and yesterday: $range")
    println(s"Expected: $expected")
    assertEquals(expected, actual)
  }

  @Test def testTodayAndYesterdaySlidBackOneDay(): Unit = {
    val range = TimeSliceResolution.yesterdayAndToday.range.slideBackDays(1)
    val expected = ReportingRangeSplits(hdfsRange = DateMidnightRange.forSingleDay(range.fromInclusive).some, hbaseRange = DateMidnightRange.forSingleDay(range.toInclusive).some)
    val actual = ReportingRangeSplits(range)

    println(s"Testing today and yesterday (slid back one day): $range")
    println(s"Expected: $expected")
    assertEquals(expected, actual)
  }

  @Test def testTwoDaysOlderThanYesterday() {
    val fromDay = grvtime.currentDay.minusDays(2)
    val range = DateMidnightRange(fromDay.minusDays(1), fromDay)
    val expected = ReportingRangeSplits(hdfsRange = range.some, hbaseRange = None)
    val actual = ReportingRangeSplits(range)

    println(s"Testing two days that are before yesterday: $range")
    println(s"Expected: $expected")
    assertEquals(expected, actual)
  }

  @Test def testLast7daysSlidBack2days() {
    val last7back2 = TimeSliceResolution.lastSevenDays.range.slideBackDays(2)
    val expected = ReportingRangeSplits(hdfsRange = last7back2.some, hbaseRange = None)
    val actual = ReportingRangeSplits(last7back2)

    println(s"Testing last 7 days (slid back 2): $last7back2")
    println(s"Expected: $expected")
    assertEquals(expected, actual)
  }
}