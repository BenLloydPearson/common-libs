package com.gravity.domain

import com.gravity.test.domainTesting
import com.gravity.utilities.BaseScalaTest
import org.joda.time.{DateTime, MutableDateTime}

import scalaz.{Failure, Success}

/**
 * Created by robbie on 06/17/2015.
 *            _ 
 *          /  \
 *         / ..|\
 *        (_\  |_)
 *        /  \@'
 *       /     \
 *   _  /  \   |
 *  \\/  \  | _\
 *   \   /_ || \\_
 *    \____)|_) \_)
 *
 */
class GrvDurationTest extends BaseScalaTest with domainTesting {
  val setThisToTrueToSeeExpectedFailureOutput = false
  
  def assertParse(input: String, expected: GrvDuration): GrvDuration = {
    GrvDuration.parse(input) match {
      case Success(result) =>
        result should equal (expected)
        result
      case Failure(fails) =>
        fail(fails.list.map(_.messageWithExceptionInfo).mkString(" AND "))
        GrvDuration.empty
    }
  }
  
  def assertFailure(input: String, testName: String): Unit = {
    GrvDuration.parse(input) match {
      case Success(unexpected) =>
        fail(s"Expected failure ($testName) but got success with result: " + unexpected)

      case Failure(fails) =>
        if (setThisToTrueToSeeExpectedFailureOutput) {
          println(s"Expected Failure ($testName): \n\t${fails.list.map(_.messageWithExceptionInfo).mkString("\n\t AND ")}\n")
        }
    }
  }

  // PARSE TESTING
  test("valid durations") {
    val oneWeek = "1w"
    val oneWeekTwoDays = "1w|2d"
    val oneWeekTwoDaysThreeHours = "1w|2d|3h"
    val oneWeekThreeHours = "1w|3h"

    val oneWeekDuration = assertParse(oneWeek, GrvDuration(1, 0, 0))
    oneWeekDuration.toString should equal (oneWeek)

    val oneWeekTwoDaysDuration = assertParse(oneWeekTwoDays, GrvDuration(1, 2, 0))
    oneWeekTwoDaysDuration.toString should equal (oneWeekTwoDays)

    val oneWeekTwoDaysThreeHoursDuration = assertParse(oneWeekTwoDaysThreeHours, GrvDuration(1, 2, 3))
    oneWeekTwoDaysThreeHoursDuration.toString should equal (oneWeekTwoDaysThreeHours)

    val oneWeekThreeHoursDuration = assertParse(oneWeekThreeHours, GrvDuration(1, 0, 3))
    oneWeekThreeHoursDuration.toString should equal (oneWeekThreeHours)
  }

  test("negative values failing") {
    assertFailure("-1w", "negative values failing")
    assertFailure("1w|-2d", "negative values failing")
    assertFailure("1w|2d|-3h", "negative values failing")
  }

  test("invalid codes") {
    assertFailure("1W", "invalid codes")
    assertFailure("1w|2e", "invalid codes")
    assertFailure("1w|2d|3c", "invalid codes")
  }

  test("duplicate codes") {
    assertFailure("1w|3w", "duplicate codes")
    assertFailure("1w|2d|5d", "duplicate codes")
    assertFailure("1w|2d|3h|6h", "duplicate codes")
  }

  test("complete nonsense inputs") {
    assertFailure("|", "complete nonsense inputs")
    assertFailure("||", "complete nonsense inputs")
    assertFailure("|||", "complete nonsense inputs")
    assertFailure("1234", "complete nonsense inputs")
    assertFailure("wdh", "complete nonsense inputs")
    assertFailure("", "complete nonsense inputs")
  }

  // DURATION CALCULATION TESTING
  test("duration calculations") {
    val commonFromDateTime = new DateTime(2014, 1, 24, 9, 0)

    def mutateDateTime(work: MutableDateTime => Unit): DateTime = {
      val mutableDateTime = commonFromDateTime.toMutableDateTime
      work(mutableDateTime)
      mutableDateTime.toDateTime
    }

    val expectedTwoWeeks = mutateDateTime(_.addWeeks(2))

    val expectedTwoWeeksThreeDays = mutateDateTime(mdt => {
      mdt.addWeeks(2)
      mdt.addDays(3)
    })

    val expectedTwoWeeksThreeDaysSixHours = mutateDateTime(mdt => {
      mdt.addWeeks(2)
      mdt.addDays(3)
      mdt.addHours(6)
    })

    val twoWeeks = assertParse("2w", GrvDuration(2, 0, 0))
    expectedTwoWeeks should equal (twoWeeks.fromTime(commonFromDateTime))

    val twoWeeksThreeDays = assertParse("2w|3d", GrvDuration(2, 3, 0))
    expectedTwoWeeksThreeDays should equal (twoWeeksThreeDays.fromTime(commonFromDateTime))

    val twoWeeksThreeDaysSixHours = assertParse("2w|3d|6h", GrvDuration(2, 3, 6))
    expectedTwoWeeksThreeDaysSixHours should equal (twoWeeksThreeDaysSixHours.fromTime(commonFromDateTime))
  }
}
