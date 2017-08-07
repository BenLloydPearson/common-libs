package com.gravity.utilities.time

/**
 * Created by IntelliJ IDEA.
 * Author: Robbie Coleman
 * Date: 9/29/11
 * Time: 5:03 PM
 */

import org.junit.Test
import org.joda.time.format.DateTimeFormat
import org.scalatest.Matchers
import org.scalatest.junit.AssertionsForJUnit

class DateHourTest extends AssertionsForJUnit with Matchers {
  @Test def testHoursBetween() {
    val toExclusive = new GrvDateMidnight(2011, 11, 11)
    val fromInclusive = toExclusive.minusDays(2)

    val hours = DateHour.hoursBetween(fromInclusive, toExclusive)
    hours.size should be (48)

    println("Total hours between %s and %s: %d".format(fromInclusive.toString(DateTimeFormat.longDateTime()), toExclusive.toString(DateTimeFormat.longDateTime()), hours.size))

    hours.foreach(h => println(h.toString()))
  }
}

class DateMonthTest extends AssertionsForJUnit with Matchers {
  @Test def testMonthsBetween() {
    val toExclusive = new GrvDateMidnight(2011, 1, 1)
    val fromInclusive = toExclusive.minusMonths(6)

    val months = DateMonth.monthsBetween(fromInclusive, toExclusive)
    months.size should be (6)

    println("Total month between %s and %s: %d".format(fromInclusive.toString(DateTimeFormat.longDateTime()), toExclusive.toString(DateTimeFormat.longDateTime()), months.size))

    months.foreach(h => println(h.toString()))
  }
}