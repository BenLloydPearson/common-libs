package com.gravity.utilities

/**
 * Created by IntelliJ IDEA.
 * Author: Robbie Coleman
 * Date: 9/6/11
 * Time: 4:47 PM
 */

import com.gravity.utilities.time.{DateHour, GrvDateMidnight}
import org.junit.Assert._
import org.junit.Test
import org.joda.time.DateTime
import com.gravity.utilities.grvtime._

class GrvtimeTest {
  def validateFormattedDuration(expectedDuration: String, millis: Long) {
    val actualDuration = formatDuration(millis)
    println(actualDuration)
    assertEquals("Resulting duration was not as expected!", expectedDuration, actualDuration)
  }

  @Test def testFirstDayOfThisWeek(): Unit = {
    val forDate = new GrvDateMidnight(2015, 1, 31).toDateTime
    val expectedDate = new GrvDateMidnight(2015, 1, 26).toDateTime
    val firstDayOfThisWeek = grvtime.firstDayOfThisWeek(forDate).toDateTime
    println("original date:\n\t" + forDate)
    println("1st day of its week:\n\t" + firstDayOfThisWeek)
    assertEquals("First day of the week for the date specified did not equal the expected date!", expectedDate, firstDayOfThisWeek)
  }

  @Test def testPercentIntoHour() {
    println(grvtime.percentIntoCurrentHour)
  }

  @Test def testYearDayString() {
    val janFourth2014 = new GrvDateMidnight(2014, 1, 4)
    assertEquals("Incorrect YEAR_DAY format!", "2014_004", janFourth2014.toYearDayString)
  }

  @Test def testMonthHandling() {
    val currentMonth = grvtime.currentMonth
    println(currentMonth.toString())

    println(grvtime.monthsAgo(1))
  }
  @Test def testFormatDuration() {
    validateFormattedDuration("1 day, 3 hours, 2 minutes, 28 seconds and 954 milliseconds", ONE_DAY_MILLIS + (ONE_HOUR_MILLIS * 3) + (ONE_MINUTE_MILLIS * 2) + (ONE_SECOND_MILLIS * 28) + 954)
    validateFormattedDuration("1 hour, 2 minutes, 28 seconds and 954 milliseconds", ONE_HOUR_MILLIS + (ONE_MINUTE_MILLIS * 2) + (ONE_SECOND_MILLIS * 28) + 954)
    validateFormattedDuration("1 minute and 28 seconds", ONE_MINUTE_MILLIS + (ONE_SECOND_MILLIS * 28))
    validateFormattedDuration("1 day and 28 seconds", ONE_DAY_MILLIS + (ONE_SECOND_MILLIS * 28))
    validateFormattedDuration("28 seconds and 1 millisecond", (ONE_SECOND_MILLIS * 28) + 1)
    validateFormattedDuration("1 day", ONE_DAY_MILLIS)
    validateFormattedDuration("1 hour", ONE_HOUR_MILLIS)
    validateFormattedDuration("1 minute", ONE_MINUTE_MILLIS)
    validateFormattedDuration("1 millisecond", 1)
    validateFormattedDuration("0 milliseconds", 0)
  }

  private case class FormatRelativeDurationTestVals(timeMultiplier: Int, adjective: String, ensurePast: Boolean)

  @Test def testFormatRelativeDuration() {
    assertEquals("a minute ago", formatRelativeDuration( new DateTime().getMillis ))

    List(
      FormatRelativeDurationTestVals(-1, "ago", ensurePast = false),  // In the past
      FormatRelativeDurationTestVals(1, "later", ensurePast = false), // In the future
      FormatRelativeDurationTestVals(1, "ago", ensurePast = true)     // In the future but ensurePast TRUE
    ).foreach {
      case testVals => 
        val mult = testVals.timeMultiplier
        val adj = testVals.adjective
        val ensurePast = testVals.ensurePast
        assertEquals("a minute " + adj, formatRelativeDuration( new DateTime().plusSeconds(mult * 5).getMillis, ensurePast = ensurePast ))
        assertEquals("a minute " + adj, formatRelativeDuration( new DateTime().plusSeconds(mult * 50).getMillis, ensurePast = ensurePast ))
        assertEquals("a minute " + adj, formatRelativeDuration( new DateTime().plusSeconds(mult * 90).getMillis, ensurePast = ensurePast ))
        assertEquals("2 minutes " + adj, formatRelativeDuration( new DateTime().plusMinutes(mult * 2).plusSeconds(mult * 5).getMillis, ensurePast = ensurePast ))
        assertEquals("5 minutes " + adj, formatRelativeDuration( new DateTime().plusMinutes(mult * 5).plusSeconds(mult * 5).getMillis, ensurePast = ensurePast ))
        assertEquals("44 minutes " + adj, formatRelativeDuration( new DateTime().plusMinutes(mult * 44).plusSeconds(mult * 5).getMillis, ensurePast = ensurePast ))
        assertEquals("an hour " + adj, formatRelativeDuration( new DateTime().plusMinutes(mult * 61).getMillis, ensurePast = ensurePast ))
        assertEquals("2 hours " + adj, formatRelativeDuration( new DateTime().plusMinutes(mult * 2 * 61).getMillis, ensurePast = ensurePast ))
        assertEquals("5 hours " + adj, formatRelativeDuration( new DateTime().plusMinutes(mult * 5 * 61).getMillis, ensurePast = ensurePast ))
        assertEquals("a day " + adj, formatRelativeDuration( new DateTime().plusHours(mult * 25).getMillis, ensurePast = ensurePast ))
        assertEquals("3 days " + adj, formatRelativeDuration( new DateTime().plusHours(mult * 3 * 25).getMillis, ensurePast = ensurePast ))
        assertEquals("a week " + adj, formatRelativeDuration( new DateTime().plusWeeks(mult * 1).plusDays(mult * 1).getMillis, ensurePast = ensurePast ))
        assertEquals("3 weeks " + adj, formatRelativeDuration( new DateTime().plusWeeks(mult * 3).plusDays(mult * 3).getMillis, ensurePast = ensurePast ))
        assertEquals("a month " + adj, formatRelativeDuration( new DateTime().plusSeconds(mult * (ONE_MONTH_SECONDS + ONE_DAY_SECONDS)).getMillis, ensurePast = ensurePast ))
        assertEquals("3 months " + adj, formatRelativeDuration( new DateTime().plusSeconds(mult * 3 * (ONE_MONTH_SECONDS + ONE_DAY_SECONDS)).getMillis, ensurePast = ensurePast ))
        assertEquals("11 months " + adj, formatRelativeDuration( new DateTime().plusSeconds(mult * 11 * (ONE_MONTH_SECONDS + ONE_DAY_SECONDS)).getMillis, ensurePast = ensurePast ))
        assertEquals("a year " + adj, formatRelativeDuration( new DateTime().plusYears(mult * 1).plusDays(mult * 1).getMillis, ensurePast = ensurePast ))
        assertEquals("13 years " + adj, formatRelativeDuration( new DateTime().plusYears(mult * 13).plusDays(mult * 1 * 13).getMillis, ensurePast = ensurePast ))
    }
  }

  @Test def testCachedDateHour {
    assertEquals(currentHour.getMillis, cachedCurrentHour.getMillis)
  }

  @Test def testFormatTimecode {
    assertEquals("0:00", formatTimecode(-1))
    assertEquals("0:00", formatTimecode(0))
    assertEquals("0:01", formatTimecode(1))
    assertEquals("0:42", formatTimecode(42))
    assertEquals("1:00", formatTimecode(60))
    assertEquals("2:16", formatTimecode(136))
    assertEquals("13:00", formatTimecode(13 * 60))
  }
}

object grvtimewait extends App {
  val currentHour: DateHour = grvtime.cachedCurrentHour
  println("starting with currentHour " + currentHour)
  scala.io.StdIn.readLine("waiting...\n")
}