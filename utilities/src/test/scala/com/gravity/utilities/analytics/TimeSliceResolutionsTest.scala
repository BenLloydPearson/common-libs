package com.gravity.utilities.analytics

/**
 * Created by IntelliJ IDEA.
 * Author: Robbie Coleman
 * Date: 5/28/11
 * Time: 2:21 PM
 */
import com.gravity.utilities.grvstrings
import org.junit.Test
import org.junit.Assert._
import org.joda.time._
import com.gravity.utilities.grvtime._
import com.gravity.utilities.time.{GrvDateMidnight, DateHour}

class TimeSliceResolutionsTest {
  val INVALID: TimeSliceResolution = TimeSliceResolution(grvstrings.emptyString, -1, -1)

  @Test def testDateMidnightRangePercentOfMonths() {
    val singleMonthRange = DateMidnightRange(new GrvDateMidnight(2013, 5, 1), new GrvDateMidnight(2013, 5, 7))
    val singleExpectedPercent = 7.toDouble / 31.toDouble

    println("Verifying singleMonthRange.percentOfMonthsContained for range: " + singleMonthRange)
    assertEquals(singleMonthRange.toApiParamValue + " ==> should have a % of 7/31!", singleExpectedPercent, singleMonthRange.percentOfMonthsContained, 0.0)

    val twoMonthRange = DateMidnightRange(new GrvDateMidnight(2013, 4, 15), new GrvDateMidnight(2013, 5, 7))
    val twoMonthExpectedPercent = (15.toDouble / 30.toDouble) + (7.toDouble / 31.toDouble)

    println("Verifying twoMonthRange.percentOfMonthsContained for range: " + twoMonthRange)
    assertEquals(twoMonthRange.toApiParamValue + " ==> should have a % of (15 / 30) + (7 / 31)!", twoMonthExpectedPercent, twoMonthRange.percentOfMonthsContained, 0.0)

    val fourMonthRange = DateMidnightRange(new GrvDateMidnight(2013, 2, 15), new GrvDateMidnight(2013, 5, 7))
    val fourMonthExpectedPercent = ((28 - 15).toDouble / 28.toDouble) + (7.toDouble / 31.toDouble) + 2.toDouble

    println("Verifying fourMonthRange.percentOfMonthsContained for range: " + fourMonthRange)
    assertEquals(fourMonthRange.toApiParamValue + " ==> should have a % of ((28 - 15) / 28) + (7 / 31) + 2!\n\tDifference of: " + (fourMonthRange.percentOfMonthsContained - fourMonthExpectedPercent), fourMonthExpectedPercent, fourMonthRange.percentOfMonthsContained, 0.0)
  }

  @Test def testTodayMinusN() {
    val daysBack = 8
    val input = "today-" + daysBack

    TimeSliceResolutions.parse(input) match {
      case Some(tsr) => {
        val resultingRange = tsr.range
        printf("Parsed \"%s\" into: %n\t%s%n", input, resultingRange)

        assertEquals("From & To dates should equal for a single day!", resultingRange.fromInclusive, resultingRange.toInclusive)

        val expectedDate = new GrvDateMidnight().minusDays(daysBack)

        assertEquals("The day returned should equal %d days ago!".format(daysBack), expectedDate, resultingRange.fromInclusive)
      }
      case None => fail("Falied to parse: " + input)
    }
  }

  @Test def contains() {
    val range = DateMidnightRange(fromInclusive = new GrvDateMidnight(2013, 1, 31), toInclusive = new GrvDateMidnight(2013, 2, 4))
    assertTrue(range.contains(new GrvDateMidnight(2013, 1, 31)))
    assertTrue(range.contains(new GrvDateMidnight(2013, 2, 4)))
    assertFalse(range.contains(new GrvDateMidnight(2013, 2, 5)))
    assertTrue(range.contains(new DateTime(2013, 2, 4, 23, 59, 59, 999)))
    assertTrue(range.contains(DateHour(2013, 2, 4, 23)))
    assertFalse(range.contains(DateHour(1969, 12, 31, 23)))
  }

  @Test def testParse() {
    val day1 = new GrvDateMidnight(2012, 1, 1)
    val day7 = new GrvDateMidnight(2012, 1, 7)

    val str1 = "daily_2012_7"
    val dmRange1 = DateMidnightRange.parse(str1) getOrElse (throw new AssertionError(str1 + " didn't parse to DateMidnightRange"))
    assertEquals(dmRange1, DateMidnightRange(day7, day7))

    val str2 = "daily_2012_1:daily_2012_7"
    val dmRange2 = DateMidnightRange.parse(str2) getOrElse (throw new AssertionError(str2 + " didn't parse to DateMidnightRange"))
    assertEquals(dmRange2, DateMidnightRange(day1, day7))

    val str3 = "daily_2012_1~daily_2012_7" // case for avoiding the `:` for job runner processing
    val dmRange3 = DateMidnightRange.parse(str3) getOrElse (throw new AssertionError(str3 + " didn't parse to DateMidnightRange"))
    assertEquals(dmRange3, DateMidnightRange(day1, day7))
  }

  @Test def testParseIntoFields() {

    val lastthirtydays_1_1 = "lastthirtydays_1_1"

    val alltime_1_1 = "alltime_1_1"

    val resLast30 = assertParse(lastthirtydays_1_1)

    val curDate = new GrvDateMidnight()

    assertEquals("last 30 days resolution should match!", TimeSliceResolutions.LAST_30_DAYS, resLast30.resolution)
    assertEquals("last 30 days should replace year: 1 with current year!", curDate.getYear, resLast30.year)
    assertEquals("last 30 days should replace day: 1 with current day!", curDate.getDayOfYear, resLast30.point)

    val resAllTime = assertParse(alltime_1_1)

    assertEquals("All Time resolution should match!", TimeSliceResolutions.ALL_TIME, resAllTime.resolution)
    assertEquals("All Time should always have year == 1!", 1, resAllTime.year)
    assertEquals("All Time should always have day == 1!", 1, resAllTime.point)

    val resAllTimeWithOther = assertParse("alltime_2011_148")

    assertEquals("All Time resolution should match!", TimeSliceResolutions.ALL_TIME, resAllTimeWithOther.resolution)
    assertEquals("All Time should always have year == 1!", 1, resAllTimeWithOther.year)
    assertEquals("All Time should always have day == 1!", 1, resAllTimeWithOther.point)

    val expectedYear = 2011
    val expectedDay = 148
    val expectedResolution = "daily"
    val daily_2011_148 = List(expectedResolution, expectedYear, expectedDay).mkString("_")

    val resDaily = assertParse(daily_2011_148)

    assertEquals("Resolution should be as expected!", TimeSliceResolutions.DAILY, resDaily.resolution)
    assertEquals("Year should be as expected!", expectedYear, resDaily.year)
    assertEquals("Day should be as expected!", expectedDay, resDaily.point)

    val resMonthly = assertParse("monthly_2011_4")

    assertEquals("Resolution should be as expected!", TimeSliceResolutions.MONTHLY, resMonthly.resolution)
    assertEquals("Year should be as expected!", expectedYear, resMonthly.year)
    assertEquals("Month should be as expected!", 4, resMonthly.point)

    val resToday = assertParse("today")

    assertEquals("today resolution should be DAILY!", TimeSliceResolutions.DAILY, resToday.resolution)
    assertEquals("Year should be as expected!", curDate.getYear, resToday.year)
    assertEquals("Day should be as expected!", curDate.getDayOfYear, resToday.point)

    val resYesterday = assertParse("yesterday")
    val yesterday = curDate.minusDays(1)

    assertEquals("yesterday resolution should be DAILY!", TimeSliceResolutions.DAILY, resYesterday.resolution)
    assertEquals("Year should be as expected!", yesterday.getYear, resYesterday.year)
    assertEquals("Day should be as expected!", yesterday.getDayOfYear, resYesterday.point)
  }
  
  def assertParse(input: String): TimeSliceResolution = TimeSliceResolutions.parseIntoFields(input) match {
    case Some((res, year, point, point2)) => TimeSliceResolution(res, year, point, point2)
    case None => fail("Failed to parse time slice resolution from string: " + input); INVALID
  }

  @Test def testGetGrvDateMidnightBoundsDaily() {
    val year = 2011
    val dayOfYear = 185
    val daily = TimeSliceResolutions.getDateMidnightRange(TimeSliceResolutions.DAILY, year, dayOfYear)
    val dailyByHour = TimeSliceResolutions.getDateHourRange(TimeSliceResolutions.DAILY, year, dayOfYear)

//    println("Got the following daily:\n" + daily)

    assertEquals("Daily resolutions are a single day and from should equal to!", daily.fromInclusive, daily.toInclusive)

    assertEquals("The 'year' passed in should equal the resulting values' year!", year, daily.fromInclusive.getYear)
    assertEquals("The 'year' passed in should equal the resulting values' year!", year, dailyByHour.fromInclusive.getYear)
    assertEquals("The 'dayOfYear' passed in should equal the resulting values' dayOfYear!", dayOfYear, daily.fromInclusive.getDayOfYear)
    assertEquals("The 'dayOfYear' passed in should equal the resulting values' dayOfYear!", dayOfYear, dailyByHour.fromInclusive.getDayOfYear)
  }

  @Test def testGetGrvDateMidnightBoundsWeekly() {
    val year = 2011
    val weekOfYear = 7
    val weekly = TimeSliceResolutions.getDateMidnightRange(TimeSliceResolutions.WEEKLY, year, weekOfYear)
    val weeklyByHour = TimeSliceResolutions.getDateHourRange(TimeSliceResolutions.WEEKLY, year, weekOfYear)

//    println("Got the following weekly:\n" + weekly)

    assertEquals("Weekly resolutions are 7 days and to minus from should equal 6!", weekly.fromInclusive, weekly.toInclusive.minusDays(6))
    assertEquals("Weekly resolutions are 7 days and to minus from should equal 6!", weeklyByHour.fromInclusive, weeklyByHour.toInclusive.toDateTime.minusDays(6))

    assertEquals("The 'year' passed in should equal the resulting values' year!", year, weekly.fromInclusive.getYear)
    assertEquals("The 'year' passed in should equal the resulting values' year!", year, weeklyByHour.fromInclusive.getYear)

    assertEquals("The 'weekOfYear' passed in should equal the resulting 'from' value's getWeekOfWeekyear!", weekOfYear, weekly.fromInclusive.getWeekOfWeekyear)
    assertEquals("The 'weekOfYear' passed in should equal the resulting 'from' value's getWeekOfWeekyear!", weekOfYear, weeklyByHour.fromInclusive.getWeekOfWeekyear)
  }

  @Test def testGetGrvDateMidnightBoundsMonthly() {
    val year = 2011
    val monthOfYear = 7
    val monthly = TimeSliceResolutions.getDateMidnightRange(TimeSliceResolutions.MONTHLY, year, monthOfYear)
    val monthlyByHour = TimeSliceResolutions.getDateHourRange(TimeSliceResolutions.MONTHLY, year, monthOfYear)

 ////   println("Got the following monthly:\n" + monthly)

    assertEquals("The 'year' passed in should equal the resulting from value's year!", year, monthly.fromInclusive.getYear)
    assertEquals("The 'year' passed in should equal the resulting from value's year!", year, monthlyByHour.fromInclusive.getYear)

    assertEquals("The 'year' passed in should equal the resulting to value's year!", year, monthly.toInclusive.getYear)
    assertEquals("The 'year' passed in should equal the resulting to value's year!", year, monthlyByHour.toInclusive.getYear)

    assertEquals("The 'monthOfYear' passed in should equal the resulting 'from' value's getMonthOfYear!", monthOfYear, monthly.fromInclusive.getMonthOfYear)
    assertEquals("The 'monthOfYear' passed in should equal the resulting 'from' value's getMonthOfYear!", monthOfYear, monthlyByHour.fromInclusive.getMonthOfYear)

    assertEquals("The 'monthOfYear' passed in should equal the resulting 'to' value's getMonthOfYear!", monthOfYear, monthly.toInclusive.getMonthOfYear)
    assertEquals("The 'monthOfYear' passed in should equal the resulting 'to' value's getMonthOfYear!", monthOfYear, monthlyByHour.toInclusive.getMonthOfYear)

    assertEquals("The dayOfMonth for `from' should be 1!", 1, monthly.fromInclusive.getDayOfMonth)
    assertEquals("The dayOfMonth for `from' should be 1!", 1, monthlyByHour.fromInclusive.getDayOfMonth)

    assertEquals("The dayOfMonth for `to.plusDays(1)' should be 1!", 1, monthly.toInclusive.plusDays(1).getDayOfMonth)
    assertEquals("The dayOfMonth for `to.plusDays(1)' should be 1!", 1, monthlyByHour.toInclusive.toDateTime.plusDays(1).getDayOfMonth)
  }

  @Test def testGetGrvDateMidnightBoundsYearly() {
    val year = 2011
    val yearly = TimeSliceResolutions.getDateMidnightRange(TimeSliceResolutions.YEARLY, year, year)
    val yearlyByHour = TimeSliceResolutions.getDateHourRange(TimeSliceResolutions.YEARLY, year, year)

 //   println("Got the following yearly:\n" + yearly)

    assertEquals("The 'year' passed in should equal the resulting from value's year!", year, yearly.fromInclusive.getYear)
    assertEquals("The 'year' passed in should equal the resulting from value's year!", year, yearlyByHour.fromInclusive.getYear)

    assertEquals("The 'year' passed in should equal the resulting to value's year!", year, yearly.toInclusive.getYear)
    assertEquals("The 'year' passed in should equal the resulting to value's year!", year, yearlyByHour.toInclusive.getYear)

    assertEquals("The 'from' value's getMonthOfYear should be 1!", 1, yearly.fromInclusive.getMonthOfYear)
    assertEquals("The 'from' value's getMonthOfYear should be 1!", 1, yearlyByHour.fromInclusive.getMonthOfYear)

    assertEquals("The 'from' value's getDayOfMonth should be 1!", 1, yearly.fromInclusive.getDayOfMonth)
    assertEquals("The 'from' value's getDayOfMonth should be 1!", 1, yearlyByHour.fromInclusive.getDayOfMonth)

    assertEquals("The 'to' value's getMonthOfYear should be 12!", 12, yearly.toInclusive.getMonthOfYear)
    assertEquals("The 'to' value's getMonthOfYear should be 12!", 12, yearlyByHour.toInclusive.getMonthOfYear)

    assertEquals("The dayOfMonth for `from' should be 1!", 1, yearly.fromInclusive.getDayOfMonth)
    assertEquals("The dayOfMonth for `from' should be 1!", 1, yearlyByHour.fromInclusive.getDayOfMonth)

    assertEquals("The dayOfMonth for `to.plusDays(1)' should be 1!", 1, yearly.toInclusive.plusDays(1).getDayOfMonth)
    assertEquals("The dayOfMonth for `to.plusDays(1)' should be 1!", 1, yearlyByHour.toInclusive.toDateTime.plusDays(1).getDayOfMonth)

    assertEquals("The getYear for `to.plusDays(1)' should be 2012!", year + 1, yearly.toInclusive.plusDays(1).getYear)
    assertEquals("The getYear for `to.plusDays(1)' should be 2012!", year + 1, yearlyByHour.toInclusive.toDateTime.plusDays(1).getYear)
  }
  
  @Test def testGetDateHourRangeHourly() {
    val year = 2012
    val day = 2
    val hour = 9
    val hourly = TimeSliceResolutions.getDateHourRange(TimeSliceResolutions.HOURLY, year, day, hour)
    assertEquals(year, hourly.fromInclusive.getYear)
    assertEquals(year, hourly.toInclusive.getYear)
    assertEquals(1, hourly.fromInclusive.getMonthOfYear)
    assertEquals(day, hourly.fromInclusive.getDayOfMonth)
    assertEquals(1, hourly.toInclusive.getMonthOfYear)
    assertEquals(day, hourly.toInclusive.getDayOfMonth)
    assertEquals(hour, hourly.fromInclusive.getHourOfDay)
    assertEquals(hour + 1, hourly.toInclusive.getHourOfDay)
  }

  @Test def testGetDateHourRangePast24Hours() {
    val now = new DateTime()

    val toYear = now.getYear
    val fromYear = now.minusHours(24).getYear

    val toMonth = now.getMonthOfYear
    val fromMonth = now.minusHours(24).getMonthOfYear

    val toDay = now.getDayOfYear
    val fromDay = now.minusHours(24).getDayOfYear

    val toHour = now.getHourOfDay
    val fromHour = now.minusHours(24).getHourOfDay

    val past24 = TimeSliceResolutions.getDateHourRange(TimeSliceResolutions.PAST_24_HOURS)

    assertEquals(fromYear, past24.fromInclusive.getYear)
    assertEquals(fromMonth, past24.fromInclusive.getMonthOfYear)
    assertEquals(fromDay, past24.fromInclusive.getDayOfYear)
    assertEquals(fromHour, past24.fromInclusive.getHourOfDay)
    assertEquals(toYear, past24.toInclusive.getYear)
    assertEquals(toMonth, past24.toInclusive.getMonthOfYear)
    assertEquals(toDay, past24.toInclusive.getDayOfYear)
    assertEquals(toHour, past24.toInclusive.getHourOfDay)
  }

  @Test def testGetGrvDateMidnightBoundsLastThirtyDays() {
    val today = new GrvDateMidnight()
    val last30 = TimeSliceResolutions.getDateMidnightRange(TimeSliceResolutions.LAST_30_DAYS, 1, 1)
    val last30ByHour = TimeSliceResolutions.getDateHourRange(TimeSliceResolutions.LAST_30_DAYS, 1, 1)

    println("Last 30 range:")
    println("\tfromInclusive: " + last30.fromInclusive.toString(DateMidnightRange.fmt))
    println("\ttoInclusive: " + last30.toInclusive.toString(DateMidnightRange.fmt))
    val yesterday = today.minusDays(1)

    //Happy new year!
    //assertEquals("The 'from' value's year should be the current year!", today.getYear, last30.fromInclusive.getYear)
    //Happy new year 2016!
    //assertEquals("The 'to' value's year should be the current year!", today.getYear, last30.toInclusive.getYear)
    //assertEquals("The 'to' value's year should be the current year!", today.getYear, last30ByHour.toInclusive.getYear)

    assertEquals("The 'to' value should be yesterday's date!", yesterday, last30.toInclusive)

    val expectedHours = Hours.hoursBetween(today.minusDays(31), yesterday.toDateTime.plusHours(1)).toPeriod.getHours

    assertEquals("The duration of from -> to should be 30 days!", 30, last30.period.getDays)
    assertEquals("The duration of from -> to should be %d hours!".format(expectedHours), expectedHours, last30ByHour.hoursWithin.size)

    val all30days = last30.daysWithin.toSeq.sortBy(_.getMillis)

    assertEquals("'daysWithin' should have a size of 30!", 30, all30days.size)

    all30days.zipWithIndex.foreach(d => printf("%d: %s%n", d._2 + 1, d._1.toString(DateMidnightRange.fmt)))

    val first = all30days(0)
    assertEquals("The earliest day within should be 30 days ago!", last30.fromInclusive, first)

    val last = all30days(29)
    assertEquals("The most recent day within should be yesterday!", yesterday, last)
  }

  @Test def testGetDateMidnightRangeLastSevenDays() {
    val today = new GrvDateMidnight()
    val last7 = TimeSliceResolution.lastSevenDays.range
    val last7ByHour = TimeSliceResolution.lastSevenDays.hourRange
    println("Last 7 range:")
    println("\tfromInclusive: " + last7.fromInclusive.toString(DateMidnightRange.fmt))
    println("\ttoInclusive: " + last7.toInclusive.toString(DateMidnightRange.fmt))
    val yesterday = today.minusDays(1)

    assertEquals("The 'to' value should be yesterday's date!", yesterday, last7.toInclusive)

    val expectedHours = Hours.hoursBetween(today.minusDays(8), yesterday.toDateTime.plusHours(1)).toPeriod.getHours

    assertEquals("The duration of from -> to should be 7 days!", 7, last7.period.getDays)
    assertEquals("The duration of from -> to should be %d hours!".format(expectedHours), expectedHours, last7ByHour.period.getHours)


    val all7days = last7.daysWithin.toSeq.sortBy(_.getMillis)
    assertEquals("'daysWithin' should have a size of 7!", 7, all7days.size)

    all7days.zipWithIndex.foreach(d => printf("%d: %s%n", d._2 + 1, d._1.toString(DateMidnightRange.fmt)))

    val first = all7days(0)
    assertEquals("The earliest day within should be 7 days ago!", last7.fromInclusive, first)

    val last = all7days(6)
    assertEquals("The most recent day within should be yesterday!", yesterday, last)
  }

}