package com.gravity.utilities.analytics

import com.gravity.utilities.MurmurHash
import com.gravity.utilities.ScalaMagic._
import com.gravity.utilities.grvstrings._
import com.gravity.utilities.grvtime._
import com.gravity.utilities.swagger.adapter.DefaultValueWriter
import com.gravity.utilities.time._
import org.joda.time._
import org.joda.time.format.{DateTimeFormat, DateTimeFormatter}
import play.api.libs.json._

import scala.util.matching.Regex
import scalaz.NonEmptyList
import scalaz.std.option._
import scalaz.syntax.apply._
import scalaz.syntax.std.option._


/**
* Created by Robbie Coleman
* User: robbie
* Date: 5/2/11
*/

//"last" is exclusive of today/now, "past" is inclusive
object TimeSliceResolutions {
  val HOURLY = "hourly"
  val DAILY = "daily"
  val WEEKLY = "weekly"
  val MONTHLY = "monthly"
  val YEARLY = "yearly"
  val ALL_TIME = "alltime"
  val TODAY = "today"
  val YESTERDAY = "yesterday"
  val LAST_30_DAYS = "lastthirtydays"
  val LAST_15_DAYS = "lastfifteendays"
  val LAST_7_DAYS = "lastsevendays"
  val YESTERDAY_AND_TODAY = "yesterday:today"
  val PAST_3_DAYS = "pastthreedays"
  val PAST_8_DAYS = "pasteightdays"
  val PAST_90_DAYS = "pastninetydays"
  val PAST_24_HOURS = "past24hours"
  val PAST_48_HOURS = "past48hours"
  val PAST_6_MONTHS = "pastsixmonths"

  val spliter: Regex = "[_-]".r

  val ALL_TIME_RESULT: (String, Int, Int, Int) = (ALL_TIME, 1, 1, 0)

  val possibleValues: Set[String] = Set(DAILY, WEEKLY, MONTHLY, YEARLY, ALL_TIME, LAST_30_DAYS, LAST_7_DAYS, YESTERDAY, TODAY, YESTERDAY_AND_TODAY, PAST_3_DAYS, PAST_8_DAYS, PAST_90_DAYS, PAST_24_HOURS, HOURLY)

  def isValid(resolution: String): Boolean = possibleValues.contains(resolution)

  lazy val _toString: String = possibleValues.mkString("TimeSliceResolutions {possibleValues: [", ", ", "]}")

  override def toString: String = _toString

  def getCurrentDayParts(resolution: String): (String, Int, Int, Int) = {
    val curDate = new GrvDateMidnight()
    (resolution, curDate.getYear, curDate.getDayOfYear, 0)
  }

  def getCurrentHourParts(resolution: String): (String, Int, Int, Int) = {
    val curDate = new DateTime()
    (resolution, curDate.getYear, curDate.getDayOfYear, curDate.getHourOfDay)
  }

  def parse(input: String): Option[TimeSliceResolution] = parseIntoFields(input) match {
    //case Some((res, year, point)) => Some(TimeSliceResolution(res, year, point))
    case Some((res, year, point, point2)) => Some(TimeSliceResolution(res, year, point, point2))
    case None => None
  }

  def parseIntoFields(input: String): Option[(String, Int, Int, Int)] = {
    if (isNullOrEmpty(input)) return None

    if (input.startsWith("today-")) {
      input.substring(6).tryToInt match {
        case Some(daysBack) =>
          val date = new DateTime().minusDays(daysBack)
          return Some((DAILY, date.getYear, date.getDayOfYear, 0))
        case None => return None
      }

    }

    Some(spliter.split(input.toLowerCase).toList match {
      case PAST_24_HOURS :: Nil => (PAST_24_HOURS, 1, 1, 0)
      case PAST_48_HOURS :: Nil => (PAST_48_HOURS, 1, 1, 0)
      case ALL_TIME :: Nil => (ALL_TIME, 1, 1, 0)
      case LAST_30_DAYS :: Nil => (LAST_30_DAYS, 1, 1, 0)
      case LAST_7_DAYS :: Nil => (LAST_7_DAYS, 1, 1, 0)
      case YESTERDAY_AND_TODAY :: Nil => (YESTERDAY_AND_TODAY, 1, 1, 0)
      case PAST_3_DAYS :: Nil => (PAST_3_DAYS, 1, 1, 0)
      case PAST_8_DAYS :: Nil => (PAST_8_DAYS, 1, 1, 0)
      case PAST_90_DAYS :: Nil => (PAST_90_DAYS, 1, 1, 0)
      case PAST_6_MONTHS :: Nil => (PAST_6_MONTHS, 1, 1, 0)
      case TODAY :: Nil => getCurrentDayParts(DAILY)
      case YESTERDAY :: Nil =>
        val yesterday = new GrvDateMidnight().minusDays(1)
        (DAILY, yesterday.getYear, yesterday.getDayOfYear, 0)
      case YEARLY :: yearStr :: Nil => yearStr.tryToInt match {
        case Some(year) => (YEARLY, year, year, 0)
        case None => return None
      }
      case resolution :: yearStr :: pointStr :: point2Str :: Nil =>
        for (year <- yearStr.tryToInt; point <- pointStr.tryToInt; point2 <- point2Str.tryToInt) {
          if (year == 1 && resolution != ALL_TIME) {
            return Some(getCurrentDayParts(resolution))
          }

          if (year != 1 && point != 1 && ALL_TIME == resolution) {
            return Some(ALL_TIME_RESULT)
          }

          return Some(resolution, year, point, point2)
        }
        return None
      case resolution :: yearStr :: pointStr :: Nil =>
        for (year <- yearStr.tryToInt; point <- pointStr.tryToInt) {
          if (year == 1 && resolution != ALL_TIME) {
            return Some(getCurrentHourParts(resolution))
          }

          if (year != 1 && point != 1 && ALL_TIME == resolution) {
            return Some(ALL_TIME_RESULT)
          }

          return Some(resolution, year, point, 0)
        }
        return None
      case _ => return None
    })
  }

  // TJC Note that the "range too big" errors below will not show up in DateHourRange.hoursWithin unless the
  // DateHourRange was constructed with a fromInclusive date that is exactly on-the-hour, due to the behavior
  // of DateHour.hoursBetween, which is called by DateHour.hoursWithin. But the DateHourRange is still wrong,
  // and the problem will always show up in DateHourRange.toHour (vs. DateHourRange.fromHur).
  def getDateHourRange(resolution: String, year: Int = 1, point : Int = 1, point2 : Int = 0): DateHourRange = {
    val now = new DateTime()
    val nowDateMidnight = now.toGrvDateMidnight
    resolution match {
      case PAST_24_HOURS =>
        // TJC TODO: Fix - The returned range is too big -- it includes the first hour past the end of the desired range.
        DateHourRange(now.minusHours(24), now)
      case PAST_48_HOURS =>
        // TJC TODO: Fix - The returned range is too big -- it includes the first hour past the end of the desired range.
        DateHourRange(now.minusHours(48), now)
      case HOURLY =>
        // TJC TODO: Fix - The returned range is too big -- it includes the first hour past the end of the desired range.
        val date = now.withYear(year).withDayOfYear(point).withHourOfDay(point2)
        DateHourRange(date, date.plusHours(1))
      case DAILY =>
        // TJC TODO: Fix - The returned range is too big -- it includes the first hour past the end of the desired range.
        val date = now.withYear(year).withDayOfYear(point).withHourOfDay(point2)
        DateHourRange.forSingleDay(date)
      case WEEKLY =>
        // TJC TODO: Fix - The returned range is too big -- it includes the first hour past the end of the desired range.
        val from = now.withWeekyear(year).withWeekOfWeekyear(point)
        val to = from.plusDays(6)
        DateHourRange(from, to)
      case MONTHLY =>
        // TJC TODO: Fix - The returned range is too small -- it is missing the last 23 hours of the last day.
        val from = new GrvDateMidnight(year, point, 1)
        val to = from.plusMonths(1).minusDays(1)
        DateHourRange(from, to)
      case YEARLY =>
        // TJC TODO: Fix - The returned range is too small -- it is missing the last 23 hours of the last day.
        val from = new GrvDateMidnight(year, 1, 1)
        val to = from.plusYears(1).minusDays(1)
        DateHourRange(from, to)
      case ALL_TIME => DateHourRange(new GrvDateMidnight(1,1,1), now)

      // TJC TODO: Fix - {LAST_30_DAYS, LAST_7_DAYS, YESTERDAY_AND_TODAY, PAST_3_DAYS, PAST_8_DAYS, PAST_90_DAYS, TODAY} all have the same error:
      // The returned range is wrong -- it has an extra day at the beginning and is missing the last 23 hours of the last day.
      // In all those cases, it is probably usually harmless, since the extra hour is in the future and doesn't have any metrics yet.
      case LAST_30_DAYS => DateHourRange(nowDateMidnight.minusDays(31), nowDateMidnight.minusDays(1))
      case LAST_7_DAYS => DateHourRange(nowDateMidnight.minusDays(8), nowDateMidnight.minusDays(1))
      case YESTERDAY_AND_TODAY => DateHourRange(nowDateMidnight.minusDays(1).toDateTime, nowDateMidnight.plusDays(1))
      case PAST_3_DAYS => DateHourRange(nowDateMidnight.minusDays(2), nowDateMidnight.plusDays(1))
      case PAST_8_DAYS => DateHourRange(nowDateMidnight.minusDays(7), nowDateMidnight.plusDays(1))
      case PAST_90_DAYS => DateHourRange(nowDateMidnight.minusDays(90), nowDateMidnight.plusDays(1))  // TJC: This is actually 91 days.
      case TODAY => DateHourRange.forSingleDay(now)

      // TJC TODO: Fix - The returned range is too large - it includes the first hour past the end of the desired range.
      // Note that unlike the group above, the extra hour is likely to have existing metrics.
      case YESTERDAY => DateHourRange.forSingleDay(now.minusDays(1))
    }
  }

  def getDateMidnightRange(resolution: String, year: Int = 1, point: Int = 1): DateMidnightRange = {
    val today = new GrvDateMidnight()

    resolution match {
      case DAILY =>
        val date = today.withYear(year).withDayOfYear(point)
        DateMidnightRange.forSingleDay(date)
      case WEEKLY =>
        val from = today.withWeekyear(year).withWeekOfWeekyear(point)
        val to = from.plusDays(6)
        DateMidnightRange(from, to)
      case MONTHLY =>
        val from = new GrvDateMidnight(year, point, 1)
        val to = from.plusMonths(1).minusDays(1)
        DateMidnightRange(from, to)
      case YEARLY =>
        val from = new GrvDateMidnight(year, 1, 1)
        val to = from.plusYears(1).minusDays(1)
        DateMidnightRange(from, to)
      case ALL_TIME => DateMidnightRange(new GrvDateMidnight(1,1,1), today)
      case LAST_30_DAYS => DateMidnightRange(today.minusDays(30), today.minusDays(1))
      case LAST_15_DAYS => DateMidnightRange(today.minusDays(15), today.minusDays(1))
      case LAST_7_DAYS => DateMidnightRange(today.minusDays(7), today.minusDays(1))
      case PAST_6_MONTHS => DateMidnightRange(today.minusMonths(6), today)
      case YESTERDAY_AND_TODAY => DateMidnightRange(today.minusDays(1), today)
      case PAST_3_DAYS => DateMidnightRange(today.minusDays(2), today)
      case PAST_8_DAYS => DateMidnightRange(today.minusDays(7), today)
      case PAST_90_DAYS => DateMidnightRange(today.minusDays(90), today)  // TJC: This is actually 91 days.
      case TODAY => DateMidnightRange.forSingleDay(today)
      case PAST_24_HOURS => DateMidnightRange.forSingleDay(today)
      case PAST_48_HOURS => DateMidnightRange(today.minusDays(1), today)
      case YESTERDAY => DateMidnightRange.forSingleDay(today.minusDays(1))
      case invalid => throw new IllegalArgumentException("The resolution passed: \'%s\" is not valid! %s".format(invalid, _toString))
    }
  }
}

case class DateHourRange(fromInclusive: ReadableDateTime, toInclusive: ReadableDateTime) {
  lazy val toExclusive: DateTime = toInclusive.toDateTime.plusHours(1)
  lazy val hoursWithin: Set[DateHour] = DateHour.hoursBetween(fromInclusive, DateHour(toExclusive)).toSet
  lazy val period: Period = Hours.hoursBetween(fromInclusive, toExclusive).toPeriod

  def fromHour: DateHour = DateHour(fromInclusive)
  def toHour: DateHour = DateHour(toInclusive)

  def toMidnightRange: DateMidnightRange = DateMidnightRange(fromInclusive.toDateTime.toGrvDateMidnight, toInclusive.toDateTime.toGrvDateMidnight)

  def contains(dateTime: ReadableDateTime): Boolean = {
    (fromInclusive.isEqual(dateTime) || fromInclusive.isBefore(dateTime)) && (toInclusive.isEqual(dateTime) || toInclusive.isAfter(dateTime))
  }

  def containsExclusiveTo(dateTime: ReadableDateTime): Boolean = {
    (fromInclusive.isEqual(dateTime) || fromInclusive.isBefore(dateTime)) && toInclusive.isAfter(dateTime)
  }
}

object DateHourRange {
  def forTimeSlice(timeSlice: TimeSliceResolution): DateHourRange = TimeSliceResolutions.getDateHourRange(timeSlice.resolution, timeSlice.year, timeSlice.point, timeSlice.point2)

  // TJC TODO: Fix - forSingleDay is returning a range that selects 25 hours (because of picking up the first hour of the next day).
  def forSingleDay(day: ReadableDateTime): DateHourRange = DateHourRange(new GrvDateMidnight(day).toDateTime, new GrvDateMidnight(day).plusDays(1).toDateTime)

  def forSingleHour(hour: DateHour = DateHour.currentHour): DateHourRange = {
    val dt = hour.toDateTime
    DateHourRange(dt, dt)
  }
}

case class DateMidnightRange(fromInclusive: GrvDateMidnight, toInclusive: GrvDateMidnight) {
  lazy val sortedDays: List[GrvDateMidnight] = daysWithin.toList.sortBy(_.getMillis)

  def splitAfter(oldest: GrvDateMidnight): (List[GrvDateMidnight], List[GrvDateMidnight]) = {
    val oldestIdx = sortedDays.indexOf(oldest) + 1

    if (oldestIdx < 1) return (sortedDays, Nil)

    sortedDays.splitAt(oldestIdx)
  }

  def startHourExclusive: DateHour = fromInclusive.toDateHour.minusHours(1)

  lazy val toExclusive: GrvDateMidnight = toInclusive.plusDays(1)

  lazy val toApiParamValue: String = {
    val b = new StringBuilder
    b.append("daily_").append(fromInclusive.getYear).append("_").append(fromInclusive.getDayOfYear)
    b.append(":")
    b.append("daily_").append(toInclusive.getYear).append("_").append(toInclusive.getDayOfYear)
    b.toString()
  }

  lazy val daysWithin: Set[GrvDateMidnight] = {
    if (fromInclusive.equals(toInclusive)) {
      Set(fromInclusive)
    } else if (fromInclusive.isAfter(toInclusive)) {
      Set.empty[GrvDateMidnight]
    } else {
      val maxDays = Days.daysBetween(fromInclusive, toInclusive).getDays
      (for (i <- 0 to maxDays) yield fromInclusive.plusDays(i)).toSet
    }
  }

  lazy val percentOfMonthsContained: Double = {
    val firstMonthDays = fromInclusive.dayOfMonth().getMaximumValue
    if (fromInclusive.monthOfYear() == toInclusive.monthOfYear()) {
      daysWithin.size.toDouble / firstMonthDays.toDouble
    } else {

      val firstMonthElapsedDays = firstMonthDays - fromInclusive.getDayOfMonth
      val lastMonthElapsedDays = toInclusive.getDayOfMonth
      val lastMonthDays = toInclusive.dayOfMonth().getMaximumValue

      val firstMonthPercent = firstMonthElapsedDays.toDouble / firstMonthDays.toDouble
      val lastMonthPercent = lastMonthElapsedDays.toDouble / lastMonthDays.toDouble
      val inBetweenMonthsPercent = Months.monthsBetween(fromInclusive, toInclusive).getMonths.toDouble

      firstMonthPercent + lastMonthPercent + inBetweenMonthsPercent
    }
  }

  def toHourRange: DateHourRange = DateHourRange(fromInclusive, toInclusive)

  lazy val hoursWithin: Seq[DateHour] = DateHour.hoursBetween(fromInclusive, toExclusive)

  lazy val singleDayRangesWithin: Set[DateMidnightRange] = daysWithin.map(DateMidnightRange.forSingleDay(_))

  lazy val singleDayIntervalsWithin: Set[Interval] = singleDayRangesWithin.map(_.interval)

  lazy val period: Period = Days.daysBetween(fromInclusive, toExclusive).toPeriod

  def interval: Interval = {
    // TJC TODO: Fix - This test is fragile: it will suddenly stop working when today flips over into tomorrow.
    if (this == TimeSliceResolution.allTime.range) {
      TimeSliceResolution.intervalForAllTime
    } else {
      new Interval(fromInclusive, toExclusive)
    }
  }

  def contains(dm: ReadableDateTime): Boolean = {
    dm.isEqual(fromInclusive) || (dm.isAfter(fromInclusive) && dm.isBefore(toExclusive))
  }

  def containsAny(days: Traversable[GrvDateMidnight]): Boolean = {
    for (dm <- days) if (contains(dm)) return true
    false
  }

  def slideBackDays(days: Int): DateMidnightRange = DateMidnightRange(fromInclusive.minusDays(days), toInclusive.minusDays(days))

  def slideAheadDays(days: Int): DateMidnightRange = DateMidnightRange(fromInclusive.plusDays(days), toInclusive.plusDays(days))

  def isSingleDay: Boolean = fromInclusive == toInclusive

  override def toString: String = "DateMidnightRange( from: \"" +
      fromInclusive.toString(DateMidnightRange.fmt) + "\", to: \"" +
      toInclusive.toString(DateMidnightRange.fmt) + "\" = " +
      period.getDays + " day(s) )"

  def toDateHourRange: DateHourRange = DateHourRange(fromInclusive, toInclusive.toDateHour.plusHours(23))
}

object DateMidnightRange {

  val fmt: DateTimeFormatter = DateTimeFormat.forPattern("MM/dd/yyyy")

  def forDateTimes(startDate: DateTime, endDate: DateTime): DateMidnightRange = DateMidnightRange(new GrvDateMidnight(startDate), new GrvDateMidnight(endDate))

  def forSingleDay(day: GrvDateMidnight): DateMidnightRange = DateMidnightRange(day, day)

  def forTimeSlice(timeSlice: TimeSliceResolution): DateMidnightRange = TimeSliceResolutions.getDateMidnightRange(timeSlice.resolution, timeSlice.year, timeSlice.point)

  def forInterval(interval: Interval): DateMidnightRange = DateMidnightRange(interval.getStart.toGrvDateMidnight, interval.getEnd.toGrvDateMidnight.minusDays(1))

  def singleDayRangesBetween(fromInclusive: GrvDateMidnight, toInclusive: GrvDateMidnight): Set[DateMidnightRange] = DateMidnightRange(fromInclusive, toInclusive).daysWithin.map(DateMidnightRange.forSingleDay)

  def forDays(days: NonEmptyList[GrvDateMidnight]): DateMidnightRange = {
    if (days.size == 1) return forSingleDay(days.head)
    val sortedDays = days.list.sortBy(_.getMillis)
    DateMidnightRange(sortedDays.head, sortedDays.takeRight(1).head)
  }

  def parse(input: String): Option[DateMidnightRange] = {

    def getFromTo: Option[Array[String]] = {
      if (input.contains(":")) {
        tokenize(input, ":", 2).some
      } else if (input.contains("~")) {
        tokenize(input, "~", 2).some
      } else {
        None
      }
    }

    getFromTo match {
      case Some(from_to) =>
        if (from_to.length == 2) {
          parseGrvDateMidnight(from_to(0)) tuple parseGrvDateMidnight(from_to(1)) match {
            case Some((from, to)) =>
              if (from.isBefore(to) || from.isEqual(to)) {
                Some(DateMidnightRange(from, to))
              } else {
                None
              }
            case None => None
          }
        } else {
          None
        }
      case None =>
        TimeSliceResolutions.parseIntoFields(input) match {
          case Some((res, year, point, hour)) => Some(TimeSliceResolution(res, year, point, hour).range)
          case None => None
        }
    }
  }

  private val todayMinusPrefix = TimeSliceResolutions.TODAY + "-"
  private def parseGrvDateMidnight(s: String): Option[GrvDateMidnight] = s match {
    case TimeSliceResolutions.TODAY => Some(new GrvDateMidnight())
    case TimeSliceResolutions.YESTERDAY => Some(new GrvDateMidnight().minusDays(1))
    case todayMinus if todayMinus.startsWith(todayMinusPrefix) =>
      val today = new GrvDateMidnight()
      val days = s.substring(6).tryToInt.getOrElse(0)
      Some(today.minusDays(days))
    case daily if daily.startsWith(TimeSliceResolutions.DAILY) => TimeSliceResolutions.parseIntoFields(s) match {
      case Some((res, year, point, hour)) => Some(TimeSliceResolution(res, year, point, hour).range.fromInclusive)
      case None => None
    }
    case _ => None
  }

  implicit val jsonFormat: Format[DateMidnightRange] = Json.format[DateMidnightRange]
  implicit val defaultValueWriter: DefaultValueWriter[DateMidnightRange] with Object {def serialize(t: DateMidnightRange): String} = new DefaultValueWriter[DateMidnightRange] {
    override def serialize(t: DateMidnightRange): String = t.toApiParamValue
  }
}

case class TimeSliceResolution(resolution: String, year: Int = 1, point: Int = 1, point2 : Int = 0) {

  lazy val string: String = "%s_%d_%d_%d".format(resolution, year, point, point2)

  override def toString: String = string

  lazy val hashed: Long = MurmurHash.hash64(string)

  def getMurmur: Long = hashed

  def range: DateMidnightRange = DateMidnightRange.forTimeSlice(this)
  def hourRange: DateHourRange = DateHourRange.forTimeSlice(this)

  /** @return An [begInclusive, endExclusive) interval for the selected date range (backed by DateMidnightRange.forTimeSlice) */
  def interval: Interval = if (isAllTime) {
    TimeSliceResolution.intervalForAllTime
  } else range.interval

  def isAllTime: Boolean = this == TimeSliceResolution.allTime

  def rangeAsString: String = if (isAllTime) TimeSliceResolutions.ALL_TIME else range.toString
}

object TimeSliceResolution {
  def apply(parts: (String, Int, Int)): TimeSliceResolution = TimeSliceResolution(parts._1, parts._2, parts._3)
  def apply(parts: (String, Int, Int, Int)): TimeSliceResolution = TimeSliceResolution(parts._1, parts._2, parts._3, parts._4)
  def lastThirtyDays: TimeSliceResolution = TimeSliceResolution(TimeSliceResolutions.LAST_30_DAYS)
  def lastFifteenDays: TimeSliceResolution = TimeSliceResolution(TimeSliceResolutions.LAST_15_DAYS)
  def lastSevenDays: TimeSliceResolution = TimeSliceResolution(TimeSliceResolutions.LAST_7_DAYS)
  def yesterdayAndToday: TimeSliceResolution = TimeSliceResolution(TimeSliceResolutions.YESTERDAY_AND_TODAY)
  def pastThreeDays: TimeSliceResolution = TimeSliceResolution(TimeSliceResolutions.PAST_3_DAYS)
  def pastEightDays: TimeSliceResolution = TimeSliceResolution(TimeSliceResolutions.PAST_8_DAYS)
  def pastNinetyDays: TimeSliceResolution = TimeSliceResolution(TimeSliceResolutions.PAST_90_DAYS)
  def past24Hours: TimeSliceResolution = TimeSliceResolution(TimeSliceResolutions.PAST_24_HOURS)
  def past48Hours: TimeSliceResolution = TimeSliceResolution(TimeSliceResolutions.PAST_48_HOURS)
  def past6Months: TimeSliceResolution = TimeSliceResolution(TimeSliceResolutions.PAST_6_MONTHS)
  def today: TimeSliceResolution = TimeSliceResolutions.parse("today").get
  def yesterday: TimeSliceResolution = TimeSliceResolutions.parse("yesterday").get

  val allTime: TimeSliceResolution = TimeSliceResolution(TimeSliceResolutions.ALL_TIME)
  val intervalForAllTime: Interval = new Interval(Long.MinValue, Long.MaxValue)
}