package com.gravity.utilities.time

/*
*     __         __
*  /"  "\     /"  "\
* (  (\  )___(  /)  )
*  \               /
*  /               \
* /    () ___ ()    \  erik 4/7/15
* |      (   )      |
*  \      \_/      /
*    \...__!__.../
*/

import org.joda.time._
import org.joda.time.format.{DateTimeFormatter, ISODateTimeFormat}
import com.gravity.utilities.grvtime
import grvtime._

class DateMinute private(val dateTime: DateTime) extends Serializable with Ordered[DateMinute] {
  def toString(formatter: DateTimeFormatter): String = formatter.print(dateTime)

  override def toString: String = toString(DateMinute.formatter)

  def plusHours(hours: Int): DateMinute = new DateMinute(dateTime.plusHours(hours))
  def minusHours(hours: Int): DateMinute = new DateMinute(dateTime.minusHours(hours))
  def plusMinutes(minutes: Int): DateMinute = new DateMinute(dateTime.plusMinutes(minutes))
  def minusMinutes(minutes: Int): DateMinute = new DateMinute(dateTime.minusMinutes(minutes))
  def plusDays(days: Int): DateMinute = new DateMinute(dateTime.plusDays(days))
  def minusDays(days: Int): DateMinute = new DateMinute(dateTime.minusDays(days))
  def toDateHour: DateHour = dateTime.toDateHour
  def toGrvDateMidnight: GrvDateMidnight = dateTime.toGrvDateMidnight

  def asMidnightHour: DateMinute = {
    val toTheDay = dateTime.toMutableDateTime
    toTheDay.setHourOfDay(0)
    toTheDay.setMinuteOfHour(0)
    toTheDay.setSecondOfMinute(0)
    toTheDay.setMillisOfSecond(0)
    new DateMinute(toTheDay.toDateTime)
  }

  def asWeekHour: DateMinute = {
    val toTheWeek = dateTime.toMutableDateTime
    toTheWeek.setDayOfWeek(1)
    toTheWeek.setHourOfDay(0)
    toTheWeek.setMinuteOfHour(0)
    toTheWeek.setSecondOfMinute(0)
    toTheWeek.setMillisOfSecond(0)
    new DateMinute(toTheWeek.toDateTime)
  }

  def asMonthHour: DateMinute = {
    val toTheMonth = dateTime.toMutableDateTime
    toTheMonth.setDayOfMonth(1)
    toTheMonth.setHourOfDay(0)
    toTheMonth.setMinuteOfHour(0)
    toTheMonth.setSecondOfMinute(0)
    toTheMonth.setMillisOfSecond(0)
    new DateMinute(toTheMonth.toDateTime)
  }
  def getMillis: Long = dateTime.getMillis
  def isBefore(y: DateMinute): Boolean = dateTime.isBefore(y.dateTime)
  def isBefore(y: DateTime): Boolean = dateTime.isBefore(y)
  def isBefore(y: ReadableInstant): Boolean = dateTime.isBefore(y)
  def isAfter(y: DateMinute): Boolean = dateTime.isAfter(y.dateTime)
  def isAfter(y: DateTime): Boolean = dateTime.isAfter(y)
  def isAfter(y: ReadableInstant): Boolean = dateTime.isAfter(y)
  def isEqual(y: DateMinute): Boolean = dateTime.isEqual(y.dateTime)

  def isBetween(startInclusive: ReadableInstant, endExclusive: ReadableInstant): Boolean =
    (dateTime.isAfter(startInclusive) || dateTime.isEqual(startInclusive)) && dateTime.isBefore(endExclusive)

  def isBetween(startInclusive: DateMinute, endExclusive: DateMinute): Boolean =
    (dateTime.isAfter(startInclusive.dateTime) || dateTime.isEqual(startInclusive.dateTime)) && dateTime.isBefore(endExclusive.dateTime)

  override val hashCode: Int = dateTime.hashCode()

  override def equals(a: Any): Boolean = a match {
    case other: DateMinute => dateTime.equals(other.dateTime)
    case _ => dateTime.equals(a)
  }

  override def compare(that: DateMinute): Int = dateTime.getMillis.compareTo(that.dateTime.getMillis)
}

object DateMinute {

  implicit val ordering: Ordering[DateMinute] = new Ordering[DateMinute] {
 import com.gravity.logging.Logging._
    override def compare(x: DateMinute, y: DateMinute): Int = {
      // TJC: Changed from x.dateTime.compareTo(y.dateTime), which fails on production boxes, with this error:
      // java.lang.NoSuchMethodError: org.joda.time.DateTime.compareTo(Lorg/joda/time/ReadableInstant;)I
      x.getMillis.compareTo(y.getMillis)
    }
  }

  implicit def asReadableDateTime(dh: DateMinute): ReadableDateTime = dh.dateTime
  implicit def asReadableInstant(dh: DateMinute): ReadableInstant = dh.dateTime

  val formatter: DateTimeFormatter = ISODateTimeFormat.dateHourMinute

  val epoch: DateMinute = DateMinute(grvtime.epochDateTime)

  val empty: DateMinute = DateMinute(grvtime.emptyDateTime)

  def currentHour: DateHour = grvtime.currentHour

  implicit def apply(dateTime: ReadableDateTime): DateMinute = {
    val toTheHour = dateTime.toMutableDateTime
    toTheHour.setSecondOfMinute(0)
    toTheHour.setMillisOfSecond(0)
    new DateMinute(toTheHour.toDateTime)
  }

  def apply(instant: Long): DateMinute = {
    DateMinute(new DateTime(instant))
  }

  def apply(year: Int, dayOfYear: Int, hourOfDay: Int): DateMinute = {
    val toTheHour = new MutableDateTime()
    toTheHour.setYear(year)
    toTheHour.setDayOfYear(dayOfYear)
    toTheHour.setHourOfDay(hourOfDay)
    DateMinute(toTheHour)
  }

  def apply(year: Int, month: Int, day: Int, hour: Int, minute: Int): DateMinute = DateMinute(new DateTime(year, month, day, hour, minute, 0, 0))

  def hoursBetween(fromInclusive: ReadableDateTime, toExclusive: ReadableDateTime): Seq[DateMinute] = {
    if (fromInclusive.isAfter(toExclusive)) return Seq.empty[DateMinute]

    val maxHour = Hours.hoursBetween(fromInclusive, toExclusive).getHours
    val startHour = DateMinute(fromInclusive)

    for (h <- 0 until maxHour) yield startHour.plusHours(h)
  }
}
