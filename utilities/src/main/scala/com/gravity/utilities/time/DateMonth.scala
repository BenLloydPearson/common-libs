package com.gravity.utilities.time

import com.gravity.utilities.grvtime
import org.joda.time.{Months, ReadableDateTime, ReadableInstant, DateTime}
import org.joda.time.format.{ISODateTimeFormat, DateTimeFormatter}

/*
*     __         __
*  /"  "\     /"  "\
* (  (\  )___(  /)  )
*  \               /
*  /               \
* /    () ___ ()    \  erik 2/20/15
* |      (   )      |
*  \      \_/      /
*    \...__!__.../
*/

class DateMonth private(val dateTime: DateTime) extends Serializable with Ordered[DateMonth] {
  def toString(formatter: DateTimeFormatter): String = formatter.print(dateTime)

  override def toString: String = toString(DateHour.formatter)

  def plusMonths(months: Int): DateMonth = new DateMonth(dateTime.plusMonths(months))
  def minusMonths(months: Int): DateMonth = new DateMonth(dateTime.minusMonths(months))

  def minusYears(years: Int): DateMonth = new DateMonth(dateTime.minusYears(years))

  def toDateHour: DateHour = DateHour(dateTime)

  def getMillis: Long = dateTime.getMillis

  def isBefore(y: DateHour): Boolean = dateTime.isBefore(y.dateTime)
  def isBefore(y: DateTime): Boolean = dateTime.isBefore(y)
  def isBefore(y: ReadableInstant): Boolean = dateTime.isBefore(y)
  def isAfter(y: DateHour): Boolean = dateTime.isAfter(y.dateTime)
  def isAfter(y: DateTime): Boolean = dateTime.isAfter(y)
  def isAfter(y: ReadableInstant): Boolean = dateTime.isAfter(y)
  def isEqual(y: DateHour): Boolean = dateTime.isEqual(y.dateTime)

  def isBetween(startInclusive: ReadableInstant, endExclusive: ReadableInstant): Boolean =
    (dateTime.isAfter(startInclusive) || dateTime.isEqual(startInclusive)) && dateTime.isBefore(endExclusive)

  def isBetween(startInclusive: DateHour, endExclusive: DateHour): Boolean =
    (dateTime.isAfter(startInclusive.dateTime) || dateTime.isEqual(startInclusive.dateTime)) && dateTime.isBefore(endExclusive.dateTime)

  override val hashCode: Int = dateTime.hashCode()

  override def equals(a: Any): Boolean = a match {
    case other: DateMonth => dateTime.equals(other.dateTime)
    case _ => dateTime.equals(a)
  }

  override def compare(that: DateMonth): Int = dateTime.getMillis.compareTo(that.dateTime.getMillis)
}

object DateMonth {

  implicit val ordering: Ordering[DateMonth] = new Ordering[DateMonth] {
 import com.gravity.logging.Logging._
    override def compare(x: DateMonth, y: DateMonth): Int = {
      // TJC: Changed from x.dateTime.compareTo(y.dateTime), which fails on production boxes, with this error:
      // java.lang.NoSuchMethodError: org.joda.time.DateTime.compareTo(Lorg/joda/time/ReadableInstant;)I
      x.getMillis.compareTo(y.getMillis)
    }
  }

  implicit def asReadableDateTime(dh: DateMonth): ReadableDateTime = dh.dateTime
  implicit def asReadableInstant(dh: DateMonth): ReadableInstant = dh.dateTime

  val formatter: DateTimeFormatter = ISODateTimeFormat.yearMonth

  val epoch: DateMonth = DateMonth(grvtime.epochDateTime)

  val empty: DateMonth = DateMonth(grvtime.emptyDateTime)

  def currentHour: DateHour = grvtime.currentHour

  implicit def apply(dateTime: ReadableDateTime): DateMonth = {
    val toTheMonth = dateTime.toMutableDateTime
    toTheMonth.setDayOfMonth(1)
    toTheMonth.setHourOfDay(0)
    toTheMonth.setMinuteOfHour(0)
    toTheMonth.setSecondOfMinute(0)
    toTheMonth.setMillisOfSecond(0)
    new DateMonth(toTheMonth.toDateTime)
  }

  def apply(instant: Long): DateMonth = {
    DateMonth(new DateTime(instant))
  }

  def apply(year: Int, month: Int): DateMonth = DateMonth(new DateTime(year, month, 1, 0, 0, 0, 0))

  def monthsBetween(fromInclusive: ReadableDateTime, toExclusive: ReadableDateTime): Seq[DateMonth] = {
    if (fromInclusive.isAfter(toExclusive)) return Seq.empty[DateMonth]

    val maxMonth = Months.monthsBetween(fromInclusive, toExclusive).getMonths
    val startMonth = DateMonth(fromInclusive)

    for (m <- 0 until maxMonth) yield startMonth.plusMonths(m)
  }
}

