package com.gravity.utilities.time

import com.gravity.utilities.swagger.adapter.DefaultValueWriter
import org.joda.time._
import org.joda.time.format.{DateTimeFormatter, ISODateTimeFormat}
import com.gravity.utilities.grvtime
import grvtime._
import play.api.libs.json._

class DateHour private(val dateTime: DateTime) extends Serializable with Ordered[DateHour] {
  def toString(formatter: DateTimeFormatter): String = formatter.print(dateTime)

  override def toString: String = toString(DateHour.formatter)

  def plusHours(hours: Int): DateHour = new DateHour(dateTime.plusHours(hours))
  def minusHours(hours: Int): DateHour = new DateHour(dateTime.minusHours(hours))
  def minusDays(days: Int): DateHour = new DateHour(dateTime.minusDays(days))
  def toGrvDateMidnight: GrvDateMidnight = dateTime.toGrvDateMidnight
  def toDateMinute: DateMinute = DateMinute(dateTime)

  def asMidnightHour: DateHour = {
    val toTheDay = dateTime.toMutableDateTime
    toTheDay.setHourOfDay(0)
    toTheDay.setMinuteOfHour(0)
    toTheDay.setSecondOfMinute(0)
    toTheDay.setMillisOfSecond(0)
    new DateHour(toTheDay.toDateTime)
  }

  def asWeekHour: DateHour = {
    val toTheWeek = dateTime.toMutableDateTime
    toTheWeek.setDayOfWeek(1)
    toTheWeek.setHourOfDay(0)
    toTheWeek.setMinuteOfHour(0)
    toTheWeek.setSecondOfMinute(0)
    toTheWeek.setMillisOfSecond(0)
    new DateHour(toTheWeek.toDateTime)
  }

  def asMonthHour: DateHour = {
    val toTheMonth = dateTime.toMutableDateTime
    toTheMonth.setDayOfMonth(1)
    toTheMonth.setHourOfDay(0)
    toTheMonth.setMinuteOfHour(0)
    toTheMonth.setSecondOfMinute(0)
    toTheMonth.setMillisOfSecond(0)
    new DateHour(toTheMonth.toDateTime)
  }
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
    case other: DateHour => dateTime.equals(other.dateTime)
    case _ => dateTime.equals(a)
  }

  override def compare(that: DateHour): Int = dateTime.getMillis.compareTo(that.dateTime.getMillis)
}

object DateHour {

  implicit val ordering: Ordering[DateHour] = new Ordering[DateHour] {
 import com.gravity.logging.Logging._
    override def compare(x: DateHour, y: DateHour): Int = {
      // TJC: Changed from x.dateTime.compareTo(y.dateTime), which fails on production boxes, with this error:
      // java.lang.NoSuchMethodError: org.joda.time.DateTime.compareTo(Lorg/joda/time/ReadableInstant;)I
      x.getMillis.compareTo(y.getMillis)
    }
  }

  implicit def asReadableDateTime(dh: DateHour): ReadableDateTime = dh.dateTime
  implicit def asReadableInstant(dh: DateHour): ReadableInstant = dh.dateTime

  val formatter: DateTimeFormatter = ISODateTimeFormat.dateHour

  val epoch: DateHour = DateHour(grvtime.epochDateTime)

  val empty: DateHour = DateHour(grvtime.emptyDateTime)

  def currentHour: DateHour = grvtime.currentHour

  implicit def apply(dateTime: ReadableDateTime): DateHour = {
    val toTheHour = dateTime.toMutableDateTime
    toTheHour.setMinuteOfHour(0)
    toTheHour.setSecondOfMinute(0)
    toTheHour.setMillisOfSecond(0)
    new DateHour(toTheHour.toDateTime)
  }

  def apply(instant: Long): DateHour = {
    DateHour(new DateTime(instant))
  }

  def apply(year: Int, dayOfYear: Int, hourOfDay: Int): DateHour = {
    val toTheHour = new MutableDateTime()
    toTheHour.setYear(year)
    toTheHour.setDayOfYear(dayOfYear)
    toTheHour.setHourOfDay(hourOfDay)
    DateHour(toTheHour)
  }

  def apply(year: Int, month: Int, day: Int, hour: Int): DateHour = DateHour(new DateTime(year, month, day, hour, 0, 0, 0))

  def hoursBetween(fromInclusive: ReadableDateTime, toExlusive: ReadableDateTime): Seq[DateHour] = {
    if (fromInclusive.isAfter(toExlusive)) return Seq.empty[DateHour]

    val maxHour = Hours.hoursBetween(fromInclusive, toExlusive).getHours
    val startHour = DateHour(fromInclusive)

    for (h <- 0 until maxHour) yield startHour.plusHours(h)
  }

  implicit val jsonFormat: Format[DateHour] = Format[DateHour](
    Reads[DateHour] {
      case JsString(dateTimeStr) => JsSuccess(new DateHour(DateHour.formatter.parseDateTime(dateTimeStr)))
      case _ => JsError()
    },
    Writes[DateHour](dh => JsString(dh.toString(DateHour.formatter)))
  )

  implicit val defaultValueWriter: DefaultValueWriter[DateHour] with Object {def serialize(t: DateHour): String} = new DefaultValueWriter[DateHour] {
    override def serialize(t: DateHour): String = t.getMillis.toString
  }
}