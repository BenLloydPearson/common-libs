package com.gravity.utilities

import java.util.Date

import akka.actor.ActorSystem
import com.gravity.hbase.schema._
import com.gravity.utilities.components.FailureResult
import com.gravity.utilities.grvakka.Configuration
import com.gravity.utilities.grvenum.GrvEnum
import com.gravity.utilities.grvjson._
import com.gravity.utilities.grvstrings._
import com.gravity.utilities.grvtime._
import com.gravity.utilities.grvz._
import com.gravity.utilities.grvtime._
import com.gravity.utilities.swagger.adapter.DefaultValueWriter
import com.gravity.utilities.time.{DateHour, DateMinute, DateMonth, GrvDateMidnight}
import com.gravity.valueclasses.ValueClassesForUtilities.{Seconds => _, _}
import org.apache.commons.lang.time.StopWatch
import org.joda.time._
import org.joda.time.format.{DateTimeFormat, DateTimeFormatter}
import play.api.libs.json._

import scala.concurrent.duration.{Duration, _}
import scala.util.Try
import scala.util.matching.Regex
import scalaz.ValidationNel

/**
 * Created by IntelliJ IDEA.
 * Author: Robbie Coleman
 * Date: 1/22/14
 * Time: 11:57 AM
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
object grvtime {
 import com.gravity.logging.Logging._
  val epochDateTime: DateTime = new DateTime(0l)
  val epochMillis: Long = epochDateTime.getMillis
  val emptyDateTime: DateTime = new DateTime(2000,1,1,0,0,0,0)

  val julianDayRegex: Regex = "(\\d{4})_(\\d{3})".r

  def fromEpochSeconds(epochSeconds: Int): DateTime = epochDateTime.plusSeconds(epochSeconds)

  private val maxMillisForSecondsFromEpoch = fromEpochSeconds(Int.MaxValue).getMillis

  trait HasDateTime[T] {
    def getDateTime(t: T): DateTime
  }

  implicit class DateTimeConverter(val orig: DateTime) extends AnyVal {
    def toGrvDateMidnight : GrvDateMidnight = new GrvDateMidnight(orig)
  }

  implicit class DateHourConverter(val orig: DateHour) extends AnyVal {
    def toGrvDateMidnight : GrvDateMidnight = new GrvDateMidnight(orig)
  }

  implicit class RichReadableDateTime(val orig: ReadableDateTime) extends AnyVal {
    def ageString: String = formatRelativeDuration(orig.getMillis, currentTime.getMillis)
    def toYearDay: YearDay = YearDay(orig.getYear, orig.getDayOfYear)
    def toYearDayString: String = f"${orig.getYear}_${orig.getDayOfYear}%03d"
    def toDateMonth: DateMonth = DateMonth(orig)
    def toDateHour: DateHour = DateHour(orig)
    def toDateMinute: DateMinute = DateMinute(orig)
    def secondsAgo: Int = Seconds.secondsBetween(orig, currentTime).getSeconds
    def minutesAgo: Int = Minutes.minutesBetween(orig, currentTime).getMinutes
    def hoursAgo: Int = Hours.hoursBetween(orig, currentTime).getHours
    def daysAgo: Int = Days.daysBetween(orig, currentTime).getDays

    def isMoreRecentThan(that: DateTime): Boolean = orig.compareTo(that) > 0
    def isOnOrAfter(that: DateTime): Boolean = orig.compareTo(that) > -1
    def isOlderThan(that: DateTime): Boolean = orig.compareTo(that) < 0
    def isOnOrBefore(that: DateTime): Boolean = orig.compareTo(that) < 1

    def durationUntil(that: ReadableDateTime): FiniteDuration = FiniteDuration(that.getMillis - orig.getMillis, MILLISECONDS)

    def getSeconds: Int = {
      if (orig.getMillis >= maxMillisForSecondsFromEpoch) {
        Int.MaxValue
      } else {
        Seconds.secondsBetween(epochDateTime, orig).getSeconds
      }
    }
  }

  implicit class DurationToDateTime(val dur: Duration) extends AnyVal {
    def ago: DateTime = currentTime.minus(dur.toMillis)
    def fromNow: DateTime = currentTime.plus(dur.toMillis)
  }

  implicit class intToDateTime(val i: Int) extends AnyVal {
    def secondsAgo: DateTime = currentTime.minusSeconds(i)
    def minutesAgo: DateTime = currentTime.minusMinutes(i)
    def hoursAgo: DateTime = currentTime.minusHours(i)
    def daysAgo: DateTime = currentTime.minusDays(i)
    def monthsAgo: DateTime = currentTime.minusMonths(i)
    def yearsAgo: DateTime = currentTime.minusYears(i)
    def secondsFromEpoch: DateTime = fromEpochSeconds(i)
  }

  implicit object DateTimeOrdering extends Ordering[DateTime] {
    def compare(x: DateTime, y: DateTime): Int = x.getMillis.compareTo(y.getMillis)
  }

  implicit object LocaTimeOrdering extends Ordering[LocalTime] {
    def compare(x: LocalTime, y: LocalTime): Int = x.getMillisOfDay().compareTo(y.getMillisOfDay())
  }

  implicit class javaUtilDateToDateTime(val d: Date) extends AnyVal {
    def toDateTime: DateTime = new DateTime(d.getTime)
  }

  case class LocalTimeWithZone(time: LocalTime, zone: DateTimeZone = DateTimeZone.getDefault) {
    def toDateTime(dateTime: DateTime = DateTime.now): DateTime = time.toDateTime(dateTime.withZoneRetainFields(zone))
  }

  /**
    *
    * @param julianDate param should be formatted as YYYY_DOY, e.g. 2016_005
    * @return
    */
  def fromJulian(julianDate: String): Option[GrvDateMidnight] = {
    julianDate match {
      case julianDayRegex(y, d) =>
        val year = y.toInt
        val day = d.toInt
        val md = new MutableDateTime(year, 1, 1, 0, 0, 0, 0)
        md.setDayOfYear(day)

        Some(new GrvDateMidnight(md.getMillis))
      case _ => None
    }
  }

  object LocalTimeWithZone {
 import com.gravity.logging.Logging._

    def parse(time: String, zone: DateTimeZone = DateTimeZone.getDefault): ValidationNel[FailureResult, LocalTimeWithZone] = {
      Try(LocalTimeWithZone(LocalTime.parse(time), zone)).toValidationNel(ex => FailureResult(ex.getMessage, Some(ex)))
    }

    import grvjson._

    implicit val jsonFormat: Format[LocalTimeWithZone] = new Format[LocalTimeWithZone] {
      override def reads(json: JsValue): JsResult[LocalTimeWithZone] = for {
        time <- (json \ "time").validate[String].flatMap(m => Try(LocalTime.parse(m)) match {
          case scala.util.Success(lt) => JsSuccess(lt)
          case scala.util.Failure(ex) => { warn(ex, "Invalid time: " + m); JsError("Invalid time: " + m) }
        })
        zone <- (json \ "zone").validateOptWithNoneOnAnyFailure[String].flatMap(tz => Try(tz.map(id => DateTimeZone.forID(id))) match {
          case scala.util.Success(lt) => JsSuccess(lt)
          case scala.util.Failure(ex) => { warn(ex, "Invalid zone: " + tz.getOrElse("")); JsError("Invalid zone: " + tz.getOrElse("")) }
        })
      } yield LocalTimeWithZone(time, zone.getOrElse(DateTimeZone.getDefault))

      override def writes(o: LocalTimeWithZone): JsValue = Json.obj(
        "time" -> o.time.toString(),
        "zone" -> o.zone.getID
      )
    }
  }

  implicit val localtimeJsonFormat: Format[LocalTime] = new Format[LocalTime] {
    override def reads(json: JsValue): JsResult[LocalTime] = json.validate[String].flatMap(s => Try(LocalTime.parse(s)) match {
      case scala.util.Success(lt) => JsSuccess(lt)
      case scala.util.Failure(ex) => JsError("Invalid time: " + s)
    })

    override def writes(o: LocalTime): JsValue = o.toString
  }

  def percentIntoCurrentHour: Double = currentTime.getMinuteOfHour / 60.0
  def percentIntoCurrentDay: Double = currentDay.getHourOfDay / 24.0

  //Date month should be recreated as a DateMonth object.
  def dateToMonth(date:ReadableDateTime): GrvDateMidnight = {
    val mt = date.toMutableDateTime
    mt.setDayOfMonth(1)
    new GrvDateMidnight(mt.toDateTime)
  }

  def monthsAgo(monthsAgo:Int = 0): GrvDateMidnight = {
    val mt = currentDay.minusMonths(monthsAgo).toMutableDateTime
    mt.setDayOfMonth(1)
    mt.toDateTime.toGrvDateMidnight
  }

  def firstDayOfThisWeek(forDate: ReadableDateTime): GrvDateMidnight = {
    val mt = forDate.toMutableDateTime
    mt.setDayOfWeek(1)
    mt.toDateTime.toGrvDateMidnight
  }

  def currentHour: DateHour = DateHour(currentTime)
  def nextHour: DateHour = currentHour.plusHours(1)
  def currentHourMillis: Long = currentHour.getMillis

  def currentDay: GrvDateMidnight = currentTime.toGrvDateMidnight
  def nextDay: GrvDateMidnight = currentDay.plusDays(1)

  def currentMonth: GrvDateMidnight = dateToMonth(currentDay)
  def nextMonth: GrvDateMidnight = currentMonth.plusMonths(1)

  def currentMinute: DateMinute = DateMinute(currentTime)

  def hoursAgoAsDateHour(hoursAgo:Int): DateHour = currentHour.minusHours(hoursAgo)
  def currentTime: DateTime = new DateTime(currentMillis)

  val simpleDateTimeFormat: DateTimeFormatter = DateTimeFormat.forPattern("MMM dd yyyy, HH:mm:ss a")
  val timeWithZoneFormat: DateTimeFormatter = DateTimeFormat.forPattern("K:mm:ss a")
  val elasticSearchDataMartFormat: DateTimeFormatter = DateTimeFormat.forPattern("yyyy-MM-dd")
  val elasticSearchDataMartYearMonthWildcardFormat: DateTimeFormatter = DateTimeFormat.forPattern("yyyy-MM-*")

  def formatTime(time: DateTime, format: DateTimeFormatter): String = format.print(time)
  def currentTimeString(format: DateTimeFormatter = simpleDateTimeFormat): String = formatTime(currentTime, format)
  def currentMillis: Long = DateTimeUtils.currentTimeMillis()
  def currMillis: Millis = DateTimeUtils.currentTimeMillis().asMillis

  /**
   *
   * @deprecated This can throw a java.lang.ArithmeticException when results are too big to fit into an int
   *
   * @see hoursBetweenStandard
   *
   * @param instant1
   * @param instant2
   * @return
   */
  def hoursBetween(instant1:ReadableInstant, instant2:ReadableInstant): Int = {

    try {
      org.joda.time.Hours.hoursBetween(instant1, instant2).getHours.abs
    }
    catch {
      case e: ArithmeticException => {
        // results too big to fit into int
        Int.MaxValue
      }
    }
  }

  def hoursBetweenStandard(start:ReadableInstant, end:ReadableInstant): Long = {
    val duration = new org.joda.time.Duration(start, end)
    duration.getStandardHours.abs
  }

  private var currentHourVar = DateHour(currentTime)
  private var currentHourMillisVar = currentHourVar.getMillis
  def cachedCurrentHour: DateHour = currentHourVar
  def cachedCurrentHourMillis: Long = currentHourMillisVar

  val ONE_SECOND_MILLIS: Long = 1000
  val ONE_MINUTE_MILLIS: Long = ONE_SECOND_MILLIS * 60
  val ONE_HOUR_MILLIS: Long = ONE_MINUTE_MILLIS * 60
  val ONE_DAY_MILLIS: Long = ONE_HOUR_MILLIS * 24
  val ONE_WEEK_MILLIS: Long = ONE_DAY_MILLIS * 7
  val ONE_YEAR_MILLIS: Long = ONE_WEEK_MILLIS * 52
  val ONE_MONTH_MILLIS: Long = ONE_YEAR_MILLIS / 12

  val ONE_MINUTE_SECONDS: Int = 60
  val ONE_HOUR_SECONDS: Int = ONE_MINUTE_SECONDS * 60
  val ONE_DAY_SECONDS: Int = ONE_HOUR_SECONDS * 24
  val ONE_WEEK_SECONDS: Int = ONE_DAY_SECONDS * 7
  val ONE_YEAR_SECONDS: Int = ONE_WEEK_SECONDS * 52
  val ONE_MONTH_SECONDS: Int = ONE_YEAR_SECONDS / 12

  implicit val system: ActorSystem = ActorSystem("grvtime", Configuration.defaultConf)
  import com.gravity.utilities.grvtime.system.dispatcher
  system.scheduler.schedule(1.second, 1.second)(checkUpdateCurrentHour())

  def checkUpdateCurrentHour() {
    val nowMillis = currentMillis
    val cutoff = currentHourMillisVar + ONE_HOUR_MILLIS
    if(nowMillis > cutoff) {
      info(s"GRVTIME: Current millis is $nowMillis currentHour is at $currentHourMillisVar, cutoff is $cutoff, updating.")
      currentHourVar = DateHour(currentTime)
      currentHourMillisVar = currentHourVar.getMillis
      info("GRVTIME: Current hour is now " + cachedCurrentHour)
    }
  }

  def stop() {
    system.shutdown()
  }

  def millisFromSeconds(seconds: Int): Long = ONE_SECOND_MILLIS * seconds

  def millisFromMinutes(minutes: Int): Long = ONE_MINUTE_MILLIS * minutes

  def millisFromHours(hours: Int): Long = ONE_HOUR_MILLIS * hours

  def millisFromDays(days: Int): Long = ONE_DAY_MILLIS * days

  def millisFromWeeks(weeks: Int): Long = ONE_WEEK_MILLIS * weeks

  def millisFromMonths(months: Int): Long = ONE_MONTH_MILLIS * months

  def millisFromYears(years: Int): Long = ONE_YEAR_MILLIS * years

  def secondsFromMinutes(minutes: Int): Int = ONE_MINUTE_SECONDS * minutes

  def secondsFromHours(hours: Int): Int = ONE_HOUR_SECONDS * hours

  def secondsFromDays(days: Int): Int = ONE_DAY_SECONDS * days

  def secondsFromWeeks(weeks: Int): Int = ONE_WEEK_SECONDS * weeks

  def secondsFromMonths(months: Int): Int = ONE_MONTH_SECONDS * months

  def secondsFromYears(years: Int): Int = ONE_YEAR_SECONDS * years

  private object STR {
    val COMMA = ", "
    val S = "s"
    val DAY = " day"
    val HOUR = " hour"
    val MINUTE = " minute"
    val SECOND = " second"
    val MILLISECOND = " millisecond"
    val AND = " and "
    val ZERO = "0 milliseconds"
  }

  /**
   * converts time (in milliseconds) to a human-readable format
    *
    * @param duration the total milliseconds to convert
   * @return "w days, h hours, m minutes, s seconds and z millis"
   */
  def formatDuration(duration: Long): String = {
    val buffer = new StringBuilder
    var temp = 0L
    var remaining = duration
    var needsAnd = true

    def pluralize(str: String, count: Long) {
      buffer.append(str)
      if (count > 1) buffer.append(STR.S)
    }

    if (remaining > 0) {
      temp = remaining / ONE_DAY_MILLIS
      if (temp > 0) {
        remaining -= temp * ONE_DAY_MILLIS
        buffer.append(temp)
        pluralize(STR.DAY, temp)
        if (remaining >= ONE_SECOND_MILLIS) buffer.append(STR.COMMA)
      }
      temp = remaining / ONE_HOUR_MILLIS
      if (temp > 0) {
        remaining -= temp * ONE_HOUR_MILLIS
        buffer.append(temp)
        pluralize(STR.HOUR, temp)
        if (remaining >= ONE_SECOND_MILLIS) buffer.append(STR.COMMA)
      }
      temp = remaining / ONE_MINUTE_MILLIS
      if (temp > 0) {
        remaining -= temp * ONE_MINUTE_MILLIS
        buffer.append(temp)
        pluralize(STR.MINUTE, temp)
        if (remaining >= ONE_SECOND_MILLIS) buffer.append(STR.COMMA)
      }
      temp = remaining / ONE_SECOND_MILLIS
      if (temp > 0) {
        remaining -= temp * ONE_SECOND_MILLIS
        buffer.append(temp)
        pluralize(STR.SECOND, temp)
      }
      if (remaining > 0) {
        needsAnd = false
        if (!buffer.isEmpty) buffer.append(STR.AND)
        buffer.append(remaining)
        pluralize(STR.MILLISECOND, remaining)
      }

      if (needsAnd) {
        val lastComma = buffer.lastIndexOf(STR.COMMA)
        if (lastComma > -1) {
          buffer.replace(lastComma, lastComma + 2, STR.AND)
        }
      }

      buffer.toString()

    }
    else {
      STR.ZERO
    }
  }

  /**
   * @param from          Millis, a point in time.
   * @param withRespectTo Millis, a fixed point in time from which the relative duration to from is derived. If "from"
   *                      is in the future with respect to withRespectTo, the duration will be in the future tense,
   *                      e.g. "3 hours later".
   * @param ensurePast    TRUE to assume relative duration is in the past; a convenient safe guard when the passed
   *                      from timestamp might be a few seconds in the future due to time sync issues (in that case, if
   *                      you ensurePast, you will still get "a minute ago" whereas if ensurePast is FALSE you would get
   *                      "a minute later").
    * @return Human-friendly string like "3 months ago".
   */
  def formatRelativeDuration(from: Long, withRespectTo: Long = System.currentTimeMillis(), ensurePast: Boolean = false): String = {
    val (_from, _to) = if(from <= withRespectTo) (from, withRespectTo) else (withRespectTo, from)
    val period = new Period(_from, _to)
    val relativeAdjective = if(from <= withRespectTo || ensurePast) "ago" else "later"

    if(period.getYears > 1)
      period.getYears + " years " + relativeAdjective
    else if(period.getYears == 1)
      "a year " + relativeAdjective
    else if(period.getMonths > 1)
      period.getMonths + " months " + relativeAdjective
    else if(period.getMonths == 1)
      "a month " + relativeAdjective
    else if(period.getWeeks > 1)
      period.getWeeks + " weeks " + relativeAdjective
    else if(period.getWeeks == 1)
      "a week " + relativeAdjective
    else if(period.getDays > 1)
      period.getDays + " days " + relativeAdjective
    else if(period.getDays == 1)
      "a day " + relativeAdjective
    else if(period.getHours > 1)
      period.getHours + " hours " + relativeAdjective
    else if(period.getHours == 1)
      "an hour " + relativeAdjective
    else if(period.getMinutes > 1)
      period.getMinutes + " minutes " + relativeAdjective
    else
      "a minute " + relativeAdjective
  }

  /**
   * converts time (in milliseconds) to a human-readable format
    *
    * @param duration the total milliseconds to convert
   * @return "&lt;w&gt; days, &lt;x&gt; hours, &lt;y&gt; minutes, (z) seconds and 0000 millis"
   */
  def millisToLongFormat(duration: Long): String = {
    var durationRes = duration
    val ONE_SECOND = 1000l
    val SECONDS = 60l

    val ONE_MINUTE = ONE_SECOND * 60l
    val MINUTES = 60

    val ONE_HOUR = ONE_MINUTE * 60
    val HOURS = 24l

    val ONE_DAY = ONE_HOUR * 24

    val res = new StringBuilder()
    var temp: Long = 0l
    if (durationRes > 0) {
      temp = durationRes / ONE_DAY
      if (temp > 0) {
        durationRes -= temp * ONE_DAY;
        res.append(temp).append(" day").append(if(temp > 1) "s" else "")
        .append(if(durationRes >= ONE_MINUTE) ", " else "");
      }

      temp = durationRes / ONE_HOUR;
      if (temp > 0) {
        durationRes -= temp * ONE_HOUR;
        res.append(temp).append(" hour").append(if(temp > 1) "s" else "")
        .append(if(durationRes >= ONE_MINUTE) ", " else "");
      }

      temp = durationRes / ONE_MINUTE;
      if (temp > 0) {
        durationRes -= temp * ONE_MINUTE;
        res.append(temp).append(" minute").append(if(temp > 1) "s" else "")
        .append(if(durationRes >= ONE_SECOND) ", " else "");
      }

      temp = durationRes / ONE_SECOND;
      if (temp > 0) {
        durationRes -= temp * ONE_SECOND;
        res.append(temp).append(" second").append(if(temp > 1) "s" else "");
      }

      if (!res.toString().equals("") && durationRes > 0) {
        res.append(" and ");
      }

      temp = durationRes;
      if (temp > 0) {
        res.append(temp).append(" millisecond").append(if(temp > 1) "s" else "");
      }
      res.toString();
    } else {
      "0 seconds";
    }
  }

  /**
   * @return Timecode like "m:ss".
   */
  def formatTimecode(seconds: Int): String = {
    val uSeconds = Math.max(seconds, 0)
    val minutes = uSeconds / 60
    val remainderSeconds = uSeconds % 60
    minutes + ":" + remainderSeconds.toString.padLeft(2, '0')
  }

  /**
   * Times the specified operation (`op`) and wraps its result in a [[com.gravity.utilities.grvtime.TimedResult]]
    *
    * @param op the operation to be timed
   * @tparam T the `return` type of the specified operation (`op`)
   * @return
   */
  def timedOperation[T](op: => T): TimedResult[T] = {
    val sw = new StopWatch
    sw.start()
    val result = op
    sw.stop()
    TimedResult(result, sw.getTime)
  }

  /**
   * Time the specified operation (`op`) and printlns the time
    *
    * @param op the operation to be timed
   * @tparam T the `return` type of the specified operation (`op`)
   * @return
   */
  def printTime[T](label: String)(op: => T): T = stringOpTime(label)(println)(op)

  def stringOpTime[T](label: String)(stringOp: String => Any)(op: => T): T = {
    val sw = new StopWatch
    stringOp("starting operation: " + label)
    sw.start()
    val result = op
    sw.stop()
    stringOp("done operation: " + label + " time: " + sw.getTime)
    result
  }

  case class TimedResult[T](result: T, duration: Long) {
    lazy val formattedDuration: String = formatDuration(duration)
  }

  /** This emulates legacy format we used with Lift JSON serialization. */
  implicit val grvDateMidnightFormat: Format[GrvDateMidnight] = Format(
    Reads.StringReads.map(dateStr => JodaFormats.dateFormat.parseDateTime(dateStr).toGrvDateMidnight),
    Writes[GrvDateMidnight](dm => JsString(dm.toString(JodaFormats.dateFormat)))
  )

  implicit object GrvDateMidnightDefaultValueWriter extends DefaultValueWriter[GrvDateMidnight] {
    override def serialize(t: GrvDateMidnight): String = t.getMillis.toString
  }
}


@SerialVersionUID(255414934630829772l)
object DayOfWeek extends GrvEnum[Byte] {

  case class Type(i: Byte, n: String) extends ValueTypeBase(i, n)

  override def mkValue(id: Byte, name: String): Type = Type(id, name)

  val monday: Type = Value(1, "monday")
  val tuesday: Type = Value(2, "tuesday")
  val wednesday: Type = Value(3, "wednesday")
  val thursday: Type = Value(4, "thursday")
  val friday: Type = Value(5, "friday")
  val saturday: Type = Value(6, "saturday")
  val sunday: Type = Value(7, "sunday")

  val defaultValue: Type = monday

  val JsonSerializer: EnumNameSerializer[DayOfWeek.type] = new EnumNameSerializer(this)
  implicit val jsonFormat: Format[Type] = makeJsonFormat[Type]

}


case class LocalTimeWithZoneRange(start: LocalTimeWithZone, end: LocalTimeWithZone) {

  def toLocalInterval(dateTime: DateTime): Interval = {
    new Interval(
      start.time.toDateTime(dateTime).withZoneRetainFields(start.zone),
      end.time.toDateTime(dateTime).withZoneRetainFields(end.zone)
    )
  }

  def toDisplayString: String = timeWithZoneFormat.print(start.toDateTime()) + " to " + timeWithZoneFormat.print(end.toDateTime())
}

object LocalTimeWithZoneRange {

  val entireDay: LocalTimeWithZoneRange = LocalTimeWithZoneRange(LocalTimeWithZone(LocalTime.parse("00:00:00.000")), LocalTimeWithZone(LocalTime.parse("23:59:59.999")))

  implicit val byteConverter: ComplexByteConverter[LocalTimeWithZoneRange] = new ComplexByteConverter[LocalTimeWithZoneRange] {
    val version = 1

    override def write(data: LocalTimeWithZoneRange, output: PrimitiveOutputStream): Unit = {
      output.writeInt(version)
      output.writeUTF(data.start.time.toString)
      output.writeUTF(data.start.zone.getID)
      output.writeUTF(data.end.time.toString)
      output.writeUTF(data.end.zone.getID)
    }

    override def read(input: PrimitiveInputStream): LocalTimeWithZoneRange = {
      input.readInt() match {
        case v if v == 1 => LocalTimeWithZoneRange(LocalTimeWithZone(LocalTime.parse(input.readUTF()), DateTimeZone.forID(input.readUTF())),
          LocalTimeWithZone(LocalTime.parse(input.readUTF()), DateTimeZone.forID(input.readUTF())))
      }
    }
  }

  implicit val jsonFormat: Format[LocalTimeWithZoneRange] = new Format[LocalTimeWithZoneRange] {
    override def writes(o: LocalTimeWithZoneRange): JsValue = Json.obj(
      "start" -> o.start,
      "end" -> o.end
    )

    override def reads(json: JsValue): JsResult[LocalTimeWithZoneRange] = for {
      start <- (json \ "start").validate[LocalTimeWithZone]
      end <- (json \ "end").validate[LocalTimeWithZone]
    } yield {
        LocalTimeWithZoneRange(start, end)
      }
  }

}

object Stardate {
  // based on https://github.com/pioz/stardate/blob/master/lib/stardate.rb
  // Note: This currently fails to work like its ruby counterpart. Fails miserably.
  // maybe try basing it on the python version: https://github.com/eternalthinker/stardate/blob/master/stardate.py
  val starEpochYear: Int = 2323
  val avgDaysInYear: Double = 365.2425D
  val stardateConstantX: Double = avgDaysInYear * 24 * 3600
  val stardateConstantY: Double = 1000 / avgDaysInYear / 24 / 3600

  def convert(time: DateTime): Double = {
    val daysSinceBeginningOfYear = time.getDayOfYear - 1
    val secondsSinceBeginningOfDay = time.getSecondOfDay

    val result = ((time.getYear - starEpochYear) + daysSinceBeginningOfYear / avgDaysInYear + secondsSinceBeginningOfDay / stardateConstantX) * 1000
    result
  }

  def parse(stardate: Double): DateTime = {
    val year = (stardate / 1000).toLong
    val seconds = ((stardate - (year * 1000)) / stardateConstantY).toInt

    val result = new DateTime(year).plusSeconds(seconds)
    result
  }
}

object UseStardate extends App {
  val now: DateTime = grvtime.currentTime
  println(s"Converting $now to a stardate...")
  val sdate: Double = Stardate.convert(now)
  println(s"Resulted in stardate: $sdate")

  val parsed: DateTime = Stardate.parse(sdate)
  println(s"When parsed back, $sdate becomes: $parsed")
}


