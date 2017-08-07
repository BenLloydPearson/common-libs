package com.gravity.utilities

/**
 * Created by IntelliJ IDEA.
 * Author: Robbie Coleman
 * Date: 6/7/11
 * Time: 2:23 PM
 */

import com.gravity.utilities.time.GrvDateMidnight
import org.joda.time.format.{DateTimeFormat, DateTimeFormatter}
import java.util.{Calendar, Date}
import java.text.SimpleDateFormat
import org.joda.time.{DateTimeZone, DateTime}
import com.gravity.utilities.grvtime._
object GrvDateHelpers {

  val mySQLTimestampFormat: DateTimeFormatter = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss")
  val gravityTimeZone: DateTimeZone = DateTimeZone.forID("America/Los_Angeles")
  val gravityTimestampFormat: DateTimeFormatter = mySQLTimestampFormat.withZone(gravityTimeZone)
  val providedTimestampFormat: DateTimeFormatter = mySQLTimestampFormat.withZone(DateTimeZone.UTC)

  // grab the current datetime as a mysql friendly formatted timestamp
  def getMySQLFormattedDateString: String = {
    val DATE_FORMAT_NOW = "yyyy-MM-dd HH:mm:ss"
    val cal = Calendar.getInstance
    val sdf = new SimpleDateFormat(DATE_FORMAT_NOW)
    sdf.format(cal.getTime)
  }

  /**
  * returns a string representing a valid MySQL datetime formatted string for insertion
  * into a database
  */
  def dateTimeToMySqlDateTime(dt: DateTime): String = {
    val parser1 = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss")
    val strOutputDateTime = parser1.print(dt)
    strOutputDateTime
  }

  def mySqlDateTimeToDateTime(timeString: String): Option[DateTime] = {
    grvdates.parseDate(mySQLTimestampFormat, timeString)
  }
}

package object grvdates {
  implicit def ZZ_dt2d(x: DateTime): Date = x.toDate

  implicit def ZZ_d2dt(x: Date): DateTime = new DateTime(x.getTime)

  val minYear: Int = -292275054
  val maxYear = 292278993

  val minDateTime: DateTime = new DateTime(minYear, 1, 1, 0, 0, 0, 0)
  val maxDateTime: DateTime = new DateTime(maxYear, 12, 31, 23, 59, 59, 999)

  val minDateMidnight: GrvDateMidnight = minDateTime.toGrvDateMidnight
  val maxDateMidnight: GrvDateMidnight = maxDateTime.toGrvDateMidnight

  def parseDate(formatter: DateTimeFormatter, input: String): Option[DateTime] = {
    try {
      Some(formatter.parseDateTime(input))
    } catch {
      case _: UnsupportedOperationException => None
      case _: IllegalArgumentException => None
    }
  }

  def parseDate(pattern: String, input: String): Option[DateTime] = {
    val formatter = try {
      DateTimeFormat.forPattern(pattern)
    } catch {
      case _: IllegalArgumentException => return None
    }

    parseDate(formatter, input)
  }

  def formatDate(formatter: DateTimeFormatter, date: DateTime): String = formatter.print(date)

  def formatDate(pattern: String, date: DateTime): String = {
    val formatter = try {
      DateTimeFormat.forPattern(pattern)
    } catch {
      case _: IllegalArgumentException => return date.toString
    }

    formatDate(formatter, date)
  }


}