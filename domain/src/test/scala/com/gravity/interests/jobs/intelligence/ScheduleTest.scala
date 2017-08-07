package com.gravity.interests.jobs.intelligence

import com.gravity.test.domainTesting
import com.gravity.utilities._
import com.gravity.utilities.grvjson._
import com.gravity.utilities.grvtime.LocalTimeWithZone
import com.gravity.utilities.grvz._
import org.joda.time._
import play.api.libs.json.Json

import scala.collection._
import scalaz.{Failure, Success}


/*
 *    __   _         __
 *   / /  (_)__  ___/ /__  ____
 *  / _ \/ / _ \/ _  / _ \/ _  /
 * /_//_/_/_//_/\_,_/\___/\_, /
 *                       /___/
 */
class ScheduleTest extends BaseScalaTest with domainTesting {

  val time8am: LocalTimeWithZone = LocalTimeWithZone.parse("8:00").valueOr(_ => fail()) // 8am PST
  val time5pm: LocalTimeWithZone = LocalTimeWithZone.parse("20:00", DateTimeZone.forID("America/New_York")).valueOr(_ => fail()) // 5pm PST
  val time6pm: LocalTimeWithZone = LocalTimeWithZone.parse("21:00", DateTimeZone.forID("America/New_York")).valueOr(_ => fail()) // 6pm PST
  val time10pm: LocalTimeWithZone = LocalTimeWithZone.parse("22:00").valueOr(_ => fail()) // 10pm PST

  test("schedule normalize") {
    val mon1 = DaySchedule(DayOfWeek.monday, Seq(LocalTimeWithZoneRange(time8am, time5pm)), enabled = true) // enabled=true should be honored
    val mon2 = DaySchedule(DayOfWeek.monday, Seq(LocalTimeWithZoneRange(time5pm, time10pm)), enabled = false)

    // monday is duplicated and the times are not in order
    val week = WeekSchedule(Seq(mon2, mon1))
    assertResult(1)(week.days.size) // we have 1 monday entry
    assertResult(mon1.schedule ++ mon2.schedule)(week.days.head.schedule) // we have 2 times, and they are ordered
  }

  test("convert local datetime to/from json") {
    import LocalTimeWithZoneRange._

    val timeRange = (for {
      start <- LocalTimeWithZone.parse("23:00")
      end <- LocalTimeWithZone.parse("23:59")
    } yield LocalTimeWithZoneRange(start, end)).valueOr(fails => fail(fails.list.mkString(", ")))

    val json = jsonFormat.writes(timeRange)
    val read = jsonFormat.reads(json).toFailureResultValidationNel.valueOr(fails => fail(fails.list.mkString(", ")))

    assertResult(timeRange)(read)
  }

  test("schedule parse invalid time format") {
    val json =
      """
      {
        "days": [
          {
            "day": "wednesday",
            "enabled": true,
            "schedule": [
              {
                "start": {
                  "time": "22:00",
                  "zone": "America/New_York"
                },
                "end": {
                  "time": "24:00",
                  "zone": "America/New_York"
                }
              }
            ]
          }
        ]
      }
      """
    assertResult(true)(Json.fromJson[WeekSchedule](Json.parse(json)).toFailureResultValidationNel.isFailure)
  }

    test("schedule parse missing time") {
    val json =
      """
      {
        "days": [
          {
            "day": "wednesday",
            "enabled": true,
            "schedule": [
              {
                "start": {
                  "time": "22:00",
                  "zone": "America/New_York"
                },
                "end": {
                  "zone": "America/New_York"
                }
              }
            ]
          }
        ]
      }
      """
    assertResult(true)(Json.fromJson[WeekSchedule](Json.parse(json)).toFailureResultValidationNel.isFailure)
  }


  test("schedule validate overlap") {
    val mon1 = DaySchedule(DayOfWeek.monday, Seq(LocalTimeWithZoneRange(time8am, time5pm)), enabled = true)
    val mon2 = DaySchedule(DayOfWeek.monday, Seq(LocalTimeWithZoneRange(time5pm.copy(time = time5pm.time.minusMillis(1)), time10pm)), enabled = true)

    val week = WeekSchedule(Seq(mon1, mon2))
    val result = WeekSchedule.validate(week)

    result match {
      case Success(_) => fail("Expected failure")
      case Failure(fails) => assertResult(true)(fails.list.exists(_.message.contains("has overlapping schedule")))
    }
  }

  test("schedule end before start") {
    val mon1 = DaySchedule(DayOfWeek.monday, Seq(LocalTimeWithZoneRange(time5pm, time8am)), enabled = true)

    val week = WeekSchedule(Seq(mon1))
    val result = WeekSchedule.validate(week)

    result match {
      case Success(_) => fail("Expected failure")
      case Failure(fails) => assertResult(true)(fails.list.exists(_.message.contains("has end time before start ")))
    }
  }

  test("schedule is active or not") {
    val mon1 = DaySchedule(DayOfWeek.monday, Seq(LocalTimeWithZoneRange(time8am, time5pm)), enabled = true)
    val mon2 = DaySchedule(DayOfWeek.monday, Seq(LocalTimeWithZoneRange(time6pm, time10pm)), enabled = true)

    val week = WeekSchedule(Seq(mon1, mon2))
    val result = WeekSchedule.validate(week)

    val mon = grvtime.currentTime.withDayOfWeek(1) // set to Monday
    val tue = grvtime.currentTime.withDayOfWeek(2) // set to Tuesday

    result match {
      case Success(s) => {
        assertResult(false)(s.isScheduleActive(mon.withHourOfDay(0).withMinuteOfHour(0).withSecondOfMinute(0).withMillisOfSecond(0))) // 12:00am
        assertResult(true)(s.isScheduleActive(mon.withHourOfDay(8).withMinuteOfHour(0))) // 8:00am
        assertResult(true)(s.isScheduleActive(mon.withHourOfDay(16).withMinuteOfHour(0))) // 4:00pm
        assertResult(false)(s.isScheduleActive(mon.withHourOfDay(17).withMinuteOfHour(1))) // 5:01pm
        assertResult(true)(s.isScheduleActive(mon.withHourOfDay(18).withMinuteOfHour(1))) // 6:01pm
        assertResult(true)(s.isScheduleActive(mon.withHourOfDay(21).withMinuteOfHour(59))) // 9:59pm
        assertResult(false)(s.isScheduleActive(mon.withHourOfDay(22).withMinuteOfHour(1))) // 10:01pm
        assertResult(false)(s.isScheduleActive(mon.withHourOfDay(23).withMinuteOfHour(59).withSecondOfMinute(59).withMillisOfSecond(999))) // 12:59pm

        // tues there is no schedule
        assertResult(false)(s.isScheduleActive(tue.withDayOfWeek(2).withHourOfDay(8).withMinuteOfHour(0))) // 8:00am
        assertResult(false)(s.isScheduleActive(tue.withDayOfWeek(2).withHourOfDay(18).withMinuteOfHour(1))) // 6:01pm
      }
      case Failure(fails) => fail("Expected success but got: " + fails.list.mkString(", "))
    }
  }

  test("empty schedule defaults to active") {
    val s = WeekSchedule(Seq.empty)

    for {
      dow <- 1 to 7
      hod <- 0 to 23
      moh <- 0 to 59
    } yield {
      try {
        assertResult(true)(s.isScheduleActive(grvtime.currentTime.withDayOfWeek(dow).withHourOfDay(hod).withMinuteOfHour(moh)))
      } catch {
        case ex: IllegalFieldValueException => // ignore
      }
    }

  }

  test("days enabled and disabled") {
    val mon1 = DaySchedule(DayOfWeek.monday, Seq(LocalTimeWithZoneRange(time8am, time5pm), LocalTimeWithZoneRange(time6pm, time10pm)), enabled = true)
    val tue1 = DaySchedule(DayOfWeek.tuesday, Seq(LocalTimeWithZoneRange(time6pm, time10pm)), enabled = false)
    val wed1 = DaySchedule(DayOfWeek.wednesday, Seq.empty, enabled = true)
    val thu1 = DaySchedule(DayOfWeek.thursday, Seq.empty, enabled = false)

    val week = WeekSchedule(Seq(mon1, tue1, wed1, thu1))
    val result = WeekSchedule.validate(week)

    val mon = grvtime.currentTime.withDayOfWeek(1) // set to Monday
    val tue = grvtime.currentTime.withDayOfWeek(2) // set to Tuesday
    val wed = grvtime.currentTime.withDayOfWeek(3) // set to Wednesday
    val thu = grvtime.currentTime.withDayOfWeek(4) // set to Thursday

    result match {
      case Success(s) => {
        assertResult(false)(s.isScheduleActive(mon.withHourOfDay(0).withMinuteOfHour(0).withSecondOfMinute(0).withMillisOfSecond(0))) // 12:00am
        assertResult(true)(s.isScheduleActive(mon.withHourOfDay(8).withMinuteOfHour(0))) // 8:00am
        assertResult(true)(s.isScheduleActive(mon.withHourOfDay(16).withMinuteOfHour(0))) // 4:00pm
        assertResult(false)(s.isScheduleActive(mon.withHourOfDay(17).withMinuteOfHour(1))) // 5:01pm
        assertResult(true)(s.isScheduleActive(mon.withHourOfDay(18).withMinuteOfHour(1))) // 6:01pm
        assertResult(true)(s.isScheduleActive(mon.withHourOfDay(21).withMinuteOfHour(59))) // 9:59pm
        assertResult(false)(s.isScheduleActive(mon.withHourOfDay(22).withMinuteOfHour(1))) // 10:01pm
        assertResult(false)(s.isScheduleActive(mon.withHourOfDay(23).withMinuteOfHour(59).withSecondOfMinute(59).withMillisOfSecond(999))) // 12:59pm

        // tues is disabled
        assertResult(false)(s.isScheduleActive(tue.withHourOfDay(0).withMinuteOfHour(0).withSecondOfMinute(0).withMillisOfSecond(0))) // 12:00am
        assertResult(false)(s.isScheduleActive(tue.withHourOfDay(8).withMinuteOfHour(0))) // 8:00am
        assertResult(false)(s.isScheduleActive(tue.withHourOfDay(16).withMinuteOfHour(0))) // 4:00pm
        assertResult(false)(s.isScheduleActive(tue.withHourOfDay(17).withMinuteOfHour(1))) // 5:01pm
        assertResult(false)(s.isScheduleActive(tue.withHourOfDay(18).withMinuteOfHour(1))) // 6:01pm
        assertResult(false)(s.isScheduleActive(tue.withHourOfDay(21).withMinuteOfHour(59))) // 9:59pm
        assertResult(false)(s.isScheduleActive(tue.withHourOfDay(22).withMinuteOfHour(1))) // 10:01pm
        assertResult(false)(s.isScheduleActive(tue.withHourOfDay(23).withMinuteOfHour(59).withSecondOfMinute(59).withMillisOfSecond(999))) // 11:59pm

        // wed is enabled all-day
        assertResult(true)(s.isScheduleActive(wed.withHourOfDay(0).withMinuteOfHour(0).withSecondOfMinute(0).withMillisOfSecond(0))) // 12:00am
        assertResult(true)(s.isScheduleActive(wed.withHourOfDay(8).withMinuteOfHour(0))) // 8:00am
        assertResult(true)(s.isScheduleActive(wed.withHourOfDay(16).withMinuteOfHour(0))) // 4:00pm
        assertResult(true)(s.isScheduleActive(wed.withHourOfDay(17).withMinuteOfHour(1))) // 5:01pm
        assertResult(true)(s.isScheduleActive(wed.withHourOfDay(18).withMinuteOfHour(1))) // 6:01pm
        assertResult(true)(s.isScheduleActive(wed.withHourOfDay(21).withMinuteOfHour(59))) // 9:59pm
        assertResult(true)(s.isScheduleActive(wed.withHourOfDay(22).withMinuteOfHour(1))) // 10:01pm
        assertResult(true)(s.isScheduleActive(wed.withHourOfDay(23).withMinuteOfHour(59).withSecondOfMinute(59).withMillisOfSecond(998))) // 11:59pm

        // thu is disabled
        assertResult(false)(s.isScheduleActive(thu.withHourOfDay(0).withMinuteOfHour(0).withSecondOfMinute(0).withMillisOfSecond(0))) // 12:00am
        assertResult(false)(s.isScheduleActive(thu.withHourOfDay(8).withMinuteOfHour(0))) // 8:00am
        assertResult(false)(s.isScheduleActive(thu.withHourOfDay(16).withMinuteOfHour(0))) // 4:00pm
        assertResult(false)(s.isScheduleActive(thu.withHourOfDay(17).withMinuteOfHour(1))) // 5:01pm
        assertResult(false)(s.isScheduleActive(thu.withHourOfDay(18).withMinuteOfHour(1))) // 6:01pm
        assertResult(false)(s.isScheduleActive(thu.withHourOfDay(21).withMinuteOfHour(59))) // 9:59pm
        assertResult(false)(s.isScheduleActive(thu.withHourOfDay(22).withMinuteOfHour(1))) // 10:01pm
        assertResult(false)(s.isScheduleActive(thu.withHourOfDay(23).withMinuteOfHour(59).withSecondOfMinute(59).withMillisOfSecond(999))) // 12:59pm

      }
      case Failure(fails) => fail("Expected success but got: " + fails.list.mkString(", "))
    }
  }
  
  test("sunday PST with monday EST active") {

    val time1amNY = LocalTimeWithZone.parse("1:00", DateTimeZone.forID("America/New_York")).valueOr(_ => fail()) // 10pm PST
    val time2amNY = LocalTimeWithZone.parse("2:00", DateTimeZone.forID("America/New_York")).valueOr(_ => fail()) // 11pm PST

    val mon1 = DaySchedule(DayOfWeek.monday, Seq(LocalTimeWithZoneRange(time1amNY, time2amNY)), true)

    val week = WeekSchedule(Seq(mon1))
    val result = WeekSchedule.validate(week)

    result match {
      case Success(s) =>
        val sun = grvtime.currentTime.withDayOfWeek(7) // Sunday
        assertResult(false)(s.isScheduleActive(sun.withHourOfDay(9).withMinuteOfHour(59))) // 9:5pm PST
        assertResult(true)(s.isScheduleActive(sun.withHourOfDay(22).withMinuteOfHour(0))) // 10:00pm PST
        assertResult(true)(s.isScheduleActive(sun.withHourOfDay(22).withMinuteOfHour(59))) // 10:59pm PST
        assertResult(false)(s.isScheduleActive(sun.withHourOfDay(23).withMinuteOfHour(0))) // 11:00pm PST

        val mon = grvtime.currentTime.withDayOfWeek(1) // Monday
        assertResult(false)(s.isScheduleActive(sun.withHourOfDay(1).withMinuteOfHour(0))) // 1:00am PST

      case Failure(fails) => fail(fails.list.mkString(", "))
    }

  }

}
