package com.gravity.interests.jobs.intelligence

import com.gravity.hbase.schema._
import com.gravity.utilities._
import com.gravity.utilities.components.FailureResult
import com.gravity.utilities.grvjson._
import com.gravity.utilities.grvz._
import org.joda.time._
import play.api.libs.json.Writes._
import play.api.libs.json._

import scala.collection._
import scalaz.ValidationNel
import scalaz.syntax.validation._


/*
 *    __   _         __         
 *   / /  (_)__  ___/ /__  ____
 *  / _ \/ / _ \/ _  / _ \/ _  /
 * /_//_/_/_//_/\_,_/\___/\_, / 
 *                       /___/
 */

class DaySchedule private (val day: DayOfWeek.Type, val schedule: Seq[LocalTimeWithZoneRange], val enabled: Boolean = true) {

  def isScheduleActive(dateTime: DateTime = grvtime.currentTime): Boolean = {
    (for {
      offset <- -1 to 1 // check yesterday, today and tomorrow due to time zone shifts
      offsetDate = dateTime.plusDays(offset)
      if day.id == offsetDate.getDayOfWeek
      if enabled
    } yield {
      schedule.exists(_.toLocalInterval(offsetDate).contains(dateTime)) ||
      (schedule.isEmpty && LocalTimeWithZoneRange.entireDay.toLocalInterval(offsetDate).contains(dateTime)) /* all day */
    }).contains(true) // any 'true's mean we are active
  }

}

object DaySchedule {

  def apply(day: DayOfWeek.Type, schedule: Seq[LocalTimeWithZoneRange], enabled: Boolean): DaySchedule = new DaySchedule(day, schedule.sortBy(_.start.time.getMillisOfDay), enabled)

  implicit val byteConverter: ComplexByteConverter[DaySchedule] {def write(data: DaySchedule, output: PrimitiveOutputStream): Unit; val version: Int; def read(input: PrimitiveInputStream): DaySchedule} = new ComplexByteConverter[DaySchedule] {
    val version = 1

    override def write(data: DaySchedule, output: PrimitiveOutputStream): Unit = {
      output.writeInt(version)
      output.writeByte(data.day.id)
      output.writeInt(data.schedule.size)
      for (s <- data.schedule) output.writeObj(s)
      output.writeBoolean(data.enabled)
    }

    override def read(input: PrimitiveInputStream): DaySchedule = {
      val version = input.readInt()
      val dayId = input.readByte()
      val size = input.readInt()

      version match {
        case v if v == 1 =>
          DaySchedule(
            DayOfWeek(dayId),
            for (i <- 0 until size) yield input.readObj[LocalTimeWithZoneRange],
            input.readBoolean()
          )
        case v => throw new IllegalStateException("Unknown version: " + v)
      }
    }
  }

  implicit val jsonFormat: Format[DaySchedule] with Object {def writes(o: DaySchedule): JsValue; def reads(json: JsValue): JsResult[DaySchedule]} = new Format[DaySchedule] {
    override def writes(o: DaySchedule): JsValue = Json.obj(
      "day" -> o.day.name,
      "enabled" -> o.enabled,
      "schedule" -> o.schedule,
      "isCurrentlyActive" -> o.isScheduleActive()
    )

    override def reads(json: JsValue): JsResult[DaySchedule] = for {
      day <- (json \ "day").validate[DayOfWeek.Type]
      enabled <- (json \ "enabled").validate[Boolean]
      schedule <- (json \ "schedule").validate[Seq[LocalTimeWithZoneRange]]
    } yield DaySchedule(day, schedule, enabled)
  }

}

class WeekSchedule private (val days: Seq[DaySchedule]) {

  def isEmpty: Boolean = days.isEmpty
  def nonEmpty: Boolean = !isEmpty

  def isScheduleActive(dateTime: DateTime = grvtime.currentTime): Boolean = {
    // empty/no-schedule will be considered active
    if (days.isEmpty) {
      return true
    }

    // check if any daily schedules are active
    days.exists(_.isScheduleActive(dateTime))
  }
}

object WeekSchedule {

  // This method will attempt to perform some organization of the supplied DaySchedule's.  For example, if a particular DaySchedule is specified twice,
  // the schedules in each will be merged into a single DaySchedule and 'enabled' flag will be set to 'true' if any of the duplicated days are set to 'true'.
  // By doing this, we allow a bit more flexibility in how we can construct schedules
  def apply(days: Seq[DaySchedule]): WeekSchedule = {
    new WeekSchedule(days.groupBy(_.day).map { case (k, v) => DaySchedule(k, v.flatMap(_.schedule).sortBy(_.start.time.getMillisOfDay), v.exists(_.enabled)) }.toList.sortBy(_.day.id))
  }

  def empty: WeekSchedule = WeekSchedule(Seq.empty)

  def validate(schedule: WeekSchedule): ValidationNel[FailureResult, WeekSchedule] = {

    val failures = new mutable.ArrayBuffer[FailureResult]

    for (day <- schedule.days) {
      day.schedule.foreach(d => {
        if (d.end.time.isBefore(d.start.time)) failures += FailureResult(day.day.name + " has end time before start time: [" + d.toDisplayString + "]")
      })

      if (failures.isEmpty) {
        day.schedule.reduceLeftOption((prev, cur) => {
          if (cur.start.time.isBefore(prev.end.time)) failures += FailureResult(day.day.name + " has overlapping schedule: [" + prev.toDisplayString + "] and [" + cur.toDisplayString + "]")
          cur
        })
      }
    }

    if (failures.nonEmpty) {
      nel(failures.head, failures.tail: _*).failure
    } else {
      schedule.successNel
    }
  }

  implicit val byteConverter: ComplexByteConverter[WeekSchedule] {def write(data: WeekSchedule, output: PrimitiveOutputStream): Unit; val version: Int; def read(input: PrimitiveInputStream): WeekSchedule} = new ComplexByteConverter[WeekSchedule] {
    val version = 1

    override def write(data: WeekSchedule, output: PrimitiveOutputStream): Unit = {
      output.writeInt(version)
      output.writeInt(data.days.size)
      for (d <- data.days) output.writeObj(d)
    }

    override def read(input: PrimitiveInputStream): WeekSchedule = {
      val version = input.readInt()
      val size = input.readInt()

      version match {
        case v if v == 1 =>
          WeekSchedule(
            for (i <- 0 until size) yield input.readObj[DaySchedule]
          )
        case v => throw new IllegalStateException("Unknown version: " + v)
      }
    }
  }

  implicit val jsonFormat: Format[WeekSchedule] with Object {def writes(o: WeekSchedule): JsValue; def reads(json: JsValue): JsResult[WeekSchedule]} = new Format[WeekSchedule] {
    override def reads(json: JsValue): JsResult[WeekSchedule] = for {
      schedule <- (json \ "days").validate[Seq[DaySchedule]]
    } yield WeekSchedule(schedule)

    override def writes(o: WeekSchedule): JsValue = Json.obj(
      "days" -> o.days,
      "isCurrentlyActive" -> o.isScheduleActive()
    )
  }
}
