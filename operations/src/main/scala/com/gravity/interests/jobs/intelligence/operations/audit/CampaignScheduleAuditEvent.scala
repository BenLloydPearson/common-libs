package com.gravity.interests.jobs.intelligence.operations.audit

import com.gravity.interests.jobs.intelligence.WeekSchedule
import com.gravity.utilities.grvtime._
import play.api.libs.json.Json

import scala.collection.Seq

case class CampaignScheduleAuditEvent(days: Seq[DayScheduleAuditEvent])
case class ScheduleAuditEvent(from: String, to: String)
case class DayScheduleAuditEvent(day: String, enabled: Boolean, schedule: Seq[ScheduleAuditEvent])

object ScheduleAuditEvent {
  implicit val jsonWrites = Json.writes[ScheduleAuditEvent]
}

object DayScheduleAuditEvent {
  implicit val jsonWrites = Json.writes[DayScheduleAuditEvent]
}

object CampaignScheduleAuditEvent {
  def fromWeekSchedule(week: WeekSchedule) = {
    CampaignScheduleAuditEvent(week.days.map(d => {
      DayScheduleAuditEvent(d.day.name, d.enabled, d.schedule.map(s => {
        ScheduleAuditEvent(timeWithZoneFormat.print(s.start.toDateTime()), timeWithZoneFormat.print(s.end.toDateTime()))
      }))
    }))
  }

  implicit val jsonWrites = Json.writes[CampaignScheduleAuditEvent]
}