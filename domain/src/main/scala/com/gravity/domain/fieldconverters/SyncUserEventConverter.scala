package com.gravity.domain.fieldconverters

import com.gravity.domain.{FieldConverters, SyncUserEvent}
import com.gravity.interests.jobs.intelligence.NothingKey
import com.gravity.interests.jobs.intelligence.hbase.ScopedKey
import com.gravity.utilities.eventlogging.{FieldRegistry, FieldValueRegistry}
import com.gravity.utilities.grvfields.FieldConverter
import com.gravity.utilities.grvtime
import org.joda.time.DateTime

trait SyncUserEventConverter {
  this: FieldConverters.type =>

  implicit object SyncUserEventConverter extends FieldConverter[SyncUserEvent] {
    override def toValueRegistry(o: SyncUserEvent): FieldValueRegistry = new FieldValueRegistry(fields)
      .registerFieldValue(0, o.dateTime)
      .registerFieldValue(1, o.partnerKey)
      .registerFieldValue(2, o.partnerUserGuid)
      .registerFieldValue(3, o.grvUserGuid)

    override def fromValueRegistry(reg: FieldValueRegistry): SyncUserEvent = {
      SyncUserEvent(
        reg.getValue[DateTime](0),
        reg.getValue[ScopedKey](1),
        reg.getValue[String](2),
        reg.getValue[String](3))
    }

    override val fields: FieldRegistry[SyncUserEvent] =
      new FieldRegistry[SyncUserEvent]("SyncUserEvent")
        .registerDateTimeField("dateTime", 0, grvtime.epochDateTime)
        .registerField[ScopedKey]("partnerKey", 1, NothingKey.toScopedKey)
        .registerStringField("partnerUserGuid", 2)
        .registerStringField("grvUserGuid", 3)
  }
}