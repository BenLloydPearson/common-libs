package com.gravity.domain.fieldconverters

import com.gravity.domain.FieldConverters
import com.gravity.interests.jobs.intelligence.operations.AuxiliaryClickEvent
import com.gravity.utilities.eventlogging.{FieldRegistry, FieldValueRegistry}
import com.gravity.utilities.grvfields.FieldConverter
import com.gravity.utilities.grvstrings._
import com.gravity.utilities.grvtime

trait AuxiliaryClickEventConverter {
  this: FieldConverters.type =>

  implicit object AuxiliaryClickEventConverter extends FieldConverter[AuxiliaryClickEvent] {
    val logCategory = "auxiliaryClickEvent"

    val fields: FieldRegistry[AuxiliaryClickEvent] = new FieldRegistry[AuxiliaryClickEvent]("AuxiliaryClickEvent")
    fields.registerDateTimeField("date", 0, grvtime.epochDateTime)
    fields.registerUnencodedStringField("impressionHash", 1, emptyString)
    fields.registerUnencodedStringField("clickHash", 2, emptyString)
    fields.registerStringField("pageUrl", 3, emptyString)
    fields.registerUnencodedStringField("userGuid", 4, emptyString)
    fields.registerStringField("referrer", 5, emptyString)
    fields.registerStringField("userAgent", 6, emptyString)
    fields.registerUnencodedStringField("remoteIp", 7, emptyString)

    def getFields: FieldRegistry[AuxiliaryClickEvent] = fields

    def fromValueRegistry(reg: FieldValueRegistry): AuxiliaryClickEvent = new AuxiliaryClickEvent(reg)

    def toValueRegistry(o: AuxiliaryClickEvent): FieldValueRegistry = {
      import o._
      new FieldValueRegistry(fields)
        .registerFieldValue(0, date)
        .registerFieldValue(1, impressionHash)
        .registerFieldValue(2, clickHash)
        .registerFieldValue(3, pageUrl)
        .registerFieldValue(4, userGuid)
        .registerFieldValue(5, referrer)
        .registerFieldValue(6, userAgent)
        .registerFieldValue(7, remoteIp)
    }
  }
}