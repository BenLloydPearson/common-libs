package com.gravity.domain.fieldconverters

import com.gravity.domain.FieldConverters
import com.gravity.interests.jobs.intelligence.operations.recommendations.model.WidgetDomReadyEvent
import com.gravity.utilities.eventlogging.{FieldRegistry, FieldValueRegistry}
import com.gravity.utilities.grvfields.FieldConverter
import com.gravity.utilities.grvtime

trait WidgetDomReadyEventConverter {
  this: FieldConverters.type =>

  implicit object WidgetDomReadyEventConverter extends FieldConverter[WidgetDomReadyEvent] {
    val fields = new FieldRegistry[WidgetDomReadyEvent]("WidgetDomReadyEvent")
      .registerDateTimeField("dt", 0, grvtime.currentTime)
      .registerUnencodedStringField("pageViewIdHash", 1, "")
      .registerUnencodedStringField("siteGuid", 2, "")
      .registerUnencodedStringField("userGuid", 3, "")
      .registerLongField("sitePlacementId", 4, 0)
      .registerUnencodedStringField("userAgent", 5, "")
      .registerUnencodedStringField("remoteIp", 6, "")
      .registerDateTimeField("clientTime", 7, grvtime.epochDateTime, "Actual time of DOM ready generated on client side")
      .registerUnencodedStringField("gravityHost", 8, "")
      .registerUnencodedStringField("hashHex", 9, "")


    def toValueRegistry(o: WidgetDomReadyEvent) = {
      import o._
      new FieldValueRegistry(fields)
        .registerFieldValue(0, dt)
        .registerFieldValue(1, pageViewIdHash)
        .registerFieldValue(2, siteGuid)
        .registerFieldValue(3, userGuid)
        .registerFieldValue(4, sitePlacementId)
        .registerFieldValue(5, userAgent)
        .registerFieldValue(6, remoteIp)
        .registerFieldValue(7, clientTime)
        .registerFieldValue(8, gravityHost)
        .registerFieldValue(9, hashHex)
    }

    def fromValueRegistry(reg: FieldValueRegistry) = {
      new WidgetDomReadyEvent(reg)
    }
  }
}