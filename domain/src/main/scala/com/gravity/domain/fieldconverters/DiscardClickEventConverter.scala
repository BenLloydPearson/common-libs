package com.gravity.domain.fieldconverters

import com.gravity.domain.FieldConverters
import com.gravity.interests.jobs.intelligence.operations.{ClickEvent, DiscardEvent}
import com.gravity.utilities.eventlogging.{FieldRegistry, FieldValueRegistry}
import com.gravity.utilities.grvfields.FieldConverter
import com.gravity.utilities.grvstrings._

trait DiscardClickEventConverter {
  this: FieldConverters.type =>

  implicit object DiscardClickEventConverter extends FieldConverter[DiscardEvent[ClickEvent]] {
    val fields: FieldRegistry[DiscardEvent[ClickEvent]] = new FieldRegistry[DiscardEvent[ClickEvent]]("DiscardEvent")
      .registerStringField("discardReason", 0, emptyString, "reason discarded")
      .registerField[ClickEvent]("event", 1, ClickEvent.empty, "default empty event")

    def fromValueRegistry(reg: FieldValueRegistry): DiscardEvent[ClickEvent] = new DiscardEvent[ClickEvent](
      reg.getValue[String](0),
      reg.getValue[ClickEvent](1)
    )

    def toValueRegistry(o: DiscardEvent[ClickEvent]): FieldValueRegistry = {
      import o._
      new FieldValueRegistry(fields)
        .registerFieldValue(0, discardReason)
        .registerFieldValue(1, event)
    }
  }
}