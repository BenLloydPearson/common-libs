package com.gravity.domain.fieldconverters

import com.gravity.domain.FieldConverters
import com.gravity.interests.jobs.intelligence.operations.{DiscardEvent, ImpressionEvent}
import com.gravity.utilities.eventlogging.{FieldRegistry, FieldValueRegistry}
import com.gravity.utilities.grvfields.FieldConverter
import com.gravity.utilities.grvstrings._

trait DiscardImpressionEventConverter {
  this: FieldConverters.type =>

  implicit object DiscardImpressionEventConverter extends FieldConverter[DiscardEvent[ImpressionEvent]] {
    val fields: FieldRegistry[DiscardEvent[ImpressionEvent]] = new FieldRegistry[DiscardEvent[ImpressionEvent]]("DiscardEvent")
      .registerStringField("discardReason", 0, emptyString, "reason discarded")
      .registerField[ImpressionEvent]("event", 1, ImpressionEvent.empty, "default empty event")

    def fromValueRegistry(reg: FieldValueRegistry): DiscardEvent[ImpressionEvent] = new DiscardEvent[ImpressionEvent](
      reg.getValue[String](0),
      reg.getValue[ImpressionEvent](1)
    )

    def toValueRegistry(o: DiscardEvent[ImpressionEvent]): FieldValueRegistry = {
      import o._
      new FieldValueRegistry(fields)
        .registerFieldValue(0, discardReason)
        .registerFieldValue(1, event)
    }
  }
}