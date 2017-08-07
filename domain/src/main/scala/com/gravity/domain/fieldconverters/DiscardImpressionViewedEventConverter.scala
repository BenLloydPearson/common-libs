package com.gravity.domain.fieldconverters

import com.gravity.domain.FieldConverters
import com.gravity.interests.jobs.intelligence.operations.{DiscardEvent, ImpressionViewedEvent}
import com.gravity.utilities.eventlogging.{FieldRegistry, FieldValueRegistry}
import com.gravity.utilities.grvfields.FieldConverter
import com.gravity.utilities.grvstrings._

trait DiscardImpressionViewedEventConverter {
  this: FieldConverters.type =>

  implicit object DiscardImpressionViewedEventConverter extends FieldConverter[DiscardEvent[ImpressionViewedEvent]] {
    val fields: FieldRegistry[DiscardEvent[ImpressionViewedEvent]] = new FieldRegistry[DiscardEvent[ImpressionViewedEvent]]("DiscardEvent")
      .registerStringField("discardReason", 0, emptyString, "reason discarded")
      .registerField[ImpressionViewedEvent]("event", 1, ImpressionViewedEvent.empty, "default empty event")

    def fromValueRegistry(reg: FieldValueRegistry): DiscardEvent[ImpressionViewedEvent] = new DiscardEvent[ImpressionViewedEvent](
      reg.getValue[String](0),
      reg.getValue[ImpressionViewedEvent](1)
    )

    def toValueRegistry(o: DiscardEvent[ImpressionViewedEvent]): FieldValueRegistry = {
      import o._
      new FieldValueRegistry(fields)
        .registerFieldValue(0, discardReason)
        .registerFieldValue(1, event)
    }
  }
}