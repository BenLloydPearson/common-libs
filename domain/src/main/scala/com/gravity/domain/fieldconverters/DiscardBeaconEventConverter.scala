package com.gravity.domain.fieldconverters

import com.gravity.domain.{BeaconEvent, FieldConverters}
import com.gravity.interests.jobs.intelligence.operations.DiscardEvent
import com.gravity.utilities.eventlogging.{FieldRegistry, FieldValueRegistry}
import com.gravity.utilities.grvfields.FieldConverter
import com.gravity.utilities.grvstrings._

trait DiscardBeaconEventConverter {
  this: FieldConverters.type =>

  implicit object DiscardBeaconEventConverter extends FieldConverter[DiscardEvent[BeaconEvent]] {
    val fields: FieldRegistry[DiscardEvent[BeaconEvent]] = new FieldRegistry[DiscardEvent[BeaconEvent]]("DiscardEvent")
      .registerStringField("discardReason", 0, emptyString, "reason discarded")
      .registerField[BeaconEvent]("event", 1, BeaconEvent.empty, "default empty event")

    def fromValueRegistry(reg: FieldValueRegistry): DiscardEvent[BeaconEvent] = new DiscardEvent[BeaconEvent](
      reg.getValue[String](0),
      reg.getValue[BeaconEvent](1)
    )

    def toValueRegistry(o: DiscardEvent[BeaconEvent]): FieldValueRegistry = {
      import o._
      new FieldValueRegistry(fields)
        .registerFieldValue(0, discardReason)
        .registerFieldValue(1, event)
    }
  }
}