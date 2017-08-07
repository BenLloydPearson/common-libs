package com.gravity.domain.fieldconverters

import com.gravity.domain.FieldConverters
import com.gravity.interests.jobs.intelligence.operations.user.{ClickstreamEntry, UserClickstream, UserRequestKey}
import com.gravity.utilities.eventlogging.{FieldRegistry, FieldValueRegistry}
import com.gravity.utilities.grvfields.FieldConverter

trait UserClickstreamConverter {
  this: FieldConverters.type =>

  implicit object UserClickstreamConverter extends FieldConverter[UserClickstream] {
    val fields: FieldRegistry[UserClickstream] = new FieldRegistry[UserClickstream]("UserClickstream", version = 1)
      .registerUnencodedStringField("userGuid", 0)
      .registerUnencodedStringField("siteGuid", 1)
      .registerSeqField[ClickstreamEntry]("clickstream", 2, Seq.empty[ClickstreamEntry])

    def toValueRegistry(o: UserClickstream): FieldValueRegistry = {
      import o._
      new FieldValueRegistry(fields, version = 1)
        .registerFieldValue(0, key.userGuid)
        .registerFieldValue(1, key.siteGuid)
        .registerFieldValue(2, clickstream)
    }

    def fromValueRegistry(vals: FieldValueRegistry): UserClickstream = {
      val event = UserClickstream(new UserRequestKey(
        vals.getValue[String](0),
        vals.getValue[String](1)),
        vals.getValue[Seq[ClickstreamEntry]](2)
      )
      event
    }
  }
}
