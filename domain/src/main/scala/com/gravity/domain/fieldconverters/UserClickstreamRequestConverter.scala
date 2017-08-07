package com.gravity.domain.fieldconverters

import com.gravity.domain.FieldConverters
import com.gravity.interests.jobs.intelligence.operations.user.{UserClickstreamRequest, UserRequestKey}
import com.gravity.utilities.eventlogging.{FieldRegistry, FieldValueRegistry}
import com.gravity.utilities.grvfields.FieldConverter

trait UserClickstreamRequestConverter {
  this: FieldConverters.type =>

  implicit object UserClickstreamRequestConverter extends FieldConverter[UserClickstreamRequest] {
    val fields: FieldRegistry[UserClickstreamRequest] = new FieldRegistry[UserClickstreamRequest]("UserClickstreamRequest", version = 1)
      .registerUnencodedStringField("userGuid", 0)
      .registerUnencodedStringField("siteGuid", 1)


    def toValueRegistry(o: UserClickstreamRequest): FieldValueRegistry = {
      import o._
      new FieldValueRegistry(fields, version = 1)
        .registerFieldValue(0, key.userGuid)
        .registerFieldValue(1, key.siteGuid)
    }

    def fromValueRegistry(vals: FieldValueRegistry): UserClickstreamRequest = {
      val event = UserClickstreamRequest(new UserRequestKey(
        vals.getValue[String](0),
        vals.getValue[String](1)
      ))
      event
    }
  }
}
