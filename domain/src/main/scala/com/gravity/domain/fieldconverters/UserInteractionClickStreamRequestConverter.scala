package com.gravity.domain.fieldconverters

import com.gravity.domain.FieldConverters
import com.gravity.interests.jobs.intelligence.operations.user.{UserInteractionClickStreamRequest, UserRequestKey}
import com.gravity.utilities.eventlogging.{FieldRegistry, FieldValueRegistry}
import com.gravity.utilities.grvfields.FieldConverter

trait UserInteractionClickStreamRequestConverter {
  this: FieldConverters.type =>

  implicit object UserInteractionClickStreamRequestConverter extends FieldConverter[UserInteractionClickStreamRequest] {
    val fields: FieldRegistry[UserInteractionClickStreamRequest] = new FieldRegistry[UserInteractionClickStreamRequest]("UserInteractionClickStreamRequest", version = 1)
      .registerUnencodedStringField("userGuid", 0)
      .registerUnencodedStringField("siteGuid", 1)


    def toValueRegistry(o: UserInteractionClickStreamRequest): FieldValueRegistry = {
      import o._
      new FieldValueRegistry(fields, version = 1)
        .registerFieldValue(0, key.userGuid)
        .registerFieldValue(1, key.siteGuid)
    }

    def fromValueRegistry(vals: FieldValueRegistry): UserInteractionClickStreamRequest = {
      val event = UserInteractionClickStreamRequest(new UserRequestKey(
        vals.getValue[String](0),
        vals.getValue[String](1)
      ))
      event
    }
  }
}
