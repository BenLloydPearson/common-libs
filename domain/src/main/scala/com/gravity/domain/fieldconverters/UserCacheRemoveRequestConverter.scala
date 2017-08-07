package com.gravity.domain.fieldconverters

import com.gravity.domain.FieldConverters
import com.gravity.interests.jobs.intelligence.operations.user.UserCacheRemoveRequest
import com.gravity.utilities.eventlogging.{FieldRegistry, FieldValueRegistry}
import com.gravity.utilities.grvfields.FieldConverter
import com.gravity.valueclasses.ValueClassesForDomain.UserGuid

/*
              ___...---''
  ___...---'\'___
''       _.-''  _`'.______\\.
        /_.)  )..-  __..--'\\
       (    __..--''
        '-''\@


 Ⓐ Ⓐ Ⓐ Ⓐ Ⓐ Ⓐ Ⓐ Ⓐ Ⓐ Ⓐ Ⓐ
*/

trait UserCacheRemoveRequestConverter {
  this: FieldConverters.type =>

  implicit object UserCacheRemoveRequestConverter extends FieldConverter[UserCacheRemoveRequest] {
    val fields: FieldRegistry[UserCacheRemoveRequest] = new FieldRegistry[UserCacheRemoveRequest]("UserCacheRemoveRequest", version = 0)
      .registerUnencodedStringField("userGuid", 0)


    def toValueRegistry(o: UserCacheRemoveRequest): FieldValueRegistry = {
      import o._
      new FieldValueRegistry(fields, version = 0)
        .registerFieldValue(0, userGuid.raw)
    }

    def fromValueRegistry(vals: FieldValueRegistry): UserCacheRemoveRequest = {
      val event = UserCacheRemoveRequest(UserGuid(
        vals.getValue[String](0)
      ))
      event
    }
  }
}
