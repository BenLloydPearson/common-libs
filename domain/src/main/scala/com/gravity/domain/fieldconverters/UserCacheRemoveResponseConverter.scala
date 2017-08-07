package com.gravity.domain.fieldconverters

import com.gravity.domain.FieldConverters
import com.gravity.interests.jobs.intelligence.operations.user.UserCacheRemoveResponse
import com.gravity.utilities.eventlogging.{FieldRegistry, FieldValueRegistry}
import com.gravity.utilities.grvfields.FieldConverter

/*
              ___...---''
  ___...---'\'___
''       _.-''  _`'.______\\.
        /_.)  )..-  __..--'\\
       (    __..--''
        '-''\@


 Ⓐ Ⓐ Ⓐ Ⓐ Ⓐ Ⓐ Ⓐ Ⓐ Ⓐ Ⓐ Ⓐ
*/

trait UserCacheRemoveResponseConverter {
  this: FieldConverters.type =>

  implicit object UserCacheRemoveResponseConverter extends FieldConverter[UserCacheRemoveResponse] {
    val fields: FieldRegistry[UserCacheRemoveResponse] = new FieldRegistry[UserCacheRemoveResponse]("UserCacheRemoveResponse", version = 0)
      .registerBooleanField("wasCached", 0)


    def toValueRegistry(o: UserCacheRemoveResponse): FieldValueRegistry = {
      import o._
      new FieldValueRegistry(fields, version = 0)
        .registerFieldValue(0, wasCached)
    }

    def fromValueRegistry(vals: FieldValueRegistry): UserCacheRemoveResponse = {
      val event = UserCacheRemoveResponse(
        vals.getValue[Boolean](0)
      )
      event
    }
  }
}
