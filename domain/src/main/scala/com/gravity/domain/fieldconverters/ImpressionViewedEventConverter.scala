package com.gravity.domain.fieldconverters

import com.gravity.domain.FieldConverters
import com.gravity.interests.jobs.intelligence.ArticleKey
import com.gravity.interests.jobs.intelligence.operations.{ImpressionViewedEvent, ImpressionViewedEventVersion, OrdinalArticleKeyPair}
import com.gravity.utilities.eventlogging.{FieldRegistry, FieldValueRegistry}
import com.gravity.utilities.grvfields.FieldConverter
import com.gravity.utilities.grvstrings._
import com.gravity.utilities.grvtime

trait ImpressionViewedEventConverter {
  this: FieldConverters.type =>

  implicit object ImpressionViewedEventConverter extends FieldConverter[ImpressionViewedEvent] {
    val logCategory = "impressionViewedEvent"

    val fields: FieldRegistry[ImpressionViewedEvent] = new FieldRegistry[ImpressionViewedEvent]("ImpressionViewedEvent")
    fields.registerLongField("date", 0, 0l)
    fields.registerUnencodedStringField("pageViewIdHash", 1, emptyString)
    fields.registerUnencodedStringField("siteGuid", 2, emptyString)
    fields.registerUnencodedStringField("userGuid", 3, emptyString)
    fields.registerLongField("sitePlacementId", 4, 0)
    fields.registerUnencodedStringField("userAgent", 5, emptyString)
    fields.registerUnencodedStringField("remoteIp", 6, emptyString)
    fields.registerLongField("clientTime", 7, 0l, "Actual time of impression viewed generated on client side")
    fields.registerUnencodedStringField("gravityHost", 8, emptyString)
    fields.registerIntField("iveVersion", 9, ImpressionViewedEventVersion.UNKNOWN.id)
    fields.registerIntField("iveError", 10, 0)
    fields.registerUnencodedStringField("iveErrorExtraData", 11, emptyString)
    fields.registerUnencodedStringField("hashHex", 12, emptyString)
    fields.registerSeqField[OrdinalArticleKeyPair]("ordinalArticleKeys", 13, Nil)
    fields.registerStringField("notes", 14)
    (Manifest.singleType[OrdinalArticleKeyPair](OrdinalArticleKeyPair(0, ArticleKey(0L))), OrdinalArticleKeyPairConverter) //weirdness because the converter is in this object; I couldn't just import it

    def getFields: FieldRegistry[ImpressionViewedEvent] = fields

    def fromValueRegistry(reg: FieldValueRegistry): ImpressionViewedEvent = {
      new ImpressionViewedEvent(reg)
    }

    def toValueRegistry(o: ImpressionViewedEvent): FieldValueRegistry = {
      import o._
      new FieldValueRegistry(fields)
        .registerFieldValue(0, date)
        .registerFieldValue(1, pageViewIdHash)
        .registerFieldValue(2, siteGuid)
        .registerFieldValue(3, userGuid)
        .registerFieldValue(4, sitePlacementId)
        .registerFieldValue(5, userAgent)
        .registerFieldValue(6, remoteIp)
        .registerFieldValue(7, clientTime)
        .registerFieldValue(8, gravityHost)
        .registerFieldValue(9, iveVersion)
        .registerFieldValue(10, iveError)
        .registerFieldValue(11, iveErrorExtraData)
        .registerFieldValue(12, hashHex)
        .registerFieldValue(13, ordinalArticleKeys)
        .registerFieldValue(14, notes)
    }
  }
}