package com.gravity.domain.fieldconverters

import com.gravity.domain.FieldConverters
import com.gravity.interests.interfaces.userfeedback.{UserFeedbackPresentation, UserFeedbackVariation}
import com.gravity.interests.jobs.intelligence.operations.recommendations.model.{ImpressionFailedEvent, ImpressionFailedEventDetails}
import com.gravity.interests.jobs.intelligence.operations.{EventExtraFields, ImpressionPurpose, LogMethod}
import com.gravity.utilities.eventlogging.{FieldRegistry, FieldValueRegistry}
import com.gravity.utilities.grvfields.FieldConverter
import com.gravity.utilities.grvstrings._
import com.gravity.utilities.grvtime
import org.joda.time.DateTime

trait ImpressionFailedEventConverter {
  this: FieldConverters.type =>

  implicit object ImpressionFailedEventConverter extends FieldConverter[ImpressionFailedEvent] {
    val fields = new FieldRegistry[ImpressionFailedEvent]("ImpressionFailedEvent")
      .registerDateTimeField("date", 0, grvtime.epochDateTime)
      .registerIntField("id", 1, -666)
      .registerStringField("message", 2, "")
      .registerStringField("exception", 3, "")
      .registerUnencodedStringField("siteGuid", 4, "")
      .registerUnencodedStringField("failureType", 5, "error")
      .registerUnencodedStringField("userGuid", 6, "no user Guid")
      .registerUnencodedStringField("currentUrl", 7, "currentUrl")
      .registerIntField("geoLocationId", 8, 0)
      .registerUnencodedStringField("geoLocationDesc", 9, emptyString)
      .registerBooleanField("isMobile", 10, false)
      .registerUnencodedStringField("pageViewIdWidgetLoaderWindowUrl", 11, emptyString, "URL of the window in which widget loader was sourced")
      .registerDateTimeField("pageViewIdTime", 12, grvtime.epochDateTime, "Millis time when page view ID was generated on the client")
      .registerLongField("pageViewIdRand", 13, 0L, "Random number generated on the client to increase cardinality of page view ID")
      .registerUnencodedStringField("userAgent", 14, emptyString, "the user agent of the browser the impression was displayed to")
      .registerLongField("clientTime", 15, 0L, "the time provided by the viewer's browser")
      .registerUnencodedStringField("ipAddress", 16, emptyString, "ipAddress of client")
      .registerUnencodedStringField("gravityHost", 17, emptyString, "hostname that served the impressions")
      .registerBooleanField("isMaintenance", 18, false, "Whether we are in maintenance mode or not")
      .registerUnencodedStringField("affiliateId", 19, emptyString, "added for conduit. not sure what it does.")
      .registerBooleanField("isOptedOut", 20, false, "is the user opted out")
      .registerStringField("renderType", 21, emptyString, "api or widget")
      .registerLongField("sitePlacementId", 22)
      .registerStringField("partnerPlacementId", 23)
      .registerIntField("recoBucket", 24)
      .registerBooleanField("usesAbp", 25)
      .registerStringField("referrer", 26)
      .registerStringField("contextPath", 27)

    def fromValueRegistry(reg: FieldValueRegistry) = {
      ImpressionFailedEvent(
        reg.getValue[DateTime](0),
        reg.getValue[Int](1),
        reg.getValue[String](4),
        reg.getValue[String](2),
        reg.getValue[String](3),
        ImpressionFailedEventDetails(
          reg.getValue[String](5),
          reg.getValue[String](6),
          reg.getValue[String](7),
          reg.getValue[Int](8),
          reg.getValue[String](9),
          reg.getValue[Boolean](10),
          reg.getValue[String](11),
          reg.getValue[DateTime](12),
          reg.getValue[Long](13),
          reg.getValue[String](14),
          reg.getValue[Long](15),
          reg.getValue[String](16),
          reg.getValue[String](17),
          reg.getValue[Boolean](18),
          reg.getValue[String](19),
          reg.getValue[Boolean](20),
          EventExtraFields(
            reg.getValue[String](21),
            reg.getValue[Long](22),
            reg.getValue[String](23),
            reg.getValue[Int](24),
            reg.getValue[String](26),
            reg.getValue[String](27),
            UserFeedbackVariation.none.id,
            "",
            ImpressionPurpose.defaultValue.id,
            UserFeedbackPresentation.none.id,
            "",
            LogMethod.unknown.id
          )
        )
      )
    }


    def toValueRegistry(o: ImpressionFailedEvent): FieldValueRegistry = {
      import o._
      new FieldValueRegistry(fields)
        .registerFieldValue(0, date)
        .registerFieldValue(1, id)
        .registerFieldValue(2, message)
        .registerFieldValue(3, exception)
        .registerFieldValue(4, siteGuid)
        .registerFieldValue(5, failedImpression.failureType)
        .registerFieldValue(6, failedImpression.userGuid)
        .registerFieldValue(7, failedImpression.currentUrl)
        .registerFieldValue(8, failedImpression.geoLocationId)
        .registerFieldValue(9, failedImpression.geoLocationDesc)
        .registerFieldValue(10, failedImpression.isMobile)
        .registerFieldValue(11, failedImpression.pageViewIdWidgetLoaderWindowUrl)
        .registerFieldValue(12, failedImpression.pageViewIdTime)
        .registerFieldValue(13, failedImpression.pageViewIdRand)
        .registerFieldValue(14, failedImpression.userAgent)
        .registerFieldValue(15, failedImpression.clientTime)
        .registerFieldValue(16, failedImpression.ipAddress)
        .registerFieldValue(17, failedImpression.gravityHost)
        .registerFieldValue(18, failedImpression.isMaintenance)
        .registerFieldValue(19, failedImpression.affiliateId)
        .registerFieldValue(20, failedImpression.isOptedOut)
        .registerFieldValue(21, failedImpression.more.renderType)
        .registerFieldValue(22, failedImpression.more.sitePlacementId)
        .registerFieldValue(23, failedImpression.more.partnerPlacementId)
        .registerFieldValue(24, failedImpression.more.recoBucket)
        .registerFieldValue(25, value = false) // usesAbp (deprecated)
        .registerFieldValue(26, failedImpression.more.referrer)
        .registerFieldValue(27, failedImpression.more.contextPath)
    }
  }
}