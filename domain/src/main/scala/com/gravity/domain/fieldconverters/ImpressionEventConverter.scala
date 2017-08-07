package com.gravity.domain.fieldconverters

import com.gravity.domain.FieldConverters
import com.gravity.interests.jobs.intelligence.SectionPath
import com.gravity.interests.jobs.intelligence.operations._
import com.gravity.utilities.eventlogging.{FieldRegistry, FieldValueRegistry}
import com.gravity.utilities.grvfields.FieldConverter
import com.gravity.utilities.grvstrings._
import com.gravity.utilities.grvtime

trait ImpressionEventConverter {
  this: FieldConverters.type =>

  implicit object ImpressionEventConverter extends FieldConverter[ImpressionEvent] {
    val fields: FieldRegistry[ImpressionEvent] = new FieldRegistry[ImpressionEvent]("ImpressionEvent", version = 6)
      .registerLongField("date", 0, grvtime.epochDateTime.getMillis)
      .registerUnencodedStringField("siteGuid", 1)
      .registerUnencodedStringField("userGuid", 2)
      .registerUnencodedStringField("hashHex", 3, emptyString, required = true)
      .registerBooleanField("isColdStart", 4, false, "deprecated per JK March 19")
      .registerBooleanField("doNotTrack", 5, false, "deprecated per JK March 19")
      .registerBooleanField("isInControlGroup", 6, false)
      .registerIntField("articlesInClickstreamCount", 7, -1, "deprecated per JK March 19")
      .registerIntField("topicsInGraphCount", 8, -1, "deprecated per JK March 19")
      .registerIntField("conceptsInGraphCount", 9, -1, "deprecated per JK March 19")
      .registerUnencodedStringField("currentUrl", 10)
      .registerSeqField[ArticleRecoData]("articlesInReco", 11, Seq.empty[ArticleRecoData])
      .registerIntField("geoLocationId", 12, 0)
      .registerUnencodedStringField("geoLocationDesc", 13, emptyString)
      .registerBooleanField("isMobile", 14, false)
      .registerUnencodedStringField("pageViewIdWidgetLoaderWindowUrl", 15, emptyString, "URL of the window in which widget loader was sourced")
      .registerLongField("pageViewIdTime", 16, grvtime.epochDateTime.getMillis, "Millis time when page view ID was generated on the client")
      .registerLongField("pageViewIdRand", 17, 0L, "Random number generated on the client to increase cardinality of page view ID")
      .registerUnencodedStringField("userAgent", 18, emptyString, "the user agent of the browser the impression was displayed to")
      .registerLongField("clientTime", 19, 0L, "the time provided by the viewer's browser")
      .registerUnencodedStringField("pageViewIdHash", 20, emptyString, "A hash of the page view ID values joinable to widgetDomReady and impressionViewed events")
      .registerUnencodedStringField("ipAddress", 21, emptyString, "ipAddress of client")
      .registerUnencodedStringField("gravityHost", 22, emptyString, "hostname that served the impressions")
      .registerBooleanField("isMaintenance", 23, false, "Whether we are in maintenance mode or not")
      .registerUnencodedStringField("currentSectionPath", 24, emptyString, "Chris?")
      .registerUnencodedStringField("recommendationScope", 25, emptyString, "Chris?")
      .registerUnencodedStringField("desiredSectionPaths", 26, emptyString, "Chris?")
      .registerUnencodedStringField("affiliateId", 27, emptyString, "added for conduit. not sure what it does.")
      .registerBooleanField("isOptedOut", 28, false, "is the user opted out")
      .registerStringField("renderType", 29, emptyString, "api or widget")
      .registerLongField("sitePlacementId", 30)
      .registerStringField("partnerPlacementId", 31)
      .registerIntField("recoBucket", 32)
      .registerBooleanField("usesAbp", 33)
      .registerStringField("referrer", 34)
      .registerStringField("contextPath", 35)
      .registerIntField("userFeedbackVariation", 36, minVersion = 1)
      .registerStringField("referrerImpressionHash", 37, minVersion = 2,
        description = "Impression hash of a placement whose clicked article led directly to this placement's impression")
      .registerIntField("impressionPurpose", 38, ImpressionPurpose.defaultId, "See ImpressionPurpose docs",
        minVersion = 3)
      .registerIntField("userFeedbackPresentation", 39, minVersion = 4)
      .registerStringField("pageViewGuid", 40, minVersion = 5)
      .registerIntField("logMethod", 41, defaultValue = 0, minVersion = 6)

    def toValueRegistry(o: ImpressionEvent): FieldValueRegistry = {
      import o._
      new FieldValueRegistry(fields, version = 6)
        .registerFieldValue(0, date)
        .registerFieldValue(1, siteGuid)
        .registerFieldValue(2, userGuid)
        .registerFieldValue(3, getHashHex)
        //4-9 were deprecated. without full versioning support we can't do anything with them
        .registerFieldValue(10, currentUrl)
        .registerFieldValue(11, articlesInReco)
        .registerFieldValue(12, countryCodeId)
        .registerFieldValue(13, geoLocationDesc)
        .registerFieldValue(14, isMobile)
        .registerFieldValue(15, pageViewIdWidgetLoaderWindowUrl)
        .registerFieldValue(16, pageViewIdTime)
        .registerFieldValue(17, pageViewIdRand)
        .registerFieldValue(18, userAgent)
        .registerFieldValue(19, clientTime)
        .registerFieldValue(20, pageViewIdHash)
        .registerFieldValue(21, ipAddress)
        .registerFieldValue(22, gravityHost)
        .registerFieldValue(23, isMaintenance)
        .registerFieldValue(24, currentSectionPath.toString)
        .registerFieldValue(25, recommendationScope)
        .registerFieldValue(26, desiredSectionPaths.toString)
        .registerFieldValue(27, affiliateId)
        .registerFieldValue(28, isOptedOut)
        .registerFieldValue(29, more.renderType)
        .registerFieldValue(30, more.sitePlacementId)
        .registerFieldValue(31, more.partnerPlacementId)
        .registerFieldValue(32, more.recoBucket)
        .registerFieldValue(33, value = false) // usesAbp (deprecated)
        .registerFieldValue(34, more.referrer)
        .registerFieldValue(35, more.contextPath)
        .registerFieldValue(36, more.userFeedbackVariation)
        .registerFieldValue(37, more.referrerImpressionHash)
        .registerFieldValue(38, more.impressionPurpose)
        .registerFieldValue(39, more.userFeedbackPresentation)
        .registerFieldValue(40, more.pageViewGuid)
        .registerFieldValue(41, more.logMethod)
    }

    def fromValueRegistry(vals: FieldValueRegistry): ImpressionEvent = {
      val event = new ImpressionEvent(
        vals.getValue[Long](0),
        vals.getValue[String](1),
        vals.getValue[String](2),
        vals.getValue[String](10),
        vals.getValue[Seq[ArticleRecoData]](11),
        vals.getValue[Int](12),
        vals.getValue[String](13),
        vals.getValue[Boolean](14),
        vals.getValue[String](15),
        vals.getValue[Long](16),
        vals.getValue[Long](17),
        vals.getValue[String](18), vals.getValue[Long](19),
        vals.getValue[String](21), vals.getValue[String](22), vals.getValue[Boolean](23), SectionPath.fromParam(vals.getValue[String](24)).getOrElse(SectionPath.empty),
        vals.getValue[String](25), SectionPath.fromParam(vals.getValue[String](26)).getOrElse(SectionPath.empty), vals.getValue[String](27), vals.getValue[Boolean](28),
        EventExtraFields(vals.getValue[String](29), vals.getValue[Long](30), vals.getValue[String](31),
          vals.getValue[Int](32), vals.getValue[String](34), vals.getValue[String](35),
          vals.getValue[Int](36), vals.getValue[String](37), vals.getValue[Int](38),
          vals.getValue[Int](39), vals.getValue[String](40), vals.getValue[Int](41))
      )
      event.setHashHexOverride(vals.getValue[String](3))
      event
    }
  }
}
