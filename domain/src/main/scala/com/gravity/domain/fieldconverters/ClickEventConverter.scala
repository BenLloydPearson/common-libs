package com.gravity.domain.fieldconverters

import com.gravity.domain.FieldConverters
import com.gravity.interests.interfaces.userfeedback.UserFeedbackOption
import com.gravity.interests.jobs.intelligence.SectionPath
import com.gravity.interests.jobs.intelligence.operations.{ArticleRecoData, ClickEvent, ClickFields}
import com.gravity.utilities.eventlogging.{FieldRegistry, FieldValueRegistry}
import com.gravity.utilities.grvfields.FieldConverter
import com.gravity.utilities.grvstrings._

trait ClickEventConverter {
  this: FieldConverters.type =>

  implicit object ClickEventConverter extends FieldConverter[ClickEvent] {
    val fields: FieldRegistry[ClickEvent] = new FieldRegistry[ClickEvent]("ClickEvent", version = 2)
      .registerLongField("date", 0, 0l, "The date that the impression from which this click came was served")
      .registerUnencodedStringField("siteGuid", 1)
      .registerUnencodedStringField("userGuid", 2)
      .registerUnencodedStringField("hashHex", 3, emptyString, required = true)
      .registerBooleanField("isColdStart", 4)
      .registerBooleanField("doNotTrack", 5)
      .registerBooleanField("isInControlGroup", 6)
      .registerIntField("articlesInClickstreamCount", 7, -1)
      .registerIntField("topicsInGraphCount", 8, -1)
      .registerIntField("conceptsInGraphCount", 9, -1)
      .registerUnencodedStringField("currentUrl", 10)
      .registerLongField("clickDate", 11, 0l)
      .registerUnencodedStringField("clickUrl", 12, emptyString)
      .registerUnencodedStringField("clickUserAgent", 13, emptyString)
      .registerUnencodedStringField("clickReferrer", 14, emptyString)
      .registerUnencodedStringField("rawUrl", 15, emptyString)
      .registerField[ArticleRecoData]("articleClicked", 16, ArticleRecoData.empty)
      .registerIntField("geoLocationId", 17, 0)
      .registerUnencodedStringField("geoLocationDesc", 18, emptyString)
      .registerUnencodedStringField("impressionHash", 19)
      .registerUnencodedStringField("ipAddress", 20, emptyString)
      .registerUnencodedStringField("hostname", 21, emptyString)
      .registerBooleanField("isMaintenance", 22)
      .registerUnencodedStringField("currentSectionPath", 23, emptyString, "Chris?")
      .registerUnencodedStringField("recommendationScope", 24, emptyString, "Chris?")
      .registerUnencodedStringField("desiredSectionPaths", 25, emptyString, "Chris?")
      .registerUnencodedStringField("affiliateId", 26, emptyString, "something for conduit")
      .registerUnencodedStringField("conversionClickHash", 27, emptyString, "The click hash stored in the conversion cookie, which should be the hash of the redirect click which brought the user to the site")
      .registerUnencodedStringField("trackingParamId", 28, emptyString, "The id associated with this conversion beacon")
      .registerStringField("partnerPlacementId", 29)
      .registerUnencodedStringField("advertiserSiteGuid", 30)
      .registerLongField("sitePlacementId", 31, 0L, "Site-placement ID for the unit in which the click occurred", minVersion = 1)
      .registerIntField("chosenUserFeedbackOption", 32, -1, "-1 means None; 0-n means Some(UserFeedbackOption.get(n))", minVersion = 1)
      .registerBooleanField("toInterstitial", 33, defaultValue = false, "If TRUE, click event led to an interstitial page", minVersion = 2)
      .registerStringField("interstitialUrl", 34, emptyString, "Interstitial URL for click events that led to interstitial pages", minVersion = 2)

    def fromValueRegistry(vals: FieldValueRegistry, clickFieldsOpt: Option[ClickFields]): ClickEvent = {
      val chosenUserFeedbackOption = vals.getValue[Int](32) match {
        // Note that a 0 value corresponds to Some(UserFeedbackOption.none), which means user unvoted
        case -1 => None
        case id => UserFeedbackOption.get(id)
      }

      val clickFieldsToUse = clickFieldsOpt match { //make sure that passing none here doesn't override serialized values
        case Some(clickFields) =>
          Some(clickFields)
        case None =>
          vals.getValueIfPresent[Long](11) match {
            case Some(clickDate) =>
              val conversionClickHash = vals.getValue[String](27) match {
                case `emptyString` => None
                case a: String => Some(a)
                case _ => None
              }
              val trackingParamId = vals.getValue[String](28) match {
                case `emptyString` => None
                case a: String => Some(a)
                case _ => None
              }
              Some(ClickFields(clickDate, vals.getValue[String](12), vals.getValue[String](13),
                vals.getValue[String](14), vals.getValue[String](15), conversionClickHash, trackingParamId))
            case None =>
              None
          }
      }

      val event = ClickEvent(
        vals.getValue[Long](0), vals.getValue[String](1), vals.getValue[String](30), vals.getValue[String](2),
        vals.getValue[String](10), vals.getValue[ArticleRecoData](16), vals.getValue[Int](17), vals.getValue[String](18),
        vals.getValue[String](19), vals.getValue[String](20), vals.getValue[String](21), vals.getValue[Boolean](22),
        SectionPath.fromParam(vals.getValue[String](23)).getOrElse(SectionPath.empty), vals.getValue[String](24),
        SectionPath.fromParam(vals.getValue[String](25)).getOrElse(SectionPath.empty), vals.getValue[String](26),
        vals.getValue[String](29), vals.getValue[Long](31), chosenUserFeedbackOption, vals.getValue[Boolean](33),
        vals.getValue[String](34), clickFieldsToUse
      )
      event.setHashHexOverride(vals.getValue[String](3))
      event
    }

    def fromValueRegistry(vals: FieldValueRegistry): ClickEvent = {
      val event = fromValueRegistry(vals, None) //None triggers reading from the value registry
      event
    }

    def toValueRegistry(o: ClickEvent): FieldValueRegistry = {
      import o._
      val reg = new FieldValueRegistry(fields, version = 2)
        .registerFieldValue(0, date)
        .registerFieldValue(1, pubSiteGuid)
        .registerFieldValue(2, userGuid)
        .registerFieldValue(3, hashHex)
        .registerFieldValue(10, currentUrl)
        .registerFieldValue(16, article)
        .registerFieldValue(17, countryCodeId)
        .registerFieldValue(18, geoLocationDesc)
        .registerFieldValue(19, impressionHash)
        .registerFieldValue(20, ipAddress)
        .registerFieldValue(21, gravityHost)
        .registerFieldValue(22, isMaintenance)
        .registerFieldValue(23, currentSectionPath.toString)
        .registerFieldValue(24, recommendationScope)
        .registerFieldValue(25, desiredSectionPaths.toString)
        .registerFieldValue(26, affiliateId)
        .registerFieldValue(29, partnerPlacementId)
        .registerFieldValue(30, advertiserSiteGuid)
        .registerFieldValue(31, sitePlacementId)
        .registerFieldValue(32, getChosenUserFeedbackOption.fold(-1)(_.id))
        .registerFieldValue(33, toInterstitial)
        .registerFieldValue(34, interstitialUrl)

      clickFields.map { f =>
        reg.registerFieldValue(11, f.dateClicked)
          .registerFieldValue(12, f.url)
          .registerFieldValue(13, f.userAgent)
          .registerFieldValue(14, f.referrer)
          .registerFieldValue(15, f.rawUrl)
          .registerFieldValue(27, f.conversionClickHash.getOrElse(emptyString))
          .registerFieldValue(28, f.trackingParamId.getOrElse(emptyString))
      }
      reg
    }
  }
}