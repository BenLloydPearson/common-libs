package com.gravity.domain.fieldconverters

import com.gravity.domain.{BeaconEvent, FieldConverters}
import com.gravity.utilities.eventlogging.{FieldRegistry, FieldValueRegistry}
import com.gravity.utilities.grvfields.FieldConverter
import org.joda.time.DateTime

trait BeaconEventConverter {
  this: FieldConverters.type =>

  implicit object BeaconEventConverter extends FieldConverter[BeaconEvent] {
    override def toValueRegistry(o: BeaconEvent): FieldValueRegistry = {
      import o._
      new FieldValueRegistry(fields)
        .registerFieldValue(0,timestamp)
        .registerFieldValue(1,action)
        .registerFieldValue(2,siteGuid)
        .registerFieldValue(3,userAgent)
        .registerFieldValue(4,pageUrl)
        .registerFieldValue(5,referrer)
        .registerFieldValue(6,ipAddress)
        .registerFieldValue(7,userName)
        .registerFieldValue(8,userGuid)
        .registerFieldValue(9,externalUserId)
        .registerFieldValue(10,subSite)
        .registerFieldValue(11,articleId)
        .registerFieldValue(12,articleAuthorId)
        .registerFieldValue(13,articleTitle)
        .registerFieldValue(14,articleCategories)
        .registerFieldValue(15,articleTags)
        .registerFieldValue(16,email)
        .registerFieldValue(17,hashedEmail)
        .registerFieldValue(18,payloadField)
        .registerFieldValue(19,dataField)
        .registerFieldValue(20,publishDate)
        .registerFieldValue(21,authorName)
        .registerFieldValue(22,authorGender)
        .registerFieldValue(23,articleKeywords)
        .registerFieldValue(24,doNotTrackStatus)
        .registerFieldValue(25,rawHtml)
        .registerFieldValue(26,image)
        .registerFieldValue(27,articleSummary)
        .registerFieldValue(28,href)
        .registerFieldValue(29,superSiteGuid)
        .registerFieldValue(30,sectionId)
        .registerFieldValue(31,subSectionId)
        .registerFieldValue(32,doRecommendationsStr)
        .registerFieldValue(33,isRedirect)
        .registerFieldValue(34,auctionId)
        .registerFieldValue(35,redirectClickHash)
        .registerFieldValue(36,redirectCampaignKey)
        .registerFieldValue(37,isOptedOut)
        .registerFieldValue(38,value = false) // usesAbp (deprecated)
        .registerFieldValue(39,conversionTrackingParamUuid)
    }

    override def fromValueRegistry(reg: FieldValueRegistry): BeaconEvent = {
      new BeaconEvent(
        reg.getValue[Long](0),
        reg.getValue[String](1),
        reg.getValue[String](2),
        reg.getValue[String](3),
        reg.getValue[String](4),
        reg.getValue[String](5),
        reg.getValue[String](6),
        reg.getValue[String](7),
        reg.getValue[String](8),
        reg.getValue[String](9),
        reg.getValue[String](10),
        reg.getValue[String](11),
        reg.getValue[String](12),
        reg.getValue[String](13),
        reg.getValue[String](14),
        reg.getValue[String](15),
        reg.getValue[String](16),
        reg.getValue[String](17),
        reg.getValue[String](18),
        reg.getValue[String](19),
        reg.getValue[String](20),
        reg.getValue[String](21),
        reg.getValue[String](22),
        reg.getValue[String](23),
        reg.getValue[Int](24),
        reg.getValue[String](25),
        reg.getValue[String](26),
        reg.getValue[String](27),
        reg.getValue[String](28),
        reg.getValue[String](29),
        reg.getValue[String](30),
        reg.getValue[String](31),
        reg.getValue[String](32),
        reg.getValue[Boolean](33),
        reg.getValue[String](34),
        reg.getValue[String](35),
        reg.getValue[String](36),
        reg.getValue[Boolean](37),
        reg.getValue[String](39)
      )
    }

    override val fields: FieldRegistry[BeaconEvent] = new FieldRegistry[BeaconEvent]("BeaconEvent", 0)
      .registerLongField("timestamp", 0)
      .registerStringField("action", 1)
      .registerStringField("siteGuid", 2)
      .registerStringField("userAgent", 3)
      .registerStringField("pageUrl", 4)
      .registerStringField("referrer", 5)
      .registerStringField("ipAddress", 6)
      .registerStringField("userName", 7)
      .registerStringField("userGuid", 8)
      .registerStringField("externalUserId", 9)
      .registerStringField("subSite", 10)
      .registerStringField("articleId", 11)
      .registerStringField("articleAuthorId", 12)
      .registerStringField("articleTitle", 13)
      .registerStringField("articleCategories", 14)
      .registerStringField("articleTags", 15)
      .registerStringField("email", 16)
      .registerStringField("hashedEmail", 17)
      .registerStringField("payloadField", 18)
      .registerStringField("dataField", 19)
      .registerStringField("publishDate", 20)
      .registerStringField("authorName", 21)
      .registerStringField("authorGender", 22)
      .registerStringField("articleKeywords", 23)
      .registerIntField("doNotTrackStatus", 24)
      .registerStringField("rawHtml", 25)
      .registerStringField("image", 26)
      .registerStringField("articleSummary", 27)
      .registerStringField("href", 28)
      .registerStringField("superSiteGuid", 29)
      .registerStringField("sectionId", 30)
      .registerStringField("subSectionId", 31)
      .registerStringField("doRecommendations", 32)
      .registerBooleanField("isRedirect", 33)
      .registerStringField("auctionId", 34)
      .registerStringField("redirectClickHash", 35)
      .registerStringField("redirectCampaignKey", 36)
      .registerBooleanField("isOptedOut", 37)
      .registerBooleanField("usesAbp", 38)
      .registerStringField("conversionTrackingParamUuid", 39)
  }
}