package com.gravity.domain

import java.net.URL
import javax.servlet.http.HttpServletRequest

import com.gravity.interests.jobs.intelligence.operations.DiscardableEvent
import com.gravity.utilities.analytics.URLUtils
import com.gravity.utilities.analytics.articles.ArticleWhitelist
import com.gravity.utilities.grvstrings._
import com.gravity.utilities.web.ContentUtils
import com.gravity.utilities.web.http._
import org.joda.time.DateTime
import org.joda.time.format.{DateTimeFormat, DateTimeFormatter}
import org.jsoup.parser.Parser
/*
              ___...---''
  ___...---'\'___
''       _.-''  _`'.______\\.
        /_.)  )..-  __..--'\\
       (    __..--''
        '-''\@


 Ⓐ Ⓐ Ⓐ Ⓐ Ⓐ Ⓐ Ⓐ Ⓐ Ⓐ Ⓐ Ⓐ
*/

/**
 * The types of many of these fields could be refined, normalized, formalized, etc. However, at this time, some of the
 * types are not well defined and so are just left as String.
  *
  * @param pageUrl Canonical page URL.
  * @param href Page URL as it appears in the browser's address bar.
 */
case class BeaconEvent(
                        timestamp: Long,
                        action: String,
                        siteGuid: String,
                        userAgent: String,
                        pageUrl: String,
                        referrer: String,
                        ipAddress: String,
                        userName: String,
                        userGuid: String,
                        externalUserId: String,
                        subSite: String,
                        articleId: String,
                        articleAuthorId: String,
                        articleTitle: String,
                        articleCategories: String,
                        articleTags: String,
                        email: String,
                        hashedEmail: String,
                        payloadField: String,
                        dataField: String,
                        publishDate: String,
                        authorName: String,
                        authorGender: String,
                        articleKeywords: String,
                        doNotTrackStatus: Int,
                        rawHtml: String,
                        image: String,
                        articleSummary: String,
                        href: String,
                        superSiteGuid: String,
                        sectionId: String,
                        subSectionId: String,
                        doRecommendationsStr: String,
                        isRedirect: Boolean,
                        auctionId: String,
                        redirectClickHash: String,
                        redirectCampaignKey: String,
                        isOptedOut: Boolean,
                        conversionTrackingParamUuid: String
) extends DiscardableEvent {

  def timestampDate: DateTime = new DateTime(timestamp)
  def doRecommendations() : Boolean = {
    doRecommendationsStr.tryToBoolean match {
      case Some(b) => b
      case None => false
    }
  }

  def currentAgeInSeconds : Int = {
    val now = new DateTime()
    org.joda.time.Seconds.secondsBetween(timestampDate, now).getSeconds
  }

  private def emptyToNone(string: String) : Option[String] = if(string.isEmpty) None else Some(string)
  def siteGuidOpt : Option[String]= emptyToNone(siteGuid)
  def sectionIdOpt : Option[String]= emptyToNone(sectionId)
  def actionOpt : Option[String]= emptyToNone(action)
  def userGuidOpt : Option[String]= emptyToNone(userGuid)
  def hrefOpt : Option[String]= emptyToNone(href)
  def userAgentOpt : Option[String]= emptyToNone(userAgent)
  def redirectClickHashOpt : Option[String] = emptyToNone(redirectClickHash)
  def ipAddressOpt : Option[String] = emptyToNone(ipAddress)
  def referrerOpt : Option[String] = emptyToNone(referrer)
  def conversionTrackingParamUuidOpt : Option[String] = emptyToNone(conversionTrackingParamUuid)
  def pageUrlOpt : Option[String] = emptyToNone(pageUrl)

  def referrerAsURL: Option[URL] = referrer.tryToURL


  def doNotTrack(): Boolean = doNotTrackStatus == 2

  def domainOpt : Option[String] = pageUrl.tryToURI.flatMap(uri => Option(uri.getHost))

  def standardizedUrl : Option[String] = {
    if (pageUrl.nonEmpty)
      Some(URLUtils.normalizeUrl(pageUrl))
    else
      None
  }

  def userIdentifier: String = {
    if(userName.nonEmpty && userName != "NULL") userName
    else userGuid
  }

  def publishDateTimeOpt : Option[DateTime] = publishTimestampAsLong match {
    case Some(ts) => Some(new DateTime(ts))
    case None => Some(timestampDate)

  }

  def isPublishDateTimeAvailable : Boolean = publishTimestampAsLong.isDefined

  def getPublishedDateOption: Option[DateTime] = {
    if (pageUrl.nonEmpty) {
      if (isPublishDateTimeAvailable) {
        return publishDateTimeOpt
      }

      val shouldBeFilledInButIsnt = ArticleWhitelist.isValidPartnerArticle(pageUrl) && ContentUtils.hasPublishDateExtractor(pageUrl)
      if (shouldBeFilledInButIsnt) {
        None
      }
      else {
        Some(timestampDate)
      }
    }
    else None
  }

  lazy val publishTimestampAsLong : Option[Long] ={
    publishDate.tryToLong
  }

  /**
   * used to compare the publish datetime of two magellan beacons -> will return true if the current beacon
   * has a newer timestamp than the previous beacon
   */
  def isNewerThan(thatBeacon: BeaconEvent): Boolean = {
    if (isPublishDateTimeAvailable && !thatBeacon.isPublishDateTimeAvailable) return true

    val thisPublishDateTime = publishDateTimeOpt
    if (thatBeacon.publishDateTimeOpt.isEmpty && thisPublishDateTime.isDefined) return true

    if (thisPublishDateTime.isEmpty && thatBeacon.publishDateTimeOpt.isDefined) return false

    if (thisPublishDateTime.get.isAfter(thatBeacon.publishDateTimeOpt.get.toInstant)) true else false
  }

  def dateStr : String = timestampDate.getMillis.toString

  def prettyPrint(): Unit = {
    println("timestamp: " + timestampDate)
    println("action: " + action)
    println("siteGuid: " + siteGuid)
    println("userAgent: " + userAgent)
    println("pageUrl: " + pageUrl)
    println("referrer: " + referrer)
    println("ipAddress: " + ipAddress)
    println("userName: " + userName)
    println("userGuid: " + userGuid)
    println("externalUserId: " + externalUserId)
    println("subSite: " + subSite)
    println("articleId: " + articleId)
    println("articleAuthorId: " + articleAuthorId)
    println("articleTitle: " + articleTitle)
    println("articleCategories: " + articleCategories)
    println("articleTags: " + articleTags)
    println("email: " + email)
    println("hashedEmail: " + hashedEmail)
    println("payloadField: " + payloadField)
    println("dataField: " + dataField)
    println("publishDate: " + publishDate)
    println("authorName: " + authorName)
    println("authorGender: " + authorGender)
    println("articleKeywords: " + articleKeywords)
    println("doNotTrackStatus: " + doNotTrackStatus)
    println("rawHtml: " + rawHtml)
    println("image: " + image)
    println("articleSummary: " + articleSummary)
    println("href: " + href)
    println("superSiteGuid: " + superSiteGuid)
    println("sectionId: " + sectionId)
    println("subSectionId: " + subSectionId)
    println("doRecommendationsStr: " + doRecommendationsStr)
    println("isRedirect: " + isRedirect)
    println("auctionId: " + auctionId)
    println("redirectClickHash: " + redirectClickHash)
    println("redirectCampaignKey: " + redirectCampaignKey)
    println("isOptedOut: " + isOptedOut)
    println("conversionTrackingParamUuid: " + conversionTrackingParamUuid)
  }
}

object BeaconEvent {
  val conversionAction = "conversion"

  val dateTimeFmtString = "yyyy-MM-dd HH:mm:ss"
  lazy val dateTimeFmt: DateTimeFormatter = DateTimeFormat.forPattern(dateTimeFmtString)
  def cleanTitle(title: String): String = org.apache.commons.lang.StringEscapeUtils.unescapeHtml(stripHtml(title))

  def stripHtml(input: String): String = Parser.parseBodyFragment(input, emptyString).text()

  val DATA_TAGS_AS_TAGS = "tags_as_tags"

  def buildTagsAsTags(areThey: Boolean): String = "{\"" + DATA_TAGS_AS_TAGS + "\":" + areThey + "}"

  def doNotTrackStatusFromRequest(implicit req: HttpServletRequest): Int = {
    if (req.isUserOptedOut)
      2
    else if (req.getHeaderNullSafe("DNT") == "1")
      1
    else
      0
  }

  val empty: BeaconEvent = BeaconEvent()
  def apply(
    timestampDate: DateTime = new DateTime,
    action: String = emptyString,
    siteGuid: String = emptyString,
    userAgent: String = emptyString,
    pageUrl: String = emptyString,
    referrer: String = emptyString,
    ipAddress: String = emptyString,
    userName: String = emptyString,
    userGuid: String = emptyString,
    externalUserId: String = emptyString,
    subSite: String = emptyString,
    articleId: String = emptyString,
    articleAuthorId: String = emptyString,
    articleTitle: String = emptyString,
    articleCategories: String = emptyString,
    articleTags: String = emptyString,
    email: String = emptyString,
    hashedEmail: String = emptyString,
    payloadField: String = emptyString,
    dataField: String = emptyString,
    publishDate: String = emptyString,
    authorName: String = emptyString,
    authorGender: String = emptyString,
    articleKeywords: String = emptyString,
    doNotTrackStatus: Int = 0,
    rawHtml: String = emptyString,
    image: String = emptyString,
    articleSummary: String = emptyString,
    href: String = emptyString,
    superSiteGuid: String = emptyString,
    sectionId: String = emptyString,
    subSectionId: String = emptyString,
    doRecommendationsStr: String = emptyString,
    isRedirect: Boolean = false,
    auctionId: String = emptyString,
    redirectClickHash: String = emptyString,
    redirectCampaignKey: String = emptyString,
    isOptedOut: Boolean = false,
    conversionTrackingParamUuid: String = emptyString
  ) = new BeaconEvent(
  timestamp =  timestampDate.getMillis,
  action =   action,
  siteGuid =   siteGuid,
  userAgent =   userAgent,
  pageUrl =   pageUrl,
  referrer =   referrer,
  ipAddress =   ipAddress,
  userName =   userName,
  userGuid =   userGuid,
  externalUserId =   externalUserId,
  subSite =   subSite,
  articleId =   articleId,
  articleAuthorId =   articleAuthorId,
  articleTitle =   articleTitle,
  articleCategories =   articleCategories,
  articleTags =   articleTags,
  email =   email,
  hashedEmail =   hashedEmail,
  payloadField =   payloadField,
  dataField =   dataField,
  publishDate =   publishDate,
  authorName =   authorName,
  authorGender =   authorGender,
  articleKeywords =   articleKeywords,
  doNotTrackStatus =   doNotTrackStatus,
  rawHtml =   rawHtml,
  image =   image,
  articleSummary =   articleSummary,
  href =   href,
  superSiteGuid =   superSiteGuid,
  sectionId =   sectionId,
  subSectionId =   subSectionId,
  doRecommendationsStr =   doRecommendationsStr,
  isRedirect =   isRedirect,
  auctionId =   auctionId,
  redirectClickHash =   redirectClickHash,
  redirectCampaignKey =   redirectCampaignKey,
  isOptedOut =   isOptedOut,
  conversionTrackingParamUuid = emptyString

  )
}