package com.gravity.interests.jobs.intelligence.operations

import javax.servlet.http.HttpServletRequest

import com.gravity.utilities.analytics.URLUtils
import com.gravity.utilities.grvstrings
import com.gravity.utilities.web.http._
import com.gravity.utilities.eventlogging.FieldValueRegistry
import org.joda.time.DateTime

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
 * Campaigns may be opted into using this via [[com.gravity.interests.jobs.intelligence.CampaignRow.enableAuxiliaryClickEvent]].
 * If a campaign is opted into this, advertisers place an AuxiliaryClickEventPixel (TODO) on landing pages for that
 * campaign. Clicks are logged via that pixel and can then be compared/reconciled against Gravity 302 clicks and clicks
 * from external services (Google Analytics, etc.).
 */
case class AuxiliaryClickEvent(
  date: DateTime,
  impressionHash: String,
  clickHash: String,
  pageUrl: String,
  userGuid: String,
  referrer: String,
  userAgent: String,
  remoteIp: String
) {
  def this(vals: FieldValueRegistry) = this(
    vals.getValue[DateTime](0),
    vals.getValue[String](1),
    vals.getValue[String](2),
    vals.getValue[String](3),
    vals.getValue[String](4),
    vals.getValue[String](5),
    vals.getValue[String](6),
    vals.getValue[String](7)
  )
}

object AuxiliaryClickEvent {
  /**
   * @param referrerToPageUrl Typically passed as a URL param to the aux click event endpoint, this value cannot be
   *                          obtained from the Referer HTTP header; it is specifically the referring page to pageUrl,
   *                          which is only available on the client-side. The Referer HTTP header to the aux click event
   *                          endpoint would usually be pageUrl.
   *
   * @return None is only returned in the case that the given pageUrl should never have triggered an auxiliary click
   *         event logging (specifically, if it lacks the "grvAce" param entirely).
   */
  def maybe(pageUrl: String, userGuid: Option[String], referrerToPageUrl: Option[String] = None)
           (implicit request: HttpServletRequest): Option[AuxiliaryClickEvent] = {
    val grvAceValue = URLUtils.getParamValue(pageUrl, "grvAce").getOrElse(return None)
    val tokens = grvstrings.tokenizeBoringly(grvAceValue, "^", 2)
    val impressionHash = tokens.headOption.getOrElse("no imp hash")
    val clickHash = tokens.lift(1).getOrElse("no click hash")

    Some(AuxiliaryClickEvent(
      new DateTime,
      impressionHash,
      clickHash,
      pageUrl,
      userGuid.getOrElse("no user guid"),
      referrerToPageUrl.getOrElse("no referrer"),
      request.getUserAgentStrOpt.getOrElse("no ua"),
      request.getGravityRemoteAddr
    ))
  }
}