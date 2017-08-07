package com.gravity.interests.jobs.intelligence.operations

import com.gravity.utilities.grvstrings._
import com.gravity.utilities.grvtime
import com.gravity.valueclasses.ValueClassesForDomain._
import com.gravity.valueclasses.ValueClassesForUtilities._

/*
 *    __   _         __         
 *   / /  (_)__  ___/ /__  ____
 *  / _ \/ / _ \/ _  / _ \/ _  /
 * /_//_/_/_//_/\_,_/\___/\_, / 
 *                       /___/
 */

case class ImpressionViewedHash(siteGuid: SiteGuid, userGuid: Option[UserGuid], hashString: ImpressionViewedHashString) {
  def callbackv2(requestedQueryString: QueryString, spId: SitePlacementId, https: Boolean): Url = {
    val copiedQueryString = if (requestedQueryString.isEmpty) {
      requestedQueryString
    } else {
      val paramPairs = requestedQueryString.raw.splitBetter("&")
      val sb = new StringBuilder
      var pastFirst = false
      for (pair <- paramPairs) {
        val keyVal = pair.splitBetter("=")
        for {
          paramName <- keyVal.lift(0)
          if paramName.nonEmpty
          if paramName != "c" // ignore originating JSONp param as it was not intended for this call
          if paramName != "ih" // in case the original call already had `ih`, we don't want it to conflict with our own
          if paramName != "ct" // in case the original call already had `ct`, we don't want it to conflict with our own
          paramValue <- keyVal.lift(1)
        } {
          if (pastFirst) {
            sb.append('&')
          } else {
            pastFirst = true
          }
          sb.append(paramName).append('=').append(paramValue)
        }
      }

      sb.toString()
    }

    val ug = userGuid.map(_.raw).filter(_.nonEmpty).getOrElse("unknown")

    val scheme = if(https) "https" else "http"
    val hostname = if(https) "secure-api.gravity.com" else "api.gravity.com"
    val impressionViewedCallback = s"$scheme://$hostname/v0/site/${siteGuid
      .raw}/user/$ug" +
      s"/impressionViewed?$copiedQueryString&ih=${hashString.raw}&ct=${grvtime.currentMillis}&plugin_id=${spId.raw}"
    impressionViewedCallback.asUrl
  }
}
