package com.gravity.interests.jobs.intelligence.ui

import com.gravity.interests.jobs.intelligence.ExchangeMisc
import com.gravity.interests.jobs.intelligence.operations.ImpressionEvent.TrackingParamsMacroEvalFn
import com.gravity.interests.jobs.intelligence.operations._
import com.gravity.utilities.HasGravityRoleProperties
import com.gravity.utilities.grvstrings._

import scala.collection._
import scalaz.Validation
import scalaz.syntax.std.option._
import scalaz.syntax.validation._


object RedirectUtils extends HasGravityRoleProperties {

  val grcc2Param = "grcc2"
  val grcc3Param = "grcc3"

  val redirectPath = "/302/redirect"

  case class SpecialRedirectHost(hostname: String, supportsHttps: Boolean)
  val sourceHostToRedirectHostMap: Map[String, SpecialRedirectHost] = Map(
    "huffingtonpost.com" -> SpecialRedirectHost("grvrdr.huffingtonpost.com", supportsHttps = false),
    "www.huffingtonpost.com" -> SpecialRedirectHost("grvrdr.huffingtonpost.com", supportsHttps = false),
    "kitchendaily.com" -> SpecialRedirectHost("grvrdr.kitchendaily.com", supportsHttps = false),
    "www.kitchendaily.com" -> SpecialRedirectHost("grvrdr.kitchendaily.com", supportsHttps = false),
    "aol.com" -> SpecialRedirectHost("grvrdr.aol.com", supportsHttps = false),
    "www.aol.com" -> SpecialRedirectHost("grvrdr.aol.com", supportsHttps = false),
    "mail.aol.com" -> SpecialRedirectHost("grvrdr.todaypage.aol.com", supportsHttps = false),
    "alpo-todaypage.mail.aol.com" -> SpecialRedirectHost("grvrdr.todaypage.aol.com", supportsHttps = false),
    "alpo-aim.mail.aol.com" -> SpecialRedirectHost("grvrdr.todaypage.aol.com", supportsHttps = false),
    "autos.aol.com" -> SpecialRedirectHost("grvrdr.aol.com", supportsHttps = false),
    "on.aol.com" -> SpecialRedirectHost("grvrdr.aol.com", supportsHttps = false),
    "www.autoblog.com" -> SpecialRedirectHost("grvrdr.autoblog.com", supportsHttps = false),
    "autoblog.com" -> SpecialRedirectHost("grvrdr.autoblog.com", supportsHttps = false)
  )

  /**
   * @param redirectUrl an ugly URL that goes through Gravity's 302 service
   * @param targetUrl a pretty URL that doesn't have our grcc2 tacked on, suitable for display only. The same as the final destination URL after the 302.
   */
  case class RedirectUrls(redirectUrl: String, targetUrl: String)

  /**
   * @param dev TRUE if in dev environment. The reason this is exposed as a param and not simply left to the properties
   *            file is because the use case for this sometimes requires dev = false even in the dev environment (namely
   *            when compiling go.coffee locally for deployment to production).
   *
   * @return URL to the /cookie endpoint in RedirectServlet, which is a JSONP endpoint that yields the value of the
   *         user's grav302 cookie.
   */
  def redirectCookieUrl(secure: Boolean, dev: Boolean = false): String =
    if(dev)
      "http://localhost:8080/302/cookie"
    // Prod secure
    else if(secure)
      "https://secure-api.gravity.com/302/cookie"
    // Prod not secure
    else
      "http://rma-api.gravity.com/302/cookie"

  /**
   * Uses all the business logic we have to construct the proper 302 endpoint
   * @param currentUrl The final user destination URL
   * @param contextPath should be the result of [[com.gravity.utilities.web.http.GravityHttpServletRequest.getRequestUrlToContextPath]]
   * @return The 302 URL prefix as a String
   */
  def redirectPrefix(currentUrl: Option[String], contextPath: String, https: Boolean = false): String = {
    (for {
      currentUrlStr <- currentUrl
      curUrl <- currentUrlStr.tryToURL

      // Use explicitly defined redirect host mapped from URL's full host, then TLD if former isn't available
      redirectHost <- sourceHostToRedirectHostMap.get(curUrl.getHost)
                        .orElse(curUrl.getTopLevelDomain.collect(sourceHostToRedirectHostMap))
      scheme = if(redirectHost.supportsHttps) "https" else "http"
    } yield s"$scheme://${redirectHost.hostname}$redirectPath").getOrElse(fallbackPrefix(contextPath, https))
  }

  /**
   * @param contextPath should be the result of [[com.gravity.utilities.web.http.GravityHttpServletRequest.getRequestUrlToContextPath]]
   */
  def fallbackPrefix(contextPath: String, https: Boolean): String = {
    val settingName = {
      if(https)
        "recommendation.log.redirect.prefixSecure"
      else
        "recommendation.log.redirect.prefix"
    }
    val prefix = Option(properties.getProperty(settingName))
    prefix.filter(_.nonEmpty).getOrElse(s"$contextPath$redirectPath")
  }

  def redirectUrlForUrlWithGrcc3Param(url: String, clickEvent: ClickEvent, currentUrl: String, contextPath: String, https: Boolean,
                                      trackingParamsMacroEvalFn: TrackingParamsMacroEvalFn,
                                      addExchangeOmnitureParams: Boolean, requiresOutboundTrackingParams: Boolean
                                      )
  : Validation[Throwable, String] = {
    val grcc3Match = ClickEvent.grcc3Re.findFirstMatchIn(url) getOrElse {
      return new IllegalArgumentException("No grcc3 found in " + url).failure
    }

    val prefix = redirectPrefix(currentUrl.some, contextPath, https)

    // we have the requirement to preserve tracking params on outbound links for some partners, this will preserve them for partners
    // configured for this feature
    val queryString = (if (requiresOutboundTrackingParams) {
      // get the querystring with any macros evaluated
      trackingParamsMacroEvalFn(clickEvent, url).tryToURL.map(_.getQuery)
    } else if (addExchangeOmnitureParams) {
      //we do this only if !requiresOutboundTrackingParams since the url should already have the omniture tracking param macros in it...
      //still, rather than trying to pull the omniture tracking params out of the URL (there may be other params), let's just recreate them
      val omnitureParams = trackingParamsMacroEvalFn(clickEvent, ExchangeMisc.omnitureTrackingParams.map(_.productIterator.mkString("=")).mkString("&"))
      Option(omnitureParams + "&" + grcc3Param + "=" + grcc3Match.group("value"))
    } else {
      // otherwise return None, which will then revert to the default
      None
    }).getOrElse(grcc3Param + "=" + grcc3Match.group("value")) // the default is to only take the grcc3 parameter

    val redirectUrl = prefix + "?" + queryString

    redirectUrl.success
  }

}