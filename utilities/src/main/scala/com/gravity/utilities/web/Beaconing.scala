package com.gravity.utilities.web

import java.util.regex.Pattern
import javax.servlet.http.{HttpServletRequest, HttpServletResponse, Cookie => ServletCookie}

import com.gravity.logging.Logstashable
import com.gravity.utilities.FieldConverters._
import com.gravity.utilities.ScalaMagic._
import com.gravity.utilities._
import com.gravity.utilities.components.FailureResult
import com.gravity.utilities.eventlogging._
import com.gravity.utilities.grvstrings.emptyString
import com.gravity.utilities.web.Beaconing.{GetRedirectCookieFailure, ParseRedirectCookieFailure}
import com.gravity.utilities.web.http._
import org.apache.http.HttpHeaders
import org.joda.time.DateTime
import org.scalatra.servlet.ServletApiImplicits
import org.scalatra.{Cookie, CookieOptions}

import scala.concurrent.duration._
import scala.util.Random

trait Beaconing extends HasGravityRoleProperties with ServletApiImplicits {
  import Beaconing._
  import com.gravity.logging.Logging._
  implicit def request: HttpServletRequest
  def response: HttpServletResponse

//  val initBeaconing = GravRedirectFields.fields //assign to a val to avoid warning

  def setUserGuidCookieOnOurDomain(userGuid: String) {
    try {
      response.addCookie(generateUserGuidCookie(userGuid))
    } catch {
      case ex: IllegalArgumentException =>
        warn(ex, "Failed to set UserGuid in cookie for userGuid value: \"{0}\"", userGuid)
    }
  }

  def getUserGuidCookie: Option[String] =
    request.getCookiesNullSafe.find(_.getName == cookieNameOnOurDomain).map(_.getValue)
  
  /**
   * Sets a cookie from the gravity domain, pass in the 32 char userguid that you want to set it for.
   * If the user is opted-out, deletes their cookie on the gravity domain.
   *
   * @return the set userGuid or None if opted-out
   */
  def setUserGuidCookieOrTruncateIfOptedOut(vaguid: String): Option[String] = {
    // Opted out
    if(request.isUserOptedOut) {
      Beaconing.truncateUserGuidCookieOnOurDomain(response)
      None
    }
    // Opted in
    else {
      setUserGuidCookieOnOurDomain(vaguid)
      Some(vaguid)
    }
  }

  def setRedirectCookie2(newRedirect: GravRedirect) : Option[String] = {
    try {
      if(!request.isUserOptedOut) {
        implicit val cookieOptions = baseGravityCookieOptions.copy(
          maxAge = 60 * 60 * 24 * 30 //30 days
        )
        val existingData = GravRedirectFields2.fromRequest
        val newData = {
          if(existingData.isDefined) {
            trace("got existing redirect cookie: " + grvfields.toDelimitedFieldString(existingData.get))
            GravRedirectFields2(newRedirect, existingData.get)
          }
          else {
            GravRedirectFields2(newRedirect)
          }
        }

        val cookieVal = grvfields.toDelimitedFieldString(newData)
        response.addCookie(toServletCookie(Cookie(GravRedirectFields2.redirectCookieName2, cookieVal)))
        //trace("Setting redirect cookie to " + cookieVal)
        Some(cookieVal)
      }
      else {
        None
      }
    }
    catch {
      case e:Exception =>
        warn("Exception setting redirect cookie 2: " + ScalaMagic.formatException(e))
        None
    }
  }

}

object Beaconing {
  val vaguidPattern: Pattern = """[0-9a-f]{32}""".r.pattern

  val cookieNameOnOurDomain = "vaguid"
  val cookieNameOnPartnerDomain = "grvinsights"
  val userGuidMaxAgeSeconds: Int = (13 * 30).days.toSeconds.toInt
  lazy val ourCookieDomain: String = Settings2.getPropertyOrDefault("insights.beacons.firstPartyCookieDomain", emptyString)

  val baseGravityCookieOptions: CookieOptions = CookieOptions(domain = ourCookieDomain, path = "/")
  val userGuidCookieOptions: CookieOptions = baseGravityCookieOptions.copy(maxAge = userGuidMaxAgeSeconds)

  case class ParseRedirectCookieFailure(cookieStr: String, message: String) extends Logstashable {
    import com.gravity.logging.Logstashable._
    override def getKVs: Seq[(String, String)] = {
      Seq(Logstashable.Cookie -> cookieStr, Message -> message)
    }
  }

  case class GetRedirectCookieFailure(msg: String, exOpt: Option[Throwable]) extends FailureResult(msg, exOpt)

  def toServletCookie(cookie: org.scalatra.Cookie): ServletCookie = {
    import org.scalatra.util.RicherString._
    val sCookie = new ServletCookie(cookie.name, cookie.value)
    if(userGuidCookieOptions.domain.nonBlank) sCookie.setDomain(userGuidCookieOptions.domain)
    if(userGuidCookieOptions.path.nonBlank) sCookie.setPath(userGuidCookieOptions.path)
    sCookie.setMaxAge(userGuidCookieOptions.maxAge)
    if(userGuidCookieOptions.secure) sCookie.setSecure(userGuidCookieOptions.secure)
    if(userGuidCookieOptions.comment.nonBlank) sCookie.setComment(userGuidCookieOptions.comment)
    sCookie
  }

  def cookieDomainForPartnerUserGuidCookie(implicit request: HttpServletRequest): String = (for {
    referrer <- Option(request.getHeader(HttpHeaders.REFERER))
    registeredDomain <- SplitHost.registeredDomainFromUrl(referrer)
    crossDomain = "." + registeredDomain
  } yield crossDomain).getOrElse(emptyString)

  /** @return TRUE if user has non-empty user GUID cookie on our domain. */
  def hasNonEmptyUserGuidCookieOnOurDomain(implicit request: HttpServletRequest): Boolean =
    request.getCookiesNullSafe.exists(c => c.getName == cookieNameOnOurDomain && c.getValue.nonEmpty)

  /** Blanks out user GUID cookie on our domain. */
  def truncateUserGuidCookieOnOurDomain(implicit response: HttpServletResponse) {
    response.addCookie(toServletCookie(Cookie(cookieNameOnOurDomain, emptyString)(userGuidCookieOptions)))
  }

  /** @return JS that when executed in partner domain context, will truncate the user GUID cookie on partner domain. */
  def truncateUserGuidCookieOnPartnerDomainJs(partnerDomainForCookie: String): String = {
    val cookieOptions = userGuidCookieOptions.copy(domain = partnerDomainForCookie)
    val cookie = Cookie(cookieNameOnPartnerDomain, emptyString)(cookieOptions)
    s"(function() { document.cookie = '${cookie.toCookieString}'; })();"
  }

  /** @return User GUID from cookie if user has one, or None if user does not have one or user is opted out. */
  def userGuidFromCookie(implicit request: HttpServletRequest): Option[String] = {
    if(request.isUserOptedOut)
      return None

    request.getCookiesNullSafe.find(c => {
      c.getName == cookieNameOnOurDomain &&
        !isNullOrEmpty(c.getValue) &&
        vaguidPattern.matcher(c.getValue).matches
    }).map(_.getValue)
  }

  def generateUserGuid: String = HashUtils.md5(java.util.UUID.randomUUID().toString + new Random().nextInt())

  def generateUserGuidCookie(userGuid: String = generateUserGuid): ServletCookie =
    toServletCookie(Cookie(cookieNameOnOurDomain, userGuid)(userGuidCookieOptions))
}

@SerialVersionUID(2l)
case class GravRedirect(timeStamp: DateTime, fromSiteGuid: String, toSiteGuid: String, clickHash: String, auctionId: String, campaignKey: String)

object GravRedirectFields2 {
 import com.gravity.logging.Logging._
  private val maxAgeInDays = 30
  val maxSize = 10
  val redirectCookieName2 = "302"

  def apply(redirect: GravRedirect) : GravRedirectFields2 = GravRedirectFields2(List(redirect))

  def apply(redirect: GravRedirect, existing: GravRedirectFields2) : GravRedirectFields2 = {
    val cutOff = new DateTime().minusDays(maxAgeInDays)

    val existingFiltered = existing.redirects.filter(r => r.timeStamp.isAfter(cutOff) && r.toSiteGuid != redirect.toSiteGuid).sortBy(_.timeStamp.getMillis).takeRight(maxSize - 1)
    GravRedirectFields2(existingFiltered ++ Seq(redirect))
  }

  def fromRequest(implicit req: HttpServletRequest): Option[GravRedirectFields2] = {
    if(req.isUserOptedOut)
      return None

    try {
      req.getCookiesNullSafe.find(_.getName == redirectCookieName2).map(_.getValue).flatMap(cookieStr => {
        FieldValueRegistry.getInstanceFromString[GravRedirectFields2](cookieStr).fold(
          fails => {
            warn(ParseRedirectCookieFailure(cookieStr, "Could not parse redirect cookie value: " + cookieStr + " -> " + fails))
            None
          },
          fields => Some(fields)
        )
      })
    }
    catch {
      case e: Exception =>
        warn(GetRedirectCookieFailure("Exception getting redirect cookie: " + ScalaMagic.formatException(e), Some(e)))
        None
    }
  }
}

@SerialVersionUID(2l)
case class GravRedirectFields2(redirects: Seq[GravRedirect])

