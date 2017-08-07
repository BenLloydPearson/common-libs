package com.gravity.utilities.web

import java.net
import java.net.{InetAddress, URL}
import javax.servlet.http.{Cookie, HttpServlet, HttpServletRequest, HttpServletResponse}

import com.gravity.utilities.analytics.URLUtils
import com.gravity.utilities.grvcoll._
import com.gravity.utilities.grvstrings._
import com.gravity.utilities.swagger.adapter.DefaultValueWriter
import com.gravity.utilities.{Settings, Settings2}
import com.gravity.valueclasses.ValueClassesForUtilities.Url
import com.gravity.valueclasses.{ValueClassesForUtilities => valueclass}
import eu.bitwalker.useragentutils.{Browser, DeviceType, OperatingSystem, UserAgent}
import org.apache.commons.io.IOUtils
import org.apache.commons.validator.routines.UrlValidator
import org.apache.http.HttpHeaders
import org.openrdf.model.URI
import org.openrdf.model.impl.URIImpl
import play.api.libs.json._

import scala.util.matching.Regex
import scala.xml._

object http {
  val optOutCookieName = "IG_action"
  val optOutCookieValue = "OPT-OUT"
  val rmaApiSecureHostname = "secure-api.gravity.com"

  val versionFromAcceptHeaderRegex: Regex = ".*version=(\\d)".r

  class ValidatedUrl private (val url: Url) {
    def toURI: net.URI = new net.URI(url.toString)
  }

  object ValidatedUrl {
    val schemes: List[String] = List("http", "https")
    private val schemesSet: Set[String] = schemes.toSet
    val validator: UrlValidator = new UrlValidator(schemes.toArray)
    val validatorSansAuthority: UrlValidator = new UrlValidator(schemes.toArray, UrlValidator.ALLOW_LOCAL_URLS)

    /** @param validateTld If FALSE, allows URLs like http://localhost/, http://foo.bar/, etc. */
    def apply(candidateUrl: Url, validateTld: Boolean = true): Option[ValidatedUrl] = {
      // Added to utilize our shared URL fixes for known problems (namely URLs with Beyoncé in them)
      val cleansedUrlString = URLUtils.tryToFixUrl(candidateUrl.raw, schemesSet).getOrElse(candidateUrl.raw)
      val v = if(validateTld) validator else validatorSansAuthority
      if(v.isValid(cleansedUrlString)) Some(new ValidatedUrl(Url(cleansedUrlString)))
      else None
    }

    implicit val jsonFormat: Format[ValidatedUrl] = Format[ValidatedUrl](
      Reads[ValidatedUrl] {
        case JsString(url) => ValidatedUrl(Url(url)).toJsResult
        case _ => JsError()
      },
      Writes[ValidatedUrl](validatedUrl => JsString(validatedUrl.url.raw))
    )

    implicit val defaultValueWriter: DefaultValueWriter[ValidatedUrl] with Object {def serialize(t: ValidatedUrl): String} = new DefaultValueWriter[ValidatedUrl] {
      override def serialize(t: ValidatedUrl): String = t.url.raw
    }
  }

  def getOperatingSystemFromUserAgent(userAgent: String): OperatingSystem = try {
    OperatingSystem.parseUserAgentString(userAgent)
  }
  catch {
    case _: Exception => OperatingSystem.UNKNOWN
  }

  def device(ua: valueclass.UserAgent): Option[DeviceType] = os(ua).map(_.getDeviceType)

  def os(ua: valueclass.UserAgent): Option[OperatingSystem] = try {
    Some(OperatingSystem.parseUserAgentString(ua.raw))
  } catch {
    case _: Exception => None
  }

  private val androidOses = Set(
    OperatingSystem.ANDROID,
    OperatingSystem.ANDROID5,
    OperatingSystem.ANDROID5_TABLET,
    OperatingSystem.ANDROID4,
    OperatingSystem.ANDROID4_TABLET,
    OperatingSystem.ANDROID4_WEARABLE,
    OperatingSystem.ANDROID3_TABLET,
    OperatingSystem.ANDROID2,
    OperatingSystem.ANDROID2_TABLET,
    OperatingSystem.ANDROID1,
    OperatingSystem.ANDROID_MOBILE,
    OperatingSystem.ANDROID_TABLET
  )

  private val iosOses = Set(
    OperatingSystem.IOS,
    OperatingSystem.iOS8_1_IPHONE,
    OperatingSystem.iOS8_IPHONE,
    OperatingSystem.iOS7_IPHONE,
    OperatingSystem.iOS6_IPHONE,
    OperatingSystem.iOS5_IPHONE,
    OperatingSystem.iOS4_IPHONE,
    OperatingSystem.iOS8_1_IPAD,
    OperatingSystem.iOS8_IPAD,
    OperatingSystem.iOS7_IPAD,
    OperatingSystem.iOS6_IPAD
  )

  implicit class GravityOperatingSystem(os: OperatingSystem) {
    def isAndroid: Boolean = androidOses.contains(os)
    def isIos: Boolean = iosOses.contains(os)
  }

  implicit class GravityHttpServletRequest(request: HttpServletRequest) {
    import com.gravity.utilities.Counters._

    /** @return TRUE if the parameter exists in request query string or body, even if param's value is empty. */
    def hasParameter(paramName: String): Boolean = request.getParameter(paramName) != null

    /**
     * Gets JSONP callback name expected in a certain param, but only if it is not attempting to inject JavaScript. This
     * is the safe way to get and use a JSONP callback param name.
     *
     * @return JSONP callback name if it was present in params and safe to use.
     */
    def getJsonpCallback(callbackParamName: String): Option[String] = {
      // Supports jQuery JSONP at this time; technically JS allows many more chars in identifier names
      Option(request.getParameter(callbackParamName)).filter(jsonpCallbackRegex.findFirstIn(_).nonEmpty)
    }

    val counterCategory: String = "GravityHttpServletRequest"

    def getBrowser: Browser = getUserAgent.map(_.getBrowser).getOrElse(Browser.UNKNOWN)

    /**
     * The true/forwarded client IP, rather than the IP of Gravity's Netscaler, if available. Will accept at highest
     * precedence an IP passed in the request param "ipOverride" such as "ipOverride=1.2.3.4".
     */
    def getGravityRemoteAddr: String = Option(request.getParameter("ipOverride")) match {
      case Some(ip) =>
        countPerSecond(counterCategory, "ipOverride used")
        ip

      case _ =>
        Option(request.getHeader("True-Client-IP")).map(ip => {countPerSecond(counterCategory, "IP found in: True-Client-IP"); ip})
          .orElse (Option(request.getHeader("X-Forwarded-For")).map(ip => {countPerSecond(counterCategory, "IP found in: X-Forwarded-For"); ip}))
          .orElse (Option(request.getHeader("Grv-Client-IP")).map(ip => {countPerSecond(counterCategory, "IP found in: Grv-Client-IP"); ip}))
          .getOrElse {
          countPerSecond(counterCategory, "IP found in: getRemoteAddr")
          request.getRemoteAddr
        }
    }

    def getHostname: String =  {
      InetAddress.getLocalHost.getHostName
    }

    def requestedWithinVpn: Boolean = !(request.getServerName endsWith ".com")

    /**
      * Ordinarily only headers are checked to see if a request is over SSL. This method also checks the hostname against
      * a list of known hosts we use for SSL, which may be required for your SSL check depending on how the NetScaler
      * passes SSL traffic to the server.
      *
      * CAUTION: Our secure-api.gravity.com endpoint nowadays may also serve traffic over HTTP. Therefore we should
      * strongly consider investigating the remaining usage of this def and consider deprecating/removing it as it
      * leads you down an unsecure path.
      *
      * @see [[javax.servlet.ServletRequest#isSecure()]]
      */
    def isSecureWithHostnameCheck: Boolean = request.isSecure || request.getServerName == rmaApiSecureHostname

    def isMobile: Boolean = getOperatingSystem.exists(_.getDeviceType == DeviceType.MOBILE)

    def isUserOptedOut: Boolean = getCookiesNullSafe.exists(c => c.getName == optOutCookieName && c.getValue == optOutCookieValue)

    def disableLogging: Boolean = Option(request.getParameter("disableLogging")).flatMap(_.tryToBoolean).getOrElse(false)

    def logResult: Boolean = Option(request.getParameter("logResult")).flatMap(_.tryToBoolean).getOrElse(true)

    /** @param useCanonicalHostname TRUE to sub in the canonical Gravity hostname in place of the request hostname. */
    def getRequestUrlWithQueryString(useCanonicalHostname: Boolean = false): String = {
      val requestUrl =
          if(useCanonicalHostname) {
            URLUtils.replaceHost(request.getRequestURL.toString, Settings.CANONICAL_HOST_NAME)
          } else {
            request.getRequestURL.toString
          }

      val queryString = Option(request.getQueryString).fold(emptyString)("?" + _)

      requestUrl + queryString
    }

    /**
      * @return Request URL with appropriate "reverse rewrites" for widget URLs that came rewritten by the load balancer
      *         (i.e. the actual widget request URL sent by the browser _before_ having been rewritten by NetScaler).
      */
    def getWidgetRequestUrlWithQueryString: String = {
      val root = Settings2.webRoot
      getRequestUrlWithQueryString()
        .replace(
          "http://rma-api.gravity.com" + root + "/recommendation/",
          "http://rma-api.gravity.com/v1/api/intelligence/"
        )
    }

    def getRequestUrlToContextPath: String = {
      new URL(request.getScheme, request.getServerName, request.getServerPort, request.getContextPath).toString
    }

    def getHeaderOption(name: String): Option[String] = Option(request.getHeader(name))
    def getHeaderNullSafe(name: String): String = getHeaderOption(name).getOrElse("")

    def getCookiesOption: Option[Array[Cookie]] = Option(request.getCookies)
    def getCookiesNullSafe: Array[Cookie] = request.getCookies match {
      case null => Array.empty
      case x => x
    }

    def getOperatingSystem: Option[OperatingSystem] = Option(getOperatingSystemFromUserAgent(getUserAgentStr)) match {
      case Some(os) if os == OperatingSystem.UNKNOWN => None
      case x @ Some(_) => x
      case _ => None
    }

    def getUserAgentStr: String = Option(request.getParameter("ua")) match {
      case Some(ua) =>
        countPerSecond(counterCategory, "user-agent Override used")
        ua
        
      case _ =>
        getHeaderNullSafe(HttpHeaders.USER_AGENT)
    }

    def getUserAgentStrOpt: Option[String] = getUserAgentStr.noneForEmpty(trim = true)

    def getUserAgent: Option[UserAgent] = try {
      Option(UserAgent.parseUserAgentString(getUserAgentStr))
    }
    catch {
      case _: Exception => None
    }

    def getVersionHeader: Option[Int] = {
      getHeaderOption(HttpHeaders.ACCEPT).flatMap { acceptHeader =>
        versionFromAcceptHeaderRegex findFirstMatchIn acceptHeader map (_.group(1)) map (_.toInt)
      }
    }

    def getApiVersion: Option[ApiVersion] = {
      getVersionHeader flatMap ApiVersion.fromMajorVersion
    }

    protected var body: String = null
    def getBody: String = {
      if(body == null) {
        body = IOUtils.toString(request.getInputStream, Option(request.getCharacterEncoding).getOrElse("UTF-8"))
      }

      body
    }
  }

  implicit class GravityHttpServletResponse(response: HttpServletResponse) {
    /** @see http://stackoverflow.com/a/2068407/444692 */
    def setCacheBustingHeaders() {
      response.addHeader("Cache-Control", "no-cache, no-store, must-revalidate")
      response.addHeader("Pragma", "no-cache")
      response.addHeader("Expires", "0")
    }
  }

  /** JSONP callbacks must match this in order to be safe. */
  private val jsonpCallbackRegex = "^[A-Za-z0-9_]+$".r

  implicit val urlFormat: Format[URL] = Format[URL](
    Reads[URL] {
      case JsString(urlStr) => JsSuccess(new URL(urlStr))
      case _ => JsError()
    },
    Writes[URL](url => JsString(url.toString))
  )

  implicit val urlDefaultValueWriter: DefaultValueWriter[URL] with Object {def serialize(t: URL): String} = new DefaultValueWriter[URL] {
    override def serialize(t: URL): String = t.toString
  }

  implicit val uriFormat: Format[URI] = Format[URI](
    Reads[URI] {
      case JsString(uriStr) => JsSuccess(new URIImpl(uriStr))
      case _ => JsError()
    },
    Writes[URI](uri => JsString(uri.toString))
  )

  implicit val uriDefaultValueWriter: DefaultValueWriter[URI] with Object {def serialize(t: URI): String} = new DefaultValueWriter[URI] {
    override def serialize(t: URI): String = t.toString
  }

  private var _webXml: Elem = null
  private def webXml(implicit servlet: HttpServlet): Elem = {
    if(_webXml != null)
      return _webXml

    synchronized {
      val webXmlStream = servlet.getServletContext.getResourceAsStream("/WEB-INF/web.xml")
      _webXml = XML.load(webXmlStream)
      webXmlStream.close()
      _webXml
    }
  }

  implicit class GravityServlet(servlet: HttpServlet) {
    implicit val s: HttpServlet = servlet

    def urlMappings: Set[String] = {
      (for {
        servletMapping <- webXml.child
        if servletMapping.label == "servlet-mapping"

        mappingServletName <- servletMapping.child
        if mappingServletName.label == "servlet-name"
        if mappingServletName.text.trim == servlet.getServletName

        urlPattern <- servletMapping.child
        if urlPattern.label == "url-pattern"
      } yield urlPattern.text.trim).toSet
    }

    /** @return This will not work with super fancy URL mappings, only simple 'standard' ones that end in an asterisk. */
    def firstUrlMappingAsServletPath: String = urlMappings.headOption match {
      case Some(mapping) => mapping.replaceAllLiterally("/*", "")
      case None => "/"
    }
  }
}