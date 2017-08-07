package com.gravity.utilities.analytics

import java.io.IOException
import java.net.{MalformedURLException, URI, URL}

import com.google.common.base.Charsets
import com.google.common.net.InternetDomainName
import com.gravity.goose.Configuration
import com.gravity.utilities.ScalaMagic._
import com.gravity.utilities.components.FailureResult
import com.gravity.utilities.grvstrings._
import com.gravity.utilities.grvz._
import com.gravity.utilities.web.{GrvHtmlFetcher, HttpConnectionManager, HttpResultStreamLite}
import com.gravity.utilities._
import com.gravity.valueclasses.ValueClassesForUtilities._
import org.apache.commons.codec.net.URLCodec
import org.apache.commons.httpclient.cookie.CookiePolicy
import org.apache.commons.httpclient.methods.{GetMethod, HeadMethod}
import org.apache.commons.httpclient.{HttpClient, MultiThreadedHttpConnectionManager}
import org.jsoup.nodes.Document

import scala.collection._
import scala.util.matching.Regex
import scalaz.Scalaz._
import scalaz._

/**
* Created by Jim Plush
* User: jim
* Date: 5/2/11
*/

object URLUtils {
 import com.gravity.logging.Logging._
  import com.gravity.utilities.Counters._

  val counterCategory = "URLUtils"

  val HASH_REGEX: Regex = "#".r
  val QUERY_REGEX: Regex = "\\?".r
  val AMP_REGEX: Regex = "&".r
  val EQ_REGEX: Regex = "=".r
  val DOT_REGEX: Regex = "\\.".r
  val SLASH_REGEX: Regex = "\\/".r
  val SCHEME_REGEX: Regex = "^[^:]+://".r

  val COMBINED_REGEX: Regex = "#|\\?|&|\\/".r

  val urlCodec : URLCodec = new URLCodec(Charsets.UTF_8.name())

  def extractParameterMap(input: String): immutable.Map[String, String] = {
    if (ScalaMagic.isNullOrEmpty(input)) return Map.empty[String, String]

    (for {
      afterQuestionMark <- QUERY_REGEX.split(input).lift(1).orElse(Some(input)).toIterable
      query <- HASH_REGEX.split(afterQuestionMark).lift(0).toIterable
      param <- AMP_REGEX.split(query)
      pair = EQ_REGEX.split(param)
      key <- pair.lift(0)
      value <- pair.lift(1)
    } yield key -> value).toMap
  }

  //def urlEncode(input: String): String = if (isNullOrEmpty(input)) emptyString else URLEncoder.encode(input, Charsets.UTF_8.name())
  def urlEncode(input: String): String = if (isNullOrEmpty(input)) emptyString else urlCodec.encode(input)

  //def urlDecode(input: String): String = if (isNullOrEmpty(input)) emptyString else URLDecoder.decode(input, Charsets.UTF_8.name())
  def urlDecode(input: String): String = if (isNullOrEmpty(input)) emptyString else urlCodec.decode(input)

  final class NormalizedUrl private(val normalizedUrl: String) {
    override lazy val hashCode: Int = normalizedUrl.hashCode
    override def equals(other: Any): Boolean = normalizedUrl equals (other)
    override def toString: String = normalizedUrl
    def hostname: Option[String] = normalizedUrl.tryToURL.flatMap(url => Option(url.getHost))
  }

  /**
   * Built to allow simple HTTP calls outside of our more robust HttpConnectionManager
   */
  private val basicHttpClient = {
    val connectionManager = new MultiThreadedHttpConnectionManager()
    val maxConnectionsPerHost = Settings2.getIntOrDefault("url.utils.basic.http.client.max.connections.per.host", 35)
    val maxConnectionsTotal = Settings2.getIntOrDefault("url.utils.basic.http.client.max.connections.total", 500)
    val connectionTimeout = Settings2.getIntOrDefault("url.utils.basic.http.client.connection.timeout", 6000)
    val socketTimeout = Settings2.getIntOrDefault("url.utils.basic.http.client.socket.timeout", 6000)

    val connectionParams = connectionManager.getParams
    connectionParams.setDefaultMaxConnectionsPerHost(maxConnectionsPerHost)
    connectionParams.setMaxTotalConnections(maxConnectionsTotal)
    connectionParams.setConnectionTimeout(connectionTimeout)
    connectionParams.setSoTimeout(socketTimeout)
    connectionParams.setStaleCheckingEnabled(true)
    connectionManager.setParams(connectionParams)

    new HttpClient(connectionManager)
  }

  private def validateURL(url: String): Option[URL] = try {
    Some(new URL(url))
  } catch {
    case _: Exception => None
  }

  private def validateURI(url: String): Option[URI] = try {
    Some(new URI(url))
  } catch {
    case _: Exception => None
  }

  /**
   * Tries to fix the URL String inUrlStr to correct known weirdnesses, returning None or Some(Nicer URL String).
   * 
   * @param allowedSchemes Empty set means "allow any/all schemes." Schemes should be given in lowercase.
   */
  def tryToFixUrl(url: String, allowedSchemes: Set[String] = Set("http", "https")): Option[String] = {
    // null or empty Strings are about as None as one could imagine.
    if (url == null || url.isEmpty) return None

    val indexOfColon = url.indexOf(':')

    // either we start with a colon or there isn't one present
    if (indexOfColon < 1) return None

    val scheme = url.take(indexOfColon).toLowerCase

    // Scheme is entire URL
    if(url == scheme)
      return None
    else if(allowedSchemes.nonEmpty && !allowedSchemes.contains(scheme))
      return None

    // Now try the easiest-to-use safe URL parsers, but fall back to our own if it fails
    validateURL(url).map(_.toString).flatMap(validateURI).map(_.toASCIIString) match {
      case someValidString @ Some(_) => return someValidString
      case None => // no worries, we can just fallback to our own checks below...
    }

    // Some RSS feeds e.g. at http://www.refinery29.com/index.xml have URLs like this, with weirdly-escaped http://
    // http:\/\/s3.r29static.com/bin/entry/ecb/x,80/1263664/vesperpag.jpg
    val evil = ":\\/\\/"

    // Find the first ":", and see if it's followed by the Evil...
    val niceUrlStr = indexOfColon match {
      case idxFound if url.substring(idxFound).startsWith(evil) =>
        url.substring(0, idxFound) + "://" + url.substring(idxFound + evil.length)

      case _ => url
    }

    Some(rfc1738EncodeUnsafeQueryStringChars(niceUrlStr))
  }

  /**
   * @see http://www.rfc-editor.org/rfc/rfc1738.txt
   *
   * Some chars are allowed in URL query strings (e.g. "|") but per RFC 1738 are considered 'unsafe' and should always
   * be encoded. Notably, some Java libraries such as java.net.* may choose to fail if those chars are present.
   *
   * [[tryToFixUrl()]] makes use of this fix.
   */
  def rfc1738EncodeUnsafeQueryStringChars(url: String): String = {
    if (url == null || url.isEmpty) return emptyString

    var inQueryStringOrHash = false
    val sb = new StringBuilder(url.length)
    url.foreach(ch => {
      if(inQueryStringOrHash) {
        rfc1738UnsafeCharToEncoded.get(ch) match {
          case Some(replacement) => sb.append(replacement)
          case None => sb.append(ch)
        }
      }
      else if(ch == HttpConnectionManager.QuestionMark || ch == HttpConnectionManager.FragmentHash) {
        inQueryStringOrHash = true
        sb.append(ch)
      }
      else
        sb.append(ch)
    })

    sb.toString()
  }

  /**
   * Do not add the # or % characters to this list; it is assumed that the provider of the URL has taken care to encode
   * these chars when they are intended to be used in their non-control-char form within a URL.
   */
  private val rfc1738UnsafeCharToEncoded = Map(
    ' ' -> "%20"
    ,'"' -> "%22"
    ,'<' -> "%3C"
    ,'>' -> "%3E"
    ,'{' -> "%7B"
    ,'}' -> "%7D"
    ,'|' -> "%7C"
    ,'\\' -> "%5C"
    ,'^' -> "%5E"
    ,'~' -> "%7E"
    ,'[' -> "%5B"
    ,']' -> "%5D"
    ,'`' -> "%60"
  )

  object NormalizedUrl {

    implicit val equal: Equal[NormalizedUrl] = Equal.equalBy(_.normalizedUrl)

    val whitelistedParamStarts: scala.Seq[String] = Seq(
      "id=",
      "grvid=",
      "s=",
      "f=",
      "q=",
      "slug=",
      "article=",
      "ag=",
      "activeIngredientId=",
      "activeIngredientName=",
      "gameId="
    )

    def isParamAllowed(param: String): Boolean = {
      for (white <- whitelistedParamStarts) {
        if (param.startsWith(white)) return true
      }
      false
    }

    def apply(unnormalizedUrl: String): NormalizedUrl = new NormalizedUrl(normalizeUrl(unnormalizedUrl))
  }

  def validateAndNormalizeUrl(url: String): Validation[FailureResult, String] = {
    for {
      _ <- NonEmptyString(url).toValidation(FailureResult("url MUST NOT be null or empty!"))
      jUrl <- url.validateUrl
    } yield normalizeUrl(jUrl)
  }

  def validateUrlWithHttpGetRequest(url: String, mustHaveScheme: Boolean = false)(validateResult: HttpResultStreamLite => ValidationNel[FailureResult, Unit]): ValidationNel[FailureResult, Unit] = {
    countPerSecond(counterCategory, "validateUrlWithHttpGetRequest called")

    val postFixUrl = preRequestValidations(url, mustHaveScheme).valueOr {
      case fails: NonEmptyList[FailureResult] =>
        countPerSecond(counterCategory, "validateUrlWithHttpGetRequest failed")
        return fails.failure[Unit]
    }

    try {
      val method = new GetMethod(postFixUrl)
      try {
        method.getParams().setCookiePolicy(CookiePolicy.IGNORE_COOKIES)
        method.setRequestHeader("Cookie", "special-cookie=value")
        method.setRequestHeader("Accept-Encoding", "gzip, deflate")
        method.setRequestHeader("User-Agent", HttpConnectionManager.ordinaryUserAgent)

        val statusCode = basicHttpClient.executeMethod(method)

        countByResponseStatus(postFixUrl, statusCode)

        val inputStream = method.getResponseBodyAsStream
        val result = validateResult(HttpResultStreamLite(statusCode, inputStream.some))

        result match {
          case Success(_) => countPerSecond(counterCategory, "validateUrlWithHttpGetRequest succeeded")
          case Failure(_) =>
            countPerSecond(counterCategory, "validateUrlWithHttpGetRequest failed")
            countPerSecond(counterCategory, "validateUrlWithHttpGetRequest validateResult failed")
        }

        result
      }
      catch {
        case ex: java.io.IOException =>
          countPerSecond(counterCategory, "validateUrlWithHttpGetRequest failed")
          countPerSecond(counterCategory, "validateUrlWithHttpGetRequest failed: IOException")
          return FailureResult(s"URL '$url' threw IOException while attempting to open it!", ex).failureNel[Unit]
      }
      finally {
        method.releaseConnection()
      }

    }
    catch {
      case ex: Exception =>
        countPerSecond(counterCategory, "validateUrlWithHttpGetRequest failed")
        countPerSecond(counterCategory, "validateUrlWithHttpGetRequest failed: GetMethod creation Exception")
        FailureResult(s"URL '$postFixUrl' threw an Exception on org.apache.commons.httpclient.methods.GetMethod creation!", ex).failureNel
    }
  }

  private def preRequestValidations(url: String, mustHaveScheme: Boolean = false): ValidationNel[FailureResult, String] = {
    countPerSecond(counterCategory, "preRequestValidations called")

    if (isNullOrEmpty(url)) {
      countPerSecond(counterCategory, "preRequestValidations failed: URL is null or empty")
      return FailureResult("URL is null or empty!").failureNel[String]
    }

    if (mustHaveScheme) {
      val schemeOpt = SCHEME_REGEX.findFirstIn(url)
      if (schemeOpt.isEmpty) {
        countPerSecond(counterCategory, "preRequestValidations failed: URL must have scheme")
        return FailureResult(s"URL '$url' must have scheme and did not!").failureNel[String]
      }

      // URL is empty beyond the scheme
      if (schemeOpt.get.length == url.length) {
        countPerSecond(counterCategory, "preRequestValidations failed: URL is empty beyond scheme")
        return FailureResult(s"URL '$url' must have scheme but is empty beyond the scheme!").failureNel[String]
      }
    }

    // java.net.URL is very strict about URL encoding; this call lets us fix any determinable issues in URL encoding.
    // We will try to fix here but if we fail to, we'll not fail the user's input entirely until java.net.URL has a chance.
    val postFixUrl = tryToFixUrl(url).getOrElse(url)

    try {
      new URL(postFixUrl)
      countPerSecond(counterCategory, "preRequestValidations succeeded")
      postFixUrl.successNel[FailureResult]
    }
    catch {
      case ex: MalformedURLException =>
        countPerSecond(counterCategory, "preRequestValidations failed: MalformedURLException")
        FailureResult(s"URL '$url' threw MalformedURLException on java.net.URL creation!", ex).failureNel[String]
    }
  }

  def isSocketTimeoutException(ex: Exception): Boolean =
    ex.isInstanceOf[java.net.SocketTimeoutException]

  def isSocketException(ex: Exception): Boolean =
    ex.isInstanceOf[java.net.SocketException]

  def isConnectionReset(ex: Exception): Boolean =
    isSocketException(ex) && ex.getMessage().startsWith("Connection reset")

  def validateUrlWithHttpHeadRequestForGmsDlug(url: String): ValidationNel[FailureResult, ValidateUrlResult] = {
    // In the face of socket timeouts, we'll try with timeouts of e.g. 5 seconds, 5 seconds, 10 seconds...
    val timeLadder = Seq(5, 5, 10, 10, 30)

    // Try each value in the sequence in turn, followed by None (use the default) if we run out of values.
    def soTimeoutSecsLogic(attemptCount: Int) =
      timeLadder.drop(attemptCount).headOption

    // Try once for each value in timeLadder, plus the default (currently 60 seconds in production) at the end.
    val maxAttempts = timeLadder.size + 1

    def isRetryable(ex: Exception) = isSocketTimeoutException(ex) || isConnectionReset(ex)

    val beginMs = System.currentTimeMillis
    val result = validateUrlWithHttpHeadRequest(
      url, true, Set(200, 301, 302), soTimeoutSecsLogic, isRetryableLogic = isRetryable, maxAttempts = maxAttempts, attemptCount = 0)
    val elapMs  = System.currentTimeMillis - beginMs

    info(s"Took ${elapMs}ms in validateUrlWithHttpHeadRequestForGmsDlug(`$url`), result=$result")

    result
  }

  /**
    * Validates the specified `url` and also makes HTTP request to validate the response code matches the specified `validResponseCodes`
    *
    * @param url The `url` to validate silly
    * @param mustHaveScheme If `true`, it will validate a scheme is present and not the only portion of the `url`
    * @param validResponseCodes If `nonEmpty`, it will make an HTTP request and check that the response code is one of those specified
    * @return A `Success(Unit)` if all of the validations succeed, otherwise, a `NonEmptyList` of `FailureResult`s
    */
  def validateUrlWithHttpHeadRequest(url: String,
                                     mustHaveScheme: Boolean = false,
                                     validResponseCodes: Set[Int] = Set(200, 301, 302),
                                     soTimeoutSecsLogic: Int => Option[Int] = _ => None,
                                     isRetryableLogic: Exception => Boolean = isConnectionReset,
                                     maxAttempts: Int = 2,
                                     attemptCount: Int = 0
                                    ): ValidationNel[FailureResult, ValidateUrlResult] = {
    countPerSecond(counterCategory, "validateUrlWithHttpHeadRequest Called")

    var _fullHostOption: Option[FullHost] = null
    def getFullHost: Option[FullHost] = {
      if (_fullHostOption == null) _fullHostOption = SplitHost.fullHostFromUrl(url)
      _fullHostOption
    }

    def ctrForDomain(labelPrefix: String): Unit = {
      getFullHost.map(_.registeredDomain).foreach(domain => {
        countPerSecond(counterCategory, labelPrefix + domain)
      })
    }

    val attemptLabel = s"on attempt #${attemptCount + 1}"

    val postFixUrl = preRequestValidations(url, mustHaveScheme).valueOr {
      case fails: NonEmptyList[FailureResult] =>
        countPerSecond(counterCategory, "validateUrlWithHttpHeadRequest Failed")
        // preRequestValidations increments failure-detail counters.
        return fails.failure[ValidateUrlResult]
    }

    val result = if (validResponseCodes.nonEmpty) {
      var start = 0L
      try {
        val method = new HeadMethod(postFixUrl)

        try {
          method.getParams().setCookiePolicy(CookiePolicy.IGNORE_COOKIES)

          soTimeoutSecsLogic(attemptCount).foreach { secs =>
            method.getParams().setSoTimeout(secs * 1000)
          }

          method.setRequestHeader("Cookie", "special-cookie=value")
          method.setRequestHeader("Accept-Encoding", "gzip, deflate")
          method.setRequestHeader("User-Agent", HttpConnectionManager.ordinaryUserAgent)

          start = System.currentTimeMillis()
          val statusCode = basicHttpClient.executeMethod(method)
          val end = System.currentTimeMillis()
          val duration = end - start

          countByResponseStatus(postFixUrl, statusCode)

          if (!validResponseCodes.contains(statusCode)) {
            ctrForDomain("validateUrlWithHttpHeadRequest Failed: ")
            countPerSecond(counterCategory, "validateUrlWithHttpHeadRequest Failed")
            countPerSecond(counterCategory, s"validateUrlWithHttpHeadRequest bad status $statusCode")
            return FailureResult(s"URL '$url' returned status: $statusCode which was not in expected codes: $validResponseCodes").failureNel[ValidateUrlResult]
          }

          ValidateUrlResult(statusCode, duration.toInt)
        } finally {
          method.releaseConnection()
        }
      } catch {
        case ex: java.io.IOException =>
          ctrForDomain("validateUrlWithHttpHeadRequest Failed: ")
          countPerSecond(counterCategory, s"validateUrlWithHttpHeadRequest ${ex.getClass.getSimpleName} " + attemptLabel)

          if (attemptCount < maxAttempts && isRetryableLogic(ex)) {
            countPerSecond(counterCategory, "validateUrlWithHttpHeadRequest Failed (retrying)")
            return validateUrlWithHttpHeadRequest(url, mustHaveScheme, validResponseCodes, soTimeoutSecsLogic, isRetryableLogic, maxAttempts, attemptCount + 1)
          } else {
            countPerSecond(counterCategory, "validateUrlWithHttpHeadRequest Failed")
            val end = System.currentTimeMillis()
            val duration = end - start
            return FailureResult(s"URL '$url' threw ${ex.getClass.getSimpleName} while attempting to open it! The total milliseconds elapsed for this call was: $duration.", ex).failureNel[ValidateUrlResult]
          }
      }
    }
    else {
      ValidateUrlResult.empty
    }

    countPerSecond(counterCategory, "validateUrlWithHttpHeadRequest Succeeded " + attemptLabel)

    result.successNel
  }

  def countByResponseStatus(url: String, statusCode: Int): Unit = {
    def getHost: String = {
      val host = url.tryToURL.flatMap(u => Option(u.getAuthority)).getOrElse(url)
      countPerSecond(counterCategory, "Bad Responses for: " + host)
      host
    }

    statusCode match {
      case 200 | 301 | 302 =>
        // no counter required

      case fourHundreds if fourHundreds > 399 && fourHundreds < 500 =>
        countPerSecond(counterCategory, "400 series responses for: " + getHost)

      case fiveHundreds if fiveHundreds > 499 && fiveHundreds < 600 =>
        countPerSecond(counterCategory, "500 series responses for: " + getHost)

      case other =>
        countPerSecond(counterCategory, s"$other status code responses for: $getHost")
    }
  }

  /**
   * @param mustHaveScheme TRUE if the input URL is supposed to have a scheme.
   * @param validResponseCodes If non-empty, a request will be made to ensure the URL returns one of the acceptable
   *                           response codes. If an empty set is given (the default), this method will not incur
   *                           network overhead.
   */
  def urlIsValid(url: String, mustHaveScheme: Boolean = false, validResponseCodes: Set[Int] = Set.empty): Boolean = {
    validateUrlWithHttpHeadRequest(url, mustHaveScheme, validResponseCodes).isSuccess
  }

  // this and the method `extractName` exist to cover 2 versions of guava. So... yay
  private val internetDomainNameRegex = """InternetDomainName\{name=(.*)\}""".r

  def extractName(internetDomainName: InternetDomainName): String = {
    val rawToString = internetDomainName.toString
    if (rawToString.startsWith("InternetDomainName{name=")) {
      rawToString match {
        case internetDomainNameRegex(matches) =>
          matches
        case _ =>
          rawToString
      }
    }
    else {
      rawToString
    }
  }


  /**
   * This is what our data pipeline uses for domain. So it's probably what you should use too.
   *
   * @param url Raw URL you wish to retrieve the top level domain from
   * @return If the `url` is parsable, the top level domain will be returned, otherwise returns a FailureResult
   */
  def extractTopLevelDomain(url: String): Validation[FailureResult, String] = {
    if (isNullOrEmpty(url)) return FailureResult("url MUST NOT be null or empty!").failure

    try {
      val jUrl = new URL(url)
      extractName(InternetDomainName.from(jUrl.getHost).topPrivateDomain).success
    }
      catch {
        case e: Exception => FailureResult(s"Failed to parse url: `$url`", e).failure
    }
  }

  /**
   * You should really think about using validateAndNormalizeUrl instead.
    *
    * @param url Raw URL you wish to normalize
   * @return If the `url` is parsable, only whitelisted parameters will remain and ref will be removed, otherwise it is left unchanged
   */
  def normalizeUrl(url: String): String = {
    if (isNullOrEmpty(url)) return emptyString

    validateAndNormalizeUrl(url) | url
  }

  def normalizeUrl(jUrl: java.net.URL): String = {
    val sb = new StringBuilder
    sb.append(jUrl.getProtocol).append(":")
    val host = jUrl.getAuthority
    if (!isNullOrEmpty(host)) {
      sb.append("//").append(host)
    }
    val path = jUrl.getPath
    if (!isNullOrEmpty(path)) {
      sb.append(path)
    }

    val query = jUrl.getQuery
    if (isNullOrEmpty(query)) {
      return sb.toString()
    } else {
      var pastFirst = false
      for {
        paramAndValue <- AMP_REGEX.split(query)
        if NormalizedUrl.isParamAllowed(paramAndValue)
      } {
        if (pastFirst) {
          sb.append("&")
        } else {
          sb.append("?")
          pastFirst = true
        }

        sb.append(paramAndValue)
      }
    }

    sb.toString()
  }

  def clean(url: String): String = {
    if (isNullOrEmpty(url)) return url

    QUERY_REGEX.split(HASH_REGEX.split(url).lift(0).getOrElse(url)).lift(0).getOrElse(url)
  }

  // converts a relative url like /aboutus.html to http://sitename.com/aboutus.html
  def convertRelativeLinkToAbsolute(baseURL: String, newURL: String): String = {
    if (isNullOrEmpty(newURL) || isNullOrEmpty(baseURL)) return newURL

    if (newURL.startsWith("http://") || newURL.startsWith("https://")) return newURL

    baseURL.tryToURL match {
      case Some(url) => convertRelativeLinkToAbsolute(url, newURL)

      case None => newURL
    }
  }

  def convertRelativeLinkToAbsolute(url: URL, newURL: String): String = {
    try {
      (new URL(url, newURL)).toString
    } catch {
      case ex: Exception => info(ex, "unable to parse: " + url.toString, newURL)
      newURL
    }
  }


  def getTitleFromJsoupDoc(doc: Document): String = {
    doc.getElementsByTag("title").first.text
  }

  /**
   * make sure the url is from the same domain as the baseurl
   */
  def isSameDomainLink(baseURL: String, targetURL: String): Boolean = {
    if (isNullOrEmpty(baseURL) || isNullOrEmpty(targetURL)) return false

    baseURL.tryToURL match {
      case Some(url) => {
        convertRelativeLinkToAbsolute(url, targetURL).tryToURL match {
          case Some(url2) => getRawSiteName(url) == getRawSiteName(url2)
          case None => false
        }
      }
      case None => false
    }
  }

  def getRawSiteName(host: String): String = {
    val tokens = DOT_REGEX.split(host.toLowerCase)
    tokens.takeRight(2).mkString(".")
  }

  def getRawSiteName(url: URL): String = getRawSiteName(url.getHost)

  def fetchHTML(config: Configuration, urlString: String): String = {
    try {
      GrvHtmlFetcher.getHtml(config, urlString).getOrElse{
        warn("HtmlFetcher returned none for " + urlString)
        emptyString}
    } catch {
      case e: MalformedURLException => info(e.toString); emptyString
      case e: IOException => info(e.toString); emptyString
    }

  }

  def urlAppearsToBeHomePage(url:String): Boolean = !checkForParamsOrSections(url)

  def checkForParamsOrSections(url: String) : Boolean = {

    val strippedNUrl = url.stripPrefix("http://").stripSuffix("/")

    COMBINED_REGEX.findFirstIn(strippedNUrl).isDefined

  }

  def splitUrlParams(input: String): Array[String] = EQ_REGEX.split(input.stripPrefix("?").stripPrefix("&"))

  /**
   * Given a URL, will append a parameter with a ? or & depending on the scenario.
    *
    * @param url
   * @param param Expressed as a full string, e.g. "joe=schmoe"
   * @return
   *
   * TODO: this is broken in the case of #anchor tags on the end of URLs!!
   *
   */
  def appendParameter(url:String, param:String): String = {
    val cleanParam = if(param.startsWith("?") || param.startsWith("&")) param.substring(1,param.length) else param
    if(url.contains('?')) url + "&" + cleanParam
    else url + "?" + cleanParam
  }

  /** Handles param value encoding. */
  def appendParameter(url: String, paramName: String, paramValue: String): String =
    appendParameter(url, s"$paramName=${urlEncode(paramValue)}")

  // somewhat naive approach for appending a parameter only if it is not already present, and preserving the existing value if it is
  def mergeParameter(url: String, param:String): String = {
    splitUrlParams(param).headOption match {
      case Some(paramName) if url.contains("?" + paramName + "=") || url.contains("&" + paramName + "=") => url
      case _ => appendParameter(url, param)
    }
  }

  /**
   * @param url     URL with at least host and scheme like "foo://bar".
   * @param newHost Host to sub in like "new.host".
   */
  def replaceHost(url: String, newHost: String): String = {
    val schemeEndIndex = url.indexOf("//") + 2
    val scheme = url.substring(0, schemeEndIndex)
    val pathStartIndex = url.indexOf("/", schemeEndIndex)
    val path = try {
      url.substring(pathStartIndex)
    } catch {
      case _: IndexOutOfBoundsException => ""
    }
    scheme + newHost + path
  }

  def safeReplaceHost(url: Url, newHost: String): String = {
    val replaced = for {
      schemeEndIndex <- url.raw.safeIndexOf("//")
      scheme <- url.raw.safeSubstring(0.asIndex, schemeEndIndex + 2)
      pathStartIndex = url.raw.safeIndexOf("/", schemeEndIndex + 2)
      path = pathStartIndex.flatMap(ix => url.raw.safeSubstring(ix)).getOrElse("")
      port = pathStartIndex.flatMap(ix => url.raw.safeIndexOf(":", schemeEndIndex).flatMap(colon => url.raw.safeSubstring(colon, ix))).getOrElse("")
    } yield {
      scheme + newHost + port + path
    }
    replaced.getOrElse(url.raw)
  }

  // somewhat naive approach for replacing an existing parameter if it exists, otherwise adding it if it doesn't
  def replaceParameter(url: String, param:String): String = {
    splitUrlParams(param).headOption match {
      case Some(paramName) if url.contains("?" + paramName + "=") => url.replaceAll("\\?" + paramName + "=[^&#]+", "?" + param)
      case Some(paramName) if url.contains("&" + paramName + "=") => url.replaceAll("\\&" + paramName + "=[^&#]+", "&" + param)
      case _ => appendParameter(url, param)
    }
  }

  // somewhat naive approach for prepending a parameter (and removing that parameter from later in the query string if it exists)
  def prependAndReplaceParameter(url: String, param:String): String = {
    splitUrlParams(param).headOption match {
      case Some(paramName) if url.contains("?") => url.replaceFirst("\\?", "?" + param + "&").replaceAll("\\&" + paramName + "=[^&#]+", "")
      case Some(_) => appendParameter(url, param)
      case None => url
    }
  }

  // given a Map of parameters, apply a merge/append/etc function against an existing url
  def applyParameters(f: (String, String) => String = appendParameter)
                     (url:String, params: Map[String, String]): String = {
    params.foldLeft(url)((current, next) => f(current, next._1 + "=" + next._2))
  }

  def appendParameters: (String, Map[String, String]) => String = applyParameters(appendParameter) _
  def appendParameters(url: String, params: NonEmptyList[String]): String = params.list.foldLeft(url)(appendParameter)
  def appendParameters(url: String, params: (String, String)*): String = appendParameters(url, params.toMap)
  def mergeParameters: (String, Map[String, String]) => String = applyParameters(mergeParameter) _
  def replaceParameters: (String, Map[String, String]) => String = applyParameters(replaceParameter) _
  def prependAndReplaceParameters: (String, Map[String, String]) => String = applyParameters(prependAndReplaceParameter) _

  def create302(url: String): String = {
    url
  }

  def nextQueryStringParamSeparator(url: String): Char = if(url.lastIndexOf('?') == -1) '?' else '&'

  def getParamValue(url: String, paramName: String): Option[String] = {
    val paramClauseStart = {
      val pos = url.lastIndexOf(s"?$paramName=")
      if(pos != -1) pos else url.lastIndexOf(s"&$paramName=")
    }

    if(paramClauseStart == -1)
      return None

    val valueStart = paramClauseStart + paramName.length + 2 /* +2 offset for ('?' or '&') and '=' */
    val valueAccum = new StringBuilder(url.length - valueStart)
    for {
      pos <- valueStart until url.length
      ch = url.charAt(pos)
    } {
      if(ch != '&')
        valueAccum.append(ch)
      // Ran into another param
      else
        return valueAccum.toString().some
    }

    valueAccum.toString().some
  }

  /**
   * Fixes up informal URLs to have any missing http:// header.
   *
   * @param inUrlStr A possibly-empty informal URL String.
   * @return The corrected URL String.
   */
  def addHttpIfMissing(inUrlStr: String): String = {
    if (inUrlStr == null)
      inUrlStr
    else {
      val urlStr = inUrlStr.trim

      if (urlStr.isEmpty)
        urlStr
      else {
        val lowUrl = urlStr.toLowerCase

        if (lowUrl.startsWith("http:") || lowUrl.startsWith("https:"))
          urlStr
        else if (lowUrl.startsWith("//"))
          "http:" + urlStr
        else
          "http://" + urlStr
      }
    }
  }

  /**
    * @return A version of anyText that is suitable for use in a URL. Note that the result of this def still needs to be
    *         URL encoded because it may include potentially "unsafe" (for older browsers) Unicode characters.
    */
  def seoize(anyText: String): String = {
    seoizeRegex.replaceAllIn(anyText, seoizeReplacement.toString)
      .trimLeft(seoizeReplacement)
      .trimRight(seoizeReplacement)
      .toLowerCase
  }

  /** \p{L} is char class for Unicode letters */
  private val seoizeRegex = """[^A-Za-z\p{L}0-9]+""".r

  private val seoizeReplacement = '-'
}

case class ValidateUrlResult(statusCode: Int, millisecondsElapsed: Int) {
  def isEmpty: Boolean = false
}

object ValidateUrlResult {
  val empty: ValidateUrlResult = new ValidateUrlResult(0, 0) {
    override def isEmpty: Boolean = true
  }
}
