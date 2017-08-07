package com.gravity.utilities.web

import java.net._
import java.nio.charset.MalformedInputException

import com.gravity.goose.Configuration
import com.gravity.goose.network.{AbstractHtmlFetcher, MaxBytesException}
import com.gravity.utilities.analytics.URLUtils
import com.gravity.utilities.components.FailureResult
import com.gravity.utilities.grvstrings._
import com.gravity.utilities.grvz._
import org.apache.commons.io.IOUtils
import org.apache.http._
import org.apache.http.client.HttpClient
import org.apache.http.client.methods.{CloseableHttpResponse, HttpGet, HttpHead}
import org.apache.http.conn.{ConnectTimeoutException, HttpHostConnectException}
import org.apache.http.impl.client.DefaultRedirectStrategy
import org.apache.http.protocol.HttpContext
import org.apache.http.util.EntityUtils
import org.jsoup.Jsoup
import org.jsoup.nodes.Document

import scala.annotation.tailrec
import scala.collection.JavaConversions._
import scalaz.syntax.std.option._
import scalaz.syntax.validation._
import scalaz.{Failure, Success, ValidationNel}

/**
 * Created by IntelliJ IDEA.
 * Author: Robbie Coleman
 * Date: 10/13/12
 * Time: 1:36 AM
 */

object GrvHttpParamsView {
 import com.gravity.logging.Logging._
  def traceParamsSettings(where: String, params: HttpArguments): Unit = {
    trace {
      s"""|In $where...
          |  CnTimeOut is `${params.connectionTimeout}`
          |  SoTimeOut is `${params.socketTimeout}`
          |  UserAgent is `${params.userAgent}`""".stripMargin
    }
  }
}

object CanonDebugs {
 import com.gravity.logging.Logging._
}

object GrvHttpArgumentsOverrides {
  def forUrl(url: String): Option[HttpArgumentsOverrides] = {
    Option(url).flatMap(_.tryToURL).flatMap { u =>
      u.getAuthority match {
        case authority if authority.endsWith(".perfectliving.com") || authority == "perfectliving.com" =>
          Option(HttpArgumentsOverrides(optConnectionTimeout = Option(30000), optSocketTimeout = Option(30000)))

        case authority if authority.endsWith(".moderndipity.com") || authority == "moderndipity.com" =>
          Option(HttpArgumentsOverrides(optConnectionTimeout = Option(60000), optSocketTimeout = Option(60000)))

        case authority if authority.endsWith(".viralfaze.com") || authority == "viralfaze.com" =>
          Option(HttpArgumentsOverrides(optConnectionTimeout = Option(60000), optSocketTimeout = Option(60000)))

        case _ =>
          None
      }
    }
  }
}

object GrvHtmlFetcher extends AbstractHtmlFetcher {
 import com.gravity.logging.Logging._
  import com.gravity.utilities.Counters._
  val counterCategory = "GrvHtmlFetcher"

  //val spoofUserAgent = "Mozilla/5.0 (Windows NT 6.1; WOW64; rv:15.0) Gecko/20120427 Firefox/15.0a1"

  def getHtml(config: Configuration, url: String) : Option[String] = getHtml(config, url, 5242880)

  def getHtml(config: Configuration, url: String, maxBytes: Int, fixUrlRetries: Int = 5): Option[String] = {
    @tailrec def tryToFindURISyntaxException(th: Throwable): Option[URISyntaxException] = {
      if (th == null)
        None
      else if (th.isInstanceOf[URISyntaxException])
        th.asInstanceOf[URISyntaxException].some
      else
        tryToFindURISyntaxException(th.getCause)
    }

    val httpArgs = useHttpArguments(config, url, "com.gravity.utilities.web.GrvHtmlFetcher.getHtml")

    ContentUtils.getWebContentAsValidationString(url, maxBytes = maxBytes, argsOverrides = httpArgs.toHttpArgumentsOverrides.some) match {
      case Success(content) =>
        trace("Got back HTML for url: " + url)
        trace("First 300 chars:")
        trace(content.take(300) + "...")
        Some(content)
      case Failure(failed) =>
        // Was the failure due to e.g. an illegal character in the URL? If we're not out of retries, try to fix the URL and see if we get something different.
        val optRetryUrl = failed.exceptionOption
          .flatMap(tryToFindURISyntaxException)
          .flatMap(foundEx => if (fixUrlRetries <= 0) None else URLUtils.tryToFixUrl(foundEx.getInput))
          .filter(_ != url)

        optRetryUrl match {
          case Some(retryUrl) =>
            // Yay, retry with the fixed URL, but don't loop forever.
            warn(s"Retrying with fixed URL: `$retryUrl`")
            getHtml(config, retryUrl, maxBytes, fixUrlRetries - 1)

          case None =>
            failed.exceptionOption match {
              case Some(ex) =>
                ex match {
                  case bytes:MaxBytesException =>  warn("Url `{0}` had more than {1} bytes and has been deemed too large for fetching.", url, maxBytes)
                  case p:CouldNotParseException => warn("Failed to execute request to {0} due to malformed or undetectable input. Attempted charset: {1} Message from charset decoder: {2}", url, p.attemptedCharset, p.message)
                  case n:NoHttpResponseException => warn("No response from {0}", url)
                  case s:SocketTimeoutException => warn("Socket timeout to {0}", url)
                  case c:HttpHostConnectException => warn("Connection refused to {0}", url)
                  case r:UnknownHostException => warn("Could not resolve {0}", url)
                  case t:ConnectTimeoutException => warn ("Connection timed out to {0}", url)
                  case se:SocketException => warn("Socket exception {0} from {1}", se.getMessage, url)
                  case mi:MalformedInputException =>  warn("Failed to execute request to `{0}` due to malformed input. Charset was likely incorrect.", url)
                  case _ => warn(nel(failed), "Unknown failure to crawl url: " + url)
                }

              case None => warn(nel(failed), "Unknown failure to crawl url: " + url)
            }
            None
        }
    }
  }

  // NOTE THAT unlike the Goose HtmlFetcher implementation of AbstractHtmlFetcher.getHttpClient,
  // GrvHtmlFetcher.getHttpClient returns a new HttpClient on every call to getHttpClient,
  // rather than a shared HttpClient.  It uses a shared ClientConnectionManager, though.
  def getHttpClient: HttpClient = HttpConnectionManager.getClient()

  // The url being fetched (e.g. perfectliving.com, moderndipity.com, etc.) affects the timeouts of both getHtml(...) and resolveCanonicalUrl(...).
  def useHttpArguments(config: Configuration, url: String, traceClue: String): HttpArguments = {
    val httpArgs: HttpArguments = HttpArguments.fromGooseConfiguration(config)
    // .copy(userAgent = ContentUtils.gravityBotUserAgent) -- gravityBotUserAgent considered harmful -- Inergize (dothanfirst.com, etc.), or maybe even Akamai, are stalling and 417'ing us.

    GrvHttpParamsView.traceParamsSettings(traceClue, httpArgs)

    httpArgs.withOverrides(GrvHttpArgumentsOverrides.forUrl(url))
  }

  def resolveCanonicalUrl(origUrl: String, optDebugClue: Option[String] = None): ValidationNel[FailureResult, CanonicalResult] = {
    def debugClueStr = {
      optDebugClue.flatMap(_.noneForEmpty).fold(emptyString)(debugClue => s", in resolveCanonicalUrl debugClue=`$debugClue`")
    }

    // Maybe consider using HttpClient URIUtils.resolve(httpget.getURI(), target, redirectLocations) -- @ahiniker 10/7/2013
    // Jsoup follows redirects, but doesn't give us information into whether it redirected or not.  So here we use HttpClient to
    // follow the redirects and get the page content, and then we pass the page content into Jsoup for further processing
    def redirectAndGet(url: String): ValidationNel[FailureResult, CanonicalResult] = {

      val debuggingConnectionLeaks = false

      // When debugging connection leaks, make the per-route connection pools (much) smaller to make any leaks happen sooner.
      val useDefaultMaxPerRoute =
        if (debuggingConnectionLeaks) 1 else HttpArguments.defaults.defaultMaxPerRoute

      // For URL canonicalization of non-special URL's, use socket timeouts of 20 seconds.
      val httpArgs: HttpArguments = {
        val gooseHttpArgs = useHttpArguments(new Configuration(), url, "com.gravity.utilities.web.GrvHtmlFetcher.resolveCanonicalUrl/redirectAndGet")

        gooseHttpArgs.copy(
          defaultMaxPerRoute = useDefaultMaxPerRoute,
          socketTimeout = Math.max(gooseHttpArgs.socketTimeout, 20000))
      }

      val httpClientBuilder = HttpConnectionManager.getClientBuilder(httpArgs)
      var redirectedLocationUri = new URI(url)

      // replace location uri on each redirect
      httpClientBuilder.setRedirectStrategy(new DefaultRedirectStrategy() {
        override def getLocationURI(request: HttpRequest, response: HttpResponse, context: HttpContext): URI = {
          countPerSecond(counterCategory, "Canonicalization redirect via DefaultRedirectStrategy")
          redirectedLocationUri = super.getLocationURI(request, response, context)
          redirectedLocationUri
        }
      })

      // Incredibly, Apache HTTP doesn't make getHeaders publicly available in the interface returned by HEAD.
      // Use Java Reflection to access it, because Geez.
      def getHeaders(response: CloseableHttpResponse, name: String): Seq[Header] = {
        try {
          val rspClass = response.getClass

          val getHeadersMethod = rspClass.getMethod("getHeaders", "".getClass)
          getHeadersMethod.setAccessible(true)

          val headers = getHeadersMethod.invoke(response, name)

          val headersAsArray = headers.asInstanceOf[Array[Header]]

          headersAsArray
        } catch {
          case th: Throwable =>
            warn(th, "Failed to execute getHeaders method.")
            Nil
        }
      }

      // Examine the headers returned by HEAD or GET, and abort the canonicalization early the file is e.g. too large, or a video, etc.
      def earlyAnswer(contentTypes: Seq[String], contentLengths: Seq[Long]): Option[ValidationNel[FailureResult, CanonicalResult]] = {
        val sizLimit = 1024L * 1024L

        val badStarts = Seq(
          "video"
        ).map(_.toLowerCase)

        val badEnds   = Seq(
          "mp3",
          "mp4",
          "quicktime"
        ).map(_.toLowerCase)

        val mustStop = {
          if (contentTypes.map(_.toLowerCase).exists(ct => badStarts.exists(ct.startsWith(_)) || badEnds.exists(ct.endsWith(_)))) {
            countPerSecond(counterCategory, s"Canonicalization Stopped, ${contentTypes.mkString("`", "`, `", "`")}")
            true
          } else if (contentLengths.exists(_ >= sizLimit)) {
            countPerSecond(counterCategory, "Canonicalization Stopped, Too Large")
            true
          } else {
            false
          }
        }

        if (mustStop) {
          val redirectedUrl = url

          CanonicalResult(origUrl, // original url
            if (redirectedUrl != origUrl) redirectedUrl.some else None, // set only if we redirected to a different url
            None).successNel.some // set only if rel="canonical" is present and resolvable
        } else {
          None
        }
      }

      // Examine the headers returned by HEAD, and abort the canonicalization early the file is e.g. too large, or a video, etc.
      def earlyAnswerFromHead(response: CloseableHttpResponse): Option[ValidationNel[FailureResult, CanonicalResult]] = {
        val contentTypes   = getHeaders(response, "Content-Type"  ).toSeq.map(_.getValue).filterNot(_ == null)
        val contentLengths = getHeaders(response, "Content-Length").toSeq.map(_.getValue).filterNot(_ == null).flatMap(_.tryToLong)

        earlyAnswer(contentTypes, contentLengths)
      }

      // Examine the headers returned by GET, and abort the canonicalization early the file is e.g. too large, or a video, etc.
      def earlyAnswerFromGet(possiblyNullEntitly: HttpEntity): Option[ValidationNel[FailureResult, CanonicalResult]] = {
        Option(possiblyNullEntitly).flatMap { entity =>
          val contentTypes   = Option(entity.getContentType).flatMap(hdr => Option(hdr.getValue)).toSeq
          val contentLengths = entity.getContentLength.some.filter(_ >= 0).toSeq

          earlyAnswer(contentTypes, contentLengths)
        }
      }

      val httpClient = httpClientBuilder.build()

      def answerFromHead: Option[ValidationNel[FailureResult, CanonicalResult]] = {
        val rspHead = httpClient.execute(new HttpHead(url))

        try {
          earlyAnswerFromHead(rspHead)
        } finally {
          // Ensure that the stream is consumed to prevent connection leaks, using consumeQuietly() rather than consume() to avoid throwing IOExceptions on I/O errors.
          // (See http://hc.apache.org/httpcomponents-client-ga/tutorial/html/fundamentals.html section "Ensuring release of low level resources")
          EntityUtils.consumeQuietly(rspHead.getEntity)
        }
      }

      def answerFromGet: ValidationNel[FailureResult, CanonicalResult] = {
        val rspGet  = httpClient.execute(new HttpGet(url))

        try {
          if (rspGet.getStatusLine.getStatusCode == 200) {
            earlyAnswerFromGet(rspGet.getEntity) match {
              case Some(asnwer) => answerFromGet

              case None =>
                val document = Jsoup.parse(IOUtils.toString(rspGet.getEntity.getContent), redirectedLocationUri.toString)

                def followMetaRedirects(currentUrl: String, document: Document): (String, Document) = {
                  val meta = document.select("html head meta")
                  meta.iterator.filter(meta => meta.attr("http-equiv").equalsIgnoreCase("refresh")).toList.headOption.flatMap(elem => {
                    val redirectUrl = meta.attr("content").split("=").tail.mkString("=").stripPrefix("'").stripSuffix("'")
                    redirectUrl.tryToURL.map(u => {
                      // There's only one timeout available in Jsoup.parse -- using the maximum of connectionTimeout and socketTimeout.
                      // Could also use the minimum here.  But why are we getting the HTML a different way here? Needs restructure?

                      val useTimeout = Math.max(httpArgs.connectionTimeout, httpArgs.socketTimeout)
                      countPerSecond(counterCategory, "Canonicalization redirect via meta tag")
                      followMetaRedirects(u.toString, Jsoup.parse(new URL(redirectUrl), useTimeout))
                    })
                  }).getOrElse((currentUrl, document))
                }

                val (redirectedUrl, doc) = followMetaRedirects(redirectedLocationUri.toString, document)

                val canonical = doc.select("link[rel=canonical]").attr("href")
                val baseUri   = doc.baseUri()

                val relCanonicalOption = try {
                  if (!canonical.isEmpty) new URI(baseUri).resolve(canonical).toString.some else None
                } catch {
                  case ex: IllegalArgumentException =>
                    info(s"Unable to resolve canonical URL, origUrl=`$origUrl`, baseUri=`$baseUri`, canonical=`$canonical`, why=`${ex.getClass.getCanonicalName}: ${ex.getMessage}`")
                    None
                }

                CanonicalResult(origUrl,                                      // original url
                  if (redirectedUrl != origUrl) redirectedUrl.some else None, // set only if we redirected to a different url
                  relCanonicalOption).successNel                              // set only if rel="canonical" is present and resolvable
            }
          } else {
            FailureResult("Unable to determine canonical URL (received status code: %s)".format(rspGet.getStatusLine.getStatusCode)).failureNel
          }
        } finally {
          // Ensure that the stream is consumed to prevent connection leaks, using consumeQuietly() rather than consume() to avoid throwing IOExceptions on I/O errors.
          // (See http://hc.apache.org/httpcomponents-client-ga/tutorial/html/fundamentals.html section "Ensuring release of low level resources")
          EntityUtils.consumeQuietly(rspGet.getEntity)
        }
      }

      answerFromHead.getOrElse(answerFromGet)
    }

    try {
      countPerSecond(counterCategory, "Canonicalization requested")
      redirectAndGet(origUrl)
    } catch {
      case er: OutOfMemoryError =>
        warn(er, s"OutOfMemoryError while canonicalizing origUrl: `$origUrl`")
        Thread.sleep(1000L)
        throw er

      case ex: ConnectTimeoutException =>
        warn(ex, s"Connect time out while trying to determine canonical URL, origUrl=`$origUrl`$debugClueStr")
        FailureResult(s"Timeout while determining canonical URL, origUrl=`$origUrl").failureNel

      case ex: SocketTimeoutException =>
        warn(ex, s"Socket time out while trying to determine canonical URL, origUrl=`$origUrl`$debugClueStr")
        FailureResult(s"Timeout while determining canonical URL, origUrl=`$origUrl").failureNel

      case ex: Exception =>
        warn(ex, s"Unable to determine canonical URL, origUrl=`$origUrl`$debugClueStr")
        FailureResult(s"Unable to determine canonical URL (%s), origUrl=`$origUrl".format(Option(ex.getMessage).getOrElse(ex.getClass.getSimpleName))).failureNel
    }
  }
}


case class CanonicalResult(originalUrl: String, redirectedUrlOption: Option[String] = None, relCanonicalOption: Option[String] = None) {
  def canonicalUrl: String = relCanonicalOption.getOrElse(redirectedUrlOption.getOrElse(originalUrl))
  def hasRedirect: Boolean = redirectedUrlOption.isDefined
  def hasCanonical: Boolean = relCanonicalOption.isDefined
}
