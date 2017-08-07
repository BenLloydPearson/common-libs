package com.gravity.interests.jobs.intelligence.operations.urlvalidation

import java.io.IOException
import java.util.concurrent.TimeUnit

import com.gravity.interests.jobs.intelligence.ArticleKey
import com.gravity.interests.jobs.intelligence.operations.FieldConverters.{ImageUrlValidationRequestMessageConverter, UrlValidationRequestMessageConverter, UrlValidationResponseConverter}
import com.gravity.interests.jobs.intelligence.operations.StaticWidgetNotificationService
import com.gravity.service.remoteoperations._
import com.gravity.utilities.analytics.URLUtils
import com.gravity.utilities.cache.SingletonCache
import com.gravity.utilities.grvfields.FieldConverter
import com.gravity.utilities.web.HttpResultStreamLite
import com.gravity.utilities._
import com.sksamuel.scrimage.Image
import com.gravity.grvlogging._
import scala.concurrent.duration.Duration
import com.gravity.utilities.components.FailureResult
import com.gravity.utilities.grvz._

import scalaz.{Failure, Success, ValidationNel}
import scalaz.syntax.validation._

/**
 * Created by robbie on 10/21/2015.
 *            _ 
 *          /  \
 *         / ..|\
 *        (_\  |_)
 *        /  \@'
 *       /     \
 *   _  /  \   |
 *  \\/  \  | _\
 *   \   /_ || \\_
 *    \____)|_) \_)
 *
 */
class UrlValidationServer extends RemoteOperationsServer(2425, components = Seq(new UrlValidationComponent)) {
  info("Starting up...")
}

object UrlValidationServer {
 import com.gravity.logging.Logging._

  val counterCategory: String = "UrlValidationServer"

  val maxFailuresAllowed: Int = 1            // No more than 1 failure per every
  val maxFailuresWithinSeconds: Int = 4 * 60 // 4 minutes

  lazy val instance: UrlValidationServer = {
    if (!Settings.isProductionServer) {
      com.gravity.grvlogging.updateLoggerToTrace("com.gravity.interests.jobs.intelligence.operations.urlvalidation")
    }
    new UrlValidationServer()
  }

  // Field Converters for request and response messages
  private[urlvalidation] val requestConverter: Option[FieldConverter[UrlValidationRequestMessage]] = Some(UrlValidationRequestMessageConverter)
  private[urlvalidation] val requestImageConverter: Option[FieldConverter[ImageUrlValidationRequestMessage]] = Some(ImageUrlValidationRequestMessageConverter)
  private[urlvalidation] val responseConverter: Option[FieldConverter[UrlValidationResponse]] = Some(UrlValidationResponseConverter)

  private[urlvalidation] def validateUrl(url: String): UrlValidationResponse = {
    val response = UrlValidationCacher.get(url)

    if (!response.returnedFromCache) {
      val failCount = UrlValidationFailureCountCacher.updateAndGet(url, response)
      if (failCount > maxFailuresAllowed) {
        warn("REJECTING URL: {0}", url)
        val rejectedMsg = s"URL '$url' Failed more than $maxFailuresAllowed time(s) within $maxFailuresWithinSeconds seconds and was AUTO-REJECTED!"
        StaticWidgetNotificationService.submitSimpleFailure(url, response.errorMessage.getOrElse(rejectedMsg))
        return UrlValidationResponse(None, Some(rejectedMsg), returnedFromCache = false)
      }
    }

    response
  }

  private[urlvalidation] def validateImageUrl(url: String): UrlValidationResponse = {
    UrlValidationCacher.get(url, validateImage = true)
  }

}

object UrlValidationService {
  import UrlValidationServer.counterCategory
  import com.gravity.utilities.Counters._
  import com.gravity.logging.Logging._

  RemoteOperationsHelper.registerReplyConverter(UrlValidationResponseConverter)

  private val client = RemoteOperationsClient.clientInstance

  private val timeout = Duration(61, TimeUnit.SECONDS)

  private def makeRemoteRequest[R <: RoutableUrlMessage[R]](req: R)(implicit m: Manifest[R]): ValidationNel[FailureResult, UrlValidationResponse] = {

    client.requestResponse[R, UrlValidationResponse](
      payload = req,
      routeId = req.routeId,
      timeOut = timeout,
      fieldConverterOpt = req.fieldConverterOpt
    ) match {
      case succeeded @ Success(_) =>
        countPerSecond(counterCategory, req.instrumentationLabel + ".succeeded")
        succeeded
      case Failure(fails) =>
        countPerSecond(counterCategory, req.instrumentationLabel + ".failed")
        val msg = "URL Validation Client Failed to make " + req.instrumentationLabel + " request to validate image url: \"" + req.url + "\""
        warn(msg)

        Failure(fails.<::(FailureResult(msg)))
    }
  }

  def validateUrl(url: String): ValidationNel[FailureResult, UrlValidationResponse] = {
    countPerSecond(counterCategory, "remote.url.validation.calls")
    makeRemoteRequest(UrlValidationRequestMessage(url))
  }

  def validateImageUrl(url: String): ValidationNel[FailureResult, UrlValidationResponse] = {
    countPerSecond(counterCategory, "remote.image.validation.calls")
    makeRemoteRequest(ImageUrlValidationRequestMessage(url))
  }
}

trait RoutableUrlMessage[R] {
  def url: String

  /**
    * Uses our own [[ArticleKey]] to create an articleId from the reverse of the URL to better distribute
    * @return articleId of the reversed url
    */
  def routeId: Long = {
    val reversedUrl = url.reverse
    ArticleKey(reversedUrl).articleId
  }

  def fieldConverterOpt: Option[FieldConverter[R]]

  def instrumentationLabel: String
}

case class UrlValidationRequestMessage(url: String) extends RoutableUrlMessage[UrlValidationRequestMessage] {
  def fieldConverterOpt: Option[FieldConverter[UrlValidationRequestMessage]] = UrlValidationServer.requestConverter

  def instrumentationLabel: String = "remote.url.validation"
}

case class ImageUrlValidationRequestMessage(url: String) extends RoutableUrlMessage[ImageUrlValidationRequestMessage] {
  def fieldConverterOpt: Option[FieldConverter[ImageUrlValidationRequestMessage]] = UrlValidationServer.requestImageConverter

  def instrumentationLabel: String = "remote.image.validation"
}

case class UrlValidationResponse(statusCode: Option[Int], errorMessage: Option[String], returnedFromCache: Boolean) {
  def isEmpty: Boolean = statusCode.isEmpty && errorMessage.isEmpty
  def isFailure: Boolean = statusCode.isEmpty || errorMessage.isDefined
  def isSuccess: Boolean = !isFailure
}

object UrlValidationResponse {
  val empty: UrlValidationResponse = UrlValidationResponse(None, None, returnedFromCache = false)

  val okResponseFromCache: UrlValidationResponse = UrlValidationResponse(Some(200), None, returnedFromCache = true)
}

class UrlValidationComponentActor extends ComponentActor {
  import UrlValidationServer.{counterCategory, responseConverter, validateUrl, validateImageUrl}
  import com.gravity.logging.Logging._
  import com.gravity.utilities.Counters._

  def handleMessage(w: MessageWrapper): Unit = {
    countPerSecond(counterCategory, "message.total.received")
    getPayloadObjectFrom(w) match {
      case UrlValidationRequestMessage(url) =>
        countPerSecond(counterCategory, "message.type.url.received")
        val response = validateUrl(url)
        w.reply(response, responseConverter)
      case ImageUrlValidationRequestMessage(url) =>
        countPerSecond(counterCategory, "message.type.image.received")
        val response = validateImageUrl(url)
        w.reply(response, responseConverter)
      case unknownMessage =>
        countPerSecond(counterCategory, "message.type.unknown.received")
        val unknownMessageType = unknownMessage.getClass.getCanonicalName
        warn("Unknown message type received: {0}", unknownMessageType)
        w.errorReply("Invalid message type received! Currently only support UrlValidationRequestMessage. Type received: " + unknownMessageType)
    }

  }
}

class UrlValidationComponent extends ServerComponent[UrlValidationComponentActor](componentName = "URL Validation",
  messageTypes = Seq(classOf[UrlValidationRequestMessage], classOf[ImageUrlValidationRequestMessage]),
  messageConverters = Seq(UrlValidationServer.requestConverter, UrlValidationServer.requestImageConverter).flatten,
  numActors = 60, numThreads = 60)

object UrlValidationCacher extends SingletonCache[UrlValidationResponse] {
  import UrlValidationServer.counterCategory
  import com.gravity.utilities.Counters._
  import com.gravity.logging.Logging._

  val ttlSecondsForSuccess: Int = if (Settings.isProductionServer) 120 else 2
  val ttlSecondsForFailure: Int = if (Settings.isProductionServer) 10 else 1

  override def cacheName: String = "gen-url-validation"

  private def failedResponse(msg: String): UrlValidationResponse = {
    UrlValidationResponse(None, Some(msg), returnedFromCache = true)
  }

  private def successfulResponse(code: Int): UrlValidationResponse = {
    UrlValidationResponse(Some(code), None, returnedFromCache = true)
  }

  def get(url: String, validateImage: Boolean = false): UrlValidationResponse = {
    cache.getItem(url) match {
      case Some(response) =>
        countPerSecond(counterCategory, "response.cache.hit")
        trace("++ cache hit for: {0}", url)
        response

      case None =>
        countPerSecond(counterCategory, "response.cache.miss")
        trace(".. cache MISS for: {0}", url)

        if (validateImage) {
          countPerSecond(counterCategory, "image.validations.attempted")
          try {
            URLUtils.validateUrlWithHttpGetRequest(url) {
              case result: HttpResultStreamLite =>
                if(result.status != 200) {
                  FailureResult(s"Got non-200 status ${result.status} for image url: '$url'").failureNel
                }
                else {
                  result.responseStream match {
                    case None => FailureResult(s"Got no response for url: '$url'").failureNel
                    case Some(inputStream) =>
                      Image.fromStream(inputStream)
                      unitSuccessNel
                  }
                }
            } match {
              case Success(_) =>
                countPerSecond(counterCategory, "image.validations.succeeded")
                val response = UrlValidationResponse.okResponseFromCache
                cache.putItem(url, response, ttlSecondsForSuccess)

                response

              case Failure(fails) =>
                countPerSecond(counterCategory, "image.validations.failed")
                val response = failedResponse(fails.list.map(_.messageWithExceptionInfo).mkString(" :: AND :: "))
                cache.putItem(url, response, ttlSecondsForFailure)
                response

            }
          } catch {
            case ex: IOException =>
              countPerSecond(counterCategory, "image.validations.failed")
              val response = failedResponse(s"Failed to fetch url: '$url' due to Exception: ${ScalaMagic.formatException(ex)}")
              cache.putItem(url, response, ttlSecondsForFailure)
              response
            case ex: Exception =>
              countPerSecond(counterCategory, "image.validations.failed")
              val response = failedResponse(s"Failed to validate url: '$url' (possible image corruption) due to Exception: ${ScalaMagic.formatException(ex)}")
              cache.putItem(url, response, ttlSecondsForFailure)
              response
          }
        }
        else {
          countPerSecond(counterCategory, "url.validations.attempted")
          URLUtils.validateUrlWithHttpHeadRequest(url) match {
            case Success(result) =>
              countPerSecond(counterCategory, "url.validations.succeeded")
              val response = successfulResponse(result.statusCode)
              cache.putItem(url, response, ttlSecondsForSuccess)

              response

            case Failure(fails) =>
              countPerSecond(counterCategory, "url.validations.failed")
              val response = failedResponse(fails.list.map(_.messageWithExceptionInfo).mkString(" :: AND :: "))
              cache.putItem(url, response, ttlSecondsForFailure)
              response
          }
        }
    }
  }
}

object UrlValidationFailureCountCacher extends SingletonCache[Integer] {

  val ttlSeconds: Int = UrlValidationServer.maxFailuresWithinSeconds

  private val zero = new Integer(0)

  override def cacheName: String = "gen-url-validation-failure-count"

  def updateAndGet(url: String, response: UrlValidationResponse): Int = {
    if (response.isFailure) {
      val failCount = getOrUpdate(url, {zero}, ttlSeconds) + 1
      if (failCount > UrlValidationServer.maxFailuresAllowed) {
        cache.removeItem(url)
      }
      else {
        cache.putItem(url, failCount, ttlSeconds)
      }
      failCount
    }
    else {
      cache.removeItem(url)
      0
    }
  }
}
