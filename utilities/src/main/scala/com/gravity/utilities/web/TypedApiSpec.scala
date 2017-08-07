package com.gravity.utilities.web

import com.gravity.utilities.grvstrings._
import com.gravity.utilities.grvtime
import com.gravity.valueclasses.ValueClassesForUtilities._
import org.joda.time.DateTime
import org.scalatra.{Get, HttpMethod}
import play.api.libs.json.{Json, Writes}

import scalaz.NonEmptyList
import scalaz.syntax.std.option._
/*
              ___...---''
  ___...---'\'___
''       _.-''  _`'.______\\.
        /_.)  )..-  __..--'\\
       (    __..--''
        '-''\@


 Ⓐ Ⓐ Ⓐ Ⓐ Ⓐ Ⓐ Ⓐ Ⓐ Ⓐ Ⓐ Ⓐ
*/

abstract class TypedApiSpec(
  val name: String,
  val route: String,
  val methods: List[HttpMethod] = List(Get),
  val description: String = emptyString,
  val exampleResponse: Option[TypedApiSuccessJsonResponse[_]] = None,
  val caching: CachingSpec = NoCaching,
  val isPublicApi: Boolean = false
) extends ApiSpecBase with ValidationCategoryFormatApiSpec {
 import com.gravity.logging.Logging._
 import com.gravity.utilities.Counters._

  override type ExampleResponseType = Option[TypedApiSuccessJsonResponse[_]]
  override implicit def exampleResponseSerializer: Writes[ExampleResponseType] = Writes.optionWithNull[TypedApiSuccessJsonResponse[_]](
    Writes[TypedApiSuccessJsonResponse[_]](resp => Json.toJson(resp.bodyJsValue))
  )

  val cacheName: String = route

  def cacheKeyForRequest(implicit req: ApiRequest): String = {
    var key = paramSpecs map (_.getOrDefault(req).toString) mkString ","
    if (caching.includeDayOfYearInKey) {
      key = key + new DateTime().getDayOfYear.toString
    }
    key
  }

  def handle(implicit req: ApiRequest): TypedApiResponse

  def requestReceivedTime(implicit req: ApiRequest): Option[Millis] = req.requestTimestamp.some

  def requestClientTime(implicit req: ApiRequest): Option[Millis] = req.get("clientTime").flatMap(_.tryToLong).map(_.asMillis)

  def requestedQueryString(implicit req: ApiRequest): String = Option(req.request.getQueryString) match {
    case Some(qs) => if(qs.startsWith("?") || qs.startsWith("&")) qs.substring(1) else qs
    case None => emptyString
  }

  final def timedHandle(implicit req: ApiRequest): TypedApiResponse = {
    val timedResponse = grvtime.timedOperation(handle(req))
    val resp = timedResponse.result
    resp.status.executionMillis = timedResponse.duration
    resp.status.executionTime = timedResponse.formattedDuration
    logSlowRequest(timedResponse.duration)
    resp
  }

  private val warnThresholdMillis = 500
  private val responseTimeThresholds = NonEmptyList(-1, 100, 200, 300, 400, 500, 1000, 2000, 5000, 10000)
  private val slowResponseCtrGroup = "Slow Responses" //keep
  private def logSlowRequest(durationMillis: Long)(implicit req: ApiRequest) = {
    val placementIdOpt = Option(req.request.getParameter("placement")) orElse Option(req.request.getParameter("configId"))
    for(placementId <- placementIdOpt if durationMillis > warnThresholdMillis) {
      val url = Option(req.request.getQueryString) match {
        case Some(queryString) if queryString.nonEmpty => req.request.getRequestURL.append('?').append(queryString).toString
        case _ => req.request.getRequestURL.toString
      }
      warn(SlowApiResponse(placementId, durationMillis, url))
    }

    for(thr <- responseTimeThresholds) {
      val thrDispl = thr.formatted("%05d")

      if(durationMillis > thr) {
        countPerSecond(slowResponseCtrGroup, s"${placementIdOpt.getOrElse("NONE")} - API response time > ${thrDispl}ms")
        countPerSecond(slowResponseCtrGroup, s"API response time > ${thrDispl}ms")
      }
    }
  }
}