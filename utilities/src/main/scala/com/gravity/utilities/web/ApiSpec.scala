package com.gravity.utilities.web

import com.gravity.logging.Logstashable
import com.gravity.utilities._
import com.gravity.utilities.grvstrings._
import com.gravity.utilities.web.http._
import org.joda.time.DateTime
import org.scalatra._
import play.api.libs.json.Writes

/**
 * @param enabled If set to FALSE, then requests must contain the param "cache=1" to cause caching.
 */
case class CachingSpec(
  cacheForMinutes: Int,
  includeDayOfYearInKey: Boolean = false,
  persistToDisk: Boolean = false,
  enabled: Boolean = true)

object NoCaching extends CachingSpec(0)

object GrvResponseHeaders {
  val rolesToRespondWithHostHeader = List("INTEREST_INTELLIGENCE", "DEVELOPMENT")
}

/** Strongly consider using [[TypedApiSpec]] instead of this. */
abstract class ApiSpec(
  val name: String,
  val route: String,
  val methods: List[HttpMethod] = Get :: Nil,
  val alternateRoutes: List[String] = Nil,
  val description: String = "Pending",
  val exampleResponse: ApiResponse = null,
  val live: Boolean = false,
  val caching: CachingSpec = NoCaching) extends ApiSpecBase {
  override type ExampleResponseType = ApiResponse
  override implicit def exampleResponseSerializer: Writes[ExampleResponseType] = ApiResponse.jsonWrites

  val cacheName: String = route

  def cacheKeyForRequest(request: ApiRequest): String = {
    var key = paramSpecs map (_.getOrDefault(request).toString) mkString ","
    if (caching.includeDayOfYearInKey) {
      key = key + new DateTime().getDayOfYear.toString
    }
    key
  }

  def wrappedRequest(req: ApiRequest): ApiResponse = {
    val tr = grvtime.timedOperation(request(req))

    val resp = tr.result

    resp.status.executionMillis = tr.duration
    resp.status.executionTime = tr.formattedDuration

    resp
  }

  def isMobile(req: ApiRequest): Boolean = req.request.isMobile

  def request(req: ApiRequest): ApiResponse
}

case class SlowApiResponse(placementId: String, dur: Long, url: String) extends Logstashable {
  import com.gravity.logging.Logstashable._
  override def getKVs: Seq[(String, String)] = Seq(SitePlacementId -> placementId, Duration -> dur.toString, Url -> url)
}

case class RegisteredApiSpec(servlet: ApiServletBase, spec: ApiSpecBase) {
  def routeFromServletMapping: String = servlet.firstUrlMappingAsServletPath + spec.swaggerRoute
  def routeFromContextPath: String = servlet.servletContext.getContextPath + routeFromServletMapping

  def servletDisplayName: String = servlet.getClass.getCanonicalName.takeRightUntil(_ == '.')
}

object ApiRegistry {
  val apiSpecs: GrvConcurrentSet[RegisteredApiSpec] = new GrvConcurrentSet[RegisteredApiSpec]()
  val publicApiSpecs: GrvConcurrentSet[RegisteredApiSpec] = new GrvConcurrentSet[RegisteredApiSpec]()

  // These are public versioned APIs
  val apiSpecsV0: GrvConcurrentSet[RegisteredApiSpec] = new GrvConcurrentSet[RegisteredApiSpec]()
  val apiSpecsV1: GrvConcurrentSet[RegisteredApiSpec] = new GrvConcurrentSet[RegisteredApiSpec]()
}