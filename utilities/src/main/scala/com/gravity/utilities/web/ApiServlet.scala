package com.gravity.utilities.web

import java.net.URLDecoder
import java.util.UUID
import javax.servlet.ServletContext
import javax.servlet.http.HttpServletRequest

import com.gravity.utilities.cache.EhCacher
import com.gravity.utilities.components.FailureResult
import com.gravity.utilities.grvcoll._
import com.gravity.utilities.grvjson.jsonStr
import com.gravity.utilities.grvstrings._
import com.gravity.utilities.grvz._
import com.gravity.utilities.swagger.adapter._
import com.gravity.utilities.web.MimeTypes.MimeType
import com.gravity.utilities.web.http._
import com.gravity.utilities.{grvjson, _}
import com.gravity.valueclasses.ValueClassesForUtilities._
import net.liftweb.json.Extraction.decompose
import net.liftweb.json._
import org.scalatra._
import org.scalatra.auth.Scentry
import play.api.libs.json.{JsValue, Json, Writes}

import scala.Seq
import scala.collection._
import scala.xml.NodeSeq
import scalaz.syntax.std.option._
import scalaz.{NonEmptyList, ValidationNel}

/** @param authenticatedUser The finer type of the AnyRef is determined by the scalatra auth strategy used (if any). */
case class ApiRequest(request: HttpServletRequest, origParams: Map[String, String], servletContext: ServletContext,
                      requestTimestamp: Millis, authenticatedUser: Option[AnyRef] = None) {
  private val params = origParams.map((kv: (String, String)) => (kv._1.toLowerCase, kv._2))

  def get(key: String): Option[String] = params.get(key.toLowerCase) orElse Option(request.getHeader(key))

  def contains(key: String): Boolean = params.contains(key.toLowerCase) || Option(request.getHeader(key)).isDefined
}

@SerialVersionUID(1)
case class ResponseStatus(httpCode: Int, message: String, var executionTime: String = "", var executionMillis: Long = 0l)

object ResponseStatus {
  implicit val jsonWrites: Writes[ResponseStatus] = Json.writes[ResponseStatus]

  val okStatus = 200
  val badRequestStatus = 400
  val errorStatus = 500

  val OK: ResponseStatus = createOK()
  val Created: ResponseStatus = createCreated()
  val NoContent: ResponseStatus = createNoContent()
  val Deprecated: ResponseStatus = createDeprecated()
  val NotModified: ResponseStatus = createNotModified()
  val Error: ResponseStatus = createError()
  val BadRequest: ResponseStatus = createBadRequest()
  val BadGateway: ResponseStatus = createBadGateway()
  val Unauthorized: ResponseStatus = createUnauthorized()
  val Forbidden: ResponseStatus = createForbidden()
  val Unsecure: ResponseStatus = Unauthorized
  val NotFound: ResponseStatus = createNotFound()
  val Conflict: ResponseStatus = createConflict()
  val NotImplemented: ResponseStatus = createNotImplemented()
  val MaintenanceMode: ResponseStatus = createOK("System undergoing maintenance. Please try again later.")
  val Redirect: ResponseStatus = createRedirect()

  def createOK(msg: String = "OK"): ResponseStatus = ResponseStatus(okStatus, msg)
  def createCreated(msg: String = "Created"): ResponseStatus = ResponseStatus(201, msg)
  def createNoContent(msg: String = "No Content"): ResponseStatus = ResponseStatus(204, msg)
  def createDeprecated(msg: String = "Deprecated"): ResponseStatus = ResponseStatus(301, msg)
  def createRedirect(msg: String = "Redirect"): ResponseStatus = ResponseStatus(302, msg)
  def createNotModified(msg: String = "Not Modified"): ResponseStatus = ResponseStatus(304, msg)
  def createBadRequest(msg: String = "Bad Request"): ResponseStatus = ResponseStatus(badRequestStatus, msg)
  def createUnauthorized(msg: String = "Unauthorized"): ResponseStatus = ResponseStatus(401, msg)
  def createForbidden(msg: String = "Forbidden"): ResponseStatus = ResponseStatus(403, msg)
  def createNotFound(msg: String = "Not Found"): ResponseStatus = ResponseStatus(404, msg)
  def createConflict(msg: String = "Conflict"): ResponseStatus = ResponseStatus(409, msg)
  def createError(msg: String = "Unknown error occurred"): ResponseStatus = ResponseStatus(errorStatus, msg)
  def createNotImplemented(msg: String = "Not Implemented"): ResponseStatus = ResponseStatus(501, msg)
  def createBadGateway(msg: String = "Bad Gateway"): ResponseStatus = ResponseStatus(502, msg)
}

object ApiExceptionResponse {
  def apply(throwable: Throwable) : ApiExceptionResponse = new ApiExceptionResponse(throwable)
}

@SerialVersionUID(1)
class ApiExceptionResponse(val throwable: Throwable) extends ApiResponse(throwable.getLocalizedMessage + "<br>" + throwable.getStackTrace.mkString("", "\n", "\n"), ResponseStatus.Error)

/**
 * @param meta            Field -> value pairs that do not belong in payload. If none, this field is not rendered into
 *                        the response at all.
 * @param warning         Field -> value pairs that do not belong in payload. If none, this field is not rendered into
 *                        the response at all.
 * @param headersToAppend Response name -> value headers to send.
 */
@SerialVersionUID(1)
case class ApiResponse(payload: Any, status: ResponseStatus = ResponseStatus.OK,
                       meta: Map[String, Any] = Map.empty[String, Any],
                       headersToAppend: Map[String, String] = Map.empty[String, String],
                       warning: Option[Any] = None) {
  def fieldValuesToSerialize: Map[String, Any] = Map(
    "payload" -> payload,
    "status" -> status
  ) ++ (meta.size match {
    case 0 => Nil
    case _ => Map("meta" -> meta)
  }) ++ (warning match {
    case Some(thing) => Map("warning" -> thing)
    case _ => Nil
  })
}

object ApiResponseJsonSerializer extends Serializer[ApiResponse] {
  def deserialize(implicit format: Formats): PartialFunction[(TypeInfo, JValue), ApiResponse] =
    throw new MappingException("You can't deserialize an ApiResponse.")

  def serialize(implicit format: Formats): PartialFunction[Any, JValue] = {
    case resp: ApiResponse => JObject(resp.fieldValuesToSerialize.toList.map {
      case (k, v) => JField(k, Extraction.decompose(v))
    })
  }
}

object ApiResponse {
  def ok(payload: Any = null, msg: String = ResponseStatus.OK.message): ApiResponse = ApiResponse(payload, ResponseStatus.createOK(msg))
  def created(payload: Any = null, msg: String = ResponseStatus.Created.message): ApiResponse = ApiResponse(payload, ResponseStatus.createCreated(msg))
  def noContent(payload: Any = null, msg: String = ResponseStatus.NoContent.message): ApiResponse = ApiResponse(payload, ResponseStatus.createNoContent(msg))
  def deprecated(payload: Any = null, msg: String = ResponseStatus.Deprecated.message): ApiResponse = ApiResponse(payload, ResponseStatus.createDeprecated(msg))
  def notModified(payload: Any = null, msg: String = ResponseStatus.NotModified.message): ApiResponse = ApiResponse(payload, ResponseStatus.createNotModified(msg))
  def badRequest(payload: Any = null, msg: String = ResponseStatus.BadRequest.message): ApiResponse = ApiResponse(payload, ResponseStatus.createBadRequest(msg))
  def unauthorized(payload: Any = null, msg: String = ResponseStatus.Unauthorized.message): ApiResponse = ApiResponse(payload, ResponseStatus.createUnauthorized(msg))
  def forbidden(payload: Any = null, msg: String = ResponseStatus.Forbidden.message): ApiResponse = ApiResponse(payload, ResponseStatus.createForbidden(msg))
  def notFound(payload: Any = null, msg: String = ResponseStatus.NotFound.message): ApiResponse = ApiResponse(payload, ResponseStatus.createNotFound(msg))
  def conflict(payload: Any = null, msg: String = ResponseStatus.Conflict.message): ApiResponse = ApiResponse(payload, ResponseStatus.createConflict(msg))
  def error(payload: Any = null, msg: String = ResponseStatus.Error.message, ex: Throwable = null): ApiResponse = {
    val finalMsg = if (ex != null) {
      "%s: Caused by the following exception:%n%s".format(msg, ScalaMagic.formatException(ex))
    } else msg

    ApiResponse(payload, ResponseStatus.createError(finalMsg))
  }
  def error(fails: NonEmptyList[FailureResult]): ApiResponse = error(msg = fails.list.map(_.message).mkString(" AND "))
  def notImplemented(payload: Any = null, msg: String = ResponseStatus.NotImplemented.message): ApiResponse = ApiResponse(payload, ResponseStatus.createNotImplemented(msg))
  def badGateway(payload: Any = null, msg: String = ResponseStatus.BadGateway.message): ApiResponse = ApiResponse(payload, ResponseStatus.createBadGateway(msg))

  def failed(failure: FailureResult): ApiResponse = failure.exceptionOption match {
    case Some(ex) => error(msg = failure.toString)
    case None => notFound(msg = failure.message)
  }

  def failed(fails: NonEmptyList[FailureResult]): ApiResponse = error(msg = fails.list.map(_.toString).mkString("\n"))

  implicit def ValidationNelToApiResponse[T <: Any](v: ValidationNel[FailureResult, T]): ApiResponse = v.fold(ApiResponse.failed, ApiResponse.ok(_))

  implicit val jsonWrites: Writes[ApiResponse] = Writes[ApiResponse](resp => Json.parse(ApiServlet.serializeToJson(resp)))
}

case class ApiError(error: ApiErrorPayload)

case class ApiErrorPayload(message: String, code: Int)

case class ExampleContent(text: String)

object MurmurHashApi extends ApiSpec(name = "MurmurHash A String", route = "/utils/murmurhash", methods = Get :: Post :: Nil, description = "Hashes specified string 'input' into a 64bit integer (Long) using the Murmur 64bit Hashing Alogorithm") {
  val inputParam: ApiParam[String] = &(ApiStringParam(key = "input", required = true, description = "The string you want me to hash"))

  def request(req: ApiRequest): ApiResponse = {
    val input = URLDecoder.decode(inputParam.get(req), "UTF-8")
    val result = MurmurHash.hash64(input)
    ApiResponse(Map("input" -> input, "hash" -> result, "hashstring" -> result.toString))
  }
}

trait ApiResponseSerializer {
  def serializeToJson(resp: Any, isPretty: Boolean = false, callback: Option[String] = None): String
  def serializeToXml(resp: Any): NodeSeq
}

object ApiServlet extends ApiResponseSerializer {

  private val __serializerMap = new mutable.HashMap[Class[_], Serializer[_]]

  def registerSerializer(serializer: Serializer[_]) {
    __serializerMap.put(serializer.getClass, serializer)
  }

  private def getSerializers: List[Serializer[_]] = grvjson.baseSerializers ++
    List(grvjson.MapSerializer, ApiResponseJsonSerializer) ++ __serializerMap.values

  def getFormats: Formats = new Formats {
    val dateFormat: Object with DateFormat = DefaultFormats.dateFormat
    override val typeHints: TypeHints = DefaultFormats.typeHints
    override val customSerializers: List[Serializer[_]] = getSerializers
  }

  def getApiJsonParamFormats: Formats = new Formats {
    val dateFormat: Object with DateFormat = DefaultFormats.dateFormat
    override val typeHints: TypeHints = DefaultFormats.typeHints
    override val customSerializers: List[Serializer[_]] = getSerializers.filter(_ != ApiResponseJsonSerializer)
  }

  def createError(message: String, code: Int): ApiError = new ApiError(new ApiErrorPayload(message, code))

  override def serializeToJson(value: Any, isPretty: Boolean = false, callback: Option[String] = None): String = grvjson.serialize(value, isPretty, callback)(getFormats)

  def deserializeJson(jsonString: String): JValue = grvjson.deserialize(jsonString)

  def serializeToXml(value: Any): NodeSeq = Xml.toXml(decompose(value)(getFormats))

  def swaggerJsonUrlThisServer(implicit req: HttpServletRequest): String = req.getRequestUrlToContextPath + "/api/help/swagger.json"

  def generateUuidVersion4: String = {
    // The AOL Spec states UUID v1 should be used for it's date/time tracking.
    // For now I'm using v4 b/c that is what we get from java.util.UUID for free.
    val uuid = UUID.randomUUID()
    uuid.toString
  }
}

trait ApiServletBase extends GravityScalatraServlet with GravityHeaders {
 import com.gravity.logging.Logging._
 import com.gravity.utilities.Counters._
  val counterCategory = "API" //keep

  get("/help/public/swagger.json") {
    contentType = MimeTypes.Json.contentType

    val swagger = buildSwagger(ApiRegistry.publicApiSpecs)

    jsonStr(swagger)
  }

  get("/help/swagger.json") {
    contentType = MimeTypes.Json.contentType

    val swagger = buildSwagger(ApiRegistry.apiSpecs)

    jsonStr(swagger)
  }

  get("/help/v0/swagger.json") {
    contentType = MimeTypes.Json.contentType

    val swagger = buildSwagger(ApiRegistry.apiSpecsV0)

    jsonStr(swagger)
  }

  get("/help/v1/swagger.json") {
    contentType = MimeTypes.Json.contentType

    val swagger = buildSwagger(ApiRegistry.apiSpecsV1)

    jsonStr(swagger)
  }

  private def buildSwagger(apiSpecs: GrvConcurrentSet[RegisteredApiSpec]): Swagger = {
    Swagger(
      info = Info(
        title = "Gravity API",
        termsOfService = "http://www.gravity.com/terms-of-service/".some,
        contact = Contact("Gravity Support", "http://www.gravity.com/", "support@gravity.com").some,
        version = "edge"
      ),
      basePath = contextPath.some,
      paths = {
        val routesToPathItems = apiSpecs.toSeq.sortBy(_.servletDisplayName).map({
          case registeredSpec @ RegisteredApiSpec(servlet, spec) =>
            registeredSpec.routeFromServletMapping -> spec.swaggerPathItem(
              revealClassPaths = request.requestedWithinVpn,
              operationTags = Seq(registeredSpec.servletDisplayName)
            )
        }).groupBy({ // Group the HTTP verbs
          case (route, _) => route
        })

        // Combine path items
        routesToPathItems.mapValues(_.foldLeft[PathItem](PathItem.empty)(_ ++ _._2)).sortByKeys.toImmutableSortedMap
      },
      consumes = Seq(MimeTypes.WwwFormUrlEncoded.contentType),
      produces = Seq(MimeTypes.Json.contentType),
      parameters = DashboardApiGlobalParams.params.map(paramSpec => {
        paramSpec.paramKey -> paramSpec.swaggerParameter(ParameterIn.header)
      }).toMap
    )
  }

  def responseFormat: MimeType with Product with Serializable = if (params.contains("forcexml")) MimeTypes.Xml else MimeTypes.Json

  def isPretty: Boolean = params.contains("pretty")

  def isCacheBypassEnabled: Boolean = params.contains("bypass_cache")

  def isPopCacheRequested: Boolean = params.contains("ifid")

  /**
   * Modified time in seconds of data an API depends on. For APIs that cache responses, the cached response will be
   * considered stale (and thus regenerated) if the provided mtime is greater than the cached item creation time.
   */
  def apiCacheDependentDataMtime: Option[Long] = params.getLong("apiCacheDependentDataMtime")

  //this query parameter controls whether the result is rendered as json or jsonp
  def jsonpCallbackOption: Option[String] = params.get("c").orElse(params.get("callback"))

  def createError(message: String, code: Int): ApiError = ApiServlet.createError(message, code)

  def renderPlayJson(value: JsValue): String = Json.stringify(value)

  def renderJson(value: Any) {
    renderResponse(ApiServlet.serializeToJson(value, isPretty, jsonpCallbackOption))
  }

  def renderXml(value: Any) {
    renderResponse(ApiServlet.serializeToXml(value))
  }

  def getHeaderSafe(name: String): Option[String] = {
    val requestHeader = request.getHeader(name)
    Option(requestHeader)
  }

  def useCache(spec: ApiSpec): Boolean = spec.caching.cacheForMinutes > 0 && !isCacheBypassEnabled &&
    (spec.caching.enabled || Option(this.request.getParameter("cache")).flatMap(_.tryToBoolean).getOrElse(false))

  def registerApi(spec: ApiSpec) {
    try {
      ApiRegistry.apiSpecs += RegisteredApiSpec(this, spec)

      for (method <- spec.methods) {
        val methodFn = method match {
            //Add OPTIONS here?
          case Get => get _
          case Post => post _
          case Put => put _
          case Delete => delete _
          case _ => throw new IllegalArgumentException(method.toString)
        }

        methodFn(Seq(string2RouteMatcher(spec.route))) {
          execute
        }

        for (route <- spec.alternateRoutes) {
          methodFn(Seq(string2RouteMatcher(route))) {
            execute
          }
        }
      }
    } catch {
      case ex:Exception =>
        ScalaMagic.printException("Unable to register Api Spec " + spec.route,ex)
    }

    def execute: Any = {
      val doCache = useCache(spec)
      trace("Using API cache: " + (if(doCache) spec.caching.toString else "no"))
      countPerSecond(counterCategory, spec.route + " : Request")

      implicit val servletRequest = this.request
      implicit val servletReponse = this.response

      val authenticatedUser: Option[AnyRef] = servletRequest.get(Scentry.ScentryRequestKey).flatMap {
        case scentry: Scentry[_] => scentry.userOption
        case _ => None
      }

      val request = ApiRequest(servletRequest, params, servletContext, System.currentTimeMillis.asMillis, authenticatedUser)

      val results = spec.validate(request)

      val response = if (results.nonEmpty) {
        countPerSecond(counterCategory, spec.route + " : Invalid Requests")
        ApiResponse.badRequest(Map("errors" -> results))
      } else {
        //We've validated the result, now try to get the result from cache if it's available.
        val cache = if (doCache) {
          EhCacher.getOrCreateCache[String,ApiResponse](spec.cacheName, spec.caching.cacheForMinutes * 60, spec.caching.persistToDisk)
        } else {
          null
        }

        val cacheKey = if(doCache) spec.cacheKeyForRequest(request) else null

        if(cache != null && cacheKey != null) {
          val weBePoppingCacheYo = isPopCacheRequested
          val itemOption = if (weBePoppingCacheYo) {
            cache.removeItem(cacheKey)
            None
          } else {
            try {
              apiCacheDependentDataMtime match {
                case Some(mtime) => cache.getItemIfNotModified(cacheKey, mtime)
                case _ => cache.getItem(cacheKey)
              }
            }
            catch {
              case ex: Exception =>
                printf("Exception thrown from cache get so we will now remove this item! Spec: %s : key : %s%n%s%n", spec.cacheName, cacheKey, ScalaMagic.formatException(ex))
                cache.removeItem(cacheKey)
                None
            }
          }

          itemOption match {
            case Some(apiResp) =>

              countPerSecond(counterCategory, spec.route + " : Cache Hits")
              //              println("Cache hit: Spec: " + spec.cacheName + " : key : " + cacheKey)
              apiResp
            case None =>

              countPerSecond(counterCategory, spec.route + " : Cache Misses")
              val apiResp = spec.wrappedRequest(request)
//              val action = if (weBePoppingCacheYo) "remove" else "miss"
              //              printf("Cache %s: Spec: %s : key : %s%n", action, spec.cacheName, cacheKey)
              if(apiResp.status.httpCode == 200 && apiResp.payload != null) {
//                println("Cache put: Spec: " + spec.cacheName + " : key : " + cacheKey)
                cache.putItem(cacheKey,apiResp)
              }
              apiResp
          }
        }else {
          spec.wrappedRequest(request)
        }

      }
      countPerSecond(counterCategory, spec.route + " : Response Code : " + response.status.httpCode)
      renderResponse(response)
    }
  }

  def registerVersionDispatcher(spec: StandardApiVersionDispatcher): Unit = {
    spec.apiVersionToSpec.foreach { case (apiVersion, apiSpec) =>
      apiVersion match {
        case ApiVersion0_0 => ApiRegistry.apiSpecsV0 += RegisteredApiSpec(this, apiSpec)
        case ApiVersion1_0 => ApiRegistry.apiSpecsV1 += RegisteredApiSpec(this, apiSpec)
      }
    }

    try {
      for(method <- spec.methods) {
        val methodFn = method match {
          case Get => get _
          case Post => post _
          case Put => put _
          case Delete => delete _
          case _ => throw new IllegalArgumentException(s"Invalid method $method.")
        }

        methodFn(Seq(string2RouteMatcher(spec.route))) {
          if(spec.requireSsl && !request.isSecure && !request.requestedWithinVpn)
            halt(403, ApiSpecBase.sslRequiredResponseBody, ApiSpecBase.sslRequiredResponseHeaders)

          execute(spec)
        }
      }
    } catch {
      case ex: Exception =>
        warn(ex, s"Unable to register Api Spec ${spec.route}")
    }

    def execute(spec: StandardApiVersionDispatcher): String = {
      countPerSecond(counterCategory, spec.route + " : Versioned Request")

      val requestId = getHeaderSafe("Request-ID").orElse(Some(ApiServlet.generateUuidVersion4))

      implicit val req = ApiRequest(request, params, servletContext, System.currentTimeMillis.asMillis, None)
      val apiResponse = spec.handle(req)

      countPerSecond(counterCategory, spec.route + " : Response Code : " + apiResponse.status)

      this.response.setContentType(
        (
          if (jsonpCallbackOption.isDefined) MimeTypes.Js
          else MimeTypes.Json
        ).contentType
      )
      this.response.setStatus(apiResponse.status)

      requestId.foreach(this.response.addHeader("Request-ID", _))

      for((name, value) <- apiResponse.headersToAppend)
        this.response.addHeader(name, value)

      if(apiResponse.status == 204)
        emptyString
      else {
        val responseBodyStr = Json.stringify(apiResponse.body)

        val etag = HashUtils.md5(responseBodyStr)
        this.response.addHeader("ETag", etag)

        jsonpCallbackOption.fold(responseBodyStr)(cb => s"$cb($responseBodyStr);")
      }
    }
  }

  def useCache(spec: TypedApiSpec): Boolean = spec.caching.cacheForMinutes > 0 && !isCacheBypassEnabled &&
    (spec.caching.enabled || Option(this.request.getParameter("cache")).flatMap(_.tryToBoolean).getOrElse(false))

  def registerTypedApi(spec: TypedApiSpec, corsAll: Boolean = false) {
    ApiRegistry.apiSpecs += RegisteredApiSpec(this, spec)
    if (spec.isPublicApi) ApiRegistry.publicApiSpecs += RegisteredApiSpec(this, spec)

    try {
      for(method <- spec.methods) {
        val methodFn = method match {
          case Get => get _
          case Post => post _
          case Put => put _
          case Delete => delete _
          case _ => throw new IllegalArgumentException(s"Invalid method $method.")
        }

        methodFn(Seq(string2RouteMatcher(spec.route))) {
          execute(spec, corsAll)
        }
      }
    } catch {
      case ex: Exception =>
        warn(ex, s"Unable to register Api Spec ${spec.route}")
    }
  }

  private def execute(spec: TypedApiSpec, corsAll: Boolean = false): String = {
    countPerSecond(counterCategory, spec.route + " : Request")
    countPerSecond("ApiByName", spec.name)

    val authenticatedUser = request.get(Scentry.ScentryRequestKey).flatMap {
      case scentry: Scentry[_] => scentry.userOption
      case _ => None
    }

    implicit val req = ApiRequest(request, params, servletContext, System.currentTimeMillis.asMillis, authenticatedUser)

    val paramValidationFailures = spec.validate(req)
    val apiResponse = paramValidationFailures.toNel match {
      case Some(failures) =>
        countPerSecond(counterCategory, spec.route + " : Invalid Requests")
        TypedApiFailureJsonResponse(failures, ResponseStatus.BadRequest.some)

      case None =>
        val doCache = useCache(spec)

        // We've validated the request, now try to get the result from cache if it's available.
        val cache = if (doCache) {
          EhCacher.getOrCreateCache [String, TypedApiResponse](spec.cacheName, spec.caching.cacheForMinutes * 60, spec.caching.persistToDisk)
        } else {
          null
        }

        val cacheKey = if(doCache) spec.cacheKeyForRequest(req) else null

        if (cache != null && cacheKey != null) {
          val popTheCache = isPopCacheRequested

          val itemOption = if (popTheCache) {
            cache.removeItem(cacheKey)
            None
          } else {
            try {
              apiCacheDependentDataMtime match {
                case Some(mtime) => cache.getItemIfNotModified(cacheKey, mtime)
                case _ => cache.getItem(cacheKey)
              }
            }
            catch {
              case ex: Exception =>
                printf("Exception thrown from cache get so we will now remove this item! Spec: %s : key : %s%n%s%n",
                  spec.cacheName, cacheKey, ScalaMagic.formatException(ex))
                cache.removeItem(cacheKey)
                None
            }
          }

          itemOption match {
            case Some(apiResp) =>
              countPerSecond(counterCategory, spec.route + " : Cache Hits")
              apiResp
            case None =>
              countPerSecond(counterCategory, spec.route + " : Cache Misses")
              val apiResp = spec.timedHandle(req)
              if (apiResp.status.httpCode == 200 && apiResp.body != null) {
                cache.putItem(cacheKey, apiResp)
              }
              apiResp
          }
        } else {
          spec.timedHandle(req)
        }
    }
    countPerSecond(counterCategory, spec.route + " : Response Code : " + apiResponse.status.httpCode)

    this.response.setContentType(jsonpCallbackOption.fold(apiResponse.body.contentType)(_ => MimeTypes.Js).contentType)
    this.response.setStatus(apiResponse.status.httpCode)

    if(corsAll)
      this.response.addHeader("Access-Control-Allow-Origin", "*")

    for((name, value) <- apiResponse.headersToAppend)
      this.response.addHeader(name, value)

    if(apiResponse.status.httpCode == 204)
      emptyString
    else {
      val responseBodyStr = apiResponse.body.serialized
      jsonpCallbackOption.fold(responseBodyStr)(cb => s"$cb($responseBodyStr);")
    }
  }

  private[utilities] def string2RouteMatcherMadeAvailableToTests(apiRoute: String): Seq[RouteMatcher] = {
    Seq(string2RouteMatcher(apiRoute))
  }

  protected def renderResponse(resp: ApiResponse): Any = {
    this.response.setContentType(this.responseFormat.contentType)
    this.response.setStatus(resp.status.httpCode)
    for((name, value) <- resp.headersToAppend)
      this.response.addHeader(name, value)
    this.responseFormat match {
      case MimeTypes.Json => ApiServlet.serializeToJson(resp, this.isPretty, jsonpCallbackOption)
      case MimeTypes.Xml => ApiServlet.serializeToXml(resp)
    }
  }
}