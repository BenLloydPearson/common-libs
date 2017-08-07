package com.gravity.utilities.api

import java.io.File
import javax.servlet.Servlet
import javax.servlet.http.HttpServlet

import com.gravity.test.utilitiesTesting
import com.gravity.utilities.grvz._
import com.gravity.utilities.web._
import com.gravity.utilities.web.http.GravityHttpServletRequest
import com.gravity.utilities.{BaseScalaTest, grvjson}
import com.gravity.valueclasses.ValueClassesForUtilities._
import org.fusesource.scalate.util.FileResourceLoader
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatra.test.ScalatraTests
import org.scalatra.{Delete, Get, Post, Put}
import play.api.libs.json.{Format, Json}

import scala.collection._
import scalaz.syntax.std.option._

/**
 * Base for Scalatra-enabled, functional-style unit tests, to be run by JUnit.
 *
 * Users of this class should use withApis/withTypedApi to register APIs to test
 * -- OR --
 * Override 'getTestServlet' to test a specific servlet
 *
 * TODO: we should create separate base tests for API spec tests and servlet tests
 */
@RunWith(classOf[JUnitRunner])
abstract class BootstrapServletTestBase extends BaseScalaTest with utilitiesTesting with ScalatraTests {

  private val servletMap: mutable.Map[String, HttpServlet] = mutable.Map()

  def serverPort: Int = localPort.getOrElse(throw new RuntimeException("Server not running!"))

  def getOrCreateServlet(path: String, create: => HttpServlet): HttpServlet = {
    servletMap.getOrElseUpdate(path, {
      val servlet = create
      addServlet(servlet, path)
      servlet
    })
  }

  def getOrCreateServlet(create: => HttpServlet): HttpServlet = getOrCreateServlet("/*", create)

  override def onBeforeAll: Unit = {
    super.onBeforeAll

    start()
  }

  override def onAfterAll: Unit = {
    super.onAfterAll

    stop()
  }

  def withApis[T](basePath: String, api: ApiSpec*)(work : => T) : T = {
    getOrCreateServlet(basePath, new ApiServletBase {}) match {
      case s: ApiServletBase => api.foreach(a => s.registerApi(a))
      case _ => fail("Servlet is not instance of ApiServletBase.. cannot register API")
    }
    work
  }

  def withApi[T](basePath: String, api: ApiSpec)(work: => T) : T = withApis(basePath, api)(work)
  def withApi[T](api: ApiSpec)(work: => T): T = withApi("/*", api)(work)
  def withApis[T](apis: ApiSpec*)(work: => T): T = withApis("/*", apis: _*)(work)

  def withTypedApis[T](basePath: String, apis: TypedApiSpec*)(work: => T) : T = {
    getOrCreateServlet(basePath, new ApiServletBase {}) match {
      case s: ApiServletBase => apis.foreach(a => s.registerTypedApi(a))
      case _ => fail("Servlet is not instance of ApiServletBase.. cannot register API")
    }
    work
  }

  // A `StandardApi` can only be registered in this way for testing. For production, please use
  // `StandardApiVersionDispatcher` so that the API gets all of the goodness baked into the versioner.
  implicit class StandardApiHandler(apiServletBase: ApiServletBase) {

    import com.gravity.logging.Logging._
    import com.gravity.utilities.Counters._

    def registerStandardApi(api: StandardApi) {
      ApiRegistry.apiSpecs += RegisteredApiSpec(apiServletBase, api)

      try {
        for(method <- api.methods) {
          val methodFn = method match {
            case Get => apiServletBase.get _
            case Post => apiServletBase.post _
            case Put => apiServletBase.put _
            case Delete => apiServletBase.delete _
            case _ => throw new IllegalArgumentException(s"Invalid method $method.")
          }

          methodFn(apiServletBase.string2RouteMatcherMadeAvailableToTests(api.route)) {
            // Allow intra-VPN requests to single servers through sans SSL
            if(api.requireSsl && !apiServletBase.request.isSecure && !apiServletBase.request.requestedWithinVpn)
              apiServletBase.halt(403, ApiSpecBase.sslRequiredResponseBody, ApiSpecBase.sslRequiredResponseHeaders)

            executeStandardApi(api)
          }
        }
      } catch {
        case ex: Exception =>
          warn(ex, s"Unable to register Standard Api Spec ${api.route}")
      }
    }

    private def executeStandardApi(api: StandardApi): String = {
      countPerSecond(apiServletBase.counterCategory, api.route + " : Request")
      countPerSecond("ApiByName", api.name)

      implicit val req = ApiRequest(apiServletBase.request, apiServletBase.params(apiServletBase.request),
        apiServletBase.servletContext, System.currentTimeMillis.asMillis)

      val paramValidationFailures = api.validate(req)
      val apiResponse = paramValidationFailures.toNel match {
        case Some(failures) =>
          countPerSecond(apiServletBase.counterCategory, api.route + " : Invalid Requests")
          StandardApiResponse.forParamValidationFailures(failures)

        case None =>
          api.timedHandle(req)
      }
      countPerSecond(apiServletBase.counterCategory, api.route + " : Response Code : " + apiResponse.status)

      this.apiServletBase.response.setContentType(
        (
          if(apiServletBase.jsonpCallbackOption.isDefined)
            MimeTypes.Js
          else
            MimeTypes.Json
          ).contentType
      )
      apiServletBase.response.setStatus(apiResponse.status)

      for((name, value) <- apiResponse.headersToAppend)
        apiServletBase.response.addHeader(name, value)

      val responseBodyStr = Json.stringify(apiResponse.body)
      apiServletBase.jsonpCallbackOption.some(cb => s"$cb($responseBodyStr);").none(responseBodyStr)
    }
  }

  def withStandardApi[T](api: StandardApi, rootPath: String = "/*")(work: => T) : T = {
    getOrCreateServlet(rootPath, new ApiServletBase {}) match {
      case s: ApiServletBase => s.registerStandardApi(api)
      case _ => fail("Servlet is not instance of ApiServletBase.. cannot register API")
    }
    work
  }

  def withTypedApis[T](apis: TypedApiSpec)(work: => T): T = withTypedApis("/*", apis)(work)
  def withTypedApi[T](basePath: String, api: TypedApiSpec)(work: => T) : T = withTypedApis(basePath, api)(work)
  def withTypedApi[T](api: TypedApiSpec)(work: => T) : T = withTypedApi("/*", api)(work)

  def getTypedApiResponse[T : Manifest](implicit jsonFormat: Format[T]) : Option[T] = {
    val r = response
    val content = r.getContent
//    assert(r.getStatus == 200) // Do we need to assert a 200 before extracting the payload?
    println(content)
    val json = Json.parse(content)
    val payload = json \ "payload"
    val jsResult = payload.validate[T]
    jsResult.asOpt
  }

  def getApiResult[T : Manifest] : T = {
    implicit val formats = net.liftweb.json.DefaultFormats
    val result = grvjson.deserialize(response.getContent)
    val finalResult = result \\ "payload"
    val resp = finalResult.extract[T]

    resp
  }

  def withVersionDispatcher[T](spec: StandardApiVersionDispatcher, rootPath:String = "/*")(work: => T) : T = {
    getOrCreateServlet(rootPath, new ApiServletBase{}) match {
      case s: ApiServletBase => s.registerVersionDispatcher(spec)
      case _ => fail("Servlet is not instance of ApiServletBase.. cannot register API")
    }
    work
  }

}


/** Mix into servlets intended to be run in test (non-WAR) environments. */
trait TestServlet extends BaseGravityServlet {
  override def initialize(config: ConfigT) {
    super.initialize(config)

    // Let Scalate find templates in non-root
    templateEngine.resourceLoader = new FileResourceLoader(Seq(
      new File("src/main/webapp"),
      new File("./interfaces/web/src/main/webapp")
    ))
  }
}