package com.gravity.utilities.api

import com.gravity.utilities.web._
import org.scalatra.Get
import play.api.libs.json._

import scalaz.Scalaz._

class ApiServletTest extends BootstrapServletTestBase {

  object SomeApiV1 extends StandardApi(
    name = "Some Api V1",
    route = "/simple/test",
    methods = List(Get),
    description = "Create a simple test object."
  ) {
    override def handle(implicit req: ApiRequest): StandardApiResponse = {
      "OK".success
    }
  }

  // Register with a dispatcher
  object SomeApiVersionDispatcher extends StandardApiVersionDispatcher(
    name = "Some Api",
    route = "/simple/test",
    methods = List(Get),
    description = "Create a simple test object.",
    apiVersionToSpec = Map(ApiVersion1_0 -> SomeApiV1)
  ) {

  }

  test("Not sending a Request-ID header in the request will generate one for the response") {
    withVersionDispatcher(SomeApiVersionDispatcher) {

      val headers = Map("Accept" -> "application/json; version=1")

      get("/simple/test", List.empty[(String, String)], headers) {
        assert(response.status == 200)
        val requestId = response.getHeader("Request-ID")
        assert(requestId != null)
        assert(requestId.nonEmpty)
      }
    }
  }

  test("Test that sending a Request-ID header will see the same one in the response") {
    withVersionDispatcher(SomeApiVersionDispatcher) {

      val headers = Map(
        "Accept" -> "application/json; version=1",
        "Request-ID" -> "909090"
      )

      get("/simple/test", List.empty[(String, String)], headers) {
        assert(response.status == 200)
        val requestId = response.getHeader("Request-ID")
        assert(requestId != null)
        assert(requestId == "909090")
      }
    }
  }

  test("Test that the Request-ID header is type 4") {
    withVersionDispatcher(SomeApiVersionDispatcher) {

      val headers = Map(
        "Accept" -> "application/json; version=1"
      )

      get("/simple/test", List.empty[(String, String)], headers) {
        assert(response.status == 200)
        val requestId = response.getHeader("Request-ID")
        assert(requestId != null)
        assert(requestId.length == 36)
      }
    }
  }

  test("An ETag header is returned in the response") {
    withVersionDispatcher(SomeApiVersionDispatcher) {

      val headers = Map("Accept" -> "application/json; version=1")

      get("/simple/test", List.empty[(String, String)], headers) {
        assert(response.status == 200)
        val etag = response.getHeader("ETag")
        assert(etag != null)
        assert(etag.length == 32)
      }
    }
  }

  test("StandardApi returns 403 when not contacted through SSL") {
    object SslApi extends StandardApi("SSL API", "/sslApi") {
      override def handle(implicit req: ApiRequest): StandardApiResponse = "OK".success

      override val requireSsl: Boolean = true
    }

    withStandardApi(SslApi) {
      get("/sslApi", headers = Map("Host" -> "api.gravity.com")) {
        response.status should be(403)
        (Json.parse(response.body) \ "payload") shouldNot be(JsString("OK"))
      }
    }
  }

  test("StandardApi can be restful") {
    object SampleApi extends StandardApi("Sample API", "/sample") {
      override def handle(implicit req: ApiRequest): StandardApiResponse = "OK".success
    }

    withStandardApi(SampleApi) {
      get("/sample") {
        response.status should be(200)
        Json.parse(response.body) should be(JsString("OK"))
      }
    }
  }
}
