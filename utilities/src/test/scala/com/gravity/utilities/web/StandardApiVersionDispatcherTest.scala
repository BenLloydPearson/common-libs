package com.gravity.utilities.web

import com.gravity.utilities.api.BootstrapServletTestBase
import org.scalatra.{Get, Put}
import play.api.libs.json.Json

import scalaz.Scalaz._

class StandardApiVersionDispatcherTest extends BootstrapServletTestBase {

  object SomeApiV0 extends StandardApi(
    name = "Some Api V0",
    route = "/simple/test",
    methods = List(Get),
    description = "Create a simple test object."
  ) {
    override def handle(implicit req: ApiRequest): StandardApiResponse = {
      "V0".success
    }
  }

  object SomeApiV1 extends StandardApi(
    name = "Some Api V1",
    route = "/simple/test",
    methods = List(Get),
    description = "Create a simple test object."
  ) {
    override def handle(implicit req: ApiRequest): StandardApiResponse = {
      "V1".success
    }
  }

  // Register them with a dispatcher
  object SomeApiVersionDispatcher extends StandardApiVersionDispatcher(
    name = "Some Api",
    route = "/simple/test",
    methods = List(Get),
    description = "Create a simple test object.",
    apiVersionToSpec = Map(ApiVersion0_0 -> SomeApiV0, ApiVersion1_0 -> SomeApiV1)
  ) {

  }

  test("When no version header is specified, the client will be informed of the latest available version") {
    withVersionDispatcher(SomeApiVersionDispatcher) {
      get("/simple/test", List.empty[(String, String)], Map.empty[String, String]) {
        assert(response.status == 400)
        val errorMessage = (Json.parse(response.getContent()) \\ "message").map(_.as[String])
        assert(errorMessage.head == "No version found in the 'Accept' header. If you have no version preference, use the latest with 'Accept: application/json; version=1")
      }
    }
  }

  test("A request for version 1 should hit the V1 endpoint") {
    withVersionDispatcher(SomeApiVersionDispatcher) {
      get("/simple/test", List.empty[(String, String)], Map("Accept" -> "application/json; version=1")) {
        assert(response.status == 200)
        assert(response.getContent() == "\"V1\"")
      }
    }
  }

  test("A request for version 0 should hit the V0 endpoint") {
    withVersionDispatcher(SomeApiVersionDispatcher) {
      get("/simple/test", List.empty[(String, String)], Map("Accept" -> "application/json; version=0")) {
        assert(response.status == 200)
        assert(response.getContent() == "\"V0\"")
      }
    }
  }

  test("A request for non-existent version should fail with a helpful message") {
    withVersionDispatcher(SomeApiVersionDispatcher) {
      get("/simple/test", List.empty[(String, String)], Map("Accept" -> "application/json; version=2")) {
        assert(response.status == 400)
        val errorMessage = (Json.parse(response.getContent()) \\ "message").map(_.as[String])
        assert(errorMessage.head == "Unable to find a handler for requested version: 2, available versions are [0, 1]")
      }
    }
  }

  case class SomethingToPut(name: String)
  object SomethingToPut {
    implicit val jsonFormat = Json.format[SomethingToPut]
    val example = SomethingToPut("My Name")
  }

  object SomePutApiV0 extends StandardApi(
    name = "Some Put Api V0",
    route = "/simple/test",
    methods = List(Put),
    description = "Create a simple test object."
  ) {

    val bodyContent = B(ApiJsonInputBody[SomethingToPut](SomethingToPut.example, "A simple example for test"))

    override def handle(implicit req: ApiRequest): StandardApiResponse = {
      val stp = bodyContent(req)

      stp.name.success
    }
  }

  object SomePutApiV1 extends StandardApi(
    name = "Some Put Api V1",
    route = "/simple/test",
    methods = List(Put),
    description = "Create a simple test object."
  ) {

    val bodyContent = B(ApiJsonInputBody[SomethingToPut](SomethingToPut.example, "A simple example for test"))

    override def handle(implicit req: ApiRequest): StandardApiResponse = {
      val stp = bodyContent(req)

      stp.name.success
    }
  }

  // Register them with a dispatcher
  object SomePutApiVersionDispatcher extends StandardApiVersionDispatcher(
    name = "Some Put Api",
    route = "/simple/test",
    methods = List(Put),
    description = "Create a simple test object.",
    apiVersionToSpec = Map(ApiVersion0_0 -> SomePutApiV0, ApiVersion1_0 -> SomePutApiV1)
  ) {

  }

  test("A request missing the JSON body content will fail validation") {
    withVersionDispatcher(SomePutApiVersionDispatcher) {
      put("/simple/test", "", Map("Accept" -> "application/json; version=1", "Content-Type" -> "application/json")) {
        assert(response.status == 400)
      }
    }
  }

  test("A request with the JSON body content will not fail validation") {
    val stp = SomethingToPut("Fiddler")
    val jsValue = Json.toJson(stp)
    val bodyString = jsValue.toString

    withVersionDispatcher(SomePutApiVersionDispatcher) {
      put("/simple/test", bodyString, Map("Accept" -> "application/json; version=1", "Content-Type" -> "application/json")) {
        assert(response.status == 200)
        val responseContent = response.getContent()
        assert(responseContent == "\"Fiddler\"")
      }
    }
  }

}
