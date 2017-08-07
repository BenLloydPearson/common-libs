package com.gravity.utilities.web

import com.gravity.utilities.api.BootstrapServletTestBase
import org.scalatra.Post
import play.api.libs.json.{JsValue, Json}

/**
  * Created by tdecamp on 5/18/16.
  * {{insert neat ascii diagram here}}
  */
class TypedApiSpecTest extends BootstrapServletTestBase {

  val jsonContentTypeHeaders = Map("Content-Type" -> MimeTypes.Json.contentType)

  case class SimpleTestItem(id: Int, name: String, isActive: Boolean, nicknameOpt: Option[String])

  object SimpleTestItem {
    implicit val jsonFormat = Json.format[SimpleTestItem]

    val example = SimpleTestItem(101, "My Name", isActive = true, None)
  }

  object SimpleTestApi extends TypedApiSpec(
    name = "Simple Test API",
    route = "/simple/test",
    methods = List(Post),
    description = "Create a simple test object."
  ) {

    val bodyParam = B(ApiJsonInputBody[SimpleTestItem](SimpleTestItem.example, "Simple Test Body param for Simple Test API"))

    override def handle(implicit req: ApiRequest): TypedApiResponse = {

      val body = bodyParam(req)
      assert(body.id > 0)
      assert(body.name.nonEmpty)

      TypedApiSuccessJsonResponse(body)
    }
  }

  test("It can register a body param") {
    withTypedApi(SimpleTestApi) {
      assert(SimpleTestApi.bodySpecs.size == 1)
    }
  }

  test("It can successfully read data from a body param") {

    val testItem = SimpleTestItem(999, "Some Name Here", isActive = false, Some("Mr Nickname"))
    val jsValue = Json.toJson(testItem)
    val bodyString = jsValue.toString

    withTypedApi(SimpleTestApi) {
      post("/simple/test", bodyString, jsonContentTypeHeaders) {
        println(response.getContent())
        assert(response.status == 200)
        val testItemResponseOpt = getTypedApiResponse[SimpleTestItem]
        assert(testItemResponseOpt.isDefined)
        val testItemResponse = testItemResponseOpt.get
        assert(testItem == testItemResponse)
      }
    }

  }

  test("It fails validation when the client passed a json object with a missing required field") {
    val jsonWithInvalidFields: JsValue = Json.parse(
      """
        |{
        |  "name": "Name Goes Here",
        |  "isActive": true
        |}
      """.stripMargin)
    val bodyString = jsonWithInvalidFields.toString

    withTypedApi(SimpleTestApi) {
      post("/simple/test", bodyString, jsonContentTypeHeaders) {
        println(response.getContent())
        assert(response.status == 400)
        val errorMessage = (Json.parse(response.getContent()) \ "errors" \\ "message").map(_.as[String])
        assert(errorMessage.size == 1)
        assert(errorMessage.head.contains("Errors reading JSON: /id => error.path.missing"))
      }
    }
  }

  test("It passes validation when the client passed a json object with an unexpected field") {
    val jsonWithInvalidFields: JsValue = Json.parse(
      """
        |{
        |  "id": 808,
        |  "name": "Name Goes Here",
        |  "address": "123 E Fake St",
        |  "isActive": true
        |}
      """.stripMargin)
    val bodyString = jsonWithInvalidFields.toString

    val expectedItem = SimpleTestItem(808, "Name Goes Here", isActive = true, None)

    withTypedApi(SimpleTestApi) {
      post("/simple/test", bodyString, jsonContentTypeHeaders) {
        println(response.getContent())
        assert(response.status == 200)
        val testItemResponseOpt = getTypedApiResponse[SimpleTestItem]
        assert(testItemResponseOpt.isDefined)
        val testItemResponse = testItemResponseOpt.get
        assert(expectedItem == testItemResponse)
      }
    }
  }

  object SimpleTestApiNoBodyJson extends TypedApiSpec(
    name = "Simple Test API No Body JSON",
    route = "/simple/test",
    methods = List(Post),
    description = "Create a simple test object."
  ) {

    val roleNameParam = &(ApiStringParam(key = "mykey", description = "desc", required = true))

    override def handle(implicit req: ApiRequest): TypedApiResponse = {

      val roleNameOpt = roleNameParam(req)
      assert(roleNameOpt.isDefined)

      TypedApiSuccessJsonResponse(roleNameOpt.get)
    }
  }

  test("It succeeds validation when no body param is registered") {
    withTypedApi(SimpleTestApiNoBodyJson) {
      post("/simple/test", Map("mykey" -> "THE_ROLENAME")) {
        println(response.getContent())
        assert(response.status == 200)
        val testItemResponseOpt = getTypedApiResponse[String]
        assert(testItemResponseOpt.isDefined)
        val testItemResponse = testItemResponseOpt.get
        assert("THE_ROLENAME" == testItemResponse)
      }
    }
  }

  object SimpleTestApiTooManyBodyParams extends TypedApiSpec(
    name = "Simple Test API Too Many Body Params",
    route = "/simple/test",
    methods = List(Post),
    description = "Create a simple test object."
  ) {

    val bodyParam = B(ApiJsonInputBody[SimpleTestItem](SimpleTestItem.example, "Simple Test Body param for SimpleTestApiTooManyBodyParams"))
    val bodyParamTooMany = B(ApiJsonInputBody[SimpleTestItem](SimpleTestItem.example, "Simple Test SECOND Body param for SimpleTestApiTooManyBodyParams"))

    override def handle(implicit req: ApiRequest): TypedApiResponse = {

      TypedApiSuccessJsonResponse("OK")
    }

  }

  test("It should throw an exception when attempting to register more than 1 JSON body") {
    val caughtException = intercept[IndexOutOfBoundsException] {
      withTypedApi(SimpleTestApiTooManyBodyParams) {
        fail("Should not make it here, registering the API should fail b/c it has two JSON bodies")
      }
    }

    println("Caught expected IndexOutOfBoundsException for SimpleTestApiTooManyBodyParams: " + caughtException.getMessage)
  }

}
