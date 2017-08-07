package com.gravity.utilities.web

import com.gravity.utilities.{Settings2, grvtime}
import org.scalatra.{Get, HttpMethod}
import play.api.libs.json.{JsNull, Writes}

/*
              ___...---''
  ___...---'\'___
''       _.-''  _`'.______\\.
        /_.)  )..-  __..--'\\
       (    __..--''
        '-''\@


 Ⓐ Ⓐ Ⓐ Ⓐ Ⓐ Ⓐ Ⓐ Ⓐ Ⓐ Ⓐ Ⓐ
*/

/**
  * Helps conform to the latest organization-wide API Standards.
  *
  * @see https://confluence.ops.aol.com/confluence/pages/viewpage.action?spaceKey=AOLC&title=Api+Standards#ApiStandards-responses
  */
abstract class StandardApi(val name: String, val route: String, val methods: List[HttpMethod] = List(Get),
                           val description: String = "")
extends ApiSpecBase {
  override type ExampleResponseType = Option[StandardApiResponse]
  override implicit def exampleResponseSerializer: Writes[Option[StandardApiResponse]] =
    Writes[Option[StandardApiResponse]] {
      case Some(resp) => resp.body
      case None => JsNull
    }

  override val requireSsl: Boolean = Settings2.getBooleanOrDefault("api.standardApi.requireSsl", default = false)

  def exampleResponse: Option[StandardApiResponse] = None

  def handle(implicit req: ApiRequest): StandardApiResponse

  final def timedHandle(implicit req: ApiRequest): StandardApiResponse = {
    val timedResponse = grvtime.timedOperation(handle(req))
    val resp = timedResponse.result
    resp.copy(executionMillis = timedResponse.duration)
  }
}