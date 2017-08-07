package com.gravity.utilities.web

import com.gravity.utilities.web.ApiResponseBody.ApiJsonResponseBody
import play.api.libs.json.{JsObject, Json, Writes}

/*
              ___...---''
  ___...---'\'___
''       _.-''  _`'.______\\.
        /_.)  )..-  __..--'\\
       (    __..--''
        '-''\@


 Ⓐ Ⓐ Ⓐ Ⓐ Ⓐ Ⓐ Ⓐ Ⓐ Ⓐ Ⓐ Ⓐ
*/

case class TypedApiSuccessJsonResponse[P: Writes](payload: P,
                                                  override val status: ResponseStatus = ResponseStatus.OK,
                                                  override val headersToAppend: Map[String, String] = Map.empty[String, String]) extends TypedApiResponse {
  final val bodyJsValue: JsObject = Json.obj(
    "payload" -> Json.toJson(payload),
    "status" -> Json.toJson(status)
  )

  final val body: ApiJsonResponseBody = new ApiJsonResponseBody(bodyJsValue)

  def withWarning[W: Writes](warning: W): TypedApiSuccessResponseWithWarning[P, W] = TypedApiSuccessResponseWithWarning(payload, warning, status)
}

/** A success response with an additional "warning" key beside "payload" indicating arbitrary warnings to the client. */
case class TypedApiSuccessResponseWithWarning[P: Writes, W: Writes]
                                        (payload: P, warning: W,
                                         override val status: ResponseStatus = ResponseStatus.OK,
                                         override val headersToAppend: Map[String, String] = Map.empty[String, String]) extends TypedApiResponse {
  final val body: ApiJsonResponseBody = Json.obj(
    "payload" -> Json.toJson(payload),
    "status" -> Json.toJson(status),
    "warning" -> Json.toJson(warning)
  )
}

object TypedApiSuccessJsonResponse {
  def warning[P: Writes, W: Writes](payload: P, warning: W, status: ResponseStatus = ResponseStatus.OK): TypedApiSuccessResponseWithWarning[P, W]
  = TypedApiSuccessResponseWithWarning(payload, warning, status)

  def ok: TypedApiSuccessJsonResponse[String] = TypedApiSuccessJsonResponse("OK")
}