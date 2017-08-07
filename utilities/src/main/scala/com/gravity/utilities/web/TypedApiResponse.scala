package com.gravity.utilities.web

import java.io.Serializable

import com.gravity.utilities.components.FailureResult
import play.api.libs.json.{Json, Writes}

import scalaz.{Failure, Success, ValidationNel}

/*
              ___...---''
  ___...---'\'___
''       _.-''  _`'.______\\.
        /_.)  )..-  __..--'\\
       (    __..--''
        '-''\@


 Ⓐ Ⓐ Ⓐ Ⓐ Ⓐ Ⓐ Ⓐ Ⓐ Ⓐ Ⓐ Ⓐ
*/

abstract class TypedApiResponse extends Serializable {
  val body: ApiResponseBody
  val status: ResponseStatus
  val headersToAppend: Map[String, String]
}

object TypedApiResponse {
  /** @tparam P Payload (success) type. */
  implicit def validationToTypedApiResponse[P: Writes](v: ValidationNel[FailureResult, P]): TypedApiResponse = v match {
    case Success(s) => TypedApiSuccessJsonResponse(Json.toJson(s))
    case Failure(fails) => TypedApiFailureJsonResponse(fails)
  }
}