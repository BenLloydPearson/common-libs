package com.gravity.utilities.web

import java.io.Serializable

import com.gravity.utilities.components.FailureResult
import play.api.libs.json.{JsArray, JsValue, Json, Writes}

import scalaz.{Failure, NonEmptyList, Success, ValidationNel}

/*
              ___...---''
  ___...---'\'___
''       _.-''  _`'.______\\.
        /_.)  )..-  __..--'\\
       (    __..--''
        '-''\@


 Ⓐ Ⓐ Ⓐ Ⓐ Ⓐ Ⓐ Ⓐ Ⓐ Ⓐ Ⓐ Ⓐ
*/

case class StandardApiResponse(
  body: JsValue,
  status: Int = ResponseStatus.okStatus,
  headersToAppend: Map[String, String] = Map.empty,
  executionMillis: Long = 0L
) extends Serializable

object StandardApiResponse {
  def forParamValidationFailures(fails: NonEmptyList[ParamValidationFailure]): StandardApiResponse =
    StandardApiResponse(Json.toJson(fails)(paramValidationFailureNelWriter), ResponseStatus.badRequestStatus)

  def apply[T: Writes](body: T): StandardApiResponse = new StandardApiResponse(Json.toJson(body))

  def apply[T: Writes](body: T, status: Int): StandardApiResponse = new StandardApiResponse(Json.toJson(body), status)

  def apply[T: Writes](body: T, headersToAppend: Map[String, String]): StandardApiResponse =
    new StandardApiResponse(Json.toJson(body), headersToAppend = headersToAppend)

  /** @tparam P Payload (success) type. */
  implicit def validationToStandardApiResponse[P: Writes](v: ValidationNel[FailureResult, P]): StandardApiResponse =
    v match {
      case Success(s) => StandardApiResponse(Json.toJson(s))
      case Failure(fails) =>
        throw new Exception("TODO: IMPLEMENT ME; PLEASE REFER TO THE API STANDARDS DOCUMENTATION BEFORE IMPLEMENTING")
    }

  val paramValidationFailureNelWriter = Writes[NonEmptyList[ParamValidationFailure]](pvfNel => JsArray(
    pvfNel.list.map(pvf => Json.obj(
      "paramKey" -> pvf.paramKey,
      "message" -> pvf.failures.head.msg,
      "failures" -> JsArray(pvf.failures.list.map(vfd => Json.obj(
        "category" -> vfd.category.apiString,
        "message" -> vfd.msg,
        "data" -> vfd.arbitraryData
      )))
    ))
  ))
}