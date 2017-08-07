package com.gravity.utilities.web

import com.gravity.utilities.components.FailureResult
import com.gravity.utilities.grvz._
import com.gravity.utilities.web.ApiResponseBody.ApiJsonResponseBody
import com.gravity.utilities.web.ValidationCategoryFormatApiSpec._
import play.api.libs.json._

import scala.collection._
import scalaz.NonEmptyList
import scalaz.syntax.std.option._

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
 * @param _status If not specified, uses [[TypedApiFailureJsonResponse.statusFromFails(errors)]]
 */
case class TypedApiFailureJsonResponse(errors: NonEmptyList[FailureResult],
                                       private val _status: Option[ResponseStatus] = None)
extends TypedApiResponse {
  val status: ResponseStatus = _status.getOrElse(TypedApiFailureJsonResponse.statusFromFails(errors))

  override val body: ApiJsonResponseBody = Json.obj(
    "errors" -> TypedApiFailureJsonResponse.failsToJsObject(errors),
    "status" -> Json.toJson(status)
  )

  override val headersToAppend: Predef.Map[String, String] = Map.empty[String, String]
}

object TypedApiFailureJsonResponse {
 import com.gravity.logging.Logging._
  object ForbiddenJsonResponse extends TypedApiFailureJsonResponse(nel(FailureResult("403 Forbidden")), ResponseStatus.Forbidden.some)

  def apply(fail: FailureResult, status: ResponseStatus.type => ResponseStatus): TypedApiFailureJsonResponse
        = TypedApiFailureJsonResponse(NonEmptyList(fail), Some(status(ResponseStatus)))

  def apply(fail: FailureResult): TypedApiFailureJsonResponse = TypedApiFailureJsonResponse(NonEmptyList(fail))

  def statusFromFails[F](fails: NonEmptyList[F]): ResponseStatus = {
    for(f <- fails) {
      f match {
        case CategorizedFailureResult(category, _, _) => return category.responseStatus
        case ParamValidationFailure(_, _, suggestedStatus) => return suggestedStatus
        case _ =>
      }
    }

    ResponseStatus.Error
  }

  /**
   * This is based heavily on [[ValidationCategoryFormatApiSpec]], which is deprecated in favor of this new API
   * abstraction ([[TypedApiSpec]], [[TypedApiSuccessJsonResponse]], et al).
   */
  def failsToJsObject[F](fails: NonEmptyList[F]): JsObject = fails match {
    // Single exception failure
    case NonEmptyList((f: FailureResult) :: Nil) if f.hasException =>
      val categorized = Json.obj(ValidationCategory.Other.apiString -> Json.obj("message" -> f.message))
      val byParamKey = Json.obj(paramKeyNotApplicable -> categorized)
      byParamKey

    // Some other complex or non-exceptional failure(s)
    case _ =>
      val categorizedByParamKey = fails.list.map {
        case ParamValidationFailure(paramKey, innerFailures, _) =>
          val categorized = JsObject(innerFailures.list.map {
            case ValidationFailureData(category, msg, data) =>
              category.apiString -> JsObject(Seq("message" -> JsString(msg)) ++ data.map("data" -> _))
          })
          paramKey -> categorized

        case CategorizedFailureResult(category, message, _) =>
          val categorized = Json.obj(category.apiString -> Json.obj("message" -> message))
          paramKeyNotApplicable -> categorized

        case f: FailureResult =>
          val categorized = Json.obj(ValidationCategory.Other.apiString -> Json.obj("message" -> f.message))
          paramKeyNotApplicable -> categorized

        case msg: String =>
          val categorized = Json.obj(ValidationCategory.Other.apiString -> Json.obj("message" -> msg))
          paramKeyNotApplicable -> categorized

        case x =>
          val categorized = Json.obj(ValidationCategory.Other.apiString -> Json.obj("message" -> x.toString))
          paramKeyNotApplicable -> categorized
      }

      JsObject(categorizedByParamKey)
  }
}