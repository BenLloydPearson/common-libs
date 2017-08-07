package com.gravity.utilities.web

import com.gravity.utilities.analytics.DateMidnightRange
import com.gravity.utilities.components.FailureResult
import com.gravity.utilities.grvjson._
import play.api.libs.json._

import scala.collection._
import scalaz.syntax.validation._
import scalaz.{Failure, NonEmptyList, Success, ValidationNel}

/**
 * Allows entry of several pluginIds separated by commas via the "pluginIds" param. Note that if you use this, you
 * should probably still cross join this to the list of plugins by site to ensure the requester actually has access to
 * the requested plugin IDs.
 */
trait PluginIdsParam {
  this: ApiSpecBase =>

  val pluginIdsParam: ApiParam[List[Long]] = &(ApiLongListParam(key = "pluginIds", required = false, description = "Optional list of pluginIds separated by comma."))

}

object DashboardApiGlobalParams {
  // TJC: I made these public so that I could access the names from within a ScalatraServlet
  // that provides igi endpoints to dashboard that return multi-part file downloads,
  // which is hard to do with ApiSpec and its friends.
  // Other than that special case, you still "probably don't want to use these directly."

  /** @see [[SiteGuidParams]] You probably don't want to use this directly. */
  val siteGuidParam = ApiGuidParam(key = "Grv-Site-GUID", required = true,
    description = "The siteGuid of the site this API request is for.")

  /** @see [[DashboardUserIdParams]] You probably don't want to use this directly. */
  val userIdParam = ApiLongParam(key = "Grv-User-Id", required = true,
    description = "The user ID of the currently logged in user from the Dashboard originating request.", defaultValue = -1l)

  val params: Seq[ApiParam[_]] = Seq(siteGuidParam, userIdParam)
}

/**
 * Standard Gravity authentication headers.
 *
 * These params don't do anything on their own; you must implement checks against them or support for them yourself.
 *
 * @see http://jira/browse/INTERESTS-2589
 */
trait SiteGuidParams extends DashboardUserIdParams {
  this: ApiSpecBase =>
  val siteGuidParam: ApiParam[String] = H(DashboardApiGlobalParams.siteGuidParam)
  val skipVerifySiteGuidParam: ApiParam[Boolean] = &(ApiBooleanParam(key = "skipVerifySiteGuid", required = false,
    description = "For security, the Grv-Site-GUID header is checked against the requested campaign key to ensure" +
      " the requester actually has access to that campaign. You can set this to true if you need to skip this check" +
      " for administrative users who need to fetch campaigns without knowing to what site the campaigns belong.",
    defaultValue = false))
}
trait DashboardUserIdParams {
  this: ApiSpecBase =>
  val userIdParam: ApiParam[Long] = H(DashboardApiGlobalParams.userIdParam)
}

trait SortParams {
  this: ApiSpecBase =>
  val sortString: String = "-basic.impressions"

  val sortParam: ApiParam[String] = &(ApiStringParam(key = "sort", required = false,
    description = "Field to sort the results by. Prefix with a '-' for Descending", defaultValue = sortString))
}

trait DateRangeParams {
  this: ApiSpecBase =>

  val dateRangeParam: ApiParam[DateMidnightRange] = &(ApiDateMidnightRangeParam(key = "timeperiod", required = true,
    description = "The range of metrics this report should include."))
}

/**
 * Formats param-missing and param-malformed with the newer errors-keyed-by-validation-category format.
 * @see http://jira/browse/INTERESTS-2576
 */
trait ValidationCategoryFormatApiSpec {
  this: ApiSpecBase =>

  def errorResponse[T](fails: NonEmptyList[T]): ApiResponse = ValidationCategoryFormatApiSpec.validationErrorResponse(fails)
}

object ValidationCategoryFormatApiSpec {
  case class Payload(errors: CategorizedErrorsByParamKey)
  type CategorizedErrorsByParamKey = Map[ParamKey, CategorizedErrors]
  type ParamKey = String
  type ValidationCategoryName = String
  type CategorizedErrors = Map[ValidationCategoryName, Error]
  case class Error(message: String, data: Option[Any] = None)

  val paramKeyNotApplicable = "_"

  def validationErrorResponse(fails: NonEmptyList[_], overrideResponseStatus: Option[ResponseStatus] = None): ApiResponse = fails match {
    // Single exception failure
    case NonEmptyList((f: FailureResult) :: Nil) if f.hasException =>
      val categorized: CategorizedErrors = Map(ValidationCategory.Other.apiString -> Error(f.message))
      val byParamKey: CategorizedErrorsByParamKey = Map(paramKeyNotApplicable -> categorized)
      val payload = Payload(byParamKey)
      ApiResponse.error(payload, ex = f.exceptionOption.get)

    // Some other complex or non-exceptional failure(s)
    case _ =>
      val categorizedByParamKey: CategorizedErrorsByParamKey = fails.list.map({
        case ParamValidationFailure(paramKey, innerFailures, _) =>
          val categorized: CategorizedErrors = innerFailures.list.map({ case ValidationFailureData(category, msg, data) => category.apiString -> Error(msg, data)}).toMap
          paramKey -> categorized

        case CategorizedFailureResult(category, message, _) =>
          val categorized: CategorizedErrors = Map(category.apiString -> Error(message))
          paramKeyNotApplicable -> categorized

        case f: FailureResult =>
          val categorized: CategorizedErrors = Map(ValidationCategory.Other.apiString -> Error(f.message))
          paramKeyNotApplicable -> categorized

        case msg: String =>
          val categorized: CategorizedErrors = Map(ValidationCategory.Other.apiString -> Error(msg))
          paramKeyNotApplicable -> categorized

        case wtf =>
          val categorized: CategorizedErrors = Map(ValidationCategory.Other.apiString -> Error(wtf.toString))
          paramKeyNotApplicable -> categorized
      }).toMap

      val payload = Payload(categorizedByParamKey)
      val responseStatus = overrideResponseStatus.getOrElse {
        val errorCategories = categorizedByParamKey.values.flatMap(_.keys).toSet

        if(errorCategories.contains(ValidationCategory.ServerError.apiString))
          ResponseStatus.Error
        else if(errorCategories.contains(ValidationCategory.Conflict.apiString))
          ResponseStatus.Conflict
        else if(errorCategories.contains(ValidationCategory.NotFound.apiString))
          ResponseStatus.NotFound
        else
          ResponseStatus.BadRequest
      }

      ApiResponse(payload, responseStatus)
  }

  def paramValidate[T](paramKey: String, result: ValidationNel[String, T], validationType: ValidationCategory,
                       arbitraryFailureData: Option[JsValue] = None): ValidationNel[ParamValidationFailure, T] = {
    result match {
      case Success(good) => good.successNel
      case Failure(errors) => ParamValidationFailure(paramKey, errors.map(error => ValidationFailureData(validationType,
        error, arbitraryFailureData))).failureNel
    }
  }

  def paramValidate[T, ArbitraryDataType: Writes](paramKey: String, result: ValidationNel[String, T], validationType: ValidationCategory,
                                                  arbitraryFailureData: Option[ArbitraryDataType]): ValidationNel[ParamValidationFailure, T] = {
    result match {
      case Success(good) => good.successNel
      case Failure(errors) => ParamValidationFailure(paramKey, errors.map(error => ValidationFailureData(validationType,
        error, arbitraryFailureData.map(Json.toJson(_))))).failureNel
    }
  }
}

case class ValidationFailureData(category: ValidationCategory, msg: String, arbitraryData: Option[JsValue] = None)

/** @param suggestedStatus Never automatically used for anything; intended to help you manage response status codes. */
case class ParamValidationFailure(paramKey: String, failures: NonEmptyList[ValidationFailureData],
                                  suggestedStatus: ResponseStatus = ResponseStatus.BadRequest)
extends FailureResult(failures.map(_.msg).list.mkString(". "), None)

object ParamValidationFailure {
  def apply(paramKey: String, category: ValidationCategory, msg: String): ParamValidationFailure = {
    ParamValidationFailure(paramKey, NonEmptyList(ValidationFailureData(category, msg, None)))
  }

  def apply(paramKey: String, category: ValidationCategory, msg: String, arbitraryData: Option[JsValue]): ParamValidationFailure = {
    ParamValidationFailure(paramKey, NonEmptyList(ValidationFailureData(category, msg, arbitraryData)))
  }

  def apply[ArbitraryDataType: Writes](paramKey: String, category: ValidationCategory, msg: String,
                                       arbitraryData: Option[ArbitraryDataType]): ParamValidationFailure = {
    ParamValidationFailure(paramKey, NonEmptyList(ValidationFailureData(category, msg, arbitraryData.map(Json.toJson(_)))))
  }

  def apply(paramKey: String, category: ValidationCategory, causedBy: FailureResult): ParamValidationFailure = {
    ParamValidationFailure(paramKey, NonEmptyList(ValidationFailureData(category, causedBy.message, causedBy.exceptionOption.map(Json.toJson(_)))))
  }

  def apply(paramKey: String, category: ValidationCategory, msg: String, suggestedStatus: ResponseStatus): ParamValidationFailure = {
    ParamValidationFailure(paramKey, NonEmptyList(ValidationFailureData(category, msg, None)), suggestedStatus)
  }

  def apply(paramKey: String, category: (ValidationCategories) => ValidationCategory, msg: String): ParamValidationFailure
    = ParamValidationFailure(paramKey, category(ValidationCategory), msg)

  implicit val jsonWrites: Writes[ParamValidationFailure] with Object {def writes(o: ParamValidationFailure): JsValue} = new Writes[ParamValidationFailure] {
    override def writes(o: ParamValidationFailure): JsValue = {
      JsObject(Seq(
        "paramKey" -> JsString(o.paramKey),
        "suggestedStatus" -> Json.toJson(o.suggestedStatus)
      ))
    }
  }

  implicit val jsonNelWrites: Writes[NonEmptyList[ParamValidationFailure]] = nelWrites[ParamValidationFailure]
}