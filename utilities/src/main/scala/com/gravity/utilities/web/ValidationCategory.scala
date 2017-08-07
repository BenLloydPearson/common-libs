package com.gravity.utilities.web

import com.gravity.utilities.components.FailureResult

/**
 * Enum of validation failure categories, e.g. parse failures, out-of-bounds failures.
 * @param apiString the API string representing the validation category
 */
sealed abstract class ValidationCategory(val apiString: String, val responseStatus: ResponseStatus = ResponseStatus.OK)

sealed abstract class ValidationCategories {
  case object Required extends ValidationCategory("required", ResponseStatus.BadRequest)
  case object ParseEnum extends ValidationCategory("enum", ResponseStatus.BadRequest)
  case object ParseOther extends ValidationCategory("parse", ResponseStatus.BadRequest)
  case object BeyondMin extends ValidationCategory("min", ResponseStatus.BadRequest)
  case object BeyondMax extends ValidationCategory("max", ResponseStatus.BadRequest)
  case object BeyondRange extends ValidationCategory("range", ResponseStatus.BadRequest)
  case object NonUnique extends ValidationCategory("unique", ResponseStatus.BadRequest)
  case object Conflict extends ValidationCategory("conflict", ResponseStatus.BadRequest)
  case object OtherIllegal extends ValidationCategory("_", ResponseStatus.BadRequest)
  case object NotFound extends ValidationCategory("notfound", ResponseStatus.NotFound)
  case object NotInAllowedValues extends ValidationCategory("notinallowedvalues", ResponseStatus.BadRequest)
  case object ServerError extends ValidationCategory("server", ResponseStatus.Error)
  case object TotalBudgetExceeded extends ValidationCategory("totalBudgetExceeded", ResponseStatus.BadRequest)
  case object Other extends ValidationCategory("_", ResponseStatus.Error)
}
object ValidationCategory extends ValidationCategories

case class CategorizedFailureResult(category: ValidationCategory, override val message: String, override val exceptionOption: Option[Throwable] = None)
extends FailureResult(message, exceptionOption) {
  def this(category: (ValidationCategories) => ValidationCategory, message: String) = this(category(ValidationCategory), message)
}