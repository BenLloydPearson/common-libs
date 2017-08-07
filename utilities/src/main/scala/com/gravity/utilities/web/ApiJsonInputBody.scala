package com.gravity.utilities.web

import com.gravity.utilities.grvstrings._
import com.gravity.utilities.swagger.adapter.{Parameter, ParameterIn, ParameterType, Schema}
import com.gravity.utilities.web.http.GravityHttpServletRequest
import com.gravity.utilities.web.json.JsonSchemaBuilder
import play.api.data.validation.ValidationError
import play.api.libs.json._

import scala.reflect.runtime.universe._

/*
              ___...---''
  ___...---'\'___
''       _.-''  _`'.______\\.
        /_.)  )..-  __..--'\\
       (    __..--''
        '-''\@


 Ⓐ Ⓐ Ⓐ Ⓐ Ⓐ Ⓐ Ⓐ Ⓐ Ⓐ Ⓐ Ⓐ
*/

class ApiJsonInputBody[+T: Reads](exampleValue: T, description: String)(implicit tag: TypeTag[T]) {
  val requestMapKey = "GRV_PARSED_JSON_BODY"

  def apply(implicit request: ApiRequest): T = {
    request.request.getAttribute(requestMapKey).asInstanceOf[T]
  }

  /**
    * This follows the pattern set by [[ApiParamBase]].
    *
    * @return If validation success, None.
    *         If validation fails, ("error message", ValidationCategory).
    */
  def validate(request: ApiRequest): Option[(String, ValidationCategory)] = {
    // Wrong content-type
    if(!Option(request.request.getContentType).contains(MimeTypes.Json.contentType))
      Some((s"Input must be content-type ${MimeTypes.Json.contentType}", ValidationCategory.ParseOther))
    else {
      scala.util.Try(Json.fromJson[T](Json.parse(request.request.getBody)) match {
        case JsSuccess(value, path) => request.request.setAttribute(requestMapKey, value); None
        case JsError(errors) => Some(("Errors reading JSON: " + errors.map({
          case (path: JsPath, msgs: Seq[ValidationError]) => path.toString + " => " + msgs.map(_.message).mkString("; ")
        }).mkString(". "), ValidationCategory.ParseOther))
      }).getOrElse(Some("Unable to parse as JSON", ValidationCategory.ParseOther))
    }
  }

  // Gets the fields from the exampleValue and turns them into a generic Json Schema.
  private lazy val exampleValueFieldsToSwaggerJson = JsonSchemaBuilder.build(tag.tpe)
  private lazy val requiredFieldNames = JsonSchemaBuilder.requiredFields(tag.tpe)

  def swaggerParameter: Parameter = Parameter(
    "body",
    ParameterIn.body,
    description.noneForEmpty,
    required = true,
    `type` = ParameterType.string,
    allowEmptyValue = false,
    // Can use $ref to keep each one of these simple, but then the case classes must be defined elsewhere. If we
    // define the case class as Schema here, then we don't require it to be loaded elsewhere...
    schema = Some(Schema(`type` = "object", required = requiredFieldNames, properties = exampleValueFieldsToSwaggerJson))
  )

  override def toString: String = {
    s"ApiJsonInputBody described as: '$description'"
  }
}

object ApiJsonInputBody {
  def apply[T: Reads](exampleValue: T, description: String)(implicit tag: TypeTag[T]): ApiJsonInputBody[T] = {
    new ApiJsonInputBody(exampleValue, description)
  }
}
