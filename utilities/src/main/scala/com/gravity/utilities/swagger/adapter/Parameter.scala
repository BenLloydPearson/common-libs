package com.gravity.utilities.swagger.adapter

import play.api.libs.json.{Format, JsValue, Json}

/*
              ___...---''
  ___...---'\'___
''       _.-''  _`'.______\\.
        /_.)  )..-  __..--'\\
       (    __..--''
        '-''\@


 Ⓐ Ⓐ Ⓐ Ⓐ Ⓐ Ⓐ Ⓐ Ⓐ Ⓐ Ⓐ Ⓐ
*/

/** @see http://swagger.io/specification/#parameterObject */
case class Parameter(
  name: String,
  in: ParameterIn,
  description: Option[String] = None,
  required: Boolean = false,
  schema: Option[Schema] = None,
  `type`: ParameterType,
  format: Option[String] = None,
  allowEmptyValue: Boolean = false,
  items: Option[Items] = None,
  collectionFormat: Option[String] = None,
  default: Option[JsValue] = None,
  maximum: Option[BigDecimal] = None,
  exclusiveMaximum: Option[Boolean] = None,
  minimum: Option[BigDecimal] = None,
  exclusiveMinimum: Option[Boolean] = None,
  maxLength: Option[Int] = None,
  minLength: Option[Int] = None,
  pattern: Option[String] = None,
  uniqueItems: Option[Boolean] = None,
  enum: Option[Seq[JsValue]] = None
)

object Parameter {
  implicit val jsonFormat: Format[Parameter] = Json.format[Parameter]
}