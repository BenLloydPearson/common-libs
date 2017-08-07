package com.gravity.utilities.swagger.adapter

import play.api.libs.json.{Format, Json, JsValue}

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
 * This is an incomplete representation of the Swagger Schema type, which contains several additional fields that are of
 * no immediate use and would push the length of this case class constructor to over 22 params.
 *
 * @see http://swagger.io/specification/#schemaObject
 */
case class Schema(
  $ref: Option[String] = None,
  format: Option[String] = None,
  title: Option[String] = None,
  description: Option[String] = None,
  default: Option[JsValue] = None,
  // Removing this to make room for properties field. Can use this solution if needed: http://stackoverflow.com/questions/23571677/22-fields-limit-in-scala-2-11-play-framework-2-3-case-classes-and-functions
//  multipleOf: Option[BigDecimal] = None,
  maximum: Option[BigDecimal] = None,
  exclusiveMaximum: Option[Boolean] = None,
  minimum: Option[BigDecimal] = None,
  exclusiveMinimum: Option[Boolean] = None,
  maxLength: Option[Int] = None,
  minLength: Option[Int] = None,
  pattern: Option[String] = None,
  maxItems: Option[Int] = None,
  minItems: Option[Int] = None,
  uniqueItems: Option[Boolean] = None,
  maxProperties: Option[Int] = None,
  minProperties: Option[Int] = None,
  required: Seq[String] = Seq.empty,
  enum: Seq[JsValue] = Seq.empty,
  `type`: String,
  properties: JsValue
)

object Schema {
  type SchemaName = String

  implicit val jsonFormat: Format[Schema] = Json.format[Schema]
}