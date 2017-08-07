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

/** @see http://swagger.io/specification/#itemsObject */
case class Items(
  `type`: String,
  format: Option[String] = None,
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
  maxItems: Option[Int] = None,
  minItems: Option[Int] = None,
  uniqueItems: Option[Boolean] = None,
  enum: Seq[JsValue] = Seq.empty,
  multipleOf: Option[BigDecimal] = None
)

object Items {
  implicit val jsonFormat: Format[Items] = Json.format[Items]
}