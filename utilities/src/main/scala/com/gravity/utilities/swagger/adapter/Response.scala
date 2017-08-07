package com.gravity.utilities.swagger.adapter

import com.gravity.utilities.swagger.adapter.Headers.HeaderName
import com.gravity.utilities.swagger.adapter.Response.MimeType
import play.api.libs.json._
import play.api.libs.functional.syntax._

/*
              ___...---''
  ___...---'\'___
''       _.-''  _`'.______\\.
        /_.)  )..-  __..--'\\
       (    __..--''
        '-''\@


 Ⓐ Ⓐ Ⓐ Ⓐ Ⓐ Ⓐ Ⓐ Ⓐ Ⓐ Ⓐ Ⓐ
*/

/** @see http://swagger.io/specification/#responseObject */
case class Response(
  description: String,
  schema: Option[Schema] = None,
  headers: Map[HeaderName, Header] = Map.empty,
  examples: Map[MimeType, JsValue] = Map.empty
)

object Response {
  type ResponseName = String
  type MimeType = String

  implicit val jsonFormat: Format[Response] = (
    (__ \ "description").format[String] and
    (__ \ "schema").formatNullable[Schema] and
    (__ \ "headers").format[Map[String, Header]] and
    (__ \ "examples").format[Map[String, JsValue]]
  )(Response.apply, unlift(Response.unapply))
}