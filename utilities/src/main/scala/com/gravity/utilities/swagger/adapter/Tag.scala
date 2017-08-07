package com.gravity.utilities.swagger.adapter

import play.api.libs.json.{Format, Json}

/*
              ___...---''
  ___...---'\'___
''       _.-''  _`'.______\\.
        /_.)  )..-  __..--'\\
       (    __..--''
        '-''\@


 Ⓐ Ⓐ Ⓐ Ⓐ Ⓐ Ⓐ Ⓐ Ⓐ Ⓐ Ⓐ Ⓐ
*/

/** @see http://swagger.io/specification/#tagObject */
case class Tag(name: String, description: Option[String] = None, externalDocs: Option[ExternalDocumentation] = None)

object Tag {
  implicit val jsonFormat: Format[Tag] = Json.format[Tag]
}