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

/** @see http://swagger.io/specification/#referenceObject */
case class Reference($ref: String)

object Reference {
  implicit val jsonFormat: Format[Reference] = Json.format[Reference]
}