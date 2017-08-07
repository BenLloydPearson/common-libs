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

/** see http://swagger.io/specification/#licenseObject */
case class License(name: String, url: Option[String] = None)

object License {
  implicit val format: Format[License] = Json.format[License]
}