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

/** @see http://swagger.io/specification/#infoObject */
case class Info(
  title: String,
  description: Option[String] = None,
  termsOfService: Option[String] = None,
  contact: Option[Contact] = None,
  license: Option[License] = None,
  version: String
)

object Info {
  implicit val format: Format[Info] = Json.format[Info]
}