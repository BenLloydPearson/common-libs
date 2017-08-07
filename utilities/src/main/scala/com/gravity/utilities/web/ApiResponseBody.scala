package com.gravity.utilities.web

import com.gravity.utilities.web.MimeTypes.MimeType
import play.api.libs.json._

/*
              ___...---''
  ___...---'\'___
''       _.-''  _`'.______\\.
        /_.)  )..-  __..--'\\
       (    __..--''
        '-''\@


 Ⓐ Ⓐ Ⓐ Ⓐ Ⓐ Ⓐ Ⓐ Ⓐ Ⓐ Ⓐ Ⓐ
*/

abstract class ApiResponseBody {
  def serialized: String
  def contentType: MimeType
}

object ApiResponseBody {
  implicit class ApiJsonResponseBody(body: JsValue) extends ApiResponseBody {
    def serialized: String = Json.stringify(body)
    def contentType: MimeType = MimeTypes.Json
  }

  implicit class ApiXmlResponseBody(body: scala.xml.Elem) extends ApiResponseBody {
    def serialized: String = {
      val pp = new scala.xml.PrettyPrinter(80, 2)
      pp.format(body)
    }
    def contentType: MimeType = MimeTypes.Xml
  }
}