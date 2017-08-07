package com.gravity.utilities.web

import com.gravity.utilities.web.ApiResponseBody.ApiXmlResponseBody

/*
              ___...---''
  ___...---'\'___
''       _.-''  _`'.______\\.
        /_.)  )..-  __..--'\\
       (    __..--''
        '-''\@


 Ⓐ Ⓐ Ⓐ Ⓐ Ⓐ Ⓐ Ⓐ Ⓐ Ⓐ Ⓐ Ⓐ
*/

case class TypedApiSuccessXmlResponse(xmlBody: xml.Elem, override val status: ResponseStatus = ResponseStatus.OK, override val headersToAppend: Map[String, String] = Map.empty[String, String]) extends TypedApiResponse {
  override val body: ApiXmlResponseBody = xmlBody
}
