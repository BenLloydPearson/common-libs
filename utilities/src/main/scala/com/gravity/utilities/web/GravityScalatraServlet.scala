package com.gravity.utilities.web

import com.gravity.utilities.web.MimeTypes.MimeType
import org.scalatra.ScalatraServlet

/*
              ___...---''
  ___...---'\'___
''       _.-''  _`'.______\\.
        /_.)  )..-  __..--'\\
       (    __..--''
        '-''\@


 Ⓐ Ⓐ Ⓐ Ⓐ Ⓐ Ⓐ Ⓐ Ⓐ Ⓐ Ⓐ Ⓐ
*/

trait GravityScalatraServlet extends ScalatraServlet {
  /** Sets the content type of the current response. */
  def contentType_=(mimeType: MimeType) {
    response.setContentType(mimeType.contentType)
  }
}
