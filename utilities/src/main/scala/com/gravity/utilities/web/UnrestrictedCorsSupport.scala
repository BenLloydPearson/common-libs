package com.gravity.utilities.web

import org.scalatra.{CorsSupport, ScalatraBase}

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
  * Mix this into a servlet to provide CORS access from anywhere with any headers. We would typically do this for
  * totally public APIs or APIs that might be called via GDK (i.e. deployed anywhere, called from any client).
  *
  * @see http://www.scalatra.org/2.4/guides/web-services/cors.html
  */
trait UnrestrictedCorsSupport extends CorsSupport {
  this: ScalatraBase =>

  options("/*") {
    response.setHeader("Access-Control-Allow-Headers", request.getHeader("Access-Control-Request-Headers"))
  }
}
