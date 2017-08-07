package com.gravity.domain.articles

import com.gravity.utilities.web.HtmlTagFilter

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
 * Article pixels are bits of arbitrary HTML -- usually provided by third parties, such as publishers -- that are
 * dropped on a per-article basis in widgets according to certain events such as impression, click, etc. They typically
 * call out to third party analytics mechanisms to help reconcile article event metrics against our dashboard metrics.
 */
object ArticlePixel {
  /** Whitelist of allowed HTML tags in pixel HTML. */
  val htmlTagFilter: HtmlTagFilter = new HtmlTagFilter(isWhitelist = true, Set("img", "script", "noscript"))

  val exampleValue = "<img src='http://example.com/tracking-pixel' />"

  def populateMacros(pixelHtml: String): String =
    pixelHtml.replaceAllLiterally("[[timestamp]]", System.currentTimeMillis().toString)
}
