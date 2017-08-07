package com.gravity.utilities.web

case class HtmlLink(url: String, text: String)

object HtmlLink {
  def apply(url: String): HtmlLink = HtmlLink(url, url)
}