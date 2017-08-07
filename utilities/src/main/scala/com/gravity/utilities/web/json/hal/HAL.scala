package com.gravity.utilities.web.json.hal

import play.api.libs.json.{Format, Json}

/**
 * Helpers for implementing the Hypertext Application Language (HAL) in JSON, for achieving HATEOAS.
 *
 * There is a Java library for generating these fields, but it appears to be a lot of runtime Factory
 * malarkey. As a Scala shop, can we do without? Can we instead know and document our fields at
 * compile-time?
 *
 * @see http://stateless.co/hal_specification.html
 * @see http://en.wikipedia.org/wiki/HATEOAS
 */
case class SimpleHalSelfLink(self: HalLink)

object SimpleHalSelfLink {
  implicit val jsonFormat: Format[SimpleHalSelfLink] = Json.format[SimpleHalSelfLink]

  val example = SimpleHalSelfLink(HalLink.example)

  def build(href: String, rel: String): SimpleHalSelfLink = SimpleHalSelfLink(HalLink(href, rel))
}

case class HalLink(href: String, rel: String, title: String = "", templated: Boolean = false)

object HalLink {
  implicit val jsonFormat: Format[HalLink] = Json.format[HalLink]

  val example = HalLink("href", "rel")
}