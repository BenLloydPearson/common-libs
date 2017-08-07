package com.gravity.data.reporting

import play.api.libs.json.{Format, Json}

case class ContentQualityReporting(index: String)
object ContentQualityReporting {
  implicit val jsonFormat: Format[ContentQualityReporting] = Json.format[ContentQualityReporting]

  val example = ContentQualityReporting("24.82")

  val empty = ContentQualityReporting("-")
}
