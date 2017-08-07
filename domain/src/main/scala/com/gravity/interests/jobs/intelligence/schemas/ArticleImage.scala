package com.gravity.interests.jobs.intelligence.schemas

import play.api.libs.json.{Json, Format}

case class ArticleImage(path: String = "", height: Int = -1, width: Int = -1, altText: String = "")
object ArticleImage {
  implicit val jsonFormat: Format[ArticleImage] = Json.format[ArticleImage]
}
