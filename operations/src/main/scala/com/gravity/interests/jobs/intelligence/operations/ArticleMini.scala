package com.gravity.interests.jobs.intelligence.operations

import play.api.libs.json.Json

case class ArticleMini(id: String, publishTime: Long)
object ArticleMini {
  implicit val amFormat = Json.format[ArticleMini]
}