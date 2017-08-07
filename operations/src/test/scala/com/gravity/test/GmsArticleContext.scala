package com.gravity.test

import play.api.libs.json.Json

case class GmsArticleContext(
  articleId: Long,
  millis: Long,
  impressions: Long,
  clicks: Long,
  ctr: Double)
object GmsArticleContext {
  implicit val jsonFormat = Json.format[GmsArticleContext]
}

case class GmsArticleContexts(contexts: Seq[GmsArticleContext])
