package com.gravity.interests.jobs.intelligence.operations.graphing

import java.net.URL

import com.gravity.utilities.analytics.URLUtils.NormalizedUrl
import com.gravity.utilities.grvstrings._
import play.api.libs.json.{Format, Json}
import org.joda.time.DateTime

case class HistoryLink(url: String, title: String, lastVisitedTimestamp: Long) {
  lazy val urlOption: Option[URL] = url.tryToURL
  lazy val isValid: Boolean = urlOption.isDefined
  lazy val urlNormalized: NormalizedUrl = NormalizedUrl(if (isValid) url else emptyString)
  lazy val lastVisited: DateTime = new DateTime(lastVisitedTimestamp)

  def validOption: Option[HistoryLink] = if (isValid) Some(this) else None
}

object HistoryLink {
  implicit val jsonFormat: Format[HistoryLink] = Json.format[HistoryLink]
}