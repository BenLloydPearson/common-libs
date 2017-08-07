package com.gravity.interests.jobs.intelligence.schemas

import com.gravity.interests.jobs.intelligence.{ArticleTypes, SiteKey}

case class ArticleIngestionData(sourceSiteGuid: String, ingestionNotes: String, articleType: ArticleTypes.Type, isOrganicSource: Boolean) {
  def sourceSiteKey: SiteKey = SiteKey(sourceSiteGuid)

  override def toString: String = {
    val sb = new StringBuilder
    sb.append("source site: ").append(sourceSiteGuid)
      .append("(articleType: ").append(articleType)
      .append("; isOrganicSource: ").append(isOrganicSource)
      .append(") notes: ").append(ingestionNotes)
    sb.toString()
  }
}
