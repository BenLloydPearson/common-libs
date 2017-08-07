package com.gravity.interests.jobs.intelligence.schemas

import com.gravity.interests.jobs.intelligence.ArticleKey
import org.joda.time.DateTime

case class DatedArticleViews(dateTime: DateTime, articleId: ArticleKey, doNotTrack: Boolean, referrer: ArticleKey)
