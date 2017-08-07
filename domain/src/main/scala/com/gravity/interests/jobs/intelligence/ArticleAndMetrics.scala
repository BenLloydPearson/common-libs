package com.gravity.interests.jobs.intelligence

import org.joda.time.DateTime

/**
 * @category = the section of the site the article was found in
 *
 */
case class ArticleAndMetrics(key: ArticleKey, url: String, title: String, publishDate: DateTime, metrics: StandardMetrics, category: String = "", summary: String = "", image: String = "")
