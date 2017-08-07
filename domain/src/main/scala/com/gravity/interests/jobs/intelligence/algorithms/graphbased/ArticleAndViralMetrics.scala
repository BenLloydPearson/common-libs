package com.gravity.interests.jobs.intelligence.algorithms.graphbased

import com.gravity.interests.jobs.intelligence.{ViralMetrics, StandardMetrics, ArticleKey}
import org.joda.time.DateTime

case class ArticleAndViralMetrics(key: ArticleKey, url: String, title: String, publishDate: DateTime, metrics: StandardMetrics, viralMetrics: ViralMetrics, category: String = "", summary: String = "", image: String = "")
