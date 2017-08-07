package com.gravity.interests.jobs.intelligence.operations

import com.gravity.interests.jobs.intelligence.{StandardMetrics, ViralMetrics, ArticleKey}
import org.joda.time.DateTime

case class ArticleAndDetails(articleKey: ArticleKey, url: String, title: String, totalShares: Long, viralMetrics: ViralMetrics, standardMetrics: StandardMetrics, datePublished: DateTime, domain: String, image: String, datePublishedString: String, recoScore: Double = 0, articleType: String = "Default")
