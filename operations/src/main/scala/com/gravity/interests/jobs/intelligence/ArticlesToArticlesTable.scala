package com.gravity.interests.jobs.intelligence

import com.gravity.hbase.schema.{HRow, DeserializedResult, HbaseTable}
import com.gravity.interests.jobs.intelligence.SchemaTypes._
import scala.collection._
import com.gravity.interests.jobs.intelligence.SchemaContext._
import com.gravity.utilities.analytics.articles.ArticleWhitelist
import com.gravity.utilities.time.DateHour
import com.gravity.interests.jobs.intelligence.hbase.ConnectionPoolingTableManager

/**
 * Created with IntelliJ IDEA.
 * User: mtrelinski
 */

class ArticlesToArticlesTable extends HbaseTable[ArticlesToArticlesTable, ArticleArticleKey, ArticlesToArticlesRow]("article-to-article-scores", rowKeyClass=classOf[ArticleArticleKey],tableConfig=SchemaContext.defaultConf) {

  override def rowBuilder(result: DeserializedResult) = new ArticlesToArticlesRow(result, this)

  // 432000 seconds = 5 days
  val relatedScores = family[String, Any]("rs",compressed=true, rowTtlInSeconds = 432000)

  val aprioriScore = column(relatedScores, "a", classOf[Double])
  val aprioriScoreNormalized = column(relatedScores, "an", classOf[Double])

}

class ArticlesToArticlesRow(result: DeserializedResult, table: ArticlesToArticlesTable) extends HRow[ArticlesToArticlesTable, ArticleArticleKey](result, table) {

  lazy val aprioriScore = column(_.aprioriScore).getOrElse(0.0)
  lazy val aprioriScoreNormalized = column(_.aprioriScoreNormalized).getOrElse(0.0)

}

