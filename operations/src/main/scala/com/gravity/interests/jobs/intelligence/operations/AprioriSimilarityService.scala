package com.gravity.interests.jobs.intelligence.operations

import com.gravity.interests.jobs.hbase.HBaseConfProvider
import com.gravity.interests.jobs.intelligence._

/**
 * User: mtrelinski
 * Date: 7/1/13
 */

object AprioriSimilarityService {
  implicit val conf = HBaseConfProvider.getConf.defaultConf

  def fetchRelationships(url: String, items: Seq[HasArticleKey]) = {
    val myKey = ArticleKey(url)
    val lookupSet = items.map(articleItem => ArticleArticleKey.optimalOrdering(myKey, articleItem.articleKey)).toSet
    val scores = Schema.ArticlesToArticles.query2.withKeys(lookupSet).withFamilies(_.relatedScores).withColumn(_.aprioriScore).multiMap(skipCache = false, returnEmptyRows = true)
    scores.map { keyAndScore =>
                    val (key, row) = keyAndScore
                    (key.article1, key.article2) -> row.aprioriScore }
  }

  def fetchManyRelationshipsAndAggregate(articles: Set[ArticleKey], items: Seq[HasArticleKey]) = {
    val lookupSet = for {article <- articles
                         item <- items} yield ArticleArticleKey.optimalOrdering(article, item.articleKey)
    val scores = Schema.ArticlesToArticles.query2.withKeys(lookupSet).withFamilies(_.relatedScores).withColumn(_.aprioriScore).multiMap(skipCache = false, returnEmptyRows = true)
    val myMap = scala.collection.mutable.HashMap[ArticleKey, Double]()
    scores.foreach {
      aScore =>
        val articleKey = if (articles.contains(aScore._1.article1)) aScore._1.article2 else aScore._1.article1
        val thisScore = (if (myMap.contains(articleKey)) myMap.getOrElse(articleKey, 0.0d) else 0.0d) + aScore._2.aprioriScore
        myMap.put(articleKey, thisScore)
    }

    myMap
  }

  def fetchManyRelationshipsAndAggregateNormalized(articles: Set[ArticleKey], items: Seq[HasArticleKey]) = {
    val lookupSet = for {article <- articles
                         item <- items} yield ArticleArticleKey.optimalOrdering(article, item.articleKey)
    val scores = Schema.ArticlesToArticles.query2.withKeys(lookupSet).withFamilies(_.relatedScores).withColumns(_.aprioriScoreNormalized, _.aprioriScore).multiMap(skipCache = false, returnEmptyRows = true)
    val normalized = scala.collection.mutable.HashMap[ArticleKey, Double]()
    val raw = scala.collection.mutable.HashMap[ArticleKey, Double]()
    scores.foreach {
      aScore =>
        val articleKey = if (articles.contains(aScore._1.article1)) aScore._1.article2 else aScore._1.article1
        val thisScore = (if (normalized.contains(articleKey)) normalized.getOrElse(articleKey, 0.0d) else 0.0d) + aScore._2.aprioriScoreNormalized
        val thisRaw = (if (raw.contains(articleKey)) raw.getOrElse(articleKey, 0.0d) else 0.0d) + aScore._2.aprioriScore
        normalized.put(articleKey, thisScore)
        raw.put(articleKey, thisRaw)
    }

    (normalized, raw)
  }

}
