package com.gravity.utilities.cache.article

import com.gravity.domain.articles.ArticleGraphs
import com.gravity.interests.jobs.intelligence.ArticleKey

/**
  * Created by apatel on 5/10/16.
  */
trait ArticleGraphCache[T]{

  /**
    * Uses ONLY cache and returns None if it does not exist
    *
    * @param articleKey - article to fetch
    * @param siteGuid - allows for site level configuration
    * @return Option[ArticleGraphs]
    */
  def getFromCache(articleKey: ArticleKey, siteGuid:String): Option[ArticleGraphs]

  /**
    * Uses Cache first and if not found, generates/extracts graphs from provide source object
    *
    * @param articleKey - article to fetch
    * @param siteGuid - allows for site level configuration
    * @param sourceObject - object that supports extracting needed article graphs
    * @param minTfidfScore - min tfidf score required for creating tag graph
    * @tparam T - type of object that supports extracting needed article graphs (like ArticleCandidate, ArticleCandidateReference, ArticleRow, etc)
    * @return Option[ArticleGraphs]
    */
  def getOrElseExtract(articleKey: ArticleKey, siteGuid: String)(sourceObject: T, minTfidfScore: Double): Option[ArticleGraphs]

}

