package com.gravity.utilities.cache.user

import com.gravity.domain.user.UserGraphs

/**
  * Created by apatel on 5/12/16.
  */
trait UserGraphCache[T] {

  /**
    * Uses cache first and on a miss uses source object to create user graphs
    *
    * @param sourceObject - something to create user graphs from (like ArticleRecommendationContext)
    * @param maxArticles
    * @return Option[UserGraphs]
    */
  def get(sourceObject: T, maxArticles: Int, minTfidfScore: Double): UserGraphs

  /**
    * creates UserGraphs from source object
    *
    * @param sourceObject
    * @param maxArticles
    * @return
    */
  def createUserGraphs(sourceObject: T, maxArticles: Int, minTfidfScore: Double) : UserGraphs


}
