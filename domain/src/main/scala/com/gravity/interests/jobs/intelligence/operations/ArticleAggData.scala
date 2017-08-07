package com.gravity.interests.jobs.intelligence.operations

import com.gravity.interests.jobs.intelligence.ArticleKey

/**
  * Created by jengelman14 on 7/13/16.
  */
@SerialVersionUID(1l)
case class ArticleAggData(key: ArticleKey,
                          var impressions: Long = 0L,
                          var impressionViews: Long = 0L,
                          var clicks: Long = 0L,
                          var impressionDiscards: Long = 0L,
                          var impressionViewDiscards: Long = 0L,
                          var clickDiscards: Long = 0L) {

}
