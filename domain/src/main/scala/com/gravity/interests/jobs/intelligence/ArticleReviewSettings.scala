package com.gravity.interests.jobs.intelligence

import com.gravity.utilities.ArticleReviewStatus

/**
 * Created by tdecamp on 12/16/15.
 * {{insert neat ascii diagram here}}
 */
@SerialVersionUID(1L)
case class ArticleReviewSettings(status: ArticleReviewStatus.Type = ArticleReviewStatus.defaultValue) {

}

object ArticleReviewSettings {

  val empty: ArticleReviewSettings = ArticleReviewSettings(ArticleReviewStatus.defaultValue)

}
