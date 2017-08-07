package com.gravity.interests.jobs.intelligence.operations.recommendations

//holds normalized LSH article click cluster scores
case class LSHArticleClickScore(articleId: Long, userClickScore: Int, decayedUserClickScore: Double)
