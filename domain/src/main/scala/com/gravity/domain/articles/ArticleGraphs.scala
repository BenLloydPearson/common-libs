package com.gravity.domain.articles

import scala.collection.mutable.HashMap

/**
  * Created by apatel on 5/10/16.
  */
case class ArticleGraphs(tfIdfGraphVector: HashMap[Long, Double], tfidfPhraseGraphVector: HashMap[Long, Double], tagGraphVector: HashMap[Long, Double], combinedGraphVector: HashMap[Long, Double])

