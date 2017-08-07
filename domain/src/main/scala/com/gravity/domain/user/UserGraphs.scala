package com.gravity.domain.user

import org.joda.time.DateTime
import scala.collection.mutable._

/**
  * Created by apatel on 5/12/16.
  */
case class UserGraphs(tfidfGraphVector: HashMap[Long, Double], tfidfPhraseGraphVector: HashMap[Long, Double], tagGraphVector: HashMap[Long, Double], combinedGraphVector: HashMap[Long, Double], latestTimestamp: Option[DateTime])