package com.gravity.interests.jobs.intelligence.operations

import scala.collection.Set
import com.gravity.utilities.grvstrings._

case class  ArticleText(keywords: String, content: String, tags: Set[String], summary: String = emptyString)
