package com.gravity.ontology

import org.apache.commons.lang.StringUtils
import org.joda.time.DateTime
import com.gravity.interests.graphs.graphing.{Weight, ContentType, Phrase, ContentToGraph}

object Implicits {
  /**
   * Upgrade a String to a Phrase, a class used throughout ontologizing.
   */
  implicit def dummyPhrase(str: String): Phrase = {
    val dummyContent = ContentToGraph("", new DateTime, str, Weight.Medium, ContentType.Article)
    val dummyGrams = StringUtils.countMatches(str, " ")
    Phrase(str, dummyGrams, 1, dummyContent)
  }
}