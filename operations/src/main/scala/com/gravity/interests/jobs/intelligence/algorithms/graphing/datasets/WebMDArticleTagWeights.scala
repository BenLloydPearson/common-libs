package com.gravity.interests.jobs.intelligence.algorithms.graphing.datasets

import scala.collection.mutable
import com.gravity.utilities.grvio

/**
 * Created with IntelliJ IDEA.
 * User: apatel
 * Date: 7/29/13
 * Time: 6:57 PM
 * To change this template use File | Settings | File Templates.
 */
object WebMDArticleTagWeights {
  private lazy val urlToTagWeightMap = readUrlToTagWeightFile

  def getTagWeight(url: String, tag: String): Int = {

    val tagWeights = urlToTagWeightMap.getOrElse(url, Map.empty[String, Int])
    tagWeights.getOrElse(tag, 1)
  }


  private def readUrlToTagWeightFile: (mutable.HashMap[String /*url*/ , mutable.HashMap[String /*Tag*/ , Int /*weightScore*/ ]]) = {
    val urlToTagWeights = mutable.HashMap[String, mutable.HashMap[String, Int]]()

    grvio.perResourceLine(getClass, "urlsToTagsAndWeights.tsv") {
      line =>
        val row = line.split("\t").toList
        try {
          val url = row(0).trim()
          val tag = row(1).trim()
          val weight = row(2).trim()
          val weightScore = {
            if (weight == "Low") {
              1
            }
            else if (weight == "Ave") {
              5
            }
            else if (weight == "High") {
              10
            }
            else {
              1
            }
          }

          val tagMap = urlToTagWeights.getOrElse(url, mutable.HashMap.empty[String, Int])
          tagMap(tag) = weightScore
          urlToTagWeights(url) = tagMap
        }
        catch {
          case e: Exception =>
        }
    }

    urlToTagWeights
  }
}
