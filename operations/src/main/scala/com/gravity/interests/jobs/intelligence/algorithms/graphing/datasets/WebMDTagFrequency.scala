package com.gravity.interests.jobs.intelligence.algorithms.graphing.datasets

import scala.collection.mutable
import com.gravity.utilities.grvio
import com.gravity.interests.jobs.hbase.HBaseConfProvider
import com.gravity.interests.jobs.intelligence.helpers.grvhadoop

/**
 * Created with IntelliJ IDEA.
 * User: apatel
 * Date: 7/27/13
 * Time: 11:16 PM
 * To change this template use File | Settings | File Templates.
 */

object WebMDTagFrequency {

  private lazy val tagFreqMap = readTagFrequency

  private lazy val tagBlacklist = readTagBlacklist

  private lazy val urlBlackList = readUrlBlacklist

  def isBlacklisted(tag: String): Boolean = {
    if (tagBlacklist.contains(tag)) {
      println("BLACKLISTING TAG:" + tag)
      return true
    }
    else return false
  }

  def isUrlBlacklisted(url: String): Boolean = {
    if (urlBlackList.contains(url)) {
      println("BLACKLISTING URL:" + url)
      return true
    }
    else return false
  }

  def getFrequency(tag: String): Int = {
    tagFreqMap.getOrElse(tag, 1)
  }

  def tfidfScore(tagName: String, nodeCountInDocument: Int, nodesInDocument: Int) = {
    val totalDocuments = 8000000
    val totalDocumentsWithNode = getFrequency(tagName)

    val score = (nodeCountInDocument.toDouble / nodesInDocument.toDouble) * math.log(totalDocuments.toDouble / totalDocumentsWithNode.toDouble)
    if (score.isInfinity || score.isNaN) {
      0.0
    } else
      score
  }

  private def readTagFrequencyFromHdfs: (mutable.HashMap[String, Int]) = {
    val tagFrequency = mutable.HashMap[String, Int]()

    grvhadoop.perHdfsLine(HBaseConfProvider.getConf.fs, "/user/gravity/reports/tag-frequency.csv") {
      line =>
        line.split("\t") match {
          case Array(tag, freq) => {
            try {
              tagFrequency(tag) = freq.toInt
            }
            catch {
              case e: Exception =>
            }
          }
          case _ =>
        }
    }
    tagFrequency
  }

  private def readTagFrequency: (mutable.HashMap[String, Int]) = {
    val tagFrequency = mutable.HashMap[String, Int]()

    grvio.perResourceLine(getClass, "/com/gravity/interests/jobs/intelligence/algorithms/graphing/datasets/webMDTagTFIDF3.csv") {
      line =>
        val tagAndScore = line.split(",").toList
        try {
          val tag = tagAndScore(0)
          val freq = tagAndScore(1).toInt
          tagFrequency(tag) = freq
        }
        catch {
          case e: Exception =>
        }
    }

    tagFrequency
  }

  private def readTagBlacklist: (mutable.HashSet[String]) = {
    val tagBlacklist = mutable.HashSet[String]()

    grvio.perResourceLine(getClass, "/com/gravity/interests/jobs/intelligence/algorithms/graphing/datasets/webMDTagBlacklist.csv") {
      line =>
        val tag = line.trim()
        tagBlacklist += tag.toLowerCase()
    }

    tagBlacklist
  }

  private def readUrlBlacklist: (mutable.HashSet[String]) = {
    val urlBlackList = mutable.HashSet[String]()

    grvio.perResourceLine(getClass, "/com/gravity/interests/jobs/intelligence/algorithms/graphing/datasets/webMDUrlBlacklist.csv") {
      line =>
        val url = line.trim()
        urlBlackList += url.toLowerCase()
    }

    urlBlackList
  }
}
