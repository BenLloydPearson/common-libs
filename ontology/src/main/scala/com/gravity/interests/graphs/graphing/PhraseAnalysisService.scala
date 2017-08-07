package com.gravity.interests.graphs.graphing

import gnu.trove.map.hash.TLongLongHashMap
import gnu.trove.map.TLongLongMap
import com.gravity.utilities.cache.PermaCacher
import com.gravity.utilities.MurmurHash
import com.gravity.utilities.grvstrings._
import com.gravity.interests.jobs.hbase.HBaseConfProvider
import com.gravity.interests.jobs.intelligence.helpers.grvhadoop

import scala.collection.mutable

/**
 * Created with IntelliJ IDEA.
 * User: apatel
 * Date: 10/8/13
 * Time: 3:24 PM
 */
object PhraseAnalysisService {
 import com.gravity.logging.Logging._

  private val emptyMap = new TLongLongHashMap()
  private val useFrequencyMap = true
  private val phraseHdfsPath = "/user/gravity/reports/phrase-frequency.csv"
  private val delim = '\t'

  def phraseFrequencyMap: TLongLongMap = {
    var cnt = 0
    if (useFrequencyMap) {

      PermaCacher.getOrRegister("phrase-frequency-map", {
        val troveMap = new gnu.trove.map.hash.TLongLongHashMap(1200000, 0.5f, 0l, 1l)

        try {
          grvhadoop.perHdfsLine(HBaseConfProvider.getConf.fs, phraseHdfsPath) {
            //        com.gravity.utilities.grvio.perLine("/Users/apatel/tfidf/phrase-frequency.csv"){
            line =>
              line.split(delim) match {
                case Array(phrase, count) => {
                  cnt += 1
                  troveMap.put(getPhraseId(phrase), count.tryToLong.getOrElse {
                    warn(s"Phrase: '$phrase' had a non-numeric count value of '$count' so we'll use ZERO")
                    0L
                  })
                }
                case _ =>
              }
          }
        } catch {
          case ex: Exception =>
            warn(ex, s"Failed to laod phrase-frequency-map from HDFS path: '$phraseHdfsPath', so the PhraseAnalysisService will fail to function!")
        }

        troveMap.compact()
        info("Phrase Frequency Map: Loaded {0} phrases", troveMap.size())
        info("Raw Phrases: {0}", cnt)

        troveMap
      }, 60 * 60 * 6)
    } else {
      emptyMap
    }
  }

  def getPhraseId(phrase: String): Long = {
    MurmurHash.hash64(phrase)
  }

  def frequencyForPhrase(phrase: String): Long = {
    phraseFrequencyMap.get(getPhraseId(phrase))
  }

  def phraseTfIdf(phrase: String, phraseCountInDoc: Int, totalPhrasesInDocument: Int) = {
    val maxFrequencyForPhrases = 100000
    val minFrequencyForPhrases = 7

    val phraseFreq = frequencyForPhrase(phrase)
    if (phraseFreq > maxFrequencyForPhrases || phraseFreq < minFrequencyForPhrases) {
      0.0D
    }
    else {
      tfidf(phraseCountInDoc, totalPhrasesInDocument, 8000000, phraseFreq)
    }
  }

  def tfidf(phraseCountInDocument: Int, phrasesInDocument: Int, totalDocuments: Int, totalDocumentsWithPhrase: Long) = {
    val score = (phraseCountInDocument.toDouble / phrasesInDocument.toDouble) * math.log(totalDocuments.toDouble / totalDocumentsWithPhrase.toDouble)
    if (score.isInfinity || score.isNaN) {
      0.0
    }
    else {
      score
    }
  }

}

case class PhraseTfIdfScorer(phraeCountMap: mutable.HashMap[String, Int]) {

  val phraseFrequencyMap = PhraseAnalysisService.phraseFrequencyMap
  val totalPhrases = phraeCountMap.size
  val tfidScoreMap = phraeCountMap.map{ case (phrase, count) => phrase -> phraseTfIdf(phrase, count, totalPhrases)}

  def getTfidfScore(phrase: String) : Option[Double] = {
    tfidScoreMap.get(phrase)
  }

  def phraseTfIdf(phrase: String, phraseCountInDoc: Int, totalPhrasesInDocument: Int) = {
    val maxFrequencyForPhrases = 100000
    val minFrequencyForPhrases = 7

    val phraseFreq = frequencyForPhrase(phrase)
    if (phraseFreq > maxFrequencyForPhrases || phraseFreq < minFrequencyForPhrases) {
      0.0D
    }
    else {
      PhraseAnalysisService.tfidf(phraseCountInDoc, totalPhrasesInDocument, 8000000, phraseFreq)
    }
  }

  def frequencyForPhrase(phrase: String): Long = {
    phraseFrequencyMap.get(PhraseAnalysisService.getPhraseId(phrase))
  }

}