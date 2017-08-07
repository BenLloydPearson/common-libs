package com.gravity.textutils.analysis

/**
 * Created by IntelliJ IDEA.
 * User: robbie
 * Date: 5/2/11
 * Time: 10:49 AM
 */
import scala.collection.JavaConversions._
import opennlp.tools.sentdetect.{SentenceDetectorME, SentenceModel}
import opennlp.tools.postag.{POSTaggerME, POSModel}
import opennlp.tools.chunker.{ChunkerME, ChunkerModel}

object NLPWrapper {
  private val sentenceModel = new SentenceModel(NLPResources.sentenceBin)
  private val sentenceDetector = new SentenceDetectorME(sentenceModel)
  private val posModel: POSModel = new POSModel(NLPResources.posBin)
  private val posTagger = new POSTaggerME(posModel)
  private val chunkerModel: ChunkerModel = new ChunkerModel(NLPResources.chunkerBin)
  private val chunker = new ChunkerME(chunkerModel)



  def detectSentences(text: String) = {
    sentenceDetector.synchronized {
      sentenceDetector.sentDetect(text)
    }
  }

  def extractSentences(text: String) = {

    val sentences = detectSentences(text)
    val spans = {sentenceDetector.synchronized {sentenceDetector.sentPosDetect(text)}}

    val combined = sentences.zip(spans)

    for {
      (sentence, span) <- combined
      if (sentence.length > 0 && !Patterns.matchesSentenceExceptions(sentence))
    } yield NLPSentenceInstance(sentence, span.getStart, span.getEnd)
  }

  def getPOSTags(sentence: Seq[String]) = posTagger.synchronized{posTagger.tag(sentence)}

  def getChunks(tokens: Seq[String], tags: Seq[String]) = chunker.synchronized{ chunker.chunk(tokens, tags) }
}

case class NLPSentenceInstance(sentence: String, start: Int, end: Int)

object NLPResources {
  val sentenceBin = getClass.getResourceAsStream("en-sent.bin")
  val posBin = getClass.getResourceAsStream("en-pos-maxent.bin")
  val chunkerBin = getClass.getResourceAsStream("en-chunker.bin")
}