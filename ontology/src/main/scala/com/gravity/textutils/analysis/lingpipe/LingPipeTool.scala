package com.gravity.textutils.analysis.lingpipe

/**
 * Created by IntelliJ IDEA.
 * Author: Robbie Coleman
 * Date: 5/20/11
 * Time: 6:48 PM
 */
import scala.collection.JavaConversions._
import com.gravity.utilities.ScalaMagic._
import com.gravity.utilities.grvstrings
import java.io.ObjectInputStream
import com.aliasi.tokenizer.{IndoEuropeanTokenizerFactory, RegExTokenizerFactory}
import com.aliasi.sentences.{IndoEuropeanSentenceModel, SentenceChunker}
import scala.collection.mutable.ListBuffer
import com.gravity.textutils.analysis.NLPSentenceInstance
import com.aliasi.hmm.{HmmCharLmEstimator, HmmDecoder, HiddenMarkovModel}
import com.aliasi.chunk._
import com.aliasi.lm.{NGramBoundaryLM, NGramProcessLM}

object LingPipeTool {

  lazy val hmm = new ObjectInputStream(getClass.getResourceAsStream("pos-en-general-brown.HiddenMarkovModel")).readObject().asInstanceOf[HiddenMarkovModel]
  lazy val chunkerModel = new ObjectInputStream(getClass.getResourceAsStream("ne-en-news-muc6.AbstractCharLmRescoringChunker")).readObject().asInstanceOf[AbstractCharLmRescoringChunker[CharLmHmmChunker,NGramProcessLM,NGramBoundaryLM]]
  lazy val decoder = new HmmDecoder(hmm)
  lazy val whiteSpaceTokenizerFactory = new RegExTokenizerFactory("(-|'|\\d|\\p{L})+|\\S")
  lazy val sentenceTokenizerFactory = IndoEuropeanTokenizerFactory.INSTANCE
  lazy val chunker = new HmmChunker(whiteSpaceTokenizerFactory, decoder)
  lazy val sentenceModel = {
    val forceFinalTokenSinceMostOfOurDataIsFromTheInternetAndIllFormed = true
    val balanceParens = false
    new IndoEuropeanSentenceModel(forceFinalTokenSinceMostOfOurDataIsFromTheInternetAndIllFormed, balanceParens)
  }
  lazy val sentenceChunker = new SentenceChunker(sentenceTokenizerFactory, sentenceModel)

  def tokenizeGrams(text: String): List[String] = {
    if (isNullOrEmpty(text)) return Nil

    val cs = text.toCharArray
    whiteSpaceTokenizerFactory.tokenizer(cs, 0, cs.length).tokenize().toList
  }

  def tagWithPOS(tokens: List[String]) = {
    decoder.tag(tokens)
  }

  def tagNBest(tokens: List[String], maxResults: Int = 5) = {
    decoder.tagNBest(tokens, maxResults)
  }

  def tagMarginal(tokens: List[String]) = {
    decoder.tagMarginal(tokens)
  }

  def getChunks(text: String) = {
    val chunking = chunkerModel.chunk(text)
    val chunkEvaluator = new ChunkerEvaluator(chunkerModel)
    chunkEvaluator.handle(chunking)
    chunkEvaluator.evaluation().truePositiveSet()
  }

  def extractSentences(text: String) = {
    val tokenizer = sentenceTokenizerFactory.tokenizer(text.toCharArray, 0, text.length())
    val tokenBuffer = new ListBuffer[String]
    val whitespaceBuffer = new ListBuffer[String]

    tokenizer.tokenize(tokenBuffer, whitespaceBuffer)

    val tokens = tokenBuffer.toList.toArray
    val whites = whitespaceBuffer.toList.toArray

    val sentenceBounds = sentenceModel.boundaryIndices(tokens, whites)

    val sentenceBuffer = new ListBuffer[NLPSentenceInstance]
    sentenceBuffer.sizeHint(sentenceBounds.length)

    val tokenWhites = tokenBuffer.zip(whitespaceBuffer.drop(1))

    var startTokIdx = 0
    var startSentIdx = 0
    for (endTokIdx <- sentenceBounds) {

      val sentence = (tokenWhites.subList(startTokIdx, endTokIdx).foldLeft(new StringBuilder) {
        (sb, parts) => sb.append(parts._1).append(parts._2)
      }).append(tokenBuffer(endTokIdx)).toString()
      val endOfSent = startSentIdx + sentence.length()
      sentenceBuffer += NLPSentenceInstance(sentence, startSentIdx, endOfSent)
      startTokIdx = endTokIdx + 1
      startSentIdx = endOfSent + 1
    }

    sentenceBuffer.toList
  }

  def extractPhrases(text: String, minGramSize: Int, maxGramSizeInclusive: Int) = {
    val sentences = extractSentences(text)
    new GrvHmmChunker(whiteSpaceTokenizerFactory, decoder).extractPhrases(text, sentences, minGramSize, maxGramSizeInclusive)

  }
}