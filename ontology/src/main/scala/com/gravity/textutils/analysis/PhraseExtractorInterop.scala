package com.gravity.textutils.analysis

/**
 * Created by IntelliJ IDEA.
 * User: robbie
 * Date: 4/13/11
 * Time: 10:00 AM
 */

import scala.collection.JavaConversions._
import scala.collection._
import java.lang.String
import java.util.{Set => juSet}
import org.apache.commons.lang.StringUtils
import collection.mutable.{HashSet, ListBuffer}
import com.google.common.collect.{HashMultiset, Multiset}

object PhraseExtractorInterop {
  val phraseWindow = 3
  val emptyMultiSet: Multiset[String] = HashMultiset.create()

  val Extractor: ThreadLocal[PhraseExtractor] = new ThreadLocal[PhraseExtractor] {
    override def initialValue(): PhraseExtractor = new PhraseExtractorImpl
  }

  def getInstance = Extractor.get

  val legacyExtractor = new LegacyPhraseExtractorImpl

  def getLegacyExtractor: LegacyPhraseExtractor = legacyExtractor

//  def upgradeExtractions(extractions: java.util.List[PhraseExtraction]): java.util.Set[String] = {
//    val exts = new ListBuffer[ExtractionSignificance]
//    for (ext <- extractions) {
//      exts.add(ExtractionSignificance(ext.getPhrases))
//    }
//
//    val results = getInstance.upgradeExtractions(exts)
//
//    return results.toUtilSet
//  }

  def compileExtractions(text: String): java.util.List[PhraseExtraction] = {
    val extractions = new java.util.ArrayList[PhraseExtraction]
    val originalExtractions = getInstance.compileExtractions(text)
    for (ext <- originalExtractions) {
      extractions.add(new PhraseExtraction(ext.phrases.toArray))
    }

    return extractions
  }

  def upgradeMultiset(phrases: Multiset[String], text: String) = {
    phrases.entrySet.flatMap{ entry =>
      val phrase = entry.getElement
      val pos = Seq.empty // ah well
      val gramSize = phrase.split("\\s+").size // meh
      var startIndex = -1
      0 until(entry.getCount) map { _ =>
        startIndex = text.indexOf(phrase, startIndex + 1)
        PhraseInstance(phrase, pos, gramSize, startIndex, startIndex + phrase.size - 1, text)
      }
    }.toSeq.sortBy(_.startIndex)
  }

  sealed class LegacyPhraseExtractorImpl extends LegacyPhraseExtractor {
    def extract(text: String, minGramSize: Int, maxGramSizeInclusive: Int, level: ExtractionLevel, matchedGrams: juSet[String]): Multiset[String] = {
      getInstance.extractLegacy(text, minGramSize, maxGramSizeInclusive, level, matchedGrams)
    }

    def extract(text: String, minGramSize: Int, maxGramSizeInclusive: Int, level: ExtractionLevel): Multiset[String] = {
      getInstance.extractLegacy(text, minGramSize, maxGramSizeInclusive, level)
    }
  }

}

trait PhraseExtractor {
  def extractLegacy(text: String, minGramSize: Int, maxGramSizeInclusive: Int, extractionLevel: ExtractionLevel): Multiset[String]

  def extractLegacy(text: String, minGramSize: Int, maxGramSizeInclusive: Int, extractionLevel: ExtractionLevel, matchedGrams: Set[String]): Multiset[String]

  def extractSentences(text: String): Seq[String]

  def upgradeExtractions(extractions: ListBuffer[ExtractionSignificance]): HashSet[String]

  def compileExtractions(text: String): ListBuffer[ExtractionSignificance]
}


protected sealed class PhraseExtractorImpl extends PhraseExtractor {

  import Patterns._

  override def extractLegacy(text: String, minGramSize: Int, maxGramSizeInclusive: Int, extractionLevel: ExtractionLevel): Multiset[String] = extractLegacy(text, minGramSize, maxGramSizeInclusive, extractionLevel, HashSet.empty[String])

  override def extractLegacy(text: String, minGramSize: Int, maxGramSizeInclusive: Int, extractionLevel: ExtractionLevel, matchedGrams: Set[String]): Multiset[String] = {
    if (text == null || text.trim.length == 0) return PhraseExtractorInterop.emptyMultiSet

    val trimmedText = text.trim

    // Run the appropriate extraction
    if (extractionLevel == ExtractionLevel.NGRAMS) {
      if (minGramSize > maxGramSizeInclusive) throw new RuntimeException("The minGramSize must be less than the maxGramSizeInclusive")

      val grams = HashMultiset.create[String]
      for (windowSize <- minGramSize to maxGramSizeInclusive) grams.addAll(extractNgramsToMultiset(trimmedText, windowSize))
      val gramIter = grams.iterator
      while (gramIter.hasNext) {
        val gram = gramIter.next
        if (PhraseExtractorUtil.isBlacklistedGram(gram)) gramIter.remove()
      }

      return grams
    } else if (extractionLevel == ExtractionLevel.PHRASES) {
      return extractPhrases(text, minGramSize, maxGramSizeInclusive, matchedGrams)
    } else {
      throw new RuntimeException("Invalid Extraction Level")
    }


  }

  def extractNgramsToMultiset(text: String, windowSize: Int): Multiset[String] = {

    val grams = HashMultiset.create[String]

    val splitText = Patterns.splitOnWhiteSpace(text)

    for ((txt, i) <- splitText.zipWithIndex) {
      // Unigrams are a special case
      if (windowSize == 1) {
        grams.add(removePunctuation(txt))
      } else {
        val expectedEnd = i + windowSize
        if (expectedEnd <= splitText.length) {
          val phrases = new ListBuffer[String]
          for (j <- i until expectedEnd) phrases.add(removePunctuation(splitText(j)))
          grams.add(phrases.mkString(Patterns.SPACE).trim)
        }
      }
    }

    return grams
  }

  def compileExtractions(text: String): ListBuffer[ExtractionSignificance] = {
    val extractions = new ListBuffer[ExtractionSignificance]

    val sentences = extractSentences(text)
    for (sentenceRaw <- sentences) {
      import Patterns._
      val boundaries = new ListBuffer[PhraseBoundary]
      val sNoUrls = removeUrls(sentenceRaw)
      val sentence = prepSentenceForTagging(sNoUrls)

      val tokens = splitOnSpace(sentence)

      // Remove ending punctuation from every token in the sentence
      for ((token, i) <- tokens.zipWithIndex) {
        tokens(i) = removePunctuationAtTheEnd(token)
      }

      val posTags = PhraseExtractorUtil.getPOSTags(tokens)
      val chunks = PhraseExtractorUtil.getChunks(tokens, posTags)

      // Calculate the phrase boundaries
      for ((token, i) <- tokens.zipWithIndex) {
        val posTag = posTags(i)
        val chunk = chunks(i)
        val boundary = new PhraseBoundary(token, posTag, chunk)

        // Check if the token can cross
        boundary.canCross = (
          !matchesComma(posTag) &&
              !PhraseExtractorUtil.isCrossWord(token) &&
              !B_VP.equals(chunk) &&
              !matchesWordCharacter(token))

        // Check if the token can start
        boundary.canStart =(B_NP.equals(chunk))
        if (i > 0) {
          if (I_NP.equals(chunk) && matchesCanStart(posTags(i - 1))) boundary.canStart = true
        }

        // Check if the token can end
        if (i != tokens.length - 1) {
          boundary.canEnd = !matchesNotEnd(chunks(i + 1))
        } else {
          boundary.canEnd = true
        }

        // Check the tags
        if (CC.equals(posTag)) {
          if (i > 0) boundaries(i - 1).canEnd = false
          boundary.canCross = false
        }

        if (i > 0 && CC.equals(posTags(i - 1))) {
          boundary.canStart = true
        }

        // Stop word check
        if (PhraseExtractorUtil.isStopWord(token)) {
          boundary.canEnd = false
          boundary.canStart = false
        }
        if (i != tokens.length - 1) {
          // Some stop words have single quotes that are exploded when we chunk - must scan ahead
          if (PhraseExtractorUtil.isStopWord(token + tokens(i + 1))) {
            boundary.canEnd = false
            boundary.canStart = false
          }
        }

        // Dangler quotes
        if (i > 0 && matchesDanglerQuote(token)) {
          boundary.canStart = false
          boundaries(i - 1).canEnd = false
        }

        boundary.canStart = (boundary.canStart && !matchesNotStart(posTag))
        boundary.canEnd = (boundary.canEnd && matchesCanEnd(posTag))
        boundaries.add(boundary)
      }

      // Identify phrases based upon the boundaries we have previously defined, and add all significant expressions.
      for {
        (startBoundary,start) <- boundaries.zipWithIndex
        if (startBoundary.canStart && startBoundary.canCross)
      } {
        val remainingBoundaries = boundaries.drop(start).take(PhraseExtractorInterop.phraseWindow)
        for {
          (endBoundary,endSubIndex) <- remainingBoundaries.zipWithIndex
          if (endBoundary.canCross && endBoundary.canEnd)
          curExt = ExtractionSignificance(tokens.slice(start, start + endSubIndex + 1))
          if (curExt.isSignificant)
        } extractions.add(curExt)
      }
    }

    return extractions
  }

  def extractPhrases(text: String, minGramSize: Int, maxGramSizeInclusive: Int, matchedGrams: Set[String]): Multiset[String] = {
    val phrases = HashMultiset.create[String]

    val extractions = compileExtractions(text)

    // Upgrade extractions to textutils
    val phraseReductions = upgradeExtractions(extractions)

    // Set the correct multiset counts
    for {
      phrase <- phraseReductions
      if (!PhraseExtractorUtil.isCommonUnigram(phrase))
      if (!PhraseExtractorUtil.isBlacklistedGram(phrase))
    } {
      val phraseCount = Patterns.splitOnSpace(phrase).length
      if (phraseCount >= minGramSize && phraseCount <= maxGramSizeInclusive && !matchedGrams.contains(phrase)) {
        phrases.add(phrase)
        phrases.setCount(phrase, StringUtils.countMatches(text, phrase))
      }
    }

    return phrases
  }

  def upgradeExtractions(extractions: ListBuffer[ExtractionSignificance]): HashSet[String] = {
    val phraseReductions = new HashSet[String]
    for ((ext, i) <- extractions.zipWithIndex) {
      val currentExtraction = ext
      var bestFit = currentExtraction.toString
      for {
        j <- i + 1 until extractions.size
        secondExtraction = extractions(j).toString
        if (secondExtraction.contains(bestFit))
      } {
        bestFit = secondExtraction
      }
      phraseReductions.add(bestFit)
    }

    return phraseReductions
  }

  def extractSentences(text: String): Seq[String] = {
    for {
      sentenceRaw <- PhraseExtractorUtil.detectSentences(text)
      sentence = sentenceRaw.trim
      if (sentence.length > 0 && !Patterns.matchesSentenceExceptions(sentence))
    } yield sentence
  }
}


















