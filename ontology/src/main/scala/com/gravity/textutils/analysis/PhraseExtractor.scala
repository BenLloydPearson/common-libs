package com.gravity.textutils.analysis

/**
 * Created by IntelliJ IDEA.
 * User: robbie
 * Date: 4/13/11
 * Time: 10:00 AM
 */

import com.gravity.utilities.grvmath

import scala.collection.JavaConversions._
import scala.collection._
import scala.collection.immutable.{Seq => immSeq}
import scala.collection.mutable.ListBuffer
import scala.io.Source

object MyTypes {
  type PhraseToIndexes = mutable.HashMap[String, mutable.HashSet[Int]]
}

object PhraseExtractorUtil {

  private val stopWords = loadWordsFromFile("stopwords")
  private val stopCrossWords = Set("after", "where", "when", "for", "at", "to", "with", "earlier")
  private val commonUnigrams = loadWordsFromFile("commonunigrams")
  private val blacklistedGrams = loadWordsFromFile("blacklisted_grams2.txt")

  private def loadWordsFromFile(path: String): Set[String] = {
    val results = new mutable.HashSet[String]
    try {
    val lines = Source.fromInputStream(getClass.getResourceAsStream(path)).getLines()
    for {
      line <- lines
      if line != null && line.length > 0
      lineTrimmed = line.trim
      if lineTrimmed.length > 0
    } results.add(lineTrimmed.toLowerCase)
    } catch {
      case ex:Exception =>
        println("ERROR During PhraseExtractorInitialization whilst loading path " + path)
        println(ex.getMessage)
        println(ex.getStackTrace.mkString("", "\n", "\n"))
    }
    results
  }

  def detectSentences(text: String) = NLPWrapper.detectSentences(text)

  def extractSentences(text: String) = NLPWrapper.extractSentences(text)

  def getPOSTags(sentence: Seq[String]) = NLPWrapper.getPOSTags(sentence)

  def getChunks(tokens: Seq[String], tags: Seq[String]) = NLPWrapper.getChunks(tokens, tags)

  def isStopWord(word: String) = stopWords.contains(word.toLowerCase)

  def isCrossWord(word: String) = stopCrossWords.contains(word.toLowerCase)

  def isCommonUnigram(phrase: String) = commonUnigrams.contains(phrase.toLowerCase)

  def isBlacklistedGram(phrase: String) = blacklistedGrams.contains(phrase.toLowerCase)

  def isMatchesCanEndTag(tag: String) = Patterns.matchesCanEnd(tag)

  def isMatchesCanNotEndTag(tag: String) = Patterns.matchesNotEnd(tag)

  def detectCanCross(token: String, posTag: String, chunk: String): Boolean = {
    !Patterns.matchesComma(posTag) &&
      !isCrossWord(token) &&
      !Patterns.B_VP.equals(chunk) &&
      !Patterns.matchesWordCharacter(token)
  }

  private def assertGramWindow(minGramSize: Int, maxGramSizeInclusive: Int) {
    if (minGramSize >= maxGramSizeInclusive) throw new RuntimeException("The minGramSize must be less than the maxGramSizeInclusive")
  }

  def extractNgrams(corpus: String, minGramSize: Int, maxGramSizeInclusive: Int, excludePhrases: Set[String] = Set.empty): PhraseExtractionResult = {
    assertGramWindow(minGramSize, maxGramSizeInclusive)

    val results = new ListBuffer[PhraseInstance]
    val splitUp = Patterns.splitAndCaptureOnWhiteSpace(corpus)
    val tokens = splitUp.tokens
    val length = tokens.length
    val posTags = getPOSTags(tokens)

    for (windowSize <- minGramSize to math.min(maxGramSizeInclusive, length)) {
      for (start <- 0 until length) {
        val end = math.min(start + windowSize, length)

        populatePhraseInstance(start, end, splitUp.captures, tokens, posTags, corpus, results, excludePhrases = excludePhrases)
      }
    }

    new PhraseExtractionResult(results)
  }

  def extractPhrases(corpus: String, minGramSize: Int, maxGramSizeInclusive: Int, excludePhrases: Set[String] = Set.empty): PhraseExtractionResult = {
    assertGramWindow(minGramSize, maxGramSizeInclusive)

    val instances = new ListBuffer[PhraseInstance]
    val sentences = extractSentences(corpus)

    for (sentInstance <- sentences) {
      import Patterns._
      val boundaries = new ListBuffer[PhraseBoundary]
      val sNoUrls = removeUrls(sentInstance.sentence)
      val sentence = prepSentenceForTagging(sNoUrls)
      val splitUp = Patterns.splitAndCaptureOnSpace(sentence)
      val tokens = splitUp.tokens
      val posTags = getPOSTags(tokens)
      val chunks = getChunks(tokens, posTags)

      // Calculate the phrase boundaries
      for ((token, i) <- tokens.zipWithIndex) {
        val posTag = posTags(i)
        val chunk = chunks(i)
        val boundary = new PhraseBoundary(token, posTag, chunk)

        // Check if the token can cross
        boundary.canCross = !matchesComma(posTag) &&
          !isCrossWord(token) &&
          !B_VP.equals(chunk) &&
          !matchesWordCharacter(token)

        // Check if the token can start
        boundary.canStart = B_NP.equals(chunk)
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
        if (isStopWord(token)) {
          boundary.canEnd = false
          boundary.canStart = false
        }
        if (i != tokens.length - 1) {
          // Some stop words have single quotes that are exploded when we chunk - must scan ahead
          if (isStopWord(token + tokens(i + 1))) {
            boundary.canEnd = false
            boundary.canStart = false
          }
        }

        // Dangler quotes
        if (i > 0 && matchesDanglerQuote(token)) {
          boundary.canStart = false
          boundaries(i - 1).canEnd = false
        }

        boundary.canStart = boundary.canStart && !matchesNotStart(posTag)
        boundary.canEnd = boundary.canEnd && matchesCanEnd(posTag)
        boundaries.add(boundary)
      }

      // Identify phrases based upon the boundaries we have previously defined, and add all significant expressions.
      for {
        start <- boundaries.indices
        startBoundary = boundaries(start)
        if startBoundary.canStart && startBoundary.canCross
      } {
        val remainingBoundaries = boundaries.slice(start, start + maxGramSizeInclusive)
        for {
          endSubIndex <- remainingBoundaries.indices
          endBoundary = remainingBoundaries(endSubIndex)
          if endBoundary.canCross && endBoundary.canEnd
          end = start + endSubIndex + 1
          if end - start >= minGramSize
          curExt = ExtractionSignificance(tokens.slice(start, end))
          if curExt.isSignificant
        } {
          populatePhraseInstance(start, end, splitUp.captures, tokens, posTags, corpus, instances, sentInstance.start, excludePhrases)
        }
      }
    }

    // now filter out any inner captures
    val removeThese = new ListBuffer[PhraseInstance]
    for {
      (prev,i) <- instances.zipWithIndex
      (cur,j) <- instances.zipWithIndex
      if i != j

      whatToRemove = {
        if (cur.startIndex >= prev.startIndex && cur.endIndex <= prev.endIndex) Some(cur)
        else if (prev.startIndex >= cur.startIndex && prev.endIndex <= cur.endIndex) Some(prev)
        else None
      }
      if whatToRemove.isDefined
      removeMe = whatToRemove.get
    } {
      removeThese += removeMe
    }

    val filteredInstances = instances.filterNot(removeThese.contains(_))
    new PhraseExtractionResult(filteredInstances)
  }

  private def populatePhraseInstance(start: Int, end: Int, captures: Seq[RegexCapturedResult], tokens: Seq[String], posTags: Seq[String], corpus: String, instances: ListBuffer[PhraseInstance], indexOffset: Int = 0, excludePhrases: Set[String] = Set.empty) {
    val phraseBuffer = new ListBuffer[String]

    val posTokens = for {
      i <- start until end
      token = tokens(i)
      tag = posTags(i)
      added = phraseBuffer.add(token)
    } yield POStoToken(tag, token)

    val phrase = Patterns.removePunctuationAtTheEnd(ExtractionSignificance(phraseBuffer).toString).trim

    if (phrase.length == 0 || isBlacklistedGram(phrase) || excludePhrases.contains(phrase)) return

    val size = end - start
    val globalStart = captures(start).indexStart + indexOffset
    val globalEnd = captures(end - 1).indexEnd + indexOffset

    val pInstance = PhraseInstance(phrase, posTokens, size, globalStart, globalEnd, corpus)

    instances.add(pInstance)
  }
}

case class POStoToken(posTag: String, token: String)

class PhraseInstance(val phrase: String, val posTokens: Seq[POStoToken], val gramSize: Int, val startIndex: Int, val endIndex: Int, val corpus: String) {
  lazy val appearsAt: Double = grvmath.percentOf(startIndex, corpus.length)

  override def toString: String = {
    val sb = new StringBuilder
    sb.append(getClass.getSimpleName)
    sb.append(" {'phrase': '").append(phrase).append("', 'gramSize': ").append(gramSize)
    sb.append(", 'POStags': [")
    var pastFirst = false
    posTokens.foreach(pos => {
      if (pastFirst) sb.append(", ") else pastFirst = true
      sb.append("{'tag': '").append(pos.posTag).append("', 'token': '").append(pos.token).append("'}")
    })

    sb.append("], 'appearsAt': ").append(appearsAt)
    sb.append(", 'start': ").append(startIndex).append(", 'end': ").append(endIndex)
    if (corpus.length > 50) sb.append(", 'corpusTruncated': '").append(corpus.subSequence(0, 50)).append("...") else sb.append(", 'corpus': '").append(corpus)
    sb.append("'}")

    sb.toString()
  }
}

object PhraseInstance {
  def apply(phrase: String, posTokens: Seq[POStoToken], gramSize: Int, startIndex: Int, endIndex: Int, corpus: String): PhraseInstance = new PhraseInstance(phrase, posTokens, gramSize, startIndex, endIndex, corpus)
}

case class NamedEntity(override val phrase: String, override val posTokens: Seq[POStoToken], override val gramSize: Int, override val startIndex: Int, override val endIndex: Int, override val corpus: String) extends PhraseInstance(phrase, posTokens, gramSize, startIndex, endIndex, corpus)
case class Coreference(override val phrase: String, override val posTokens: Seq[POStoToken], override val gramSize: Int, override val startIndex: Int, override val endIndex: Int, override val corpus: String, referent: NamedEntity) extends PhraseInstance(phrase, posTokens, gramSize, startIndex, endIndex, corpus) {
  override def toString = {
    super.toString.stripSuffix("}") + ", 'referent': '" + referent.phrase + "'}"
  }
}

class PhraseExtractionResult(instances: Seq[PhraseInstance]) extends immSeq[PhraseInstance] {
  protected lazy val uniqueCountTable: MyTypes.PhraseToIndexes = {
    val phraseTable = new MyTypes.PhraseToIndexes
    for ((instance, index) <- instances.zipWithIndex) {
      val phrase = instance.phrase
      phraseTable.get(phrase) match {
        case Some(pIndex) =>
          pIndex.add(index)
        case None =>
          phraseTable.put(phrase, mutable.HashSet(index))
      }
    }

    phraseTable
  }

  def contains(phrase: String): Boolean = uniqueCountTable.contains(phrase)
  def contains(instance: PhraseInstance): Boolean = contains(instance.phrase)

  def count(phrase: String): Int = uniqueCountTable.get(phrase).fold(0)(_.size)

  def indexesOf(phrase: String) = {
    uniqueCountTable.getOrElse(phrase, mutable.HashSet.empty)
  }

  def startIndexesOf(phrase: String) = {
     (indexesOf (phrase) collect this map (_.startIndex)).toSet
  }

  override def iterator: Iterator[PhraseInstance] = instances.iterator

  override def length: Int = instances.length

  override def apply(idx: Int) = instances(idx)
}

object PhraseExtractionResult {
  val empty = new PhraseExtractionResult(Seq.empty)
}

trait PhraseExtractionStep {
  def apply(acc: PhraseExtractionResult, text: String): PhraseExtractionResult
}

case class ExtractionSignificance(phrases: Traversable[String]) {

  lazy val formattedString = {
    val builder = new StringBuilder
    var pastFirst = false
    for (phrase <- phrases) {
      if (pastFirst) builder.append(Patterns.SPACE) else pastFirst = true
      builder.append(phrase)
    }
    Patterns.prepPhraseForSignificanceTest(builder.toString().trim)
  }

  lazy val isSignificant = !Patterns.matchesSignificantPhrase(formattedString)

  override def toString: String = formattedString
}

class PhraseBoundary(val token: String, val posTag: String, val chunk: String) {
  var canStart, canCross, canEnd: Boolean = false
}

