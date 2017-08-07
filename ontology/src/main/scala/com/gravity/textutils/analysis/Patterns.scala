package com.gravity.textutils.analysis

/**
 * Created by IntelliJ IDEA.
 * User: robbie
 * Date: 5/4/11
 * Time: 9:24 PM
 */

import scala.collection.JavaConversions._
import com.gravity.utilities.ScalaMagic._
import com.gravity.utilities.grvstrings
import collection.mutable.{ArrayBuffer, ListBuffer}
import util.matching.Regex

object Patterns {
  // constants
  val B_VP = "B-VP"
  val B_NP = "B-NP"
  val I_NP = "I-NP"
  val CC = "CC"
  val SPACE = " "

  // single replacements
  private val punctuation = TextReplacement("[^\\p{L}]".r)
  private val punctuationAtTheEnd = TextReplacement("[^\\p{L}]$".r)
  private val urls = TextReplacement("/\\w+://[^\\s]+?(?=\\.?(?= |$))/".r)

  def removePunctuation(text: String) = punctuation.replace(text)

  def removePunctuationAtTheEnd(text: String) = punctuationAtTheEnd.replace(text)

  def removeUrls(text: String) = urls.replace(text)

  // sequenced replacements
  private val sentencePrepForTagging: OrderedTextReplacements = {
    val replacements = new ListBuffer[TextReplacement]

    replacements += TextReplacement("/--+/".r, " -- ")
    replacements += TextReplacement("/\\{|\\[/".r, "(")
    replacements += TextReplacement("/\\}|\\]/".r, ")")

    // Accents and ticks
    replacements += TextReplacement("\u2019".r, "'")
    replacements += TextReplacement("`".r, "'")
    // Double quote variants
    replacements += TextReplacement("\u201C".r, "\"")
    replacements += TextReplacement("\u201D".r, "\"")
    // Back to normal
    replacements += TextReplacement("/([\\w])\\.\\.+([\\w])/".r, "$1 , $2")
    replacements += TextReplacement("/(^| )(\"|\\(|\\)|;|-|:|-|\\*|,)+/".r, " , ")
    replacements += TextReplacement("/(\"|\\(|\\)|;|-|:|-|\\*|,)( |$)/".r, " , ")
    replacements += TextReplacement("/(\\.+ |')/".r, " $1")
    replacements += TextReplacement("/ / /".r, " , ")
    replacements += TextReplacement("/(,|\\.) *,/".r, " $1 ")
    replacements += TextReplacement("/(,| )+$/".r, "")
    replacements += TextReplacement("/^(,| )+/".r, "")
    replacements += TextReplacement("/((?:\\.|!|\\?)+)$/".r, " $1")
    replacements += TextReplacement("/(!|\\?|\\.)+/".r, " $1 ")
    replacements += TextReplacement("/\\s+/".r, " ")
    replacements += TextReplacement("/\\}|\\]/".r, ")")

    new OrderedTextReplacements(replacements.toSeq)
  }

  def prepSentenceForTagging(sentence: String) = sentencePrepForTagging.replaceAll(sentence)

  private val sentencePrepForDetection: OrderedTextReplacements = {
    val replacements = new ListBuffer[TextReplacement]

    replacements += TextReplacement("/\\r(\\n?)/".r, "\\n")
    replacements += TextReplacement("/^\\s+$/".r)
    replacements += TextReplacement("/\\n\\n+/".r, ".\\n.\\n")
    replacements += TextReplacement("/\\n/".r, " ")
    replacements += TextReplacement("/(\\d+)\\./".r, ". $1 . ")

    new OrderedTextReplacements(replacements.toSeq)
  }

  def prepSentenceForDetection(sentence: String) = sentencePrepForDetection.replaceAll(sentence)

  private val phrasePrepForSignificanceTest: OrderedTextReplacements = {
    new OrderedTextReplacements(TextReplacement(" '".r, "'") :: TextReplacement(" \\.".r, "\\.") :: Nil)
  }

  def prepPhraseForSignificanceTest(phrase: String) = phrasePrepForSignificanceTest.replaceAll(phrase)

  // matchers
  // -- vals
  private val sentenceExceptions = TextMatcher("/^(\\.|!|\\?)+$/".r)
  private val commaMatcher = TextMatcher(",".r)
  private val wordMatcher = TextMatcher("<\\w+>".r)
  private val canStartMatcher = TextMatcher("DT|WDT|PRP|JJR|JJS|CD".r)
  private val notEndMatcher = TextMatcher("I-".r)
  private val danglerQuoteMatcher = TextMatcher("^'".r)
  private val notStartMatcher = TextMatcher("CC|PRP|IN|DT|PRP\\$|WP|WP\\$|TO|EX|JJR|JJS".r)
  private val canEndMatcher = TextMatcher("NN|NNS|NNP|NNPS|FW|CD".r)
  private val significantPhraseMatcher = TextMatcher("/^[^a-zA-Z]*$/".r)
  // -- defs
  def matchesSentenceExceptions(text: String) = sentenceExceptions.matches(text)

  def matchesComma(text: String) = commaMatcher.matches(text)

  def matchesWordCharacter(text: String) = wordMatcher.matches(text)

  def matchesCanStart(text: String) = canStartMatcher.matches(text)

  def matchesNotEnd(text: String) = notEndMatcher.matches(text)

  def matchesDanglerQuote(text: String) = danglerQuoteMatcher.matches(text)

  def matchesNotStart(text: String) = notStartMatcher.matches(text)

  def matchesCanEnd(text: String) = canEndMatcher.matches(text)

  def matchesSignificantPhrase(text: String) = significantPhraseMatcher.matches(text)

  // splitters
  // -- vals
  private val whiteSpaceSplitter = TextSplitter("\\s+".r)
  private val spaceSplitter = TextSplitter(SPACE.r)
  // -- defs
  def splitOnWhiteSpace(text: String) = whiteSpaceSplitter.split(text)

  def splitAndCaptureOnWhiteSpace(text: String) = whiteSpaceSplitter.splitAndCaptureIndicies(text)

  def splitOnSpace(text: String) = spaceSplitter.split(text)

  def splitAndCaptureOnSpace(text: String) = spaceSplitter.splitAndCaptureIndicies(text)
}
case class TokensAndCaptures(tokens: Seq[String], captures: Seq[RegexCapturedResult])

case class RegexCapturedResult(captured: String, indexStart: Int, indexEnd: Int)

case class TextReplacement(regex: Regex, replaceWith: String = grvstrings.emptyString) {
  def replace(text: String) = regex.replaceAllIn(text, replaceWith)
}

case class TextMatcher(regex: Regex) {
  def matches(text: String): Boolean = regex.pattern.matcher(text).matches
}

case class TextSplitter(regex: Regex) {
  def split(text: String) = regex.pattern.split(text)

  def splitAndCaptureIndicies(input: String): TokensAndCaptures = {
    val m = regex.pattern.matcher(input)
    val tokens = new ArrayBuffer[String]
    val captures = new ListBuffer[RegexCapturedResult]
    var index = 0
    var size = 0

    while (m.find) {
      val start = index
      val end = m.start
      val token = input.subSequence(start, end).toString
      tokens.add(token)
      captures += RegexCapturedResult(token, start, end - 1)
      size += 1
      index = m.end
    }

    // check for any remaining
    if (index < input.length - 1) {
      val token = input.subSequence(index, input.length).toString
      tokens.add(token)
      captures += RegexCapturedResult(token, index, input.length - 1)
      size += 1
    }

    return TokensAndCaptures(tokens, captures)
  }
}

class OrderedTextReplacements(replacements: Seq[TextReplacement]) {
  def replaceAll(text: String): String = {
    if (isNullOrEmpty(text)) return text

    var result: String = text
    for (repl <- replacements) {
      result = repl.replace(result)
    }

    return result.trim
  }
}