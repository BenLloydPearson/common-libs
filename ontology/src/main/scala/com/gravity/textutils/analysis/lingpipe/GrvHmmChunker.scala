package com.gravity.textutils.analysis.lingpipe

/**
 * Created by IntelliJ IDEA.
 * Author: Robbie Coleman
 * Date: 5/23/11
 * Time: 12:59 PM
 */

import scala.collection.JavaConversions._
import com.gravity.utilities.ScalaMagic._
import com.gravity.utilities.grvstrings
import com.aliasi.hmm.HmmDecoder
import scala.collection.mutable.ListBuffer
import com.aliasi.tokenizer.TokenizerFactory
import com.gravity.textutils.analysis.{NLPSentenceInstance, POStoToken, PhraseInstance}

class GrvHmmChunker(tokenizerFactory: TokenizerFactory, decoder: HmmDecoder) {
  def extractPhrases(text: String, sentences: Seq[NLPSentenceInstance], minGramSize: Int, maxGramSizeInclusive: Int): Seq[PhraseInstance] = {
    val results = for {
      sentence <- sentences
      tokenizer = tokenizerFactory.tokenizer(sentence.sentence.toCharArray, 0, sentence.sentence.length())
      tokens = tokenizer.tokenize()
      tags = decoder.tag(tokens.toList).tags()
    } yield GrvHmmChunker.extractPhrases(text, minGramSize, maxGramSizeInclusive, tokens, tags, indexOffset = sentence.start)

    for (seq <- results; itm <- seq) yield itm
  }
}

object GrvHmmChunker {

  val DETERMINER_TAGS = Set("abn","abx","ap","ap$","at","cd","cd$","dt","dt$","dti","dts","dtx","od")
  val ADJECTIVE_TAGS = Set("jj","jj$","jjr","jjs","jjt","*","ql")
  val NOUN_TAGS = Set("nn","nn$","nns","nns$","np","np$","nps","nps$","nr","nr$","nrs")
  val PRONOUN_TAGS = Set("pn","pn$","pp$","pp$$","ppl","ppls","ppo","pps","ppss")
  val VERB_TAGS = Set("vb","vbd","vbg","vbn","vbz")
  val AUXILIARY_VERB_TAGS = Set("to","md","be","bed","bedz","beg","bem","ben","ber","bez")
  val ADVERB_TAGS = Set("rb","rb$","rbr","rbt","rn","ql","*")
  val PUNCTUATION_TAGS = Set("'",".","*")

  val START_NOUN_TAGS = DETERMINER_TAGS ++ ADJECTIVE_TAGS ++ NOUN_TAGS ++ PRONOUN_TAGS
  val CONTINUE_NOUN_TAGS = START_NOUN_TAGS ++ ADVERB_TAGS ++ PUNCTUATION_TAGS
  val START_VERB_TAGS = VERB_TAGS ++ AUXILIARY_VERB_TAGS ++ ADVERB_TAGS
  val CONTINUE_VERB_TAGS = START_VERB_TAGS ++ PUNCTUATION_TAGS

  def detectBoundary(tag: String, token: String, start: Int, end: Int): POSBoundary = {
    if (isNullOrEmpty(tag)) return OtherBoundary(token, start, end)

    if (START_NOUN_TAGS.contains(tag)) return NounBoundary(token, true, CONTINUE_NOUN_TAGS.contains(tag), start, end)

    if (START_VERB_TAGS.contains(tag)) return VerbBoundary(token, true, CONTINUE_VERB_TAGS.contains(tag), start, end)

    if (CONTINUE_NOUN_TAGS.contains(tag)) return NounBoundary(token, false, true, start, end)

    if (CONTINUE_VERB_TAGS.contains(tag)) return VerbBoundary(token, false, true, start, end)

    OtherBoundary(token, start, end)
  }

  def extractPhrases(text: String, minGramSize: Int, maxGramSizeInclusive: Int, tokens: Seq[String], tags: Seq[String], whitespaces: Seq[String] = Seq.empty, indexOffset: Int = 0): Seq[PhraseInstance] = {
    val safeWhites = prepareWhitespaces(whitespaces, tokens.size)

    var pos = 0
    val boundaries = for {
      i <- 0 until tokens.length
      white = safeWhites(i)
      foo = (pos + white.length())
      start = pos
      token = tokens(i)
      end = start + token.length()
      tag = tags(i)
      bar = (pos = end + (if (i + 1 < safeWhites.length) safeWhites(i + 1).length() else 0))
    } yield detectBoundary(tag, token, start, end)

    val phrases = new ListBuffer[PhraseInstance]

    for {
      start <- 0 until boundaries.length
      boundary = boundaries(start)
      if (boundary.canBegin)
    } {
      val remainingBoundaries = boundaries.drop(start).take(maxGramSizeInclusive)
      for {
        endSubIndex <- 0 until remainingBoundaries.length
        endBoundary = remainingBoundaries(endSubIndex)
        if (!endBoundary.canContinue)
        end = start + endSubIndex + 1
        if (end - start >= minGramSize)
      } {
        val posToks = for {
          i <- start until end
          pos = "%s_%s".format(tags(i), boundaries(i).name).toUpperCase
          tok = tokens(i)
        } yield POStoToken(pos, tok)

        val textStart = boundary.start + indexOffset
        val textEnd = endBoundary.end + indexOffset

        val phrase = text.substring(textStart, textEnd)

        phrases += PhraseInstance(phrase, posToks, posToks.length, textStart, textEnd, text)

      }
    }

    val removeThese = for {
      (prev,i) <- phrases.zipWithIndex
      (cur,j) <- phrases.zipWithIndex
      if (i != j)

      whatToRemove = {
        if (cur.startIndex >= prev.startIndex && cur.endIndex <= prev.endIndex) Some(cur)
        else if (prev.startIndex >= cur.startIndex && prev.endIndex <= cur.endIndex) Some(prev)
        else None
      }
      if (whatToRemove != None)
      removeMe = whatToRemove.get
    } yield removeMe

    phrases.filterNot(removeThese.contains(_)).toSeq
  }

  val SPACE = " "
  def prepareWhitespaces(whites: Seq[String], mustBeLength: Int) = {
    val lastIndex = mustBeLength - 1
    if (isNullOrEmpty(whites)) {
      for (i <- 0 until mustBeLength) yield if (i == 0 || i == lastIndex) grvstrings.emptyString else SPACE
    } else if (whites.size < mustBeLength) {
      val buffer = new ListBuffer[String]
      buffer.sizeHint(mustBeLength)
      whites.foreach(buffer += _)
      for (x <- whites.size - 1 until mustBeLength) {
        buffer += (if (x == lastIndex) grvstrings.emptyString else SPACE)
      }
      buffer.toSeq
    } else whites
  }
}

sealed class POSBoundary(val name: String, val token: String, val canBegin: Boolean, val canContinue: Boolean, val start: Int, val end: Int)

object POSBoundary {
  def apply(name: String, token: String, canBegin: Boolean, canContinue: Boolean, start: Int, end: Int): POSBoundary = new POSBoundary(name, token, canBegin, canContinue, start, end)
}

sealed case class NounBoundary(
                                  override val token: String,
                                  override val canBegin: Boolean,
                                  override val canContinue: Boolean,
                                  override val start: Int,
                                  override val end: Int)
    extends POSBoundary("noun", token, canBegin, canContinue, start, end)

sealed case class VerbBoundary(
                                  override val token: String,
                                  override val canBegin: Boolean,
                                  override val canContinue: Boolean,
                                  override val start: Int,
                                  override val end: Int)
    extends POSBoundary("verb", token, canBegin, canContinue, start, end)

sealed case class OtherBoundary(
                                  override val token: String,
                                  override val start: Int,
                                  override val end: Int)
    extends POSBoundary("other", token, false, false, start, end)