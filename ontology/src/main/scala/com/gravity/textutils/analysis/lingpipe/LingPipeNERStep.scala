package com.gravity.textutils.analysis.lingpipe

import scala.collection.JavaConversions._
import util.matching.Regex
import java.lang.UnsupportedOperationException
import com.aliasi.coref.{WithinDocCoref, EnglishMentionFactory}
import com.gravity.textutils.analysis.{Coreference, NamedEntity, PhraseExtractionResult, PhraseExtractionStep}
import com.aliasi.chunk._

/**
 * Uses LingPipe's shallow parsing pipeline to add named entities and their coreferences to PhraseExtractionResults.
 *
 * Based on <a href="http://alias-i.com/lingpipe/web/demo-coref.html">LingPipe's Coreference Demo</a>.
 */
class LingPipeNERStep(steps: ChunkStep*) extends PhraseExtractionStep with Chunker {
  override def apply(acc: PhraseExtractionResult, text: String) = {
    val chunks = chunk(text).toSeq

    val referentIndexes = chunks collect { case mc: MentionChunk => mc } groupBy (_.entityId) map { case (entityId: Int, corefs) => {
      val firstMentionWithLongestName = corefs.zipWithIndex.sortBy{ case (c, i) => {
        val chunkLength = c.end - c.start
        (chunkLength, -i)
      }}.last._1
      (entityId, chunks.indexOf(firstMentionWithLongestName))
    }}

    val nes = chunks map { c =>
      val chunkPhrase = text.substring(c.start, c.end)
      val tags = Seq.empty
      val gramSize = LingPipeTool.tokenizeGrams(chunkPhrase).size
      NamedEntity(chunkPhrase, tags, gramSize, c.start, c.end - 1, text)
    }

    val nesAndCorefs = nes zip (chunks) map { case (ne, chunk) =>
      chunk match {
        case mc: MentionChunk if mc != chunks(referentIndexes(mc.entityId)) => {
          Coreference(ne.phrase, ne.posTokens, ne.gramSize, ne.startIndex, ne.endIndex, ne.corpus, nes(referentIndexes(mc.entityId))) }
        case _ => ne
      }
    }

    val nesPER = new PhraseExtractionResult(nesAndCorefs)
    val existing = acc filter { p =>
      val sharedPhrase = (nesPER startIndexesOf (p.phrase)) intersect (acc startIndexesOf (p.phrase))
      sharedPhrase.isEmpty
    }

    new PhraseExtractionResult((existing ++ nesAndCorefs) sortBy (_.startIndex))
  }

  override def chunk(sentences: CharSequence) = {
    val chunkedSentences = LingPipeTool.sentenceChunker.chunk(sentences).chunkSet.toSeq map (new ChunkAndCharSeq(_, sentences))

    val initialChunks = chunkedSentences.zipWithIndex flatMap { case (sent, i) =>
      LingPipeTool.chunkerModel.chunk(sent.span).chunkSet.toSeq map (c => ChunkInSentence(new ChunkAndCharSeq(c, sent.span), i))
    }

    val chunks = (steps).foldLeft(initialChunks) { (acc, step) =>
      step(acc, chunkedSentences, sentences)
    } map { case ChunkInSentence(c, i) => c.chunk }

    val chunking = new ChunkingImpl(sentences)
    chunking.addAll(chunks)
    chunking
  }

  override def chunk(cs: Array[Char], start: Int, end: Int) = throw new UnsupportedOperationException
}

object LingPipeCorefs extends ChunkStep {
  val MALE_PRONOUNS = """(?i)\b(he|him|his)\b""".r
  val FEMALE_PRONOUNS = """(?i)\b(she|her|hers)\b""".r
  val NEUTER_PRONOUNS = """(?i)\b(it|its|they|their|theirs)\b""".r
  private val MENTION_CHUNKERS = Seq((MALE_PRONOUNS, "MALE_PRONOUN"), (FEMALE_PRONOUNS, "FEMALE_PRONOUN"), (NEUTER_PRONOUNS, "NEUTER_PRONOUN"))

  override def apply(acc: ChunkAcc, chunkedSentences: Seq[ChunkAndCharSeq], originalText: CharSequence) = {
    def improveChunks(acc: ChunkAcc, regex: Regex, tag: String) = {
      val addlChunks = chunkedSentences.zipWithIndex flatMap { case (sent, i) =>
        regex.findAllIn(sent.span).matchData.map{ m =>
          val chunk = new ChunkAndCharSeq(ChunkFactory.createChunk(m.start, m.end, tag), sent.span)
          ChunkInSentence(chunk, i)
        }.toTraversable
      }
      def overlapsAnyAddlChunkInSentence(chunk: Chunk, sentenceNo: Int) = {
        addlChunks exists { case ChunkInSentence(other, j) => sentenceNo == j && math.max(chunk.start, other.chunk.start) < math.min(chunk.end, other.chunk.end) }
      }
      acc.filterNot{ case ChunkInSentence(c, i) => overlapsAnyAddlChunkInSentence(c.chunk, i) } ++ addlChunks
    }

    val improvedChunks = MENTION_CHUNKERS.foldLeft(acc) { case (acc, (regex, tag)) =>
      improveChunks(acc, regex, tag)
    }

    val mf = new EnglishMentionFactory
    val coref = new WithinDocCoref(mf)

    val corefsResolvedChunks = improvedChunks map { case ChunkInSentence(c, i) =>
      val mention = mf.create(c.span, c.chunk.`type`)
      val entityId = coref.resolveMention(mention, i)
      ChunkInSentence(new ChunkAndCharSeq(MentionChunk(c.chunk, entityId, mention), c.charSequence), i)
    }
    corefsResolvedChunks
  }
}

object FilterUnresolvedMentions extends ChunkStep {
  override def apply(acc: ChunkAcc, chunkedSentences: Seq[ChunkAndCharSeq], originalText: CharSequence) = {
    acc filter { case ChunkInSentence(c, i) =>
      c.chunk match {
        case MentionChunk(_, entityId, mention) => entityId >= 0 || !mention.isPronominal
        case _ => true
      }
    }
  }
}

object PutChunksBackInCorpus extends ChunkStep {
  override def apply(acc: ChunkAcc, chunkedSentences: Seq[ChunkAndCharSeq], originalText: CharSequence) = {
    acc map { case ChunkInSentence(c, i) =>
      val sent = chunkedSentences(i).chunk
      val plainChunkCorpusIndex = ChunkFactory.createChunk(sent.start + c.chunk.start, sent.start + c.chunk.end, c.chunk.score)
      val corpusChunk = c.chunk match {
        case MentionChunk(_, entityId, mention) => MentionChunk(plainChunkCorpusIndex, entityId, mention)
        case _ => plainChunkCorpusIndex
      }
      ChunkInSentence(new ChunkAndCharSeq(corpusChunk, originalText), i)
    }
  }
}
