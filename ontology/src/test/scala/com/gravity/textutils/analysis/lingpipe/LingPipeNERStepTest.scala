package com.gravity.textutils.analysis.lingpipe

import scala.collection.JavaConversions._
import com.gravity.utilities.BaseScalaTest
import com.aliasi.chunk.{ChunkFactory, ChunkerEvaluator, Chunking}
import com.gravity.textutils.analysis.{Coreference, NamedEntity, PhraseInstance, PhraseExtractionResult}

class LingPipeNERStepTest extends BaseScalaTest {
  val chunkPipe = Seq(LingPipeCorefs, PutChunksBackInCorpus)
  val lingPipeNERStep = new LingPipeNERStep(chunkPipe:_*)

  val article = """Facebook has just filed a motion for expedited discovery in its case against Paul Ceglia, the man
    who is claiming that he holds a major stake in Facebook based on a contract he allegedly signed with Mark Zuckerberg
    back in 2003. There was plenty of initial skepticism around the case (who could possibly forget that they owned a
    major stake in one of the most successful companies of the decade?), but people starting taking it more seriously
    when Ceglia landed representation from DLA Piper, and a series of potentially legitimate email exchanges surfaced.
    Now Facebook is striking back, and it’s not mincing its words.

    The document, embedded below, is a concerted attack on Ceglia’s character and criminal history. Facebook is asking
    the court to grant expedited, targeted discovery that would require Ceglia to produce the original contract between
    Zuckerberg and Ceglia, as well as original copies of their email exchanges."""

  val typedChunks = Set(
    ChunkFactory.createChunk(0, 8, "ORGANIZATION"),     // Facebook
    ChunkFactory.createChunk(60, 63, "NEUTER_PRONOUN"), // its
    ChunkFactory.createChunk(77, 88, "PERSON"),         // Paul Ceglia
    ChunkFactory.createChunk(123, 125, "MALE_PRONOUN"), // he
    ChunkFactory.createChunk(178, 180, "MALE_PRONOUN"), // he
    ChunkFactory.createChunk(149, 157, "ORGANIZATION"), // Facebook
    ChunkFactory.createChunk(203, 218, "PERSON"),       // Mark Zuckerberg
    ChunkFactory.createChunk(324, 328, "NEUTER_PRONOUN"), // they
    ChunkFactory.createChunk(437, 439, "NEUTER_PRONOUN"), // it
    ChunkFactory.createChunk(464, 470, "PERSON"),       // Ceglia
    ChunkFactory.createChunk(498, 507, "ORGANIZATION"), // DLA Piper
    ChunkFactory.createChunk(582, 590, "ORGANIZATION"), // Facebook
    ChunkFactory.createChunk(613, 615, "NEUTER_PRONOUN"), // it
    ChunkFactory.createChunk(630, 633, "NEUTER_PRONOUN"), // its
    ChunkFactory.createChunk(701, 707, "PERSON"),       // Ceglia
    ChunkFactory.createChunk(742, 750, "ORGANIZATION"), // Facebook
    ChunkFactory.createChunk(833, 839, "PERSON"),       // Ceglia
    ChunkFactory.createChunk(885, 895, "PERSON"),       // Zuckerberg
    ChunkFactory.createChunk(900, 906, "PERSON"),       // Ceglia
    ChunkFactory.createChunk(938, 943, "NEUTER_PRONOUN") // their
  )

  val plainChunks = typedChunks map { c => ChunkFactory.createChunk(c.start, c.end) }

  val refPlainChunking = new Chunking {
    override def chunkSet = plainChunks
    override def charSequence = article
  }

  private def getEvaluation(reference: Chunking) = {
    val evaluator = new ChunkerEvaluator(lingPipeNERStep)
    evaluator.handle(reference)
    evaluator.evaluation
  }

  test("typeless chunking") {
    val eval = getEvaluation(refPlainChunking)
    val prEval = eval.precisionRecallEvaluation()
    prEval.precision should be >= (.9)
    prEval.recall should be >= (.9)
  }

  test("Named Entity chunk overrides exactly matching Phrase Extraction chunk") {
    val namedEntities = lingPipeNERStep(PhraseExtractionResult.empty, article).toSet

    val exactPhrase = PhraseInstance("Mark Zuckerberg", Seq.empty, 2, 203, 217, article)
    val somePreviousPhrases = new PhraseExtractionResult(Seq(
      PhraseInstance("against Paul", Seq.empty, 2, 69, 80, article),
      exactPhrase,
      PhraseInstance("plenty of initial skepticism", Seq.empty, 2, 247, 274, article)
    ))
    val augmented = lingPipeNERStep(somePreviousPhrases, article)

    val augmentedNamedEntities = augmented.filter(ne => ne.isInstanceOf[NamedEntity] || ne.isInstanceOf[Coreference]).toSet
    augmentedNamedEntities should equal (namedEntities)

    somePreviousPhrases foreach { somePhrase =>
      if (somePhrase == exactPhrase) {
        augmented should not (contain (somePhrase))
      }
      else {
        augmented should contain (somePhrase)
      }
    }
    augmented.size should equal (namedEntities.size + somePreviousPhrases.size - 1)
    val namedEntityOverridingPhrase = augmentedNamedEntities find (_.phrase == exactPhrase.phrase)
    namedEntityOverridingPhrase should be ('defined)
  }

  test("Substring coreferences resolved") {
    implicit val nes = lingPipeNERStep(PhraseExtractionResult.empty, article)
    substringCorefCheck("Paul Ceglia", "Ceglia")
    substringCorefCheck("Facebook", "Facebook")
    substringCorefCheck("Mark Zuckerberg", "Zuckerberg")
  }

  private def substringCorefCheck(namedEntity: String, substr: String)(implicit nes: PhraseExtractionResult) {
    val mainEntity = nes.collectFirst{ case ne: NamedEntity if ne.phrase == namedEntity => ne }.get
    val substrCorefs = nes collect { case coref: Coreference if coref.phrase.contains(substr) => coref }
    substrCorefs should not be ('empty)
    substrCorefs foreach { _.referent should be (mainEntity) }
  }
}