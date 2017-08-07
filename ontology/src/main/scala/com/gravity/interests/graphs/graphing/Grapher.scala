package com.gravity.interests.graphs.graphing

import scala.collection.JavaConversions._
import org.joda.time.DateTime
import com.google.common.collect.Multiset
import com.gravity.textutils.analysis._
import com.gravity.utilities.grvmath
import com.gravity.ontology.annotation.BadConcepts
import com.gravity.ontology.{OntologyGraphName, TopicExtraction}

/*             )\._.,--....,'``.
.b--.        /;   _.. \   _\  (`._ ,.
`=,-,-'~~~   `----(,_..'--(,_..'`-.;.'  */


object TermVectorG {
  // AKASH-REVIEW: Scores and Thresholds
  def SCORE_LOW         = 1.0
  def SCORE_MED_THRESH  = 2.9   // Scores at or above this threshold are considered to represent "Weight.High"
  def SCORE_MED         = 3.0
  def SCORE_HIGH_THRESH = 4.9   // Scores at or above this threshold are considered to represent "Weight.Medium"
  def SCORE_HIGH        = 5.0

  def toWeight(scoredTerm: ScoredTerm) = {
    scoredTerm.score match {
      case vsThresh if vsThresh >= SCORE_HIGH_THRESH => Weight.High
      case vsThresh if vsThresh >= SCORE_MED_THRESH  => Weight.Medium
      case _                                         => Weight.Low
    }
  }

  def toContentType(scoredTerm: ScoredTerm) = ContentType.Title  // AKASH-REVIEW: What ContentType should we use?
}

case class ScoredTerm(term: String, score: Double)

class Weight extends Serializable

object Weight {

  object High extends Weight

  object Medium extends Weight

  object Low extends Weight
}

class ContentType extends Serializable

object ContentType {

  object Keywords extends ContentType

  object Article extends ContentType

  object Title extends ContentType

  object Description extends ContentType

  object Topic extends ContentType

  object Concept extends ContentType

}

case class ScoredTopicInterestMatch(topic: ScoredTopic, interestMatch: StrictInterestMatch)

case class ScoreWeights(frequencyWeight: Double, compoundWordWeight: Double, searchWeight: Double, popularityWeight: Double, properNounWeight: Double, contentTypeWeight: Double, positionalWeight: Double)

object ConceptScorers {

  object WeightedScorer extends ConceptScorer {
    def score(c: ScoredConcept) = {
      val weights = ConceptScoreWeights.defaultWeights
      val adjRelativePopularity = c.relativePopularity * weights.popularityWeight
      val adjCloseness = c.relativeCloseness * weights.closenessWeight
      val adjConvergence = c.relativeConvergence * weights.convergenceWeight
      val adjUnique = c.relativeConvergence * weights.uniquePathWeight
      val adjTopic = c.relativeTopicScores * weights.topicScoreWeight
      (adjRelativePopularity + adjCloseness + adjConvergence + adjUnique + adjTopic) / weights.productIterator.map(_.asInstanceOf[Double]).sum
    }
  }

  object ConvergenceScorer extends ConceptScorer {
    def score(c: ScoredConcept) = {
      val adjConvergence = c.convergence
      val adjUnique = c.uniquePaths
      val adjCloseness = c.averageCloseness * 0.01
      adjConvergence + adjUnique + adjCloseness
    }
  }
}

trait ConceptScorer {
  def score(c: ScoredConcept) : Double
}

case class ScoredConcept(
                                interest: StrictOntologyNode,
                                var score: Double,
                                var convergence: Int,
                                var averageDistance: Double,
                                interestMatches: Iterable[ScoredTopicInterestMatch],
                                var uniquePaths: Int,
                                var popularity: Long,
                                var averageTopicScore: Double,
                                var averageCloseness: Double,
                                var relativePopularity: Double = 0.0,
                                var relativeTopicScores: Double = 0.0,
                                var relativeUniquePaths: Double = 0.0,
                                var relativeConvergence: Double = 0.0,
                                var relativeCloseness: Double = 0.0) {
  def printDetail() {
    print(makeDetail)
  }

  def printSynopsis() {
    println("l: " + interest.level + " interest : " + interest.name + " : score : " + score + " : convergence : " + convergence + " : uniques : " + uniquePaths + " : popularity : " + popularity + " topics : " + averageTopicScore + " distance : " + averageDistance)
  }

  def makeDetail(): String = {
    val sb = new StringBuilder()
    def apd(str: String) {sb.append(str); sb.append("\n")}

    apd("Concept: " + interest.name + " (" + interest.uri + ")")
    apd("\tScore: " + score + " Convergence: " + convergence + " Average Distance: " + averageDistance + " Unique Paths: " + uniquePaths + " Popularity: " + popularity)
    apd("\tAverage Topic Score: " + averageTopicScore + " Average Closeness: " + averageCloseness)
    apd("\tRelative Popularity: " + relativePopularity + " Relative Closeness: " + relativeCloseness)
    apd("\tRelative Topic Scores: " + relativeTopicScores + " Relative Unique Paths: " + relativeUniquePaths)
    apd("\tRelative Convergence: " + relativeConvergence)
    for (mat <- interestMatches) {
      val phraseCount = grvmath.sumBy(mat.topic.phrases)(_.phrase.count)
      apd("\t\t" + phraseCount + " of them via the below path:")
      mat.interestMatch.path.steps.foreach {
        case node if !node.uri.isEmpty =>
          apd("\t\t\t" + node.uri + " (views: " + node.popularity + ") (" + node.trendingViewScore + ")")
        case relationship =>
          apd("\t\t\t" + " relationship: " + relationship.name)
      }
      //      mat.interestMatch.path.nodes().foreach{node=>
      //
      //        apd("\t\t\t" + node.getProperty(NodeBase.URI_PROPERTY).toString + " (views: " + node.getProperty(NodeBase.VIEWCOUNT_PROPERTY,0).asInstanceOf[Integer] + ") ("+node.getProperty(NodeBase.TRENDING_VIEW_SCORE,0.0d).asInstanceOf[Double]+")")
      //      }
    }
    sb.toString
  }

  def calculate(scorer:ConceptScorer) {

    this.score = scorer.score(this)
  }
}

case class ConceptWeights(popularityWeight: Double, closenessWeight: Double, convergenceWeight: Double, uniquePathWeight: Double, topicScoreWeight: Double)

object ConceptScoreWeights {
  val defaultWeights = new ConceptWeights(
    popularityWeight = 1.0,
    closenessWeight = 1.0,
    convergenceWeight = 1.0,
    uniquePathWeight = 1.0,
    topicScoreWeight = 1.0
  )
}

object ScoreWeights {
  val defaultWeights = new ScoreWeights(
    frequencyWeight = 6000,
    compoundWordWeight = 5,
    searchWeight = 1,
    popularityWeight = 5,
    properNounWeight = 1,
    contentTypeWeight = 40,
    positionalWeight = 10
  )
}

case class Phrase(text: String, grams: Int, count: Int, content: ContentToGraph)

case class PhraseAndTopic(topic: StrictScoredNode, phrase: Phrase)

case class ScoredTopic(topic: StrictOntologyNode, var score: Double, searchScore: Double, frequency: Int, relativeFrequency: Double, popularity: Int, relativePopularity: Double, grams: Int, compoundWordScore: Double, phrases: Iterable[PhraseAndTopic])

case class ContentToGraph(location: String, date: DateTime, text: String, weight: Weight, contentType: ContentType)


case class Grapher(
                    contents: Iterable[ContentToGraph],
                    weights: ScoreWeights = ScoreWeights.defaultWeights,
                    useLegacy: Boolean = true,
                    minGramSize: Int = 1,
                    maxGramSize: Int = 5,
                    level: Option[ExtractionLevel] = Some(ExtractionLevel.NGRAMS),
                    followNonAnnotated: Boolean = true,
                    maxDepth: Int = 6,
                    tagIsContentScoreMultiplier: Float = 1.2f,
                    tagIsKeywordScoreMultiplier: Float = 1.5f,
                    excludeTopicUris: Set[String] = Set.empty,
                    excludeConceptUris: BadConcepts = BadConcepts.empty,
                    grapherOntology: GrapherOntology = GrapherOntology.instance,
                    conceptScorer:ConceptScorer = ConceptScorers.WeightedScorer,
                    termVector1: Seq[ScoredTerm] = null,
                    termVector2:Seq[ScoredTerm] = null,
                    termVector3: Seq[ScoredTerm] = null,
                    termVectorG: Seq[ScoredTerm] = null,
                    phraseVectorKea:Seq[ScoredTerm] = null
                    )(implicit ogName: OntologyGraphName) {
  lazy val totalPhrases = grvmath.sumBy(phrases)(_.count)

  lazy val totalPopularity = grvmath.sumBy(topics)(_._2.head.topic.node.popularity)

  lazy val totalTopics = topics.size

  def trace(msg: => String) {
    println(msg)
  }

  lazy val levelUsed = level getOrElse ExtractionLevel.NGRAMS

  /**
   * Extract the Contents into a series of Phrases.
   */
  lazy val phrases = {
    def extractCandidate(content: ContentToGraph): Iterable[Phrase] = {
      val phraseExtract = (text: String) => if (useLegacy) {
        PhraseExtractorInterop.getInstance.extractLegacy(text, minGramSize, maxGramSize, ExtractionLevel.PHRASES)
      } else {
        PhraseExtractorUtil.extractPhrases(text, minGramSize, maxGramSize)
      }
      val ngramExtract = (text: String) => if (useLegacy) {
        PhraseExtractorInterop.getInstance.extractLegacy(text, minGramSize, maxGramSize, ExtractionLevel.NGRAMS)
      } else {
        PhraseExtractorUtil.extractNgrams(text, minGramSize, maxGramSize)
      }
      val extractUsing = (content: ContentToGraph, extractor: (String) => Object) => {
        extractor(content.text) match {
          case elements: PhraseExtractionResult => {
            elements.map(element => {
              Phrase(element.phrase, element.phrase.count(_.isWhitespace) + 1, elements.count(element.phrase), content)
            })
          }
          case elements: Multiset[_] => {
            elements.elementSet().map(elem => {
              val element = elem match {
                case s: String => s
                case _ => throw new IllegalStateException("non String found where String expected")
              }
              Phrase(element, element.count(_.isWhitespace) + 1, elements.count(element), content)
            }).toIterable

          }
        }
      }

      content.contentType match {
        case ContentType.Keywords => {
          content.text.split(',').map(element => {
            val trimmed = element.trim
            Phrase(trimmed, trimmed.count(_.isWhitespace) + 1, 1, content)
          })
        }
        case ContentType.Article => {
          extractUsing(content, if (levelUsed == ExtractionLevel.PHRASES) phraseExtract else ngramExtract)
        }
        case x if (x == ContentType.Title || x == ContentType.Description) => {
          extractUsing(content, ngramExtract)
        }
        case ContentType.Topic => Iterable.empty
        case ContentType.Concept => Iterable.empty
        case _ => throw new RuntimeException("Cannot match contenttype")
      }
    }
    contents flatMap (content => extractCandidate(content))
  }


  lazy val phrasesWithTopicsOption = grapherOntology.searchForTopics(phrases, followNonAnnotated, maxDepth, excludeTopicUris, excludeConceptUris)

  lazy val phrasesWithTopics = {
    phrasesWithTopicsOption collect {
      case phrase if !phrase.topic.isEmpty => PhraseAndTopic(phrase.topic.get, phrase.phrase)
    }
  }

  lazy val topics = {
    val extractedPhrases = phrasesWithTopics.groupBy(_.topic.node.uri)

    val providedContents = contents filter {
      content =>
        content.contentType == ContentType.Topic || content.contentType == ContentType.Concept
    } groupBy (_.text)
    val providedTopics = grapherOntology.getTopics(providedContents.keySet, followNonAnnotated, maxDepth)
    val providedPhrases = providedTopics map {
      case (uri, node) =>
        val providedContent = providedContents(uri)
        val phraseAndTopics = providedContent map (c => PhraseAndTopic(node, Phrase(uri, node.node.grams, providedContent.size, c)))
        (uri, phraseAndTopics)
    }

    extractedPhrases ++ providedPhrases
  }

  def compoundWordScore(text: Phrase) = {
    text.grams match {
      case x if x < 1 => 0d
      case 1 => 0.22d
      case 2 => 0.44d
      case 3 => 0.66d
      case 4 => 0.88d
      case x if x > 4 => 1d
    }
  }

  def contentTypeScore(text: Phrase) = {
    text.content.weight match {
      case Weight.High => 0.99d
      case Weight.Medium => 0.66d
      case Weight.Low => 0.33d
      case _ => throw new RuntimeException("Need to add a score for Content Weight " + text)
    }
  }

  def properNounScore(text: Phrase) = {
    val uppercount = text.text.count(_.isUpper)
    val grams = text.grams
    val higher = math.max(uppercount, grams)
    val lower = math.min(uppercount, grams)
    grvmath.percentOf(lower, higher)
  }

  def positionalScore(text: Phrase) = {
    val score = text.content.weight match {
      case Weight.Low => grvmath.percentOf(text.content.text.indexOf(text.text), text.content.text.length())
      case Weight.Medium => 1.0
      case Weight.High => 1.0
    }
    score
  }

  lazy val allTopics = topScoredTopics(150).map(entry=>entry.topic.uri).toSet
  lazy val bestTopics = TopicExtraction.extractUsingReductiveAlgo(allTopics).keep

  private def topScoredTopics(max: Int, minScore: Double = 0.0) = {
    scoredTopics.filter(_.score >= minScore).toSeq.sortBy(-_.score).take(max)
  }

  lazy val scoredConcepts = {
    val interests = scoredTopics.flatMap {
      topic =>
        topic.topic.interests.map(im => ScoredTopicInterestMatch(topic, im))
    }

    val grouped = interests.groupBy(_.interestMatch.interest)


    val scores = grouped.map {
      case (interest: StrictOntologyNode, matches: Iterable[ScoredTopicInterestMatch]) =>
        val Cc = grvmath.sumBy(matches.flatMap(_.topic.phrases))(_.phrase.count) //Total phrases contributing to interest
        val Tsn = grvmath.meanBy(matches.map(_.topic))(_.score) //Average topic score

        val uniquePaths = collection.mutable.HashSet[String]()
        matches.foreach(interestMatch => {
          val path = interestMatch.interestMatch.path.nodes
          val innerPath = path.drop(1).dropRight(1)
          val pathStr = innerPath.map(_.uri).mkString
          uniquePaths.add(pathStr)
        })
        val Cup = uniquePaths.size //Number of unqiue paths leading to concept
        val averageDepth = grvmath.meanBy(matches)(_.interestMatch.depth)

        val Cp = grvmath.sumBy(matches.flatMap(_.interestMatch.path.nodes.drop(1).dropRight(1)))(_.popularity) //Total popularity of nodes that led to the match

        val averageCloseness = ((maxDepth - averageDepth) / maxDepth)

        //      val score = 1d - (averageDepth / convergence.toDouble)
        //-1 is passed as the score because the calculate step overrides it.
        ScoredConcept(interest, -1.0, Cc, averageDepth, matches, Cup, Cp, Tsn, averageCloseness, 0.0d)
    }.toSeq

    grvmath.zscoresProjector(scores)(_.popularity) {case (value: ScoredConcept, d: Double) => value.relativePopularity = d}
    grvmath.zscoresProjector(scores)(_.averageCloseness) {case (value: ScoredConcept, d: Double) => value.relativeCloseness = d}
    grvmath.zscoresProjector(scores)(_.convergence) {case (value: ScoredConcept, d: Double) => value.relativeConvergence = d}
    grvmath.zscoresProjector(scores)(_.uniquePaths) {case (value: ScoredConcept, d: Double) => value.relativeUniquePaths = d}
    grvmath.zscoresProjector(scores)(_.averageTopicScore) {case (value: ScoredConcept, d: Double) => value.relativeTopicScores = d}

    for (i <- 0 until scores.size) {
      val score = scores(i)
      score.calculate(conceptScorer)
    }
    scores
  }

  def interestGraph(algorithm: GrapherAlgo = GrapherAlgo.EverythingAlgo) = algorithm(this)

  lazy val scoredTopics = {
    topics map {
      case (topicuri, phrasesIterable) =>
        val phrases = phrasesIterable.toSeq
        val topic = phrases.head.topic.node

        val frequency = phrases.foldLeft(0)((total, phrase) => total + phrase.phrase.count)
        val popularity = topic.popularity

        val relativeFrequency = grvmath.percentOf(frequency, totalPhrases)
        val relativePopularity = grvmath.percentOf(popularity, totalPopularity)
        val avgCompoundScore = grvmath.meanBy(phrases)(phrase => compoundWordScore(phrase.phrase))
        val avgProperNounScore = grvmath.meanBy(phrases)(phrase => properNounScore(phrase.phrase))
        val avgContentWeightScore = grvmath.meanBy(phrases)(phrase => contentTypeScore(phrase.phrase))
        val avgPositionalScore = grvmath.meanBy(phrases)(phrase => positionalScore(phrase.phrase))

        val grams = topic.grams

        val searchScore = phrases.foldLeft(0f) {
          (sum, phraseAndTopic) =>
            val isTagOnDomain = phraseAndTopic.topic.node.followedNodes.exists(_.isTag)
            val domainTagBonus = if (isTagOnDomain) {
              if (phraseAndTopic.phrase.content.contentType == ContentType.Keywords) tagIsKeywordScoreMultiplier else tagIsContentScoreMultiplier
            } else {
              1.0f
            }

            val score = (phraseAndTopic.topic.score * domainTagBonus)
            sum + score
        } / phrases.size

        val frequencyWeight = weights.frequencyWeight
        val compoundWordWeight = weights.compoundWordWeight
        val searchWeight = weights.searchWeight
        val popularityWeight = weights.popularityWeight
        val properNounWeight = weights.properNounWeight
        val contentTypeWeight = weights.contentTypeWeight
        val positionalWeight = weights.positionalWeight


        val adjRelativeFrequency = relativeFrequency * frequencyWeight
        val adjCompoundWordScore = avgCompoundScore * compoundWordWeight
        val adjSearchScore = searchScore * searchWeight
        val adjRelativePopularity = relativePopularity * popularityWeight
        val adjProperNounWeight = avgProperNounScore * properNounWeight
        val adjContentWeightScore = avgContentWeightScore * contentTypeWeight
        val adjPositionalScore = avgPositionalScore * positionalWeight


        val adjustments = frequencyWeight + compoundWordWeight + searchWeight + popularityWeight + properNounWeight + contentTypeWeight + positionalWeight

        val score = ((adjRelativeFrequency + adjRelativePopularity + adjCompoundWordScore + adjSearchScore + adjProperNounWeight + adjContentWeightScore + adjPositionalScore) / adjustments.toFloat)

        ScoredTopic(topic, score, searchScore, frequency, relativeFrequency, popularity, relativePopularity, grams, avgCompoundScore, phrases)

    }
  }


  /**
   * Calculates percent of item to total assuming a scale of 0 to 1, to 2 decimal precision
   */
}

object Grapher {

  def main(args: Array[String]) {

    var myseq: Seq[String] = null

    args.toSeq
    myseq = args.asInstanceOf[Seq[String]]


  }
}