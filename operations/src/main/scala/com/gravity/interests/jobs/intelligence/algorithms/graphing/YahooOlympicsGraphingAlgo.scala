package com.gravity.interests.jobs.intelligence.algorithms.graphing

import com.gravity.interests.jobs.intelligence.algorithms.{BaseSiteGrapherAlgo, SiteSpecificGraphingAlgoResult}
import com.gravity.interests.jobs.intelligence.operations.ArticleRowDomainDoNotUseAnymorePlease
import com.gravity.interests.jobs.intelligence.SchemaTypes._

import scala.io.Source
import scala.collection._
import com.gravity.textutils.analysis.PhraseExtractorUtil
import com.gravity.interests.jobs.intelligence.{ArticleLike, EdgeType, NodeType, StoredGraph}
import com.gravity.ontology.OntologyGraphName
import org.apache.commons.lang.StringUtils

/*             )\._.,--....,'``.
 .b--.        /;   _.. \   _\  (`._ ,.
`=,-,-'~~~   `----(,_..'--(,_..'`-.;.'  */
object YahooOlympicsGraphingAlgo {

  val topicsToTitles = mutable.Map[String, String]()

  val titlesToTopics = mutable.Map[String, String]()
  val titlesToTopicsLowercase = mutable.Map[String, String]()
  val concepts = mutable.Map[String, String]()
  val titlesToConcepts = mutable.Map[String, String]()

  val topicRels = mutable.Map[String, String]()

  val conceptRels = mutable.Map[String, String]()
  val topicRel = "IS A TOPIC OF"
  val conceptRel = "IS A CONCEPT UNDER"

  Source.fromInputStream(getClass.getResourceAsStream("olympics_ontology-updated2.csv")).getLines().foreach {
    line =>
      val split = line.split(',')
      def clean(itm: String) = itm.replace("\"", "")
      val subject = clean(split(1))
      val origTitle = clean(split(2))
      val replaceTitle = clean(split(3))
      val title = if (replaceTitle.size > 0) replaceTitle else origTitle
      val relType = clean(split(4))
      val target = clean(split(5))

      if (relType == conceptRel) {
        concepts += subject -> title
        conceptRels += subject -> target
        titlesToConcepts += title -> subject
      }
  }

  Source.fromInputStream(getClass.getResourceAsStream("olympics_ontology-updated2.csv")).getLines().foreach {
    line =>
      val split = line.split(',')
      def clean(itm: String) = itm.replace("\"", "")
      val subject = clean(split(1))
      val origTitle = clean(split(2))
      val replaceTitle = clean(split(3))
      val title = if (replaceTitle.size > 0) replaceTitle else origTitle
      val relType = clean(split(4))
      val target = clean(split(5))

      if (relType == topicRel) {
        if (!titlesToTopics.contains(title) && !titlesToConcepts.contains(title)) {
          topicsToTitles += subject -> title
          titlesToTopics += title -> subject
          topicRels += subject -> target
          titlesToTopicsLowercase += title.toLowerCase -> subject
        } else {
          println("NOt using " + subject)
        }
      }
  }

  def graphTopics(content: String) = {
    val grams = PhraseExtractorUtil.extractNgrams(content, 1, 2)
    val matchedTopics = mutable.Set[String]()
    grams.iterator.foreach {
      phraseRes =>
        val phraseLower = phraseRes.phrase.toLowerCase
        for ((topicTitle, topicUri) <- titlesToTopicsLowercase) {
          val levDist = StringUtils.getLevenshteinDistance(phraseLower, topicTitle)
          val maxDist = if (topicTitle.size > topicUri.size) topicTitle.size else topicUri.size
          val levPercent = com.gravity.utilities.grvmath.percentOf(levDist.toDouble, maxDist.toDouble)
          if (levDist == 0) {
            matchedTopics += topicUri
          }

          //      if(phraseLower.contains("triple") && topicTitle.contains("triple"))
          //        println("Levperct between " + topicTitle + " and " + phraseLower + " is " + levPercent)
          //      if(levPercent <= 0.20) {
          //        println("Levdist between " + phraseLower + " and " + topicTitle + " is " + levDist)
          //         matchedTopics += topicUri
          //        }
        }
    }

    val conceptUrisAdded = mutable.Set[String]()

    val sgb = StoredGraph.make
    val upperConceptUri = "http://insights.gravity.com/interest/Sports"
    val upperConceptName = "Sports"
    sgb.addNode(upperConceptUri, upperConceptName, NodeType.Interest, level = 1)
    matchedTopics.foreach {
      topicUri =>
        val topicName = topicsToTitles(topicUri)
        val conceptUri = topicRels(topicUri)
        val conceptName = concepts(conceptUri)
        sgb.addNode(topicUri, topicName, NodeType.Topic)
        if (!conceptUrisAdded.exists(_ == conceptUri)) {
          sgb.addNode(conceptUri, conceptName, NodeType.Topic)
          conceptUrisAdded += conceptUri
          sgb.relate(conceptUri, upperConceptUri, EdgeType.InterestOf)
        }

        sgb.relate(topicUri, upperConceptUri, EdgeType.InterestOf)
    }
    sgb.build
  }


}

class YahooOlympicsGraphingAlgo extends BaseSiteGrapherAlgo(5, 1) {
  override def graphArticleRowDomain(article: ArticleRowDomainDoNotUseAnymorePlease, counter: CounterFunc = CounterNoOp)(implicit ogName: OntologyGraphName): SiteSpecificGraphingAlgoResult = {

    val content = article.title + "\n" + article.content
    val graph = YahooOlympicsGraphingAlgo.graphTopics(content)
    SiteSpecificGraphingAlgoResult(autoStoredGraph = Some(graph), annoStoredGraph = None, conceptGraph = None, phraseConceptGraph = None)
  }

  // override def graphArticleRow(article: ArticleRow, counter: CounterFunc = CounterNoOp): SiteSpecificGraphingAlgoResult = {
  override def graphArticleRow(article: ArticleLike, counter: CounterFunc = CounterNoOp)(implicit ogName: OntologyGraphName): SiteSpecificGraphingAlgoResult = {

    val content = article.title + "\n" + article.content
    val graph = YahooOlympicsGraphingAlgo.graphTopics(content)
    SiteSpecificGraphingAlgoResult(autoStoredGraph = Some(graph), annoStoredGraph = None, conceptGraph = None, phraseConceptGraph = None)
  }
}
