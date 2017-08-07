package com.gravity.ontology.service

import com.gravity.interests.graphs.graphing._
import com.gravity.utilities.web._
import org.joda.time._
import com.gravity.utilities.components._

import scala.collection.JavaConversions._
import com.gravity.interests.graphs.graphing.ScoreWeights.{defaultWeights => DefWeight}

import collection.mutable.{Buffer, HashMap, ListBuffer}
import com.gravity.ontology.{InterestMatch, OntologyGraph2, OntologyGraphName, OntologyNode}

import collection.mutable
import org.openrdf.model.URI

/*             )\._.,--....,'``.
.b--.        /;   _.. \   _\  (`._ ,.
`=,-,-'~~~   `----(,_..'--(,_..'`-.;.'  */

//Slight change

case class LayeredInterestResponse(interests: Seq[LayeredInterest], topics: Seq[LayeredTopic], title: String)

case class LayeredInterest(interest: String, score: Double, level: Int)

case class LayeredTopic(topic: String, score: Double)

//case class TraversedConcept(concept:String, uri: String, topics: Seq[TraversedTopic],concepts:Seq[TraversedConcept])
case class TraversedConcept(concept: String, uri: String)

case class TraversedTopic(topic: String, uri: String)

case class InterestResponse(var topics: mutable.Set[OntologyNode], level: Int, lowerInterests: Buffer[InterestResponse], interest: OntologyNode) {
  def merge(that: InterestResponse): Boolean = {
    if (that.interest.uri != this.interest.uri || that.interest.level != this.interest.level) return false

    if (!that.topics.isEmpty) this.topics ++ that.topics

    for (lower <- that.lowerInterests; if (!this.lowerInterests.exists(_.interest.uri == lower.interest.uri))) {
      this.lowerInterests += lower
    }

    true
  }
}

case class FetchedArticle(cogs: List[ContentToGraph], articleTitle: String)

object OntologyService {

  //def fetchAndPrepareArticleForGraphing(urlTouse:String = "",crawlLink: Boolean =true,rawText:String = "",rawTitle:String="") : Option[HashMap[List[ContentToGraph],String]] = {
  def fetchAndPrepareArticleForGraphing(urlTouse: String = "", crawlLink: Boolean = true, rawText: String = "", rawTitle: String = ""): Option[FetchedArticle] = {
    if (crawlLink == true) {
      val article = ContentUtils.fetchAndExtractMetadataSafe(urlTouse)
      if (article != null) {
        Some(
          FetchedArticle(List(ContentToGraph(article.url, new DateTime, article.title, Weight.High, ContentType.Title), ContentToGraph(article.url, new DateTime, article.text, Weight.Medium, ContentType.Article)), article.title)
          //HashMap("cogList" -> List(ContentToGraph(article.url, new DateTime, article.title, Weight.High, ContentType.Title),ContentToGraph(article.url, new DateTime, article.text, Weight.Medium, ContentType.Article)), "articleTitle" -> article.title)
          //ContentToGraph(article.url, new DateTime, article.title, Weight.High, ContentType.Title) ::
          //ContentToGraph(article.url, new DateTime, article.text, Weight.Medium, ContentType.Article) ::
          //Nil
        )
      } else {
        None
      }
    }
    else {
      Some(
        FetchedArticle(List(ContentToGraph("http://rawtitle", new DateTime, rawTitle, Weight.High, ContentType.Title), ContentToGraph("http://rawtext", new DateTime, rawText, Weight.Medium, ContentType.Article)), rawTitle)
      )
    }

  }


  def prepareTopicForGraphing(topic: String): Option[List[ContentToGraph]] = {
    Some(
      ContentToGraph("http://rawtitle", new DateTime, topic, Weight.High, ContentType.Keywords) ::
              Nil)
  }

  def scoredConceptToLayered(concept: ScoredConcept) = LayeredInterest(concept.interest.name, concept.score, concept.interest.level)

  def blankLayeredInterest() = LayeredInterest("", 0, 0)

  def layeredTopicsWithInterestGraph(urlTouse: String = "", crawlLink: Boolean = true, rawText: String = "", rawTitle: String = "")(implicit ogName: OntologyGraphName): Response[LayeredInterestResponse] = {

    fetchAndPrepareArticleForGraphing(urlTouse = urlTouse, crawlLink = crawlLink, rawText = rawText, rawTitle = rawTitle) match {
      case Some(fetchedArticle) => {
        val weights = ScoreWeights(
          DefWeight.frequencyWeight,
          DefWeight.compoundWordWeight,
          DefWeight.searchWeight,
          DefWeight.popularityWeight,
          DefWeight.properNounWeight,
          DefWeight.contentTypeWeight,
          DefWeight.positionalWeight
        )

        val cogList = fetchedArticle.cogs
        val articleTitle = fetchedArticle.articleTitle
        val grapher = new Grapher(cogList, weights)
        val layeredInterests = ListBuffer[LayeredInterest]()
        var layeredTopics = new ListBuffer[LayeredTopic]
        val ig = grapher.interestGraph(GrapherAlgo.LayeredAlgo)

        for (c <- ig.concepts) {
          println(c.interest.name)
          layeredInterests += scoredConceptToLayered(c)
        }

        //Get all the topics under the Best Interest
        //val validTopics = ig2.topicsOfInterestTransitive(bestInterest1)
        var boostedTopics = new HashMap[String, Double]

        //Get all the topics that fall under the Best Interest and the Interests under it, and boost their scores by how many times they appear
        for ((topic, interests) <- ig.topicsUnderInterestTransitive(ig.concepts.head)) {
          for (i <- 0 until interests.size) {
            topic.score = topic.score * 1.20
            boostedTopics(topic.topic.name) = topic.score
          }
        }


        for (t <- boostedTopics) {
          layeredTopics += LayeredTopic(t._1, t._2)
        }

        SomeResponse(LayeredInterestResponse(layeredInterests, layeredTopics, articleTitle))
      }
      case None => ErrorResponse("Got None")
    }
  }

  case class RobbieInterest(concept: OntologyNode, topics: mutable.Set[OntologyNode])

  def conceptTopicTreeNormalized(topicUris: Seq[URI], maxDepth: Int = 3): Iterable[InterestResponse] = {
    val conceptToDirectTopics = mutable.Map[String, RobbieInterest]()

    // find the nearest interest for each topic
    for {
      topicUri <- topicUris
      topic <- OntologyGraph2.graph.topic(topicUri)
      nearestConcept <- topic.interests(maxDepth, false).toSeq.sortBy(_.path.nodes().size).headOption
    } {
      conceptToDirectTopics.get(nearestConcept.interest.uri) match {
        case Some(interest) => interest.topics += topic
        case None => conceptToDirectTopics.put(nearestConcept.interest.uri, RobbieInterest(nearestConcept.interest, mutable.Set(topic)))
      }
    }

    //    val result = Buffer[InterestResponse]()
    val resultMap = mutable.Map[String, InterestResponse]()

    def appendResponse(resp: InterestResponse) {
      resultMap.get(resp.interest.uri) match {
        case Some(existing) => existing.merge(resp)
        case None => resultMap.put(resp.interest.uri, resp)
      }
    }

    // find the level 1[, 2, 3] concepts for each concept with topic(s)
    for ((_, interest) <- conceptToDirectTopics) {
      if (interest.concept.level == 1) {
        appendResponse(InterestResponse(interest.topics, 1, Buffer[InterestResponse](), interest.concept))
      }
      else {
        def buildFromLevel1(level1node: OntologyNode) {
          val level2 = if (interest.concept.level == 2) {
            InterestResponse(interest.topics, 2, Buffer[InterestResponse](), interest.concept)
          }
          else {
            val level3opt = if (interest.concept.level == 3) {
              Some(InterestResponse(interest.topics, 3, Buffer[InterestResponse](), interest.concept))
            } else {
              interest.concept.broaderInterests.find(_.level == 3) match {
                case Some(level3node) => Some(InterestResponse(interest.topics, 3, Buffer[InterestResponse](), level3node))
                case None => None
              }
            }

            interest.concept.broaderInterests.find(_.level == 2) match {
              case Some(level2node) => {
                val lower = level3opt match {
                  case Some(level3) => Buffer(level3)
                  case None => Buffer[InterestResponse]()
                }

                InterestResponse(mutable.Set.empty[OntologyNode], 2, lower, level2node)
              }
              case None => throw new RuntimeException("How the hell did I get a level 1 interest: '%s' and a lower level %d concept: '%s' without a level 2?!".format(level1node.uri, interest.concept.level, interest.concept.uri))
            }
          }

          appendResponse(InterestResponse(mutable.Set.empty[OntologyNode], 1, Buffer(level2), level1node))
        }
        interest.concept.broaderInterests.find(_.level == 1) match {
          case Some(level1node) => buildFromLevel1(level1node)
          case None => {
            var stillLooking = true
            for (broad <- interest.concept.broaderInterests; if (stillLooking)) {
              broad.broaderInterests.find(_.level == 1) match {
                case Some(level1node) => {
                  stillLooking = false
                  buildFromLevel1(level1node)
                }
                case None =>
              }
            }
          }
        }
      }
    }

    resultMap.values
  }

  //no response, just print out the data we are getting out of grapher

  def conceptTopicTree(topicUris: Seq[URI], maxDepth: Int = 3): Iterable[InterestResponse] = {
    val matches = Buffer[InterestMatch]()

    for (topicUri <- topicUris) {

      //This is a list of topic objects
      OntologyGraph2.graph.topic(topicUri) match {
        case Some(topic) => {
          topic.interests(maxDepth, false).toSeq.sortBy(_.path.nodes().size).headOption match {
            case Some(gotit) => {
              matches += gotit
            }
            case None =>
          }
        }
        case None => {
        }
      }
    }

    val respMap = mutable.Map[String, InterestResponse]()

    for (interestMatch <- matches) {
      val resp = respMap.getOrElseUpdate(interestMatch.interest.uri, {
        InterestResponse(mutable.Set.empty[OntologyNode], interestMatch.interest.level, Buffer[InterestResponse](), interestMatch.interest)
      })
        resp.topics += interestMatch.topic
    }

    for ((uri, interestResp) <- respMap) {
      for (broaderInterest <- interestResp.interest.broaderInterestsTransitive) {
        if (!respMap.contains(broaderInterest.uri)) {
          respMap.put(broaderInterest.uri, InterestResponse(mutable.Set.empty[OntologyNode], broaderInterest.level, Buffer[InterestResponse](), broaderInterest))
        }
      }
    }

    for ((uri, interestResp) <- respMap) {
      for (broaderInterest <- interestResp.interest.broaderInterestsTransitive) {
        val broader = respMap(broaderInterest.uri)
        if (!broader.lowerInterests.exists(_.interest.uri == interestResp.interest.uri)) {
          if (broader.level == interestResp.level - 1) {
            broader.lowerInterests += interestResp
          }
        }
      }
    }

    val minLevel = if (matches.size > 0) matches.map(_.interest.level).min else 0

    for (resp <- respMap.values; if (resp.level == minLevel)) yield resp
  }

  def layeredTopicRollup(topics: Set[String])(implicit ogName: OntologyGraphName) = {

    topics.foreach(validTopic => {

      prepareTopicForGraphing(validTopic) match {
        case Some(cogList) => {
          val weights = ScoreWeights(
            DefWeight.frequencyWeight,
            DefWeight.compoundWordWeight,
            DefWeight.searchWeight,
            DefWeight.popularityWeight,
            DefWeight.properNounWeight,
            DefWeight.contentTypeWeight,
            DefWeight.positionalWeight
          )

          //new grapher
          val grapher = new Grapher(cogList, weights)
          //graph topics with layered algo
          val ig = grapher.interestGraph(GrapherAlgo.LayeredAlgo)

          println("*******")

          //loop through the topic objects
          for (topic <- ig.topics) {
            println("Topic:" + topic.topic.topic.name + " / Topic URI:" + topic.topic.topic.uri)
          }

          //get all the concepts for this topic
          for (c <- ig.concepts) {
            println("Interest:" + c.interest.name + " / Interest URI:" + c.interest.uri + " / Interest Level:" + c.interest.level)
          }

        }
        case None => Nil //ErrorResponse("Got None")
      }
    })
    //SomeResponse(LayeredInterestResponse(layeredInterests,layeredTopics))
  }
}


trait OntologyService {

}

