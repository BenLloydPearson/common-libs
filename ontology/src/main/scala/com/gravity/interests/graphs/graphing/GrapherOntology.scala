package com.gravity.interests.graphs.graphing

import java.net.SocketException

import com.google.common.collect.ForwardingObject
import com.gravity.ontology.annotation.BadConcepts
import com.gravity.ontology.{InterestMatch, OntologyGraph2, OntologyNode}
import com.gravity.utilities.ScalaMagic.bisect
import com.gravity.utilities.api.{GravityHttp, GravityHttpException}
import com.gravity.utilities.web._
import com.gravity.utilities.{MurmurHash, Settings}
import org.joda.time.DateTime
import org.neo4j.graphdb.{Node, Path, Relationship}
import org.openrdf.model.impl.URIImpl
import org.scalatra.{Get, Post}

import scala.collection.JavaConversions._
import scalaz.{Failure, Success}

/**
 * Grapher-Ontology interface which can be local neo4j or remote.
 */
trait GrapherOntology {
  def getTopic(uri: String, followNonAnnotated: Boolean, maxInterestDepth: Int): Option[StrictScoredNode] = getTopics(Traversable(uri), followNonAnnotated, maxInterestDepth).get(uri)
  def getTopics(uris: Traversable[String], followNonAnnotated: Boolean, maxInterestDepth: Int): Map[String, StrictScoredNode]
  def getTopicNames(uris: Traversable[String]): Map[String, String]
  def searchForTopics(phrases: Iterable[Phrase], followNonAnnotated: Boolean, maxInterestDepth: Int, excludeTopicUris: Set[String], excludeConceptUris: BadConcepts): Iterable[PhraseAndTopicOption]
}
object GrapherOntology {
  lazy val instance = {
    val remoteHost = Option(Settings.getProperty("graphing.ontology.host"))
    val timeout = 1000 * 60 * 3
    remoteHost map (new GrapherOntologyApi.RESTGrapherOntology(_) with IgnoreEmptyArgs) getOrElse (LocalGrapherOntology)
  }
}

class ForwardingGrapherOntology(grapherOntology: GrapherOntology) extends ForwardingObject with GrapherOntology {
  override def delegate = grapherOntology
  override def getTopics(uris: Traversable[String], followNonAnnotated: Boolean, maxInterestDepth: Int): Map[String, StrictScoredNode] = delegate.getTopics(uris, followNonAnnotated, maxInterestDepth)
  override def getTopicNames(uris: Traversable[String]): Map[String, String] = delegate.getTopicNames(uris)
  override def searchForTopics(phrases: Iterable[Phrase], followNonAnnotated: Boolean, maxInterestDepth: Int, excludeTopicUris: Set[String], excludeConceptUris: BadConcepts): Iterable[PhraseAndTopicOption] = delegate.searchForTopics(phrases, followNonAnnotated, maxInterestDepth, excludeTopicUris, excludeConceptUris)
}

trait IgnoreEmptyArgs extends GrapherOntology {
  abstract override def getTopics(uris: Traversable[String], followNonAnnotated: Boolean, maxInterestDepth: Int): Map[String, StrictScoredNode] = if (uris.isEmpty) Map.empty else super.getTopics(uris, followNonAnnotated, maxInterestDepth)
  abstract override def getTopicNames(uris: Traversable[String]): Map[String, String] = if (uris.isEmpty) Map.empty else super.getTopicNames(uris)
  abstract override def searchForTopics(phrases: Iterable[Phrase], followNonAnnotated: Boolean, maxInterestDepth: Int, excludeTopicUris: Set[String], excludeConceptUris: BadConcepts): Iterable[PhraseAndTopicOption] = if (phrases.isEmpty) Iterable.empty else super.searchForTopics(phrases, followNonAnnotated, maxInterestDepth, excludeTopicUris, excludeConceptUris)
}

case class PhraseAndTopicOption(topic: Option[StrictScoredNode], phrase: Phrase)

case class StrictInterestMatch(interest: StrictOntologyNode, depth: Int, path: InterestPath)
object StrictInterestMatch {
  def apply(im: InterestMatch, maxInterestDepth: Int, followNonAnnotated: Boolean): StrictInterestMatch = {
    StrictInterestMatch(StrictOntologyNode.forInterestNode(im.interest, maxInterestDepth, followNonAnnotated), im.depth, InterestPath(im.path))
  }
}

case class StrictScoredNode(score: Float, node: StrictOntologyNode)

case class StrictOntologyNode(name: String, uri: String, popularity: Int, level: Int, grams: Int, isTag: Boolean, interests: List[StrictInterestMatch], narrowerInterests: List[StrictOntologyNode], broaderInterests: List[StrictOntologyNode], followedNodes: List[StrictOntologyNode]) {
  def getCleanName: String = {
    if (name.contains("_")) OntologyGraph2.getTopicNameWithoutOntology(uri) else name
  }

  def id = MurmurHash.hash64(uri)
}
object StrictOntologyNode {
  def forTopicNode(ontologyNode: OntologyNode, maxInterestDepth: Int, followNonAnnotated: Boolean, followedNodes: List[OntologyNode] = List.empty, excludeConceptUris: BadConcepts = BadConcepts.empty): StrictOntologyNode = {
    val isTag = ontologyNode.indicatesNode.isDefined
    val interests = if (maxInterestDepth > 0) ontologyNode.interests(maxInterestDepth, followNonAnnotated, excludeConceptUris).toList map (StrictInterestMatch(_, maxInterestDepth, followNonAnnotated)) else List.empty
    val followed = followedNodes map (StrictOntologyNode.forTopicNode(_, 0, followNonAnnotated))
    StrictOntologyNode(ontologyNode.name, ontologyNode.uri, ontologyNode.popularity, ontologyNode.level, ontologyNode.grams, isTag, interests, List.empty, List.empty, followed)
  }

  def forInterestNode(ontologyNode: OntologyNode, maxInterestDepth: Int, followNonAnnotated: Boolean): StrictOntologyNode = {
    val isTag = false
    val interests = if (maxInterestDepth > 0) ontologyNode.interests(maxInterestDepth, followNonAnnotated).toList map (StrictInterestMatch(_, 0, followNonAnnotated)) else List.empty
    val narrower = if (maxInterestDepth > 0) ontologyNode.narrowerInterests.toList map (StrictOntologyNode.forInterestNode(_, 0, followNonAnnotated)) else List.empty
    val broader = if (maxInterestDepth > 0) ontologyNode.narrowerInterests.toList map (StrictOntologyNode.forInterestNode(_, 0, followNonAnnotated)) else List.empty
    val followed = List.empty
    StrictOntologyNode(ontologyNode.name, ontologyNode.uri, ontologyNode.popularity, ontologyNode.level, ontologyNode.grams, isTag, interests, narrower, broader, followed)
  }
}

case class InterestPath(steps: List[InterestStep]) {
  lazy val nodes = steps filterNot (_.uri.isEmpty)
  lazy val relationships = steps filter (_.uri.isEmpty)
  def iterator = steps.iterator
  def length = steps.length
}
object InterestPath {
  def apply(path: Path): InterestPath = {
    val avoidStreamThatSerializesOuterVars = path.toList
    InterestPath(avoidStreamThatSerializesOuterVars map {
      case node: Node =>
        val onode = new OntologyNode(node)
        InterestStep(
          name = onode.name,
          uri = onode.uri,
          popularity = onode.popularity,
          trendingViewScore = onode.trendingScore
        )
      case rel: Relationship => InterestStep(rel.getType.name, "", -1, 0.0d)
    })
  }
}

case class InterestStep(name: String, uri: String, popularity: Int, trendingViewScore: Double)


trait GrapherOntologyImpl extends GrapherOntology {
  val graph: OntologyGraph2

  override def getTopics(uris: Traversable[String], followNonAnnotated: Boolean, maxInterestDepth: Int) = {
    uris.flatMap{ uri =>
      graph.node(new URIImpl(uri)) map { node =>
        (uri, StrictScoredNode(1.0f, StrictOntologyNode.forTopicNode(node, maxInterestDepth, followNonAnnotated)))
      }
    }.toMap
  }

  override def getTopicNames(uris: Traversable[String]) = {
    uris.map(uri => (uri, OntologyGraph2.getTopicName(uri))).toMap
  }

  override def searchForTopics(phrases: Iterable[Phrase], followNonAnnotated: Boolean, maxInterestDepth: Int, excludeTopicUris: Set[String], excludeConceptUris: BadConcepts) = {
    phrases map { phrase =>
      val scoredOntologyNode = graph.searchForTopicRichly(phrase) filter (scored => !excludeTopicUris.contains(scored.node.uri)) map { scored =>
        StrictScoredNode(scored.score, StrictOntologyNode.forTopicNode(scored.node, maxInterestDepth, followNonAnnotated, scored.followedNodes, excludeConceptUris))
      }
      PhraseAndTopicOption(scoredOntologyNode, phrase)
    }
  }
}

class GrapherOntologyWithGraph(override val graph: OntologyGraph2) extends GrapherOntologyImpl

object LocalGrapherOntology extends GrapherOntologyWithGraph(OntologyGraph2.graph) {
  def startServer() {

  }
  def shutDown() {
    //this would also be the place to send a poison pill to the typed actor
    graph.shutdown()
  }
}

object GrapherOntologyApi {
  val LIST_PARAM_SEP = ";"

  sealed abstract class BaseTopicApi(name: String, route: String, description: String) extends ApiSpec(name, route, Get :: Post :: Nil, description = description, live = false) {
    val phrases = &(ApiRegexParam("phrases", required = true, regex = ("[^" + LIST_PARAM_SEP + "]+").r, description = "semicolon-delimited list of phrases to match to topics", exampleValue = "Barack Obama;found guilty;dog"))
    val followNonAnnotated = &(ApiBooleanParam("followNonAnnotated", required = false, description = "TODO", defaultValue = true))
    val maxInterestDepth = &(ApiIntParam("maxInterestDepth", required = true, description = "TODO"))
  }

  object TopicsByUriApi extends BaseTopicApi("Get Topics", "/ontology/topics/byuri",
    description = "Get Topics"
  ) {
    override def request(req: ApiRequest) = {
      val topics = LocalGrapherOntology.getTopics(phrases.get(req).toIterable, followNonAnnotated.getOrDefault(req), maxInterestDepth.get(req))
      ApiResponse(topics)
    }
  }

  object TopicNamesApi extends ApiSpec("Get Topic Names", "/ontology/topics/names", Get :: Post :: Nil,
    description = "Get Topic Names",
    exampleResponse = null,
    live = false
  ) {
    val uris = &(ApiRegexParam("uris", required = true, regex = ("[^" + LIST_PARAM_SEP + "]+").r, description = "semicolon-delimited list of uris to match to topics", exampleValue = "http://dbpedia.org/resource/PlayStation;http://dbpedia.org/resource/YouTube"))

    override def request(req: ApiRequest) = {
      ApiResponse(LocalGrapherOntology.getTopicNames(uris.get(req).toIterable))
    }
  }

  object SearchTopicsByPhraseApi extends BaseTopicApi("Search Topics By Phrase", "/ontology/topics/byphrase",
    description = "TODO"
  ) {
    val excludeTopicUris = &(ApiRegexParam("excludeTopicUris", required = false, regex = "[^%s]*".format(LIST_PARAM_SEP).r, description = "semicolon-delimited list of topic uris to exclude", exampleValue = "http://dbpedia.org/resource/PlayStation;http://dbpedia.org/resource/YouTube"))
    val excludeConceptUris = &(ApiJsonParam[BadConcepts]("excludeConceptUris", required = false, defaultValue = BadConcepts.empty, description = "TODO", exampleValue = "TODO"))

    override def request(req: ApiRequest) = {
      val unused = ContentToGraph("unused", new DateTime(), "doesn't matter", Weight.Medium, ContentType.Article)
      val dummyPhrases = phrases.get(req).toIterable map (Phrase(_, -1, 1, unused))
      val excludeTopics = (excludeTopicUris(req) map (_.toIterable) getOrElse (Iterable.empty)).toSet
      val excludeConcepts = excludeConceptUris getOrDefault (req)
      val matchedPhrases = LocalGrapherOntology.searchForTopics(dummyPhrases, followNonAnnotated.getOrDefault(req), maxInterestDepth.get(req), excludeTopics, excludeConcepts)
      val scoredNodesOrNulls = matchedPhrases map (_.topic.orNull)
      ApiResponse(scoredNodesOrNulls)
    }
  }

  trait RESTGrapherOntologyLike extends GrapherOntology {
    this: GravityHttp =>

    override def getTopics(uris: Traversable[String], followNonAnnotated: Boolean, maxInterestDepth: Int): Map[String, StrictScoredNode] = {
      val uriStruct = uris mkString (LIST_PARAM_SEP)
      val extraction = requestExtract[Map[String, StrictScoredNode]](TopicsByUriApi.route, Post.toString(), params = Map(
        TopicsByUriApi.phrases.paramKey -> uriStruct,
        TopicsByUriApi.followNonAnnotated.paramKey -> followNonAnnotated.toString,
        TopicsByUriApi.maxInterestDepth.paramKey -> maxInterestDepth.toString
      ))
      extraction match {
        case Failure(ex) => throw ex
        case Success(result) => result.payload
      }
    }

    override def getTopicNames(uris: Traversable[String]) = {
      def req(uriChunk: Traversable[String]) = {
        requestExtract[Map[String, String]](TopicNamesApi.route, Post.toString(), params = Map(
          TopicNamesApi.uris.paramKey -> (uriChunk mkString (LIST_PARAM_SEP))
        )) match {
          case Failure(ex) => throw ex
          case Success(result) => result.payload
        }
      }
      def tryChunkedReqs(chunksToRequest: Stream[Seq[String]]): Option[Stream[Map[String, String]]] = {
        try {
          Some(chunksToRequest map (req))
        }
        catch {
          case GravityHttpException(_, requestTooBig: SocketException, _, _) => None
        }
      }

      val successfulResponses = bisect(uris.toSeq) map (tryChunkedReqs) filter (_.isDefined)
      val successfulChunks = successfulResponses.headOption.getOrElse{
        throw new SocketException("unable to make API request in small enough chunks")
      }.get
      val recombined = successfulChunks reduce (_ ++ _)
      recombined
    }

    override def searchForTopics(phrases: Iterable[Phrase], followNonAnnotated: Boolean, maxInterestDepth: Int, excludeTopicUris: Set[String], excludeConceptUris: BadConcepts) = {
      val phrasesStruct = phrases map (_.text) mkString (LIST_PARAM_SEP)
      val excludeTopicStruct = excludeTopicUris mkString (LIST_PARAM_SEP)
      val extraction = requestExtract[List[StrictScoredNode]](SearchTopicsByPhraseApi.route, Post.toString(), params = Map(
        SearchTopicsByPhraseApi.phrases.paramKey -> phrasesStruct,
        SearchTopicsByPhraseApi.followNonAnnotated.paramKey -> followNonAnnotated.toString,
        SearchTopicsByPhraseApi.maxInterestDepth.paramKey -> maxInterestDepth.toString,
        SearchTopicsByPhraseApi.excludeTopicUris.paramKey -> excludeTopicStruct,
        SearchTopicsByPhraseApi.excludeConceptUris.paramKey -> ApiServlet.serializeToJson(excludeConceptUris)
      ))
      extraction match {
        case Failure(ex) => throw ex
        case Success(result) => result.payload zip (phrases) map { case (scoredNode, phrase) => PhraseAndTopicOption(Option(scoredNode), phrase) }
      }
    }
  }

  class RESTGrapherOntology(hostname: String) extends RESTGrapherOntologyLike with GravityHttp {
    override def host = hostname
  }
}