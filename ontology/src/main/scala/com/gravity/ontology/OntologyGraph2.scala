package com.gravity

import com.gravity.ontology.vocab.{URIType, NS}
import scala.collection._
import scala.collection.JavaConversions._
import scalaz.{Node =>_, _}
import Scalaz._
import org.openrdf.model.URI
import org.apache.commons.lang.StringUtils
import org.neo4j.graphdb._
import org.neo4j.kernel.Traversal
import com.gravity.utilities.grvstrings._


package object ontology {
  implicit def wrapNode(node: Node) = new OntologyNode(node)

  implicit def URIEqual = Equal.equalA[URI]
}

package ontology {

import annotation.BadConcepts
import importing.OntologyGraphPopulator3
import nodes._
import org.neo4j.graphdb.traversal.{Evaluation, Evaluator}
import mutable.Buffer
import org.openrdf.model.impl.URIImpl
import java.net.URLDecoder
import com.gravity.utilities.cache.EhCacher
import org.joda.time.DateTime
import org.apache.commons.io.{IOUtils, FilenameUtils}
import java.io.{File, FileInputStream}

import org.neo4j.graphalgo.GraphAlgoFactory
import com.gravity.utilities._
import com.gravity.utilities.grvmath
import com.gravity.interests.graphs.graphing.Phrase
import scala.collection
import scala.Some
import com.gravity.ontology.annotation.ConceptLink
import com.gravity.ontology.importingV2.NodeProperties

/*             )\._.,--....,'``.
.b--.        /;   _.. \   _\  (`._ ,.
`=,-,-'~~~   `----(,_..'--(,_..'`-.;.'  */

object OntologySettings {
  val useNameLookup = Settings.getProperty("ontology.namelookups.useindex", "true").toBoolean
}


case class InterestMatch(interest: OntologyNode, depth: Int, topic: OntologyNode, path: Path) {
  override def toString = interest.name + " (" + depth + ") via " + path.nodes().map(_.uri).mkString("(", ",", ")")
}

case class OntologyNode(node: Node) {
  override def toString = "OntologyNode(" + name + ", " + uri + ")"

  def getProperty(key: String, defaultValue: Any) = node.getProperty(key, defaultValue)

  def getProperty(key: String) = node.getProperty(key)

  lazy val level = node.getProperty(ConceptNode.LEVEL_PROPERTY, -1).asInstanceOf[Int]

  //match may not be exhaustive.
  //[warn] It would fail on the following inputs: DBPEDIA_CLASS, OWL, YAGO_CLASS
  //  [warn]     NS.getType(uri) match {
  def inferredNodeType: NodeType = {
    NS.getType(uri) match {
      case URIType.TOPIC => NodeType.TOPIC
      case URIType.NY_TIMES => NodeType.TOPIC
      case URIType.WIKI_CATEGORY => NodeType.CONCEPT
      case URIType.GRAVITY_INTEREST => NodeType.CONCEPT
      case _ => NodeType.AMBIGUOUS
    }
  }

  def dumpDataAsString = {
    "Name: " + name + "\n" +
      "Inferred Type: " + inferredNodeType + "\n"
    "Outgoing Disambiguations: " + getRelatedNodes(TopicRelationshipTypes.DISAMBIGUATES, Direction.OUTGOING).mkString("[", ":", "]") + "\n" +
      "Incoming Disambiguations: " + getRelatedNodes(TopicRelationshipTypes.DISAMBIGUATES, Direction.INCOMING).mkString("[", ":", "]")
  }

  def hasName = node.hasProperty(NodeBase.NAME_PROPERTY)

  lazy val broaderInterests = getRelatedNodes(TopicRelationshipTypes.BROADER_INTEREST)

  lazy val broaderInterestsTransitive = Traversers.broaderInterestTraverser().traverse(this.node) map {
    path =>
      new OntologyNode(path.endNode)
  }

  lazy val narrowerInterests = getRelatedNodes(TopicRelationshipTypes.BROADER_INTEREST, Direction.INCOMING)


  lazy val name = node.getProperty(NodeBase.NAME_PROPERTY, "No name").asInstanceOf[String].replace(" (disambiguation)", "")

  lazy val bestNameAttempt = OntologyGraph2.bestNameAttempt(new URIImpl(uri))

  lazy val uri = node.getProperty(NodeBase.URI_PROPERTY).asInstanceOf[String]

  lazy val popularity = node.getProperty(NodeBase.VIEWCOUNT_PROPERTY, 1).asInstanceOf[Int]

  lazy val pageRank = node.getProperty(NodeProperties.PageRank, 0.0d).asInstanceOf[Double]
  lazy val triangleCount = node.getProperty(NodeProperties.TriangleCount, 0).asInstanceOf[Int]
  lazy val connectedComponentId = node.getProperty(NodeProperties.ConnectedComponenetId, 0l).asInstanceOf[Long]

  lazy val trendingScore = node.getProperty(NodeBase.TRENDING_VIEW_SCORE, 0.0d).asInstanceOf[Double] match {
    case score if score.isNaN => 0.0d
    case score => score
  }

  lazy val dbpediaClass = getRelatedNode(TopicRelationshipTypes.DBPEDIA_CLASS_OF_TOPIC)

  lazy val googleSearchCount = node.getProperty(NodeBase.GOOGLE_SEARCH_COUNT_PROPERTY, 0).asInstanceOf[Int]

  lazy val validDomains = {
    val validDomainsStr = node.getProperty(NodeBase.VALID_DOMAINS, "").asInstanceOf[String]
    if (validDomainsStr.isEmpty) {
      Array.empty[String]
    } else {
      validDomainsStr.split(",") map (_.stripSuffix("/"))
    }
  }

  lazy val grams = name.count(_.isWhitespace) + 1

  def followRedirect = getRelatedNode(TopicRelationshipTypes.REDIRECTS_TO) orElse (Some(this))

  def followColloquial = colloquiallyLinksTo orElse (Some(this))

  def followIndicatedNode = indicatesNode orElse (Some(this))

  lazy val colloquiallyLinksTo = getRelatedNode(TopicRelationshipTypes.COLLOQUIAL_PHRASE_OF)

  lazy val indicatesNode = getRelatedNode(TopicRelationshipTypes.TAG_INDICATES_NODE)

  def getRelatedNode(relType: RelationshipType, direction: Direction = Direction.OUTGOING) = {
    val relation = node.getSingleRelationship(relType, direction)
    if (relation == null) {
      None
    } else {
      Some(OntologyNode(relation.getOtherNode(node)))
    }
  }

  def getRelatedNodes(relType: RelationshipType, direction: Direction = Direction.OUTGOING) = {
    node.getRelationships(relType, direction).map(rel => new OntologyNode(rel.getOtherNode(node)))
  }

  def closestInterest(depth: Int = 3, followNonAnnotated: Boolean = true, excludeConceptUris: BadConcepts = BadConcepts.empty): Option[InterestMatch] = {
    None
  }


  def interests(depth: Int = 3, followNonAnnotated: Boolean = true, excludeConceptUris: BadConcepts = BadConcepts.empty): Iterable[InterestMatch] = {
    val traversalDesc = if (followNonAnnotated) {
      Traversers.interestTraverser(depth, excludeConceptUris)
    } else {
      Traversers.interestTraverserAnnotated(depth, excludeConceptUris)
    }

    traversalDesc traverse (this.node) map {
      path =>
        InterestMatch(path.endNode(), path.nodes().size - 1, this, path)
    }
  }

  def getShortestPath(startNode: Node, targetNode: Node) {
    // : Seq[Node] =  {
    //    SingleSourceShortestPath<Integer> shortestPath = new SingleSourceShortestPathBFS(null,
    //            Direction.BOTH, TopicRelationshipTypes.BROADER_CONCEPT, TopicRelationshipTypes.CONCEPT_OF_TOPIC, TopicRelationshipTypes.TOPIC_LINK);
    //
    println("StartNode:" + startNode)
    println("TargetNode:" + targetNode)

    //val shortpaths2 = org.neo4j.graphalgo.impl.shortestpath.Util.constructAllPathsToNode(startNode,predecessors,false,false)
    //println(shortpaths2)
    //new ShortestPath(3,10)

    val finder = GraphAlgoFactory.shortestPath(Traversal.expanderForAllTypes(Direction.BOTH), 3);
    val foundPath = finder.findSinglePath(startNode, targetNode);
    println(Traversal.simplePathToString(foundPath, NodeBase.NAME_PROPERTY));
    foundPath.foreach {
      fp =>
        println(fp.getProperty("Uri"))

    }
    println(foundPath.size)
    /*
    val shortpath = new SingleSourceShortestPathBFS(startNode,Direction.BOTH,TopicRelationshipTypes.BROADER_CONCEPT,TopicRelationshipTypes.CONCEPT_OF_TOPIC)

    val expander = Traversal.expanderForAllTypes(Direction.BOTH);
    val allPaths = new AllPaths(3,expander)

    val paths = allPaths.findSinglePath(startNode,targetNode)
    //paths.foreach {
    //  path =>
        paths.foreach {
          p =>
            try {
              println(p.getProperty(NodeBase.URI_PROPERTY))
            }
            catch {
              case _ =>

            }

        }
   // }
    //println(paths)
    shortpath.limitDepth(3)
    //shortpath.setStartNode(startNode)
    //val pathExists = shortpath.calculate(targetNode)
    //println(pathExists)
    val pathAsNodes = shortpath.getPathAsNodes(targetNode)
    println(pathAsNodes)
  */
    // return pathAsNodes.toSeq

  }

  /**
   * The old Neo4j traversal API.  Much less flexible but we know it works, so we can fall back to it if there's bugs.
   */
  def interestsOld(depth: Int): Iterable[InterestMatch] = {
    val t = node.traverse(Traverser.Order.DEPTH_FIRST,
      new StopEvaluator() {
        def isStopNode(currentPos: TraversalPosition): Boolean = {
          if (currentPos.depth() >= depth) return true

          for (badCat <- Traversers.badCats; if (currentPos.currentNode().getProperty(NodeBase.URI_PROPERTY).asInstanceOf[String].contains(badCat))) {
            return true
          }

          false
        }
      },
      ReturnableEvaluator.ALL_BUT_START_NODE,
      TopicRelationshipTypes.CONCEPT_OF_TOPIC,
      Direction.OUTGOING,
      TopicRelationshipTypes.BROADER_CONCEPT,
      Direction.OUTGOING,
      TopicRelationshipTypes.INTEREST_OF,
      Direction.OUTGOING
    )

    val results = Buffer[InterestMatch]()

    def shouldFilter(node: Node): Boolean = {
      val categoryUri = node.getProperty(NodeBase.URI_PROPERTY).asInstanceOf[String]

      if (!categoryUri.contains("interest")) {
        return true
      }
      false
    }
    for {
      node <- t
    } {
      val depth = t.currentPosition().depth()
      if (!shouldFilter(node)) {
        results += InterestMatch(OntologyNode(node), depth, this, null)
      }
    }
    results
  }

  lazy val concepts = {
    val traverser = Traversers.conceptTraverser.traverse(node)
    traverser.nodes.map(new OntologyNode(_))
  }

  lazy val topicLinks = {
    val traverser = Traversers.topicTraverser.traverse(node)
    traverser.nodes.map(new OntologyNode(_))
  }

  def filterWikiPatterns = {
    if (name.toLowerCase.contains("list of")) {
      None
    } else {
      Some(this)
    }
  }

  lazy val uriType = NS.getType(uri)

  def filterByUriType(uriType: URIType) = {
    if (NS.getType(uri) == uriType) Some(this) else None
  }

  def filterOutIfNotValid = {
    if (inferredNodeType != NodeType.TOPIC) None
    else if (!hasName) None
    else Some(this)
  }

  def filterDisambiguation(filterOnIncoming: Boolean = true, filterOnOutgoing: Boolean = true) = {
    if (!filterOnIncoming && !filterOnOutgoing) Some(this)

    val direction = if (filterOnIncoming && filterOnOutgoing) Direction.BOTH else if (filterOnIncoming) Direction.INCOMING else Direction.OUTGOING

    if (hasRelatedNode(TopicRelationshipTypes.DISAMBIGUATES, direction)) {
      None
    } else {
      Some(this)
    }
  }

  def hasRelatedNode(relType: RelationshipType, direction: Direction = Direction.OUTGOING) = {
    node.hasRelationship(relType, direction)
  }
}

case class ScoredNode(score: Float, node: OntologyNode, followedNodes: List[OntologyNode])


object OntologyGraph2 {
  implicit def wrapNode(node: Node) = new OntologyNode(node)

  lazy val graph = new OntologyGraph2()

  val cacheTTLseconds = 60 * 60 * 48

  //  def getCache(creationDate: DateTime) = {
  //    EhCacher.getOrCreateCache[RichLong, RichLong]("ontology-" + creationDate.getMillis, cacheTTLseconds, true)
  //  }

  val NAME_CLEANER = buildReplacement("_", " ")

  def getTopicName(topicUri: String): String = {
    val uriImpl = new URIImpl(topicUri)

    if (OntologySettings.useNameLookup) {
      graph.node(uriImpl) match {
        case Some(node) => node.getProperty(NodeBase.NAME_PROPERTY, bestNameAttempt(uriImpl)).toString
        case None => bestNameAttempt(uriImpl)
      }
    } else {
      bestNameAttempt(uriImpl)
    }
  }

  def getTopicNameWithoutOntology(topicUri: String) = bestNameAttempt(new URIImpl(topicUri))

  def bestNameAttempt(uri: URI) = {
    val input = if (uri.getLocalName.indexOf('%') > -1) {
      URLDecoder.decode(uri.getLocalName, "UTF-8")
    } else {
      uri.getLocalName
    }

    NAME_CLEANER.replaceAll(input).toString
  }


}

object Traversers {

  def conceptTraverser = Traversal.description.depthFirst().relationships(TopicRelationshipTypes.CONCEPT_OF_TOPIC, Direction.OUTGOING)

  def topicTraverser = Traversal.description.depthFirst().relationships(TopicRelationshipTypes.TOPIC_LINK, Direction.OUTGOING)

  val badCats = Set("Families_by_nationality", "American_People", "Categories_by_nationality", "WikiProject")

  def interestTraverser(depth: Int, excludeConceptUris: BadConcepts) =
    Traversal.description.depthFirst()
      .relationships(TopicRelationshipTypes.INTEREST_OF, Direction.OUTGOING)
      .relationships(TopicRelationshipTypes.CONCEPT_OF_TOPIC, Direction.OUTGOING)
      .relationships(TopicRelationshipTypes.BROADER_ANNOTATED_CONCEPT, Direction.OUTGOING)
      .relationships(TopicRelationshipTypes.BROADER_CONCEPT, Direction.OUTGOING)
      .relationships(TopicRelationshipTypes.BROADER_INTEREST, Direction.OUTGOING)
      .evaluator(TraversalFilters.DepthEval(depth))
      .evaluator(TraversalFilters.badCategoryEval)
      .evaluator(TraversalFilters.wrongUriFilter)
      .evaluator(TraversalFilters.badConceptsFilter(excludeConceptUris))

  def broaderInterestTraverser() = Traversal.description.depthFirst().relationships(TopicRelationshipTypes.BROADER_INTEREST, Direction.OUTGOING).evaluator(TraversalFilters.levelZeroFilter)

  def interestTraverserAnnotated(depth: Int, excludeConceptUris: BadConcepts) =
    Traversal.description.depthFirst()
      .relationships(TopicRelationshipTypes.INTEREST_OF, Direction.OUTGOING)
      .relationships(TopicRelationshipTypes.CONCEPT_OF_TOPIC, Direction.OUTGOING)
      .relationships(TopicRelationshipTypes.BROADER_ANNOTATED_CONCEPT, Direction.OUTGOING)
      // .relationships(TopicRelationshipTypes.BROADER_CONCEPT, Direction.OUTGOING)
      .relationships(TopicRelationshipTypes.BROADER_INTEREST, Direction.OUTGOING)
      .evaluator(TraversalFilters.DepthEval(depth))
      .evaluator(TraversalFilters.badCategoryEval)
      .evaluator(TraversalFilters.wrongUriFilter)
      .evaluator(TraversalFilters.badConceptsFilter(excludeConceptUris))

}

object TraversalFilters {


  val badCats = Set("Families_by_nationality", "American_People", "Categories_by_nationality", "WikiProject")

  class NodeEval(val evalF: (Path) => Evaluation) extends Evaluator {
    override def evaluate(path: Path): Evaluation = evalF(path)
  }

  object NodeEval {
    def apply(evalF: (Path) => Evaluation): NodeEval = new NodeEval(evalF)
  }

  val levelZeroFilter = NodeEval {
    path => {
      if (path.endNode().level <= 0l) {
        Evaluation.EXCLUDE_AND_PRUNE
      } else if (path.startNode().getId == path.endNode().getId) {
        Evaluation.EXCLUDE_AND_CONTINUE
      } else {
        Evaluation.INCLUDE_AND_CONTINUE
      }
    }
  }

  val topInterestFilter = NodeEval {
    path =>
      if (path.endNode().uri.equals(NS.TOP_INTEREST)) {
        Evaluation.EXCLUDE_AND_PRUNE
      }
      else {
        Evaluation.INCLUDE_AND_CONTINUE
      }
  }

  case class DepthEval(depth: Int) extends NodeEval({
    path =>
      if (path.nodes().size > depth) {
        Evaluation.EXCLUDE_AND_PRUNE
      }
      else {
        Evaluation.INCLUDE_AND_CONTINUE
      }
  })

  val badCategoryEval = NodeEval {
    path =>
      val categoryUri = path.endNode().uri

      //Exclude categories whose URIs contain elements in the "stopTraversingSubstrings" set
      for (badCat <- badCats; if (categoryUri.contains(badCat))) {
        Evaluation.EXCLUDE_AND_PRUNE
      }
      Evaluation.INCLUDE_AND_CONTINUE
  }

  val wrongUriFilter = NodeEval {
    path =>
      path.endNode().uriType match {
        case URIType.GRAVITY_INTEREST => Evaluation.INCLUDE_AND_CONTINUE
        case _ => Evaluation.EXCLUDE_AND_CONTINUE
      }
  }

  def badConceptsFilter(badConcepts: BadConcepts) = NodeEval {
    path =>
      lazy val badNode = badConcepts.badNodes.contains(path.endNode.uri)
      lazy val badLink = if (path.lastRelationship != null) {
        badConcepts.badLinks.contains(ConceptLink(path.lastRelationship.getStartNode.uri, path.endNode.uri))
      }
      else {
        false
      }

      if (!badConcepts.isEmpty && (badNode || badLink)) {
        Evaluation.EXCLUDE_AND_PRUNE
      }
      else {
        Evaluation.INCLUDE_AND_CONTINUE
      }
  }
}


class OntologyGraph2(graph: OntologyGraph = OntologyGraphMgr.getInstance.getDefaultGraph) {


  val graphDb = graph.getDb
  val nameIndex = graph.getNameIndex
  val uriIndex = graph.getUriIndex
  //  val cache = OntologyGraph2.getCache(this.ontologyCreationDate)

  val EMPTY_NODE_ID = -1l

  def getNodeByNodeId(nodeId: Long): Option[OntologyNode] = {
    if (nodeId == EMPTY_NODE_ID) return None

    try {
      val node = graphDb.getNodeById(nodeId)
      if (node == null) None else Some(OntologyNode(node))
    } catch {
      case _: NotFoundException => None
    }
  }

  //  val graphDb = new EmbeddedReadOnlyGraphDatabase(locationDirectory, mutable.Map[String, String]())

  //val nameIndex = graphDb.index().forNodes("names", mutable.Map("type" -> "exact", "provider" -> "lucene"))
  //val uriIndex = graphDb.index().forNodes("uris", mutable.Map("type" -> "exact", "provider" -> "lucene"))

  def node(uri: URI): Option[OntologyNode] = node(NS.getHashKey(uri))

  def node(id: Long): Option[OntologyNode] = {
    val node = uriIndex.get(NodeBase.URI_PROPERTY, id).getSingle
    if (node != null) {
      //        cache.putItem(id, node.getId)
      Some(OntologyNode(node))
    } else {
      //        cache.putItem(id, EMPTY_NODE_ID)
      None
    }
  }

  def topic(uri: URI): Option[OntologyNode] = node(uri) match {
    case Some(node) => Some(node)
    case None => None
  }

  def topic(id: Long): Option[OntologyNode] = node(id)

  def search(name: String) = nameIndex.get(NodeBase.NAME_PROPERTY, name.toLowerCase).headOption

  def searchWithDistance(phrase: Phrase, lowestScoreAllowed: Float = 0.75f, followRedirect: Boolean = false, uriFilter: Seq[URIType] = Nil): Option[ScoredNode] = {
    val name = phrase.text
    if (name.length() <= 2) {
      return None
    }

    if (Strings.countUpperCase(name) == 0 && name.length < 2) {
      return None
    }
    if (Strings.countNonLetters(name) != 0) {
      return None
    }


    //    val candidate = "\"" + name.toLowerCase + "\""
    //    val qctx = new QueryContext(candidate).sortByScore().top(5).tradeCorrectnessForSpeed()
    //    val hits = this.nameIndex.query(NodeBase.NAME_PROPERTY,qctx)

    val candidate = name.toLowerCase
    val hits = this.nameIndex.get(NodeBase.NAME_PROPERTY, candidate)


    if (!hits.hasNext) {
      None
    } else {
      val phraseDomain = SplitHost.registeredDomainFromUrl(phrase.content.location)

      var topNode: OntologyNode = null
      var topScore = -1f
      asScalaIterator(hits) map (OntologyNode.apply) filter {
        thisNode =>
          val uriPasses = uriFilter.isEmpty || uriFilter.contains(thisNode.uriType)
          val isTagFromDomain = phraseDomain some (thisNode.validDomains.contains(_)) none (false)
          val isTagApplicable = thisNode.validDomains.isEmpty || isTagFromDomain
          uriPasses && isTagApplicable
      } foreach {
        thisNode =>
          val thisNodeScore = scoreTopicMatch(name, thisNode.name)

          if (topNode == null || thisNodeScore > topScore) {
            topNode = thisNode
            topScore = thisNodeScore
          }
      }
      if (topNode == null || topScore < lowestScoreAllowed) {
        None
      } else if (followRedirect) {
        Some(ScoredNode(topScore, topNode.followRedirect.get, List(topNode)))
      }
      else {
        Some(ScoredNode(topScore, topNode, List.empty))
      }
    }
  }

  private def scoreTopicMatch(s: String, t: String) = {
    val thisNodeDist = StringUtils.getLevenshteinDistance(s, t)
    Numbers.invertAndNormalize(thisNodeDist, math.max(s.length, t.length))
  }

  def shutdown() {
    try {
      graphDb.shutdown()
    } catch {
      case ex: Exception => {
        ScalaMagic.printException("Non fatal exception: unable to shut down graph", ex)
      }
    }
  }

  val searchCache = EhCacher.getOrCreateRefCache[String, Option[ScoredNode]]("nodesearch", Int.MaxValue, false, 10000000)

  def searchForTopicRichly(phrase: Phrase): Option[ScoredNode] = {
    val registeredDomainOption = SplitHost.registeredDomainFromUrl(phrase.content.location)

    searchCache.getItemOrUpdate(phrase.text + phrase.content.contentType + registeredDomainOption, {
      val traversals: List[(OntologyNode) => Option[OntologyNode]] = List(
        (_.followColloquial),
        (_.followRedirect),
        (_.followIndicatedNode),
        (_.node.filterDisambiguation(false, true)),
        (_.filterWikiPatterns),
        (_.filterOutIfNotValid)
      )

      def followNode(node: Option[OntologyNode], traversals: List[(OntologyNode) => Option[OntologyNode]], followedNodes: List[OntologyNode] = List.empty): (Option[OntologyNode], List[OntologyNode]) = {
        traversals match {
          case Nil => (node, followedNodes)
          case traversal :: xs => {
            val nextNode = node flatMap (traversal)
            followNode(nextNode, xs, if (nextNode == node) followedNodes else followedNodes :+ node.get)
          }
        }
      }

      searchWithDistance(phrase, uriFilter = topicUriFilter) match {
        case Some(scoredNode) => {
          //        scoredNode.node.colloquiallyLinksTo.foreach {
          //          case targetNode: Node =>
          //            if (phrase.content.contentType == ContentType.Keywords) {
          //              return Some(ScoredNode(1.0f, OntologyNode(targetNode)))
          //            }
          //        }

          val (followedToNode, followedNodes) = followNode(Some(scoredNode.node), traversals)
          followedToNode match {
            case Some(node) => Some(ScoredNode(scoredNode.score, node, followedNodes))
            case None => None
          }
        }
        case None => None
      }

    }
    )

  }

  val topicUriFilter = URIType.TOPIC :: URIType.NY_TIMES :: URIType.UNKNOWN :: Nil

  //  nameIndex.asInstanceOf[LuceneIndex].setCacheCapacity(NodeBase.NAME_PROPERTY,8000000)

  lazy val ontologyCreationDate: DateTime = {
    val versionFile = new File(FilenameUtils.concat(graph.getLocation, OntologyGraphPopulator3.versionFile))
    if (versionFile.exists()) {
      val timestamp = IOUtils.toString(new FileInputStream(versionFile), "UTF-8").trim().split(OntologyGraphPopulator3.versionFileSalutation)(1)
      val jodaCompatibleTimestamp = "GMT$".r.replaceAllIn(timestamp, "+0000")
      val date = OntologyGraphPopulator3.versionDateFormatter.parseDateTime(jodaCompatibleTimestamp)
      new DateTime(date)
    }
    else {
      new DateTime()
    }
  }

  /**
   * probably the wrong place for this, but we basically take a topic node and find all the concepts that
   * are 1 degree away from that node, then we get all the google search counts for a node and then calc the zscore
   * for each node against all the concepts that match that topic, this will give us the best social tags that we
   * can place on each topic
   */
  def getRecommendedSocialTags(topicToSearch: String, graph: OntologyGraph2, weight: Double = 1.2) = {


    graph.topic(NS.getTopicURI(topicToSearch)) match {
      case Some(node) => {

        val googleCounts = for {
          concept <- node.concepts

        } yield concept.googleSearchCount.toDouble

        val stddev = grvmath.stddev(googleCounts)
        val mean = grvmath.mean(googleCounts)

        val conceptsThatPassFilter = for {
          concept <- node.concepts

          zscore = (concept.googleSearchCount - mean) / stddev
          n = concept.name.toLowerCase
          if (zscore >= weight)
          if (!n.contains("2005"))
          if (!n.contains("2010"))
          if (!n.contains("2000"))
          if (!n.contains("1990"))
          if (!n.contains("1980"))
          if (!n.contains("1970"))
          if (!n.contains("1960"))
          if (!n.contains("1950"))
          if (!n.contains("living people"))
        } yield concept

        //        println(conceptsThatPassFilter)
        conceptsThatPassFilter
      }
      case None => Iterable.empty[OntologyNode]
    }
  }


  /**
   * Gets all L1 interests, as reachable from the single L0 interest.
   */
  def l1Interests: Iterable[OntologyNode] = {
    val l0 = new URIImpl("http://insights.gravity.com/2010/9/universe#interest")
    node(l0) map {
      node =>
        node.getRelatedNodes(TopicRelationshipTypes.BROADER_INTEREST, Direction.INCOMING)
    } getOrElse (Iterable.empty)
  }

  def shortestPathBetween(startUri: String, endUri: String, maxDepth: Int, followDbpediaOnlyRelationships: Boolean = true, followOnlyTopicLinks: Boolean = false, followOnlyConceptLinks: Boolean = false, followBidirectionalTopicLinks: Boolean = false) = {
    node(new URIImpl(startUri)) match {
      case Some(startNode) => {
        node(new URIImpl(endUri)) match {
          case Some(endNode) => {
            val algo = {

              if (followOnlyTopicLinks) {
                GraphAlgoFactory.shortestPath(Traversal.expanderForTypes(TopicRelationshipTypes.TOPIC_LINK, Direction.INCOMING), maxDepth)
              }
              else if (followOnlyConceptLinks) {
                GraphAlgoFactory.shortestPath(Traversal.expanderForTypes(TopicRelationshipTypes.BROADER_CONCEPT, Direction.BOTH, TopicRelationshipTypes.REDIRECTS_TO, Direction.BOTH), maxDepth)
              }
              else if (followDbpediaOnlyRelationships) {
                GraphAlgoFactory.shortestPath(Traversal.expanderForTypes(TopicRelationshipTypes.REDIRECTS_TO, Direction.OUTGOING, TopicRelationshipTypes.BROADER_CONCEPT, Direction.BOTH, TopicRelationshipTypes.CONCEPT_OF_TOPIC, Direction.BOTH), maxDepth)
              }
              else if (followBidirectionalTopicLinks) {
                GraphAlgoFactory.shortestPath(Traversal.expanderForTypes(TopicRelationshipTypes.TOPIC_LINK, Direction.BOTH), maxDepth)
              }

              else {
                GraphAlgoFactory.shortestPath(Traversal.expanderForAllTypes(Direction.BOTH), maxDepth)
              }
              //GraphAlgoFactory.shortestPath(Traversal.expanderForTypes(TopicRelationshipTypes.BROADER_CONCEPT,Direction.OUTGOING,TopicRelationshipTypes.CONCEPT_OF_TOPIC,Direction.OUTGOING, TopicRelationshipTypes.INTEREST_OF, Direction.OUTGOING, TopicRelationshipTypes.BROADER_INTEREST, Direction.OUTGOING),maxDepth)
              //GraphAlgoFactory.shortestPath(Traversal.expanderForTypes(TopicRelationshipTypes.DISAMBIGUATES,Direction.OUTGOING,TopicRelationshipTypes.BROADER_CONCEPT,Direction.BOTH,TopicRelationshipTypes.CONCEPT_OF_TOPIC,Direction.BOTH, TopicRelationshipTypes.INTEREST_OF, Direction.BOTH, TopicRelationshipTypes.BROADER_INTEREST, Direction.BOTH),maxDepth)
              //GraphAlgoFactory.shortestPath(Traversal.expanderForTypes(TopicRelationshipTypes.TOPIC_LINK,Direction.BOTH,TopicRelationshipTypes.BROADER_CONCEPT,Direction.BOTH,TopicRelationshipTypes.CONCEPT_OF_TOPIC,Direction.BOTH, TopicRelationshipTypes.INTEREST_OF, Direction.BOTH, TopicRelationshipTypes.BROADER_INTEREST, Direction.BOTH),maxDepth)
              //GraphAlgoFactory.shortestPath(Traversal.expanderForTypes(TopicRelationshipTypes.BROADER_CONCEPT,Direction.BOTH,TopicRelationshipTypes.CONCEPT_OF_TOPIC,Direction.BOTH),maxDepth)
            }
            val paths = algo.findAllPaths(startNode.node, endNode.node)

            Some(paths)
          }
          case None => {
            None
          }
        }
      }
      case None => {
        None
      }
    }
  }

  def singlePathBetween(startUri: String, endUri: String, maxDepth: Int, followDbpediaOnlyRelationships: Boolean = true, followOnlyTopicLinks: Boolean = false, followOnlyConceptLinks: Boolean = false, followBidirectionalTopicLinks: Boolean = false) = {
    node(new URIImpl(startUri)) match {
      case Some(startNode) => {
        node(new URIImpl(endUri)) match {
          case Some(endNode) => {
            val algo = {

              if (followOnlyTopicLinks) {
                GraphAlgoFactory.shortestPath(Traversal.expanderForTypes(TopicRelationshipTypes.TOPIC_LINK, Direction.INCOMING), maxDepth)
              }
              else if (followOnlyConceptLinks) {
                GraphAlgoFactory.shortestPath(Traversal.expanderForTypes(TopicRelationshipTypes.BROADER_CONCEPT, Direction.BOTH), maxDepth)
              }
              else if (followDbpediaOnlyRelationships) {
                GraphAlgoFactory.shortestPath(Traversal.expanderForTypes(TopicRelationshipTypes.BROADER_CONCEPT, Direction.BOTH, TopicRelationshipTypes.CONCEPT_OF_TOPIC, Direction.BOTH), maxDepth)
              }
              else if (followBidirectionalTopicLinks) {
                GraphAlgoFactory.shortestPath(Traversal.expanderForTypes(TopicRelationshipTypes.TOPIC_LINK, Direction.BOTH), maxDepth)
              }
              else {
                GraphAlgoFactory.shortestPath(Traversal.expanderForAllTypes(Direction.BOTH), maxDepth)
              }
              //GraphAlgoFactory.shortestPath(Traversal.expanderForTypes(TopicRelationshipTypes.BROADER_CONCEPT,Direction.OUTGOING,TopicRelationshipTypes.CONCEPT_OF_TOPIC,Direction.OUTGOING, TopicRelationshipTypes.INTEREST_OF, Direction.OUTGOING, TopicRelationshipTypes.BROADER_INTEREST, Direction.OUTGOING),maxDepth)
              //GraphAlgoFactory.shortestPath(Traversal.expanderForTypes(TopicRelationshipTypes.DISAMBIGUATES,Direction.OUTGOING,TopicRelationshipTypes.BROADER_CONCEPT,Direction.BOTH,TopicRelationshipTypes.CONCEPT_OF_TOPIC,Direction.BOTH, TopicRelationshipTypes.INTEREST_OF, Direction.BOTH, TopicRelationshipTypes.BROADER_INTEREST, Direction.BOTH),maxDepth)
              //GraphAlgoFactory.shortestPath(Traversal.expanderForTypes(TopicRelationshipTypes.TOPIC_LINK,Direction.BOTH,TopicRelationshipTypes.BROADER_CONCEPT,Direction.BOTH,TopicRelationshipTypes.CONCEPT_OF_TOPIC,Direction.BOTH, TopicRelationshipTypes.INTEREST_OF, Direction.BOTH, TopicRelationshipTypes.BROADER_INTEREST, Direction.BOTH),maxDepth)
              //GraphAlgoFactory.shortestPath(Traversal.expanderForTypes(TopicRelationshipTypes.BROADER_CONCEPT,Direction.BOTH,TopicRelationshipTypes.CONCEPT_OF_TOPIC,Direction.BOTH),maxDepth)
            }
            val path = algo.findSinglePath(startNode.node, endNode.node)

            Some(path)
          }
          case None => {
            None
          }
        }
      }
      case None => {
        None
      }
    }
  }


  /**
   * Gets all L1, L2, L3 interests and topics (l4), grouped by level.
   *
   * These levels are different from the level field provided by the OntologyNode itself,
   * because it is either -1 for topics and occasionally incorrect for interests. // TODO: fix the incorrect ones?
   */
  def l1To4Interests(maxlevel: Int = 4): Map[Int, Iterable[OntologyNode]] = {
    val interests = collection.mutable.Map[Int, collection.mutable.ListBuffer[OntologyNode]]().withDefault(_ => new collection.mutable.ListBuffer())
    val toVisit = collection.mutable.Queue[(Int, OntologyNode)](l1Interests.map(1 -> _).toSeq: _*)
    while (!toVisit.isEmpty) {
      val (level, node) = toVisit.dequeue()
      interests.update(level, (interests(level) += node))

      val relType = if (level < 3) TopicRelationshipTypes.BROADER_INTEREST else TopicRelationshipTypes.INTEREST_OF
      val nextNodes = if (level < maxlevel) node.getRelatedNodes(relType, Direction.INCOMING).map((level + 1) -> _) else Iterable.empty
      toVisit.enqueue(nextNodes.toSeq: _*)
    }

    interests.toMap
  }
}

}