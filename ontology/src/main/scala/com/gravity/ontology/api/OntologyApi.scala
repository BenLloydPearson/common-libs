package com.gravity.ontology.api

import com.gravity.utilities.web._
import org.neo4j.graphdb._
import org.scalatra.{Get, Post}

import scala.collection.JavaConversions._
import org.openrdf.model.impl.URIImpl
import com.gravity.ontology.nodes.NodeBase
import com.gravity.ontology.service._
import com.gravity.utilities.components._
import org.joda.time.format.ISODateTimeFormat
import com.gravity.ontology.{OntologyGraph2, OntologyGraphMgr, OntologyGraphName, OntologyNode}
import com.gravity.utilities.Settings

/*             )\._.,--....,'``.
.b--.        /;   _.. \   _\  (`._ ,.
`=,-,-'~~~   `----(,_..'--(,_..'`-.;.'  */

case class NodeResponse(name: String, uri: String, outgoing: List[NodeRelation] = Nil, incoming: List[NodeRelation] = Nil)

case class NodeRelation(name: String, uri: String, typeOfRelationship: String, tree: List[NodeRelation] = Nil)


class NodeWrapper(node: Node) {
  def prop(key: String, default: String) = {
    node.getProperty(key, default)
  }
}

object OntologyApi {

  implicit def nodeToWrapper(node: Node): NodeWrapper = new NodeWrapper(node)

  object OntologyTraversalApi extends ApiSpec(name = "Traverse ontology from topic", route = "/ontology/traverse", methods = Get :: Nil, description = "Does a lookup by topic name in the ontology, traverses by x elements, and returns the total results", live = false) {
    val topicName = &(ApiStringParam(key = "topic", description = "Name to search on", required = false, exampleValue = "Kittens"))

    val topicUri = &(ApiStringParam(key = "topicuri", description = "URI to find", required = false, exampleValue = "http://dbpedia.org/resource/Barack_Obama"))
    val topicIdParam = &(ApiLongParam(key = "topicId", description = "The MurmurHash of the topicUri", required = false))
    val depth = &(ApiIntParam(key = "outgoingdepth", description = "Depth to traverse for outgoing relationships", required = false, defaultValue = 2))
    val incomingdepth = &(ApiIntParam(key = "incomingdepth", description = "Depth to traverse for incoming relationships", required = false, defaultValue = 1))

    def request(req: ApiRequest) = {
      val node = if (topicName(req).isDefined) {
        OntologyGraphMgr.getInstance.getDefaultGraph.findTopicNode(topicName.get(req)).node
      } else if (topicIdParam(req).isDefined) {
        OntologyGraph2.graph.topic(topicIdParam.get(req)) match {
          case Some(topic) => new NodeBase(topic.node)
          case None => null
        }
      } else {
        OntologyGraphMgr.getInstance.getDefaultGraph.getTopic(new URIImpl(topicUri.get(req)))
      }
      if (node == null || node.getUnderlyingNode == null) {
        ApiResponse("Node not found")
      }

      val traversalDepth = depth.getOrDefault(req)


      if (node == null || node == null) {
        ApiResponse("Node not found")
      } else {

        val coreNode = node.getUnderlyingNode

        ApiResponse(getResponses(traversalDepth, coreNode, incomingdepth.getOrDefault(req)))
      }

    }
  }

  def getResponses(depth: Int, node: Node, incomingDepth: Int): NodeResponse = {
    NodeResponse(node.prop(NodeBase.NAME_PROPERTY, "no name").toString, node.prop(NodeBase.URI_PROPERTY, "no uri").toString, getRelations(depth, node, Direction.OUTGOING), getRelations(incomingDepth, node, Direction.INCOMING))
  }

  def getRelations(depth: Int, node: Node, direction: Direction): List[NodeRelation] = {
    println("Getting relations at depth " + depth + " and direction " + direction.toString)
    try {
      if (depth == 0) {
        Nil
      }
      else {
        val rels = node.getRelationships(direction)
        println("Got " + rels.size + " relationships")
        (for (rel <- rels) yield {
          if (direction == Direction.OUTGOING) {
            try {

              NodeRelation(rel.getEndNode.prop(NodeBase.NAME_PROPERTY, "no name").toString, rel.getEndNode.prop(NodeBase.URI_PROPERTY, "no uri").toString, rel.getType.toString, getRelations(depth - 1, rel.getEndNode, direction))
            } catch {
              case ex: Exception => NodeRelation("Hi", "hi.com", "Hi")
            }
          } else {
            try {
              NodeRelation(rel.getStartNode.prop(NodeBase.NAME_PROPERTY, "no name").toString, rel.getStartNode.prop(NodeBase.URI_PROPERTY, "no uri").toString, rel.getType.toString, getRelations(depth - 1, rel.getStartNode, direction))
            } catch {
              case ex: Exception => NodeRelation("Hi", "hi.com", "Hi")
            }

          }
        }).toList
      }
    } catch {
      case ex: Exception =>
        println("Failed to iterate relations! Exception: %s%n%s%n%s".format(ex.getClass.getCanonicalName, ex.getMessage, ex.getStackTrace.mkString("", "\n", "\n")))
        Nil
    }
  }

  object LayeredTraversalApi extends ApiSpec(name = "Temporary Layered Traversal Api", route = "/ontology/layeredTraversal", methods = Get :: Post :: Nil, description = "Temporary Api call for layered traversals", live = false) {
    implicit val ogName = new OntologyGraphName(Settings.ONTOLOGY_DEFAULT_GRAPH_NAME)

    val articleUrl = &(ApiStringParam("url", required = false, description = "Article to Graph", defaultValue = ""))
    val rawText = &(ApiStringParam("rawText", required = false, description = "Text to Graph", defaultValue = "Default Value"))
    val rawTitle = &(ApiStringParam("rawTitle", required = false, description = "Title to Graph", defaultValue = "Default Value"))

    def request(req: ApiRequest) = {

      OntologyService.layeredTopicsWithInterestGraph(urlTouse = articleUrl.get(req)) match {
        case SomeResponse(layeredInterests) =>
          ApiResponse(layeredInterests)
        case ErrorResponse(message) =>
          ApiResponse(message, ResponseStatus(500, message))
        case FatalErrorResponse(ex) =>
          ApiExceptionResponse(ex)
      }


    }
  }

  object LayeredTraversalTextApi extends ApiSpec(name = "Temporary Layered Traversal Api", route = "/ontology/layeredTraversalText", methods = Post :: Nil, description = "Temporary Api call for layered traversals with text", live = false) {
    implicit val ogName = new OntologyGraphName(Settings.ONTOLOGY_DEFAULT_GRAPH_NAME)

    val articleUrl = &(ApiStringParam("url", required = false, description = "Article to Graph", defaultValue = ""))
    val rawText = &(ApiStringParam("rawText", required = false, description = "Text to Graph", defaultValue = "Default Value"))
    val rawTitle = &(ApiStringParam("rawTitle", required = false, description = "Title to Graph", defaultValue = "Default Value"))

    def request(req: ApiRequest) = {

      OntologyService.layeredTopicsWithInterestGraph(urlTouse = "", rawText = rawText.getOrDefault(req), rawTitle = rawTitle.getOrDefault(req), crawlLink = false) match {
        case SomeResponse(layeredInterests) =>
          ApiResponse(layeredInterests)
        case ErrorResponse(message) =>
          ApiResponse(message, ResponseStatus(500, message))
        case FatalErrorResponse(ex) =>
          ApiExceptionResponse(ex)
      }
    }
  }

  object OntologyDetailsApi extends ApiSpec("Ontology Details", route = "/ontology/details", description = "Details of the currently deployed ontology.") {
    override def request(req: ApiRequest) = {
      ApiResponse(Map(
        "creationDate" -> ISODateTimeFormat.dateTimeNoMillis().print(OntologyGraph2.graph.ontologyCreationDate)
      ))
    }
  }

  object ShortestPathApi extends ApiSpec("Ontology Shortest Path", route = "/ontology/shortestpath", description = "Shortest path between two points in the ontology.") {
    val startUri = &(ApiStringParam("start", required = true, "Start URI"))
    val endUri = &(ApiStringParam("end", required = true, "End URI"))
    val maxDistance = &(ApiIntParam("maxdistance", required = false, "Maximum distance between nodes", defaultValue = 5))
    val dbpediaOnly = &(ApiBooleanParam("onlydbpedia", required = false,"Only follow dbpedia relationships", defaultValue = false))

    override def request(req: ApiRequest) = {

      val startUriExt = startUri.get(req)
      val endUriExt = endUri.get(req)
      val maxDist = maxDistance.getOrDefault(req)
      val onlyDbpedia = dbpediaOnly.getOrDefault(req)

      OntologyGraph2.graph.shortestPathBetween(startUriExt, endUriExt, maxDist,onlyDbpedia) match {
        case Some(paths) =>
          ApiResponse(Map(
            "paths" -> {
              for (path <- paths) yield {
                Map("distance" -> path.length(),
                  "path" -> {

                    val paths = path.relationships().map {case relationship: Relationship =>
                      val typeName = relationship.getType.toString
                      val startNode = OntologyNode(relationship.getStartNode)
                      val endNode = OntologyNode(relationship.getEndNode)
                      Map("start" -> startNode.uri,"end"->endNode.uri, "via"->typeName)
                    }
                    paths
                  }
                )
              }
            }
          ))
        case None =>
          ApiResponse.notFound("", "Nodes not found")
      }

    }
  }

}