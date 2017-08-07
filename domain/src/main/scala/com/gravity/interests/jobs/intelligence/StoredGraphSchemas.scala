package com.gravity.interests.jobs.intelligence

import org.joda.time.DateTime
import scala.Predef
import scala.collection._
import com.gravity.hbase.schema._
import net.liftweb.json._
import com.gravity.utilities.web.ApiServlet
import scala.collection.mutable.{PriorityQueue, Buffer}
import com.gravity.utilities.cache.CachedHashCode
import com.gravity.utilities._
import com.gravity.utilities.grvcoll.MergeReducable
import scala.annotation.tailrec
import scala.util.hashing.MurmurHash3
import scalaz.Monoid
import com.gravity.interests.jobs.intelligence.operations.graphing.NodeInfoResolver
import com.gravity.utilities.grvenum.GrvEnum
import scalaz.Show

/*             )\._.,--....,'``.
 .b--.        /;   _.. \   _\  (`._ ,.
`=,-,-'~~~   `----(,_..'--(,_..'`-.;.'  */

object NodeType {

  type GravityNode = Node

  object All extends NodeType("All", 0)

  object Topic extends NodeType("Topic", 1)

  object Interest extends NodeType("Interest", 2)

  object Term extends NodeType("Term", 3)


  val typesById: Map[Int, NodeType] = Map(1 -> Topic, 2 -> Interest, 3 -> Term)

  class NodeTypeSerializer extends Serializer[NodeType] {
    def deserialize(implicit format: Formats): PartialFunction[(TypeInfo, _root_.net.liftweb.json.JValue), NodeType] = {
      throw new RuntimeException("Cannot deserialize NodeType (yet)")
    }

    def serialize(implicit format: Formats): PartialFunction[Any, _root_.net.liftweb.json.JValue] = {
      case dm: NodeType => JsonAST.JString(dm.name)
    }

  }

  ApiServlet.registerSerializer(new NodeTypeSerializer)
}


class NodeType(val name: String, val typeId: Short)

object EdgeType {

  object InterestOf extends EdgeType("Interest Of", 1)

  object BroaderThan extends EdgeType("Broader Than", 2)

  val typesById: Map[Int, EdgeType] = Map(1 -> InterestOf, 2 -> BroaderThan)

  class EdgeTypeSerializer extends Serializer[EdgeType] {
    def deserialize(implicit format: Formats): PartialFunction[(TypeInfo, _root_.net.liftweb.json.JValue), EdgeType] = {
      throw new RuntimeException("Cannot deserialize EdgeType (yet)")
    }

    def serialize(implicit format: Formats): PartialFunction[Any, _root_.net.liftweb.json.JValue] = {
      case dm: EdgeType => JsonAST.JString(dm.name)
    }

  }

  ApiServlet.registerSerializer(new EdgeTypeSerializer)
}

class EdgeType(val name: String, val typeId: Short)

object Node {
  implicit val show: Show[Node] with Object {def shows(n: Node): String} = new Show[Node] {
    override def shows(n: Node): String = s"${n.nodeType.name}Node(${n.name}, score: ${n.score}})"
  }
}

case class Node(id: Long, var name: String, var uri: String, nodeType: NodeType, level: Short = 100, var count: Int = 1, var score: Double = 0.0) extends MergeReducable[Node, Long] {

  def groupId: Long = id

  def +=(that: Node) {
    count = count + that.count
    score = (score + that.score) / 2.0
  }

  def +(that: Node): Node = {
    //    require(id == that.id, "Can only add nodes with the same ID")
    //    require(name == that.name, "Can only add nodes with the same name")
    //    require(uri == that.uri, "Can only add nodes with the same uri")
    new Node(id, name, uri, nodeType, level, count + that.count, (score + that.score) / 2.0)
  }

  def *(times: Int): Node = new Node(id, name, uri, nodeType, level, count * times, score)

  def multiplyNodeScoreBy(p: Double): Node = new Node(id, name, uri, nodeType, level, count, score * p)

  def safeName: Option[String] = {
    if (name.length == 0) None
    else Some(name)
  }

  def safeUri: Option[String] = {
    if (uri.length == 0) None
    else Some(uri)
  }

  def tuple: (Long, String, String, NodeType, Short, Int, Double) = (id, name, uri, nodeType, level, count, score)
}

case class EdgeId(start: Long, end: Long, relTypeId: Short) extends CachedHashCode

object Edge {
  def apply(start: Long, end: Long, distance: Int, score: Double, relType: EdgeType, count: Int = 1): Edge = new Edge(new EdgeId(start, end, relType.typeId), distance, score, count, relType)


}

case class Edge(id: EdgeId, var distance: Int, var score: Double, var count: Int, relType: EdgeType) extends MergeReducable[Edge, EdgeId] {
  def start: Long = id.start

  def groupId: EdgeId = id

  def end: Long = id.end


  def +(that: Edge): Edge = {
    new Edge(id, (distance + that.distance) / 2, (score + that.score) / 2.0, count + that.count, relType)
  }

  def +=(that: Edge) {
    distance = (distance + that.distance) / 2
    score = (score + that.score) / 2.0
    count = count + that.count

  }

  def *(times: Int): Edge = Edge(id, distance, score, count * times, relType)

  def weight: Double = count.toDouble

  def hasNode(node: Node): Boolean = hasNodeId(node.id)

  def hasNodeId(nodeId: Long): Boolean = start == nodeId || end == nodeId
}

class GraphType(val name: String, val typeId: Short)

object GraphType {

  object AutoGraph extends GraphType("AutoGraph", 1)

  object AnnoGraph extends GraphType("AnnotationGraph", 2)

  object ConceptGraph extends GraphType("ConceptGraph", 3)

  object ConceptWithTfIdf extends GraphType("ConceptTfIdf", 4)

  object PhraseConceptGraph extends GraphType("PhraseConceptGraph", 5)


  val typesById: Map[Int, GraphType] = Map(1 -> AutoGraph, 2 -> AnnoGraph, 3 -> ConceptGraph, 4 -> ConceptWithTfIdf, 5 -> PhraseConceptGraph)
}

class StoredGraphBuilder {
  val nodes: mutable.Buffer[Node] = mutable.Buffer[Node]()
  val edges: mutable.Buffer[Edge] = mutable.Buffer[Edge]()


  def addNode(uri: String, name: String, nodeType: NodeType, level: Int = 100, count: Int = 1, score: Double = 1.0): StoredGraphBuilder = {
    val node = Node(MurmurHash.hash64(uri), name, uri, nodeType, level.toShort, count, score)
    nodes += node
    this
  }

  def relate(thisUri: String, thatUri: String, relType: EdgeType = EdgeType.InterestOf, distance: Int = 1, count: Int = 1, score: Double = 1.0): StoredGraphBuilder = {
    val edge = Edge(MurmurHash.hash64(thisUri), MurmurHash.hash64(thatUri), distance, score, relType, count)
    edges += edge
    this
  }

  def build: StoredGraph = StoredGraph(nodes, edges)
}

object StoredGraph {

  implicit object StoredGraphMonoid extends Monoid[StoredGraph] {
    def append(s1: StoredGraph, s2: => StoredGraph): StoredGraph = s1 + s2

    val zero: StoredGraph = {
      println("Invoking zero")
      StoredGraph.makeEmptyGraph
    }
  }


  def buildNode(uri: String, name: String, nodeType: NodeType, level: Int = 100, count: Int = 1, score: Double = 1.0): Node = {
    Node(MurmurHash.hash64(uri), name, uri, nodeType, level.toShort, count, score)
  }

  def builderFrom(graph: StoredGraph): StoredGraphBuilder = {
    val b = new StoredGraphBuilder()
    b.nodes.appendAll(graph.nodes)
    b.edges.appendAll(graph.edges)
    b
  }

  /**
   * Add two graphs together by merging intersecting nodes and edges.
   */
  def addMulti(those: Iterable[StoredGraph]): StoredGraph = {
    //Original groupby based impl took 1 min 40 secs for 2000000 iterations, impl with ScalaMagic.multiGroupBy takes 50 seconds, this version takes 36 seconds


    val combinedNodes = grvcoll.groupAndReduce[Node, Long](those.map(_.nodes).toSeq: _*)
    val combinedEdges = grvcoll.groupAndReduce[Edge, EdgeId](those.map(_.edges).toSeq: _*)

    StoredGraph(combinedNodes.toSeq, combinedEdges.toSeq)
  }


  def make: StoredGraphBuilder = new StoredGraphBuilder

  val distanceNodeIdOrdering: Ordering[(Int, Double, Node)] with Object {def compare(a: (Int, Double, Node), b: (Int, Double, Node)): Int} = new math.Ordering[(Int, Double, Node)] {
    def compare(a: (Int, Double, Node), b: (Int, Double, Node)): Int = {
      -math.Ordering.Int.compare(a._1, b._1)
    }
  }

  def EmptyGraph: StoredGraph = StoredGraph(Seq(), Seq(), new DateTime(1, 1, 1, 1, 1, 1, 1), -1)

  def makeEmptyGraph: StoredGraph = StoredGraph(Seq(), Seq(), new DateTime(), -1)

  /**
   * Overloaded constructor that builds a StoredGraph from an InterestGraph (the object returned by the Grapher).
   * In other words, this is the point of contact between the Grapher (uses the Ontology to graph content) and analytics data storage / aggregation.
   */
  def apply(ig: com.gravity.interests.graphs.graphing.InterestGraph): StoredGraph = {
    val interests = ig.concepts.map {
      concept =>
        Node(concept.interest.id, concept.interest.name, concept.interest.uri, NodeType.Interest, concept.interest.level.toShort, 1, concept.score)
    }

    val topics = ig.topics.map {
      topic =>
        Node(topic.topic.topic.id, topic.topic.topic.name, topic.topic.topic.uri, NodeType.Topic, 100, topic.topic.frequency, topic.topic.score)
    }


    val broaderThanEdges = ig.concepts.flatMap {
      concept =>
        concept.interest.narrowerInterests.filter {
          narrower => interests.exists(_.uri == narrower.uri)
        }.map {
          narrowerLink =>
            Edge(narrowerLink.id, concept.interest.id, 1, 1.0, EdgeType.BroaderThan, 1)
        }
    }

    //Topic to Interest pairings that exist in the graph
    val extantInterestOfLinks = ig.concepts.flatMap {
      concept =>
        concept.interestMatches.map {
          topicToInterest =>

            topicToInterest
        }
    }

    val includedMatchesById = extantInterestOfLinks.groupBy(_.topic.topic.id)

    val edges = (for {
      (topicId, matches) <- includedMatchesById
      thisMatch <- matches
    } yield {
      //Discard matches where there is a lower level match
      val narrowerInterests = thisMatch.interestMatch.interest.narrowerInterests
      val hasNarrowerMatch = matches.exists(thatMatch => narrowerInterests.exists(thatMatch.interestMatch.interest.id == _.id))
      if (hasNarrowerMatch) {
        None
      } else {
        val topicToInterest = thisMatch
        Some(
          Edge(topicToInterest.topic.topic.id, topicToInterest.interestMatch.interest.id, topicToInterest.interestMatch.depth, topicToInterest.topic.score, EdgeType.InterestOf, topicToInterest.topic.frequency)
        )
      }
    }).flatten






    //Edge(topicToInterest.topic.topic.id, concept.interest.id, topicToInterest.interestMatch.depth, topicToInterest.topic.score, EdgeType.InterestOf, topicToInterest.topic.frequency)




    StoredGraph(interests ++ topics, broaderThanEdges ++ edges)
  }

  implicit object NodeConverter extends ComplexByteConverter[Node] {
    override def write(node: Node, output: PrimitiveOutputStream) {
      output.writeLong(node.id)

      // re-introduce name and uri
      output.writeUTF(node.name)
      output.writeUTF(node.uri)

      output.writeShort(node.nodeType.typeId)
      output.writeShort(node.level)
      output.writeInt(node.count)
      output.writeDouble(node.score)
    }

    override def read(input: PrimitiveInputStream): Node = {
      Node(input.readLong, input.readUTF, input.readUTF, NodeType.typesById(input.readShort()), input.readShort(), input.readInt(), input.readDouble())
    }
  }

  implicit object EdgeConverter extends ComplexByteConverter[Edge] {
    override def write(edge: Edge, output: PrimitiveOutputStream) {
      output.writeLong(edge.start)
      output.writeLong(edge.end)
      output.writeInt(edge.distance)
      output.writeDouble(edge.score)
      output.writeShort(edge.relType.typeId)
      output.writeInt(edge.count)
    }

    override def read(input: PrimitiveInputStream): Edge = {
      Edge(input.readLong, input.readLong, input.readInt, input.readDouble, EdgeType.typesById(input.readShort()), input.readInt())
    }
  }

  implicit object NodeSetConverter extends SetConverter[Node]

  implicit object EdgeSetConverter extends SetConverter[Edge]

  implicit object NodeSeqConverter extends SeqConverter[Node]

  implicit object EdgeSeqConverter extends SeqConverter[Edge]


  implicit object StoredGraphConverter extends ComplexByteConverter[StoredGraph] {
    override def write(graph: StoredGraph, output: PrimitiveOutputStream) {
      output.writeByte(1)
      output.writeObj(graph.nodes)
      output.writeObj(graph.edges)
      output.writeObj(graph.date)
      output.writeInt(graph.algorithm)
    }

    override def read(input: PrimitiveInputStream): StoredGraph = {
      val version = input.readByte
      StoredGraph(input.readObj[Seq[Node]], input.readObj[Seq[Edge]], input.readObj[DateTime], input.readInt)
    }
  }

}

case class StoredGraphDistanceReport(from: Node, distances: Seq[DistancedNode])

case class DistancedNode(distance: Int, weightedDistance: Double, node: Node)

trait AdjacencyGraph {
  this: StoredGraph =>

  val matrix: Array[Array[Short]] = Array.ofDim[Short](nodes.length, edges.length)
}

case class StoredGraph(nodes: Seq[Node], edges: Seq[Edge], date: DateTime = new DateTime(), algorithm: Int = 0) {

  def size: Int = nodes.length + edges.length

  lazy val topicsAndInterests: scala.Seq[Node] = topics ++ interests

  lazy val isEmpty: Boolean = nodes.length == 0
  lazy val nonEmpty: Boolean = !isEmpty

  lazy val topics: scala.Seq[Node] = nodes.filter(_.nodeType == NodeType.Topic)

  lazy val topicsAndTerms: scala.Seq[Node] = nodes.filter(node => node.nodeType == NodeType.Topic || node.nodeType == NodeType.Term)

  lazy val interests: scala.Seq[Node] = nodes.filter(_.nodeType == NodeType.Interest)
  lazy val terms: scala.Seq[Node] = nodes.filter(_.nodeType == NodeType.Term)

  lazy val nodesById: mutable.Map[Long, Node] = grvcoll.toMap(nodes)(_.id)
  lazy val edgesById: mutable.Map[EdgeId, Edge] = grvcoll.toMap(edges)(_.id)
  lazy val edgesByStartNodeId: mutable.Map[Long, mutable.Buffer[Edge]] = grvcoll.mutableGroupBy(edges)(_.start)
  lazy val edgesByEndNodeId: mutable.Map[Long, mutable.Buffer[Edge]] = grvcoll.mutableGroupBy(edges)(_.end)


  lazy val nodesByLevel: Predef.Map[Short, scala.Seq[Node]] = nodes.groupBy(_.level)
  lazy val nodesByUri: Predef.Map[String, Node] = nodes.map(node => (node.uri, node)).toMap

  lazy val maxEdgeCount: Double = edges.foldLeft(0)(_ + _.count).toDouble

  def populateUriAndName(): Unit = {
    if (nodes.size > 0) {
      val res = NodeInfoResolver.getUriAndNames(nodesById.keys.toSeq)
      for ((nodeId, uriName) <- res.resolved;
           un <- uriName;
           node = nodesById(nodeId)) {
        node.uri = un.uri
        node.name = un.name
      }
    }
  }

  def incomingNodes(node: Node): scala.Seq[Node] = {
    val edges = edgesByEndNodeId.getOrElse(node.id, Nil)
    edges.map(e => nodeById(e.start))
  }

  def outgoingNodes(node: Node): scala.Seq[Node] = {
    val edges = edgesByStartNodeId.getOrElse(node.id, Nil)
    edges.map(e => nodeById(e.end))
  }

  def incoming(node: Node, edgeTypes: EdgeType*): scala.Seq[Edge] = {
    val edges = edgesByEndNodeId.getOrElse(node.id, Nil)
    if (edgeTypes.nonEmpty) {
      edges.filter(e => edgeTypes.contains(e.relType))
    } else {
      edges
    }
  }

  def outgoing(node: Node): mutable.Buffer[Edge] = {
    edgesByStartNodeId.getOrElse(node.id, mutable.Buffer[Edge]())
  }

  def outgoing(node: Node, edgeTypes: EdgeType*): mutable.Buffer[Edge] = {
    val edges = edgesByStartNodeId.getOrElse(node.id, mutable.Buffer[Edge]())
    if (edgeTypes.nonEmpty) {
      edges.filter(e => edgeTypes.contains(e.relType))
    } else {
      edges
    }
  }

  def edgesOf(node: Node, edgeType: EdgeType, edgesToConsider: Iterable[Edge] = edges): scala.Iterable[Edge] = edgesToConsider.filter(itm => itm.start == node.id && itm.relType == edgeType)

  def bidirectionalEdgesOf(node: Node): mutable.Buffer[Edge] = {
    edgesByStartNodeId.getOrElse(node.id, mutable.Buffer[Edge]()) ++ edgesByEndNodeId.getOrElse(node.id, mutable.Buffer[Edge]())
  }


  def incomingEdgesOf(node: Node, edgeType: EdgeType, edgesToConsider: Iterable[Edge] = edges): scala.Iterable[Edge] = edgesToConsider.filter(itm => itm.end == node.id && itm.relType == edgeType)

  def nodeByIdOption(id: Long): Option[Node] = nodesById.get(id)

  def nodeByUriOption(uri: String): Option[Node] = nodesByUri.get(uri)

  def nodeById(id: Long): Node = nodesById(id)

  def nodeByUri(uri: String): Node = nodesByUri(uri)

  def edgeById(id: EdgeId): Edge = edgesById(id)

  def edgeById(start: Long, end: Long, relType: EdgeType): Edge = edgesById(new EdgeId(start, end, relType.typeId))

  def nodesVia(node: Node, edgeType: EdgeType): scala.Iterable[Node] = edgesOf(node, edgeType).map(edge => nodeById(edge.end))

  def otherNode(edge: Edge, node: Node): Node = {
    if (edge.start == node.id) {
      nodeById(edge.end)
    }
    else {
      nodeById(edge.start)
    }
  }


  def distanceTo(node: Node, that: Node, weighted: Boolean = false): (Int, Double) = {
    dijkstraDistancesFrom(node)(that.id)
  }

  def distanceReport(node: Node, weighted: Boolean = false): scala.Seq[DistancedNode] = {
    val distances = dijkstraDistancesFrom(node, weighted)

    if (weighted) {
      distances.toSeq.sortBy(_._2._1).map(dist => DistancedNode(dist._2._1, dist._2._2, nodeById(dist._1)))
    }
    else {
      distances.toSeq.sortBy(_._2._2).map(dist => DistancedNode(dist._2._1, dist._2._2, nodeById(dist._1)))
    }
  }

  def distancesFrom(node: Node, weighted: Boolean = false): mutable.Map[Long, (Int, Double)] = dijkstraDistancesFrom(node, weighted)

  /**
   * 10 million ops in 57 seconds
   * down to 13 seconds after adding index to edges
   */
  def dijkstraDistancesFrom(node: Node, weighted: Boolean = false): mutable.Map[Long, (Int, Double)] = {
    val distanceMap = mutable.Map[Long, (Int, Double)]()
    val unconsideredNodes = new mutable.PriorityQueue[(Int, Double, Node)]()(StoredGraph.distanceNodeIdOrdering)
    unconsideredNodes.enqueue((0, 0.0, node))
    unconsideredNodes.enqueue((Int.MaxValue, Double.MaxValue, node))

    nodes.foreach {
      thisnode =>
        distanceMap(thisnode.id) = (Int.MaxValue, Double.MaxValue)
    }

    distanceMap(node.id) = (0, 0.0)

    while (unconsideredNodes.nonEmpty) {
      val (consideredDistance, consideredWeightedDistance, consideredNode) = unconsideredNodes.dequeue()
      if (consideredDistance == Int.MaxValue) {
        unconsideredNodes.clear()
      } else {
        for (v <- incomingNodes(consideredNode)) {
          val newDistance = consideredDistance + 1
          val newWeightedDistance = consideredWeightedDistance + (1.0 - (v.count.toDouble / maxEdgeCount))
          if (newDistance < distanceMap(v.id)._1) {
            distanceMap(v.id) = (newDistance, newWeightedDistance)
            unconsideredNodes.enqueue((newDistance, newWeightedDistance, v))
          }
        }
        for (v <- outgoingNodes(consideredNode)) {
          val newDistance = consideredDistance + 1
          val newWeightedDistance = consideredWeightedDistance + (1.0 - (v.count.toDouble / maxEdgeCount))
          if (newDistance < distanceMap(v.id)._1) {
            distanceMap(v.id) = (newDistance, newWeightedDistance)
            unconsideredNodes.enqueue((newDistance, newWeightedDistance, v))
          }
        }
      }
    }
    distanceMap
  }

  /** 10 million ops in 2 mins 35 seconds
    *
    */
  def dijkstraDistancesFrom_safe(node: Node, weighted: Boolean = false): mutable.Map[Long, (Int, Double)] = {
    val distanceMap = mutable.Map(nodes.map(node => (node.id, (Int.MaxValue, Double.MaxValue))): _*)
    val unconsideredNodes = mutable.Map(nodes.map(node => (node.id, node)): _*)

    def closestUnconsideredNode() = {
      var dist = Int.MaxValue
      var closest: Node = null
      for ((nodeId, node) <- unconsideredNodes) {
        if (distanceMap(node.id)._1 <= dist) {
          closest = node
          dist = distanceMap(node.id)._1
        }
      }
      (dist, closest)
    }

    distanceMap(node.id) = (0, 0.0)

    while (unconsideredNodes.nonEmpty) {
      val (consideredDistance, consideredNode) = closestUnconsideredNode()
      if (consideredDistance == Int.MaxValue) {
        //The remaining unconsideredNodes are infinitely far from the current node.
        unconsideredNodes.clear()
      } else {
        unconsideredNodes.remove(consideredNode.id)
        for (ve <- bidirectionalEdgesOf(consideredNode); v = otherNode(ve, consideredNode)) {

          val alt = {
            (distanceMap(consideredNode.id)._1 + 1, distanceMap(consideredNode.id)._2 + (1.0 - (ve.count.toDouble / maxEdgeCount)))
          }
          if (alt._1 < distanceMap(v.id)._1) {
            distanceMap(v.id) = alt
          }
        }
      }
    }
    distanceMap
  }

  def checkNode(start: Node, end: Node, current: Node, distance: Int): Int = {
    val edges = bidirectionalEdgesOf(current)
    val nodeBuff = mutable.Buffer[Node]()
    var retVal = -1
    for (edge <- edges) {
      val startNode = nodeById(edge.start)
      val endNode = nodeById(edge.end)
      if (startNode.id != start.id) nodeBuff += startNode
      if (endNode.id != start.id) nodeBuff += endNode

      if (startNode.id == end.id) retVal = distance
      if (endNode.id == end.id) retVal = distance
    }

    if (retVal > -1) retVal

    nodeBuff.foreach {
      node => retVal = checkNode(start, end, node, distance + 1)
    }

    retVal

  }


  /**
   * Add two graphs together by merging intersecting nodes and edges.
   */
  def +(that: StoredGraph): StoredGraph = {
    //Original groupby based impl took 1 min 40 secs for 2000000 iterations, impl with ScalaMagic.multiGroupBy takes 50 seconds, this version takes 36 seconds
    val combinedNodes = grvcoll.groupAndReduce[Node, Long](nodes, that.nodes)
    val combinedEdges = grvcoll.groupAndReduce[Edge, EdgeId](edges, that.edges)

    StoredGraph(combinedNodes.toSeq, combinedEdges.toSeq)
  }

  def plusOne(that: StoredGraph): StoredGraph = {
    that.nodes.foreach(node => node.count = 1)
    this + that
  }

  /**
   * Multiply a graph by a certain amount.  This will keep the graph the same, but multiply the counts by the given
   * amount.
   */
  def *(times: Int): StoredGraph = StoredGraph(nodes.map(_ * times), edges.map(_ * times), date, algorithm)

  def multiplyNodeScoreBy(p: Double): StoredGraph = StoredGraph(nodes.map(n => n.multiplyNodeScoreBy(p)), edges, date, algorithm)

  def traverse(node: Node, rels: EdgeType*): Unit = {
    val accumulator = mutable.Buffer[Node]()
    val edges = outgoing(node)
  }

  def subGraphByNode(nodeId: Long): StoredGraph = {
    val nodes = mutable.Map[Long, Node]()
    val edges = mutable.Map[EdgeId, Edge]()
    val visitedNodes = mutable.Set[Long]()

    def addNode(node: Node) {
      nodes(node.id) = node
    }

    def addEdge(edge: Edge) {
      edges(edge.id) = edge
    }

    val node = nodeById(nodeId)
    walkup(node)

    def walkup(node: Node) {
      if (visitedNodes.size < 2000 && visitedNodes.add(node.id)) {
        addNode(node)
        edgesByStartNodeId.get(node.id).foreach {
          theseEdges =>
            for (edge <- theseEdges) {
              addEdge(edge)
              val otherNode = nodeById(edge.end)
              walkup(otherNode)
            }
        }
      }
    }

    StoredGraph(nodes.values.toSeq, edges.values.toSeq, new DateTime(), 0)
  }

  /**
   * Trims the size of the graph by removing the topics with the least highest frequency, then removing the edges that link them to other nodes.
   */
  def subGraph(size: Int, privilegedId: Long = -1l): StoredGraph = {
    if (topics.length <= size) {
      this
    }

    val (keepers, losers) = if (privilegedId == -1) {
      topics.sortBy(-_.count).splitAt(size)
    } else {
      nodeByIdOption(privilegedId).map {
        centerNode =>
          val distances = distanceReport(centerNode, weighted = true)
          distances.filter(_.node.nodeType == NodeType.Topic).map(_.node).splitAt(size)
      } getOrElse {
        (Seq[Node](), topics)
      }
    }

    val reachableIds = mutable.Set[Long]()
    val keepEdges = mutable.Buffer[Edge]()

    @tailrec
    def buildReachableIds(theseedges: Seq[Edge]) {
      val reachedEdges = (for (edge <- theseedges) yield {
        if (reachableIds.add(edge.end)) {
          //Prevent cycles due to hierarchy refactoring
          edgesByStartNodeId.getOrElse(edge.end, Nil)
        } else Nil
      }).flatten

      if (reachedEdges.length > 0)
        buildReachableIds(reachedEdges)
    }


    for (keeper <- keepers) {
      reachableIds.add(keeper.id)
      val edges = outgoing(keeper)
      buildReachableIds(edges)
    }

    for (edge <- edges) {
      if (reachableIds.contains(edge.start) && reachableIds.contains(edge.end)) {
        keepEdges += edge
      }
    }

    StoredGraph(
      reachableIds.map(id => nodeById(id)).toSeq,
      keepEdges.toSeq
    )
  }


  /**
   * Trims the size of the graph by removing the topics with the least highest frequency, then removing the edges that link them to other nodes.
   */
  def subGraph_Old(size: Int, privilegedId: Long = -1l): StoredGraph = {
    if (topics.length <= size) {
      this
    }

    val (keepers, losers) = if (privilegedId == -1) {
      topics.sortBy(-_.count).splitAt(size)
    } else {
      nodeByIdOption(privilegedId).map {
        centerNode =>
          val distances = distanceReport(centerNode, weighted = true)
          distances.seq.filter(_.node.nodeType == NodeType.Topic).map(_.node).splitAt(size)
      } getOrElse {
        (Seq[Node](), topics)
      }
    }

    val filteredEdges = edges.filter {
      edge =>
        if (losers.exists(loser => loser.id == edge.start || loser.id == edge.end)) false else true
    }

    //If an interest does not have a topic relating to it in the keepers list, remove
    val filteredInterests = interests.filter {
      interest =>
        val lowerInterests = incomingEdgesOf(interest, EdgeType.BroaderThan, filteredEdges)
        val incomingTopics = incomingEdgesOf(interest, EdgeType.InterestOf, filteredEdges)

        if (incomingTopics.size == 0 && lowerInterests.size == 0) {
          false
        }
        else if (incomingTopics.size == 0 && lowerInterests.size > 0) {
          true
        }
        else {
          true
        }
    }

    val finalNodes = keepers ++ filteredInterests
    val finalEdges = filteredEdges.filter {
      edge =>
        finalNodes.exists(node => node.id == edge.start) && finalNodes.exists(node => node.id == edge.end)
    }

    val finalNodes2 = finalNodes.filter {
      node =>
        finalEdges.exists(edge => node.id == edge.start || node.id == edge.end)
    }

    StoredGraph(
      finalNodes2,
      finalEdges
    )
  }

  def toPrettyString(pullNodeInfo: Boolean = false): String = {
    if (pullNodeInfo) populateUriAndName()
    val sb = new StringBuilder()
    sb.append("\nGraph: " + date + " : Algo : " + algorithm + " : Created On : " + date.toString("MM-dd-yyyy") + " ")
    sb.append(nodes.size + " nodes, " + edges.size + " edges\n")
    sb.append("\tNodes:\n")
    nodes.foreach(node =>
      sb.append("\t\t" + node.name + " : type : " + node.nodeType.name + " : score : " + node.score + " : count : " + node.count + " : level : " + node.level + "\n")
    )

    sb.append("Edges:\t\n")
    edges.foreach {
      edge =>
        val start = nodeById(edge.start)
        val end = nodeById(edge.end)
        sb.append("\t\t" + start.name + " to " + end.name + " : type : " + edge.relType.name + " : count : " + edge.count + " : score : " + edge.score + " : distance : " + edge.distance + "\n")
    }
    sb.toString()
  }

  def vectorize(filter: (Node) => Boolean): Iterable[(Int, Double)] = {
    populateUriAndName()
    nodes.filter(filter).map(node => MurmurHash3.stringHash(node.uri).abs -> (node.count * node.level).toDouble)
  }

  override def toString(): String = toPrettyString()

  def prettyPrint(pullNodeInfo: Boolean = false) {
    println(toPrettyString(pullNodeInfo))
  }

  def prettyPrintNodes(pullNodeInfo: Boolean = false) {
    println("Nodes: " + nodes.size)
    if (pullNodeInfo) populateUriAndName()
    nodes.sortBy(-_.score).foreach(n => println(n.id + " " + n.uri + " c:" + n.count + " s:" + n.score + " " + n.nodeType.name))
  }

  def intersectScoredNodes(that: StoredGraph): scala.Seq[Node] = {
    val thisIds: Set[Long] = this.nodes.map(_.id)(breakOut)
    that.nodes.filter(node => node.score > 0 && thisIds.contains(node.id))
  }

  def percentOverlap(that: StoredGraph): Double = {
    intersectScoredNodes(that).size.toDouble / (this.nodes.size + that.nodes.size)
  }

}

/** Depth (number of nodes) options used for graph display purposes. */
@SerialVersionUID(2697116616992912318l)
object StoredGraphDisplayDepth extends GrvEnum[Byte] {
  type StoredGraphDisplayDepth = Type
  case class Type(i: Byte, n: String) extends ValueTypeBase(i, n)
  override def mkValue(id: Byte, name: String): Type = Type(id, name)

  val NARROW: Type = Value(0, "narrow")
  val WIDE: Type = Value(1, "wide")

  def defaultValue: Type = NARROW
}
