package com.gravity.interests.jobs.intelligence

import collection.mutable
import collection.mutable.ListBuffer

/**
 * Created with IntelliJ IDEA.
 * User: apatel
 * Date: 11/27/12
 * Time: 2:14 PM
 * To change this template use File | Settings | File Templates.
 */

class MutableGraphRow extends mutable.HashMap[Long, Edge]

class MutableGraphMatrix extends mutable.HashMap[Long, MutableGraphRow] {
  val inRow: mutable.HashMap[Long, MutableGraphRow] = new mutable.HashMap[Long, MutableGraphRow]()
}

case class MutableGraph(storedGraph: StoredGraph) extends MutableGraphMatrix with GraphCompactorTrait {

  private val nodesById = mutable.HashMap[Long, Node]()

  loadGraphMatrix()

  def loadGraphMatrix() {
    storedGraph.edges foreach (edge => {
      val startNode = storedGraph.nodeById(edge.start)
      val endNode = storedGraph.nodeById(edge.end)
      addEdge(startNode, endNode, edge)
    })
  }

  def toStoredGraph: StoredGraph = {
    new StoredGraph(getAllNodes, getAllEdges)
  }

  def nodeIterable: Iterable[Node] = {
    nodesById.values
  }

  def allNodeCount: Int = nodesById.size

  def getAllNodes: Seq[Node] = {
    nodesById.values.toSeq
  }

  def getAllEdges: Seq[Edge] = {
    val allEdges = new mutable.ArrayBuffer[Edge]()
    for (row <- this.values) {
      allEdges.append(row.values.toSeq: _*)
    }
    allEdges

    //    for (row <- this.values) yield  {row.values}

  }

  def getNode(nodeId: Long): Option[Node] = nodesById.get(nodeId)

  def addNode(node: Node) {
    nodesById(node.id) = node
  }

  def addEdge(nodeFrom: Node, nodeTo: Node, edge: Edge) {
    addNode(nodeFrom)
    addNode(nodeTo)

    // add edge to out bound node
    this.getOrElseUpdate(nodeFrom.id, new MutableGraphRow)(nodeTo.id) = edge

    // add edge to in bound node
    this.inRow.getOrElseUpdate(nodeTo.id, new MutableGraphRow)(nodeFrom.id) = edge
  }

  def removeNode(nodeId: Long) {

    nodesById.remove(nodeId)

    val nodesWithInConnections = this.get(nodeId)
    val nodesWithOutConnections = this.inRow.get(nodeId)

    // remove node's out row
    this.remove(nodeId)

    // remove node's in row
    this.inRow.remove(nodeId)

    // clear deleted node from nodesWithInConnections
    for (row <- nodesWithInConnections;
         (impactedNode, _) <- row;
         impactedRow <- this.inRow.get(impactedNode)) {
      impactedRow.remove(nodeId)
    }

    // clear deleted node from nodesWithOutConnections
    for (row <- nodesWithOutConnections;
         (impactedNode, _) <- row;
         impactedRow <- this.get(impactedNode)) {
      impactedRow.remove(nodeId)
    }
  }

  def removeEdge(nodeFromId: Long, nodeToId: Long) {
    for (outRow <- this.get(nodeFromId);
         inRow <- this.inRow.get(nodeToId)) {
      outRow.remove(nodeToId)
      inRow.remove(nodeFromId)
    }
  }

  def getOutEdges(nodeId: Long): mutable.Map[Long, Edge] = {
    this.getOrElse(nodeId, new MutableGraphRow())
  }

  def getInEdges(nodeId: Long): mutable.Map[Long, Edge] = {
    this.inRow.getOrElse(nodeId, new MutableGraphRow())
  }

  def removeAndConnectNode(nodeToRemove: Node) {
    removeAndConnect(nodeToRemove, this, false)
  }

  // need to implement for compactor trait
  def origGraph: StoredGraph = StoredGraph.makeEmptyGraph

  def compact(desiredTotalNodes: Int, sortFunc: GraphCompactor.NodeToDouble): StoredGraph = StoredGraph.makeEmptyGraph
}

trait GraphCompactorTrait {

  val maxDistanceForValidEdge = 5

  def origGraph: StoredGraph

  def compact(desiredTotalNodes: Int, sortFunc: GraphCompactor.NodeToDouble = GraphCompactor.sortByScore): StoredGraph

  def killNodes(graph: MutableGraph, level: Int, keep: Int, maxGraphNodes: Int, sortFunc: GraphCompactor.NodeToDouble) {

    if (graph.allNodeCount > maxGraphNodes) {
      val candidateNodes = graph.nodeIterable.filter(node => node.level == level)
      val candidateCount = candidateNodes.size
      val killList = candidateNodes.toSeq.sortBy(sortFunc).take(math.max(candidateCount - keep, 0))

      killList.foreach(node => {
        if (graph.allNodeCount > maxGraphNodes) {
          removeAndConnect(node, graph)
        }
      })
    }
  }

  def removeAndConnect(nodeToRemove: Node, mutableGraph: MutableGraph, forceDeleteUnreachableNodes: Boolean = true) {

    val nodeIdToRemove = nodeToRemove.id
    val inEdges = mutableGraph.getInEdges(nodeIdToRemove)
    val outEdges = mutableGraph.getOutEdges(nodeIdToRemove)

    if (outEdges.size == 0) {

      // remove impacted nodes & edges pointing to this node
      inEdges.values.foreach(edge => {

        // remove incoming edge
        mutableGraph.removeEdge(edge.start, edge.end)

        if (forceDeleteUnreachableNodes) {
          // if nodes that point to the node just removed exist in a void, remove them too
          val startId = edge.start
          if (mutableGraph.getOutEdges(startId).size == 0 && mutableGraph.getInEdges(startId).size == 0) {
            // node is an island, kill it
            mutableGraph.removeNode(startId)
          }
        }

      })
    }
    else if (inEdges.size == 0) {
      outEdges.values.foreach(edge => {

        // remove outgoing edge
        mutableGraph.removeEdge(edge.start, edge.end)

        if (forceDeleteUnreachableNodes) {
          // if end node of edge just deleted has nothing pointing to it, kill it as well (recursively)
          // deleting a topic kills all concepts brought in by the topic
          val endId = edge.end
          removeUnreachableNodes(mutableGraph.getNode(endId).get, mutableGraph)
        }
      })

    }
    else {
      // connect all in nodes to all out nodes
      inEdges.values.foreach(inEdge => {
        val startId = inEdge.start
        outEdges.values.foreach(outEdge => {
          val endId = outEdge.end
          val combinedDistance = inEdge.distance + outEdge.distance

          if (startId != endId && combinedDistance <= maxDistanceForValidEdge) {
            val startNode = mutableGraph.getNode(startId).get
            val endNode = mutableGraph.getNode(endId).get

            val relType = getRelType(startNode.nodeType, endNode.nodeType)
            val newEdge = Edge(
              startId,
              endId,
              inEdge.distance + outEdge.distance,
              math.max(inEdge.score, outEdge.score),
              relType,
              math.max(inEdge.count, outEdge.count))

            mutableGraph.addEdge(startNode, endNode, newEdge)
          }
        })
      })
    }

    // delete the node
    mutableGraph.removeNode(nodeIdToRemove)

  }

  private def removeUnreachableNodes(nodeToRemove: Node, mutableGraph: MutableGraph) {

    val queue = new mutable.Queue[Node]()

    queue.enqueue(nodeToRemove)
    while (queue.size > 0) {
      val node = queue.dequeue()
      val inEdges = mutableGraph.getInEdges(node.id)
      if (inEdges.size == 0) {
        val outEdges = mutableGraph.getOutEdges(node.id)
        outEdges.values.foreach(edge => {
          mutableGraph.removeEdge(edge.start, edge.end)
          queue.enqueue(mutableGraph.getNode(edge.end).get)
        })
        mutableGraph.removeNode(node.id)
      }
    }
  }

  private def getRelType(startNodeType: NodeType, endNodeType: NodeType): EdgeType = {
    if (startNodeType == NodeType.Topic && endNodeType == NodeType.Interest) {
      EdgeType.InterestOf
    }
    else {
      EdgeType.BroaderThan
    }
  }

  def nodesByLevel(sg: StoredGraph, level: Short): Seq[Node] = {
    if (sg.nodesByLevel.contains(level)) {
      sg.nodesByLevel(level)
    }
    else {
      ListBuffer.empty[Node]
    }
  }
}

object GraphCompactor {
  type NodeToDouble = (Node) => Double

  def sortByCount(node: Node): Double = {
    node.count.toDouble
  }

  def sortByScore(node: Node): Double = {
    node.score
  }

  def sortByScoreAndCount(node: Node): Double = {
    node.score * node.count
  }

}

case class GraphCompactorTopDown(storedGraph: StoredGraph) extends GraphCompactorTrait {

  val origGraph: StoredGraph = storedGraph

  def compact(desiredTotalNodes: Int, sortFunc: GraphCompactor.NodeToDouble): StoredGraph = {
    val mutableGraph = new MutableGraph(origGraph)
    performDeepTrim(desiredTotalNodes, mutableGraph, sortFunc)
    mutableGraph.toStoredGraph
  }

  private def performDeepTrim(maxNodes: Int, graph: MutableGraph, sortFunc: GraphCompactor.NodeToDouble) {
    val minL100 = math.max(8, maxNodes / 2)
    val minL3 = math.max(6, maxNodes / 4)
    val minL2 = math.max(4, maxNodes / 8)
    val minL1 = math.max(2, maxNodes / 16)

    killNodes(graph, 1, minL1, maxNodes, sortFunc)
    killNodes(graph, 2, minL2, maxNodes, sortFunc)
    killNodes(graph, 3, minL3, maxNodes, sortFunc)
    killNodes(graph, 100, minL100, maxNodes, sortFunc)

  }

}

class GraphCompactorBottomUp(storedGraph: StoredGraph) extends GraphCompactorTrait {
  val origGraph: StoredGraph = storedGraph

  def compact(desiredTotalNodes: Int, sortFunc: GraphCompactor.NodeToDouble): StoredGraph = {
    val mutableGraph = new MutableGraph(origGraph)
    performDeepTrim(desiredTotalNodes, mutableGraph, sortFunc)
    mutableGraph.toStoredGraph
  }

  private def performDeepTrim(maxNodes: Int, graph: MutableGraph, sortFunc: GraphCompactor.NodeToDouble) {
    val minL100 = math.max(8, maxNodes / 2)
    val minL3 = math.max(6, maxNodes / 4)
    val minL2 = math.max(4, maxNodes / 8)
    val minL1 = math.max(2, maxNodes / 16)

    killNodes(graph, 100, minL100, maxNodes, sortFunc)
    killNodes(graph, 3, minL3, maxNodes, sortFunc)
    killNodes(graph, 2, minL2, maxNodes, sortFunc)
    killNodes(graph, 1, minL1, maxNodes, sortFunc)

  }
}

class GraphCompactorRatioBased(storedGraph: StoredGraph) extends GraphCompactorTrait {
  val origGraph: StoredGraph = storedGraph

  def compact(desiredTotalNodes: Int, sortFunc: GraphCompactor.NodeToDouble): StoredGraph = {
    val mutableGraph = new MutableGraph(origGraph)
    val origNodes = origGraph.nodes.size

    val p1 = nodesByLevel(origGraph, 1).size.toDouble / origNodes
    val p2 = nodesByLevel(origGraph, 2).size.toDouble / origNodes
    val p3 = nodesByLevel(origGraph, 3).size.toDouble / origNodes
    val p100 = nodesByLevel(origGraph, 100).size.toDouble / origNodes

    val c1 = (p1 * desiredTotalNodes).toInt
    val c2 = (p2 * desiredTotalNodes).toInt
    val c3 = (p3 * desiredTotalNodes).toInt
    val c100 = (p100 * desiredTotalNodes).toInt

    killNodes(mutableGraph, 100, c100, desiredTotalNodes, sortFunc)
    killNodes(mutableGraph, 3, c3, desiredTotalNodes, sortFunc)
    killNodes(mutableGraph, 2, c2, desiredTotalNodes, sortFunc)
    killNodes(mutableGraph, 1, c1, desiredTotalNodes, sortFunc)

    mutableGraph.toStoredGraph
  }
}