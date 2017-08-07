package com.gravity.interests.jobs.intelligence.operations.graphing

import com.gravity.interests.jobs.intelligence._
import com.gravity.interests.jobs.intelligence.algorithms.{GraphSimilarityScore, StoredInterestSimilarityAlgos}
import com.gravity.interests.jobs.intelligence.operations.{OntologyNodeService, GraphAnalysisService}
import collection.mutable
import com.gravity.interests.jobs.intelligence.Node

/**
 * Created with IntelliJ IDEA.
 * User: apatel
 * Date: 4/12/13
 * Time: 4:16 PM
 * To change this template use File | Settings | File Templates.
 */

trait GraphExpansionAlgo {
  def expandGraph(sg: StoredGraph, depth: Int): StoredGraph
}

class OntologyDbExpander extends GraphExpansionAlgo {
  override def expandGraph(sg: StoredGraph, depth: Int): StoredGraph = {
    var sgNew = StoredGraph.makeEmptyGraph
    sg.nodes.filter(n => n.nodeType != NodeType.Term).map(n => n.id).foreach(id => {
      sgNew += StoredGraphHelper.buildGraphAroundNodeById(id, depth, org.neo4j.graphdb.Direction.OUTGOING)
    })
    sgNew
  }
}

class OntologyHbaseExpander extends GraphExpansionAlgo {
  override def expandGraph(sg: StoredGraph, depth: Int): StoredGraph = {
    sg.populateUriAndName()
    val nodesForNewGraph = new mutable.HashSet[Node]()
    // update node id for existing nodes
    nodesForNewGraph ++= sg.nodes.map(n => n.copy(id = OntologyNodeKey(n.uri).nodeId))

    val nodesForNewGraphIds = nodesForNewGraph.map(e => e.id)
    val templateConceptNode = new Node(id = 0, name = "", uri = "", nodeType = NodeType.Interest, level = 3, count = 1, score = 0)

    val nodesToExpand = nodesForNewGraph.filter(n => GraphAnalysisService.frequencyForNode(n.id) > 1)
    val nodeKeysToExpand = nodesToExpand.map(n => OntologyNodeKey(n.uri)).toSet
    val rowsForNodesToExpand = OntologyNodeService.getAdjacentNodeRows(nodeKeysToExpand)

    for (adjacentNodeRow <- rowsForNodesToExpand;
         adjacentNode <- adjacentNodeRow.getAdjacentNodesUpTo(depth)
         if (!nodesForNewGraphIds.contains(adjacentNode.nodeKey.nodeId) &&
           GraphAnalysisService.frequencyForNode(adjacentNode.nodeKey.nodeId) > 1);
         adjacentGraphNode = templateConceptNode.copy(id = adjacentNode.nodeKey.nodeId,
           uri = adjacentNode.uri,
           nodeType = if (adjacentNode.uri.contains("Category:")) NodeType.Interest else NodeType.Topic,
           score = GraphAnalysisService.frequencyForNode(adjacentNode.nodeKey.nodeId))) {
      nodesForNewGraph.add(adjacentGraphNode)
      nodesForNewGraphIds.add(adjacentGraphNode.id)
    }

    new StoredGraph(nodesForNewGraph.toSeq, Seq.empty[Edge])
  }
}

object ExpandedGraphSimScore {

  def calculateTfIdfSimScore(sg1: StoredGraph, sg2: StoredGraph, expansionDepth: Int = 2, graphExpander: GraphExpansionAlgo = new OntologyHbaseExpander()): GraphSimilarityScore = {
    //println("   orig g1:" + sg1.nodes.size + ", g2: " + sg2.nodes.size)
    val newG1 = graphExpander.expandGraph(sg1, expansionDepth)
    GraphAnalysisService.mutateGraphWithTfIdfs(newG1)
    val newG2 = graphExpander.expandGraph(sg2, expansionDepth)
    GraphAnalysisService.mutateGraphWithTfIdfs(newG2)
    //println("   new g1:" + newG1.nodes.size + ", g2: " + newG2.nodes.size)

    StoredInterestSimilarityAlgos.GraphCosineScoreSimilarity.score(newG1, newG2)
  }
}