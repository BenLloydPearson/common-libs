package com.gravity.ontology

import org.neo4j.tooling.GlobalGraphOperations
import org.joda.time.DateTime
import vocab.URIType

import scala.collection.JavaConversions._
import java.io.FileWriter

import com.gravity.ontology.importingV2.NodeProperties
import org.neo4j.graphdb.Direction


/**
 * Created with IntelliJ IDEA.
 * User: apatel
 * Date: 4/23/13
 * Time: 6:23 PM
 * To change this template use File | Settings | File Templates.
 */

trait NodeProcessor {
  def shouldProcess(node: OntologyNode): Boolean

  def processNode(node: OntologyNode)
}

object OntologyNodesLoader {

  def loadNodes(startIndex: Int, maxNodesToProcess: Int, nodeProcessors: NodeProcessor*) {
    var cntProcessed = 0
    var cntScanned = 0
    val iterator = GlobalGraphOperations.at(OntologyGraph2.graph.graphDb).getAllNodes.iterator()
    while (cntProcessed < maxNodesToProcess && iterator.hasNext) {

      if (cntScanned >= startIndex) {
        val node = new OntologyNode(iterator.next())
        var nodeProcessed = false
        for (nodeProcessor <- nodeProcessors if nodeProcessor.shouldProcess(node)) {
          if (!nodeProcessed) {
            nodeProcessed = true
            cntProcessed += 1
          }
          nodeProcessor.processNode(node)
        }
      }
      else {
        // skip node
        iterator.next()
      }

      cntScanned += 1

      // update status
      if (cntScanned % 5000 == 0) println(new DateTime().toLocalTime + "  Scanned Nodes: " + cntScanned + "  Processed Nodes: " + cntProcessed)
    }
    println("Completed Scan!  " + new DateTime().toLocalTime + "  Scanned Nodes: " + cntScanned + "  Processed Nodes: " + cntProcessed)
  }

  def loadNodes(nodeProcessor: NodeProcessor, maxNodes: Int = Int.MaxValue) {
    var cnt = 0
    var ttl = 0
    val iterator = GlobalGraphOperations.at(OntologyGraph2.graph.graphDb).getAllNodes.iterator()
    while (cnt < maxNodes && iterator.hasNext) {
      ttl += 1
      val node = new OntologyNode(iterator.next())
      if (nodeProcessor.shouldProcess(node)) {
        nodeProcessor.processNode(node)
        cnt += 1
        if (cnt % 10000 == 0) println(cnt + "/" + ttl + " @ " + new DateTime().toLocalTime)
      }
      if (ttl % 50000 == 0) println("Scanned Nodes: " + ttl + " @ " + new DateTime().toLocalTime)

    }

    println("Total matched nodes: " + cnt)
  }
}

abstract class NodeTypeValidator(validNodeTypes: Seq[URIType] = Seq.empty[URIType]) extends NodeProcessor {
  override def shouldProcess(node: OntologyNode): Boolean = {
    lazy val uriType = {
      try {
        node.uriType
      }
      catch {
        case _: Exception => URIType.UNKNOWN
      }
    }

    lazy val uri = {
      try {
        node.uri
      }
      catch {
        case _: Exception => ""
      }
    }

    uri.size > 0 && (validNodeTypes.size == 0 || validNodeTypes.contains(uriType))
  }
}

class NodePrinter(validNodeTypes: Seq[URIType] = Seq.empty[URIType]) extends NodeTypeValidator(validNodeTypes) {

  lazy val out = new FileWriter("/Users/akash/junk/ontNodeScores/NodePrinter/lastRun.txt")

  //  val goldGraphName = "gold"
  //  val goldGraph = new OntologyGraph2(OntologyGraphMgr.getInstance().getGraph(goldGraphName))

  def processNode_old(node: OntologyNode) {
    //val frequency = GraphAnalysisService.frequencyForNode(OntologyNodeKey(node.uri).nodeId)

    println(node.name) // + " freq: " + frequency)
    node.node.getPropertyKeys.foreach(key => {
      val value = node.getProperty(key)
      println("\t" + key + ": " + value)
    })
  }

  override def processNode(node: OntologyNode) {
    val TAB = "\t"

    val scoresInt = for (scoreName <- NodePrinter.scoreNamesInt) yield OntologyNodeScore.getScore(node, scoreName)
    val scoresDbl = for (scoreName <- NodePrinter.scoreNamesDbl) yield OntologyNodeScore.getScoreDouble(node, scoreName)
    val scoresLong = for (scoreName <- NodePrinter.scoreNamesLong) yield OntologyNodeScore.getScoreLong(node, scoreName)

    val scoresIntTxt = scoresInt.mkString(TAB)
    val scoresDblTxt = scoresDbl.mkString(TAB)
    val scoresLongTxt = scoresLong.mkString(TAB)

    val line = Array(node.uri, scoresIntTxt, scoresDblTxt, scoresLongTxt).mkString(TAB)

//    println(line)
    wline(line)
  }

  def wline(s: String) {
    out.write(s + "\n")
  }


}

object NodePrinter{
  val scoreNamesInt = Array(
    NodeProperties.InEdges, NodeProperties.OutEdges,

    NodeProperties.TopicL1, NodeProperties.TopicL2, NodeProperties.TopicL3, NodeProperties.TopicL4,
    NodeProperties.TopicL2Sum, NodeProperties.TopicL3Sum, NodeProperties.TopicL4Sum,

    NodeProperties.ConceptL1, NodeProperties.ConceptL2, NodeProperties.ConceptL3, NodeProperties.ConceptL4,
    NodeProperties.ConceptL2Sum, NodeProperties.ConceptL3Sum, NodeProperties.ConceptL4Sum,

    NodeProperties.TriangleCount
  )

  val scoreNamesDbl = Array(
    NodeProperties.PageRank
  )

  val scoreNamesLong = Array(
    NodeProperties.ConnectedComponenetId
  )

  lazy val header = {
    val TAB = "\t"
    Array.concat(NodePrinter.scoreNamesInt, NodePrinter.scoreNamesDbl).mkString(TAB)
  }
}

object OntologyNodesPrintScript extends App {

  println(NodePrinter.header)

//  OntologyNodesLoader.loadNodes(new NodePrinter(Seq(URIType.NY_TIMES)), 10000)
    OntologyNodesLoader.loadNodes(new NodePrinter(Seq(URIType.WIKI_CATEGORY)), 1000)
}

class GraphInfoProcessor extends NodeProcessor {

  var totalEdgeCountOut = 0
  var totalEdgeCountIn = 0
  var totalNodeCount = 0

  override def shouldProcess(node: OntologyNode) = true

  override def processNode(node: OntologyNode) {
    totalNodeCount += 1
    val edgesOut = node.node.getRelationships(Direction.OUTGOING).size
    val edgesIn = node.node.getRelationships(Direction.INCOMING).size

    totalEdgeCountOut += edgesOut
    totalEdgeCountIn += edgesIn
    //    println("Node: " + node.name + "  outEdges: " + totalEdgeCountOut )
  }


}

object graphInfo extends App {
  val p = new GraphInfoProcessor()
  OntologyNodesLoader.loadNodes(p)
  println("Nodes: " + p.totalNodeCount)
  println("Out Edges: " + p.totalEdgeCountOut)
  println("In Edges: " + p.totalEdgeCountIn)
}