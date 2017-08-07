package com.gravity.ontology.analysis

import com.gravity.ontology.{OntologyNode, OntologyGraph2, OntologyGraphMgr}
import org.neo4j.tooling.GlobalGraphOperations
import org.neo4j.graphdb.{Direction, Relationship, Node}
import scala.collection.JavaConversions._
import com.gravity.utilities.{MurmurHash, grvio}
import org.openrdf.model.impl.ValueFactoryImpl

/**
 * Created with IntelliJ IDEA.
 * User: apatel
 * Date: 10/24/13
 * Time: 3:13 PM
 * To change this template use File | Settings | File Templates.
 */


object OntologyNodeComparisonReport_ConsoleApp extends App {
  val newOnt = "graph_concept"
  val oldOnt = "graph_concept_gold"

  val edgeDiffFileSpecial = "/Users/apatel/junk/ontComparison/special_edgeDiff" + newOnt + ".txt"
  val DELIM = "||"
  // 0 nodeId
  // 1 node uri,
  // 2 outEdges,
  // 3 outEdgesOther,
  // 4 inEdges,
  // 5 inEdgesOther"


  val oi = new OntologyInfo(newOnt)
  val oiOld = new OntologyInfo(oldOnt)

  grvio.perLine(edgeDiffFileSpecial) {
    line =>
      val parts = line.split(DELIM)
      val nodeUri = parts(1)
      val outDiff = Math.abs(parts(2).toInt - parts(3).toInt)
      //val inDiff = parts(4).toInt - parts(5).toInt

      val dir =
        if (outDiff == 1) {
          Direction.OUTGOING
        }
        else {
          Direction.INCOMING
        }

      OntologyNodeComparisonReport.diff(nodeUri, oi, oiOld, dir)
  }
}


object OntologyNodeComparisonReportTestApp extends App {
  //  OntologyNodeComparisonReport.quickDump("http://dbpedia.org/ontology/Legislature", "graph_concept", "graph_concept_gold", Direction.OUTGOING)
  //  OntologyNodeComparisonReport.quickDump("http://dbpedia.org/resource/David_Klahr", "graph_concept", "graph_concept_gold", Direction.OUTGOING)
  OntologyNodeComparisonReport.quickDump("http://dbpedia.org/resource/Twilight_the_book", "graph_concept", "graph_concept_gold", Direction.OUTGOING)

  //  OntologyNodeComparisonReport.quickDump("http://dbpedia.org/ontology/Criminal", "graph_concept", "graph_concept_gold", Direction.INCOMING)


}

object OntologyNodeComparisonReport {

  def quickDump(nodeUri: String, g1: String, g2: String, dir: Direction = Direction.OUTGOING) {
    diff(nodeUri, new OntologyInfo(g1), new OntologyInfo(g2), dir)
  }

  def diff(nodeUri: String, oi1: OntologyInfo, oi2: OntologyInfo, dir: Direction) {

    val id = MurmurHash.hash64(nodeUri)
    val on = oi1.graph.node(ValueFactoryImpl.getInstance().createURI(nodeUri))
    val onOther = oi2.graph.node(ValueFactoryImpl.getInstance().createURI(nodeUri))

    //    val on = oi1.graph.getNodeByNodeId(id)
    //    val onOther = oi2.graph.getNodeByNodeId(id)


    println("node1: " + on + " node2: " + onOther)

    for (n1 <- on;
         n2 <- onOther) {

      val rel1 = n1.node.getRelationships(dir).toSet
      val rel2 = n2.node.getRelationships(dir).toSet

      println(on)

      if (rel1.size > rel2.size) {
        dump(rel1, rel2, oi1, oi2, dir)
      }
      else {
        dump(rel2, rel1, oi2, oi1, dir)
      }
    }
  }

  def dump(rel1: Set[Relationship], rel2: Set[Relationship], oi1: OntologyInfo, oi2: OntologyInfo, dir: Direction) {

    println(oi1.graphName + " over " + oi2.graphName)
    println("rel1: " + rel1.size)
    rel1.foreach(rel => {
      println("from " + OntologyNode(rel.getStartNode).uri + " to " + OntologyNode(rel.getEndNode).uri + " via " + rel.getType.name() + " dir " + dir)
    })

    println("rel2: " + rel2.size)
    rel2.foreach(rel => {
      println("from " + OntologyNode(rel.getStartNode).uri + " to " + OntologyNode(rel.getEndNode).uri + " via " + rel.getType.name() + " dir " + dir)
    })


    val diffRel = rel1 -- rel2

    for (dr <- diffRel) {
      val s = dr.getStartNode
      val e = dr.getEndNode

      val onS = OntologyNode(s)
      val onE = OntologyNode(e)

      val relName = dr.getType.name()
      println(oi1.graphName + ", " + onS.uri + ", " + onE.uri + ", " + relName + ", " + dir.name())
    }
  }
}

object OntologyComparisonReport_ConsoleApp extends App {
  val newOnt = "graph_concept"
  val oldOnt = "graph_concept_gold"

  val res = OntologyComparisonReport.compare(newOnt, oldOnt)

  println("")
  println("ComparisonResult" + res)
}

case class NodeEdges(equal: Int, more: Int, less: Int) {
  override def toString = {
    "equal: " + equal + "   more: " + more + "   less: " + less
    //    String.format("total: %d   onlyInSelf: %d   common: %d",
    //      total, onlyInSelf, common)
  }
}

object NodeEdges {
  def empty = NodeEdges(-1, -1, -1)
}

case class GraphInfo(total: Int, common: Int, onlyInSelf: Int, nodeEdges: NodeEdges) {
  override def toString = {
    "total: " + total + "   onlyInSelf: " + onlyInSelf + "   common: " + common + " Node Edges: (" + nodeEdges.toString + ")"
    //    String.format("total: %d   onlyInSelf: %d   common: %d",
    //      total, onlyInSelf, common)
  }
}

case class SetInfo(sizeSet1: Int, sizeSet2: Int, common: Int, left: Int, right: Int, leftNodeEdges: NodeEdges, rightNodeEdges: NodeEdges) {
  override def toString = {
    "set1: " + sizeSet1 + "  set2: " + sizeSet2 + "   set1Only:" + left + "   set2Only:" + right + "   common: " + common + "   set1NodeEdges: (" + leftNodeEdges + ")    set2NodeEdges: (" + rightNodeEdges + ")"
    //    String.format("set1: %d   set2: %d   set1Only: %d   set2Only: %d   common: %d",
    //      sizeSet1, sizeSet2, left, right, common
    //    )
  }
}

case class ComparisonResult(nodeComparison: SetInfo, edgeComparison: SetInfo) {
  override def toString = {
    "nodeInfo: (" + nodeComparison + ")   EdgeInfo: (" + edgeComparison + ")"
    //    String.format("nodeInfo: %s   EdgeInfo: %s",
    //      nodeComparison.toString, edgeComparison.toString
    //    )
  }
}

object OntologyComparisonReport {
  def compare(graphName1: String, graphName2: String): ComparisonResult = {

    println("Getting graphs ...")
    val oi1 = new OntologyInfo(graphName1)
    val oi2 = new OntologyInfo(graphName2)

    println("")
    println("Nodes")
    val graphNodeInfo1 = oi1.compareNodesWith(oi2)
    println(graphName1 + ": " + graphNodeInfo1)

    val graphNodeInfo2 = oi2.compareNodesWith(oi1)
    println(graphName2 + ": " + graphNodeInfo2)

    if (graphNodeInfo1.common != graphNodeInfo2.common) {
      // shit!
      println("Node Intersection count does not match")
      throw new Exception("Node Intersection count does not match")
    }

    if (graphNodeInfo1.nodeEdges.equal != graphNodeInfo2.nodeEdges.equal) {
      // shit!
      println("Node Edge count does not match")
      throw new Exception("Node Edge count does not match " + graphNodeInfo1.nodeEdges.equal + " vs " + graphNodeInfo2.nodeEdges.equal)
    }


    val nodeComp = SetInfo(graphNodeInfo1.total, graphNodeInfo2.total, graphNodeInfo1.common, graphNodeInfo1.onlyInSelf, graphNodeInfo2.onlyInSelf, graphNodeInfo1.nodeEdges, graphNodeInfo2.nodeEdges)
    println("nodeComp: " + nodeComp)


    println("")
    println("Edges")
    val edgeInfo1 = oi1.compareEdgesWith(oi2)
    println("edgeInfo " + graphName1 + ": " + edgeInfo1)

    val edgeInfo2 = oi2.compareEdgesWith(oi1)
    println("edgeInfo " + graphName2 + ": " + edgeInfo2)

    if (edgeInfo1.common != edgeInfo2.common) {
      // shit!
      println("Edge Intersection count does not match")
      throw new Exception("Edge Intersection count does not match " + edgeInfo1.common + " vs " + edgeInfo2.common)
    }




    val edgeComp = SetInfo(edgeInfo1.total, edgeInfo2.total, edgeInfo1.common, edgeInfo1.onlyInSelf, edgeInfo2.onlyInSelf, NodeEdges.empty, NodeEdges.empty)
    println("")
    println("edgeComp: " + edgeComp)

    ComparisonResult(nodeComp, edgeComp)
  }
}

class OntologyInfo(name: String) {

  val graphName = name
  lazy val graph = new OntologyGraph2(OntologyGraphMgr.getInstance().getGraph(name))


  val DELIM = "||"
  //    val edgeDiffFile = "/Users/apatel/junk/ontComparison/edgeDiff" + this.graphName + ".txt"
  val edgeDiffFileSpecial = "/Users/apatel/junk/ontComparison/special_edgeDiff" + this.graphName + ".txt"


  //  lazy val totalNodes = getTotalNodes()
  //  lazy val totalEdges = getTotalEdges()

  //  private def getTotalNodes() {
  //    getSize(nodeIterator)
  //  }
  //
  //  private def getTotalEdges() {
  //    getSize(edgeIterator)
  //  }
  //
  //  private def getSize[T](iterator: Iterator[T]) : Int = {
  //    iterator.count(true)
  ////    var cnt = 0
  ////    while (iterator.hasNext) {
  ////      cnt += 1
  ////    }
  ////    cnt
  //  }

  def nodeIterator(): Iterator[Node] = {
    GlobalGraphOperations.at(graph.graphDb).getAllNodes.iterator()
  }

  def edgeIterator(): Iterator[Relationship] = {

    GlobalGraphOperations.at(graph.graphDb).getAllRelationships.iterator()
  }

  def compareNodesWith(oi: OntologyInfo): GraphInfo = {
    val iterator = nodeIterator()
    var common = 0
    var onlyInSelf = 0
    var cnt = 0

    var edgeCountsMatch = 0
    var edgeCountsLess = 0
    var edgeCountsMore = 0



    //    grvio.appendToFile(edgeDiffFile)("this graph = " + this.graphName)
    //    grvio.appendToFile(edgeDiffFile)("other graph = " + oi.graphName)

    val edgeDiffFileHeader = "node id" + DELIM + "node uri" + DELIM + "outEdges" + DELIM + "outEdgesOther" + DELIM + "inEdges" + DELIM + "inEdgesOther"
    //    grvio.appendToFile(edgeDiffFile)(edgeDiffFileHeader)
    grvio.appendToFile(edgeDiffFileSpecial)(edgeDiffFileHeader)

    while (iterator.hasNext) {
      cnt += 1
      val node = iterator.next()
      val on = OntologyNode(node)
      oi.graph.getNodeByNodeId(node.getId) match {
        case Some(onOther) =>
          // node exists in both ontologies
          common += 1

          // compare edges
          val nodeEdges = node.getRelationships.size
          val otherNodeEdges = onOther.node.getRelationships.size

          if (nodeEdges == otherNodeEdges) {
            edgeCountsMatch += 1
          }
          else if (nodeEdges > otherNodeEdges) {
            edgeCountsMore += 1
          }
          else {
            edgeCountsLess += 1
          }


          //    val edgeDiffFileHeader = "nodeId, node uri, outEdges, outEdgesOther, inEdges, inEdgesOther"

          val diffInfoLine =
            node.getId.toString +
              DELIM + getSafeUri(onOther) + DELIM +
              node.getRelationships(Direction.OUTGOING).size + DELIM +
              onOther.node.getRelationships(Direction.OUTGOING).size + DELIM +
              node.getRelationships(Direction.INCOMING).size + DELIM +
              onOther.node.getRelationships(Direction.INCOMING).size

          //          grvio.appendToFile(edgeDiffFile)(diffInfoLine)

          // special logging
          val outDiff = node.getRelationships(Direction.OUTGOING).size - onOther.node.getRelationships(Direction.OUTGOING).size
          val inDiff = node.getRelationships(Direction.INCOMING).size - onOther.node.getRelationships(Direction.INCOMING).size
          if (Math.abs(outDiff) == 1 || Math.abs(inDiff) == 1) {
            grvio.appendToFile(edgeDiffFileSpecial)(diffInfoLine)
          }


        case _ =>
          // node exists in only current ontology
          val diffInfoLine =
            node.getId.toString +
              DELIM + getSafeUri(on) + DELIM +
              node.getRelationships(Direction.OUTGOING).size + DELIM + "-1" + DELIM +
              node.getRelationships(Direction.INCOMING).size + DELIM + "-1"

          //          grvio.appendToFile(edgeDiffFile)(diffInfoLine)

          onlyInSelf += 1
      }
    }

    val nodeEdges = NodeEdges(edgeCountsMatch, edgeCountsMore, edgeCountsLess)

    GraphInfo(cnt, common, onlyInSelf, nodeEdges)
  }

  def getSafeUri(on: OntologyNode): String = {
    try {
      on.uri
    }
    catch {
      case _: Throwable =>
        getSafeName(on)
    }
  }

  def getSafeName(on: OntologyNode): String = {
    try {
      on.name
    }
    catch {
      case _: Throwable =>
        on.node.getId.toString
    }
  }


  def compareEdgesWith(oi: OntologyInfo): GraphInfo = {
    val iterator = edgeIterator()
    var common = 0
    var onlyInSelf = 0
    var cnt = 0

    while (iterator.hasNext) {
      cnt += 1
      val edge = iterator.next()

      try {
        val edgeOther = oi.graph.graphDb.getRelationshipById(edge.getId)
        common += 1
      }
      catch {
        case e: org.neo4j.graphdb.NotFoundException =>
          onlyInSelf += 1
      }

    }

    GraphInfo(cnt, common, onlyInSelf, NodeEdges.empty)
  }

}