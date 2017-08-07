package com.gravity.interests.jobs.intelligence.operations

import com.gravity.interests.jobs.intelligence.{Node, MutableGraph, StoredGraph}
import scala.collection.{mutable, Seq}

/**
 * Created with IntelliJ IDEA.
 * User: apatel
 * Date: 9/26/13
 * Time: 3:25 PM
 * To change this template use File | Settings | File Templates.
 */
object SummaryGraphService {

  def toSummaryGraph(sg: StoredGraph, minScore: Int = 1000, isTfIdfGraph: Boolean = false): StoredGraph = {
    val sgTfIdf = if (isTfIdfGraph) sg else GraphAnalysisService.copyGraphToTfIdfGraph(sg)

    // new scores
    for (node <- sgTfIdf.nodes;
         score = KeyNodes.score(sgTfIdf, node) if score >= minScore) {
      node.score = score
    }

    // remove nodes below minScore
    val sgMutable = new MutableGraph(sgTfIdf)
    for (node <- sgTfIdf.nodes.filter(_.score < minScore)) {
      sgMutable.removeNode(node.id)
    }

    // summary graph
    sgMutable.toStoredGraph
  }
}

object KeyNodes {

  def score(sgTfIdf: StoredGraph, node: Node): Double = {

    val inNodes = sgTfIdf.incomingNodes(node)
    val outNodes = sgTfIdf.outgoingNodes(node)

    val cntIn = inNodes.size
    val cntOut = outNodes.size

    val inTopicNodes = inNodes.filter(_.level == 100)
    val cntInTopicNodes = inTopicNodes.size
    val cntInConceptNodes = cntIn - cntInTopicNodes

    val outTopicNodes = outNodes.filter(_.level == 100)
    val cntOutTopicNodes = outTopicNodes.size
    val cntOutConceptNodes = cntOut - cntOutTopicNodes


    val outInDiff = cntOut - cntIn
    val outInDiffTopics = cntOutTopicNodes - cntInTopicNodes
    val outInDiffConcepts = cntOutConceptNodes - cntInConceptNodes
    val inOut = cntIn + cntOut

    // level 1 nodes --> bad
    val levelScore = if (node.level == 2) 2000 else if (node.level == 1) -3000 else 0

    // tfidf score < .15 --> bad
    val tfidfScorePenalty = if (node.score < 0.15) -15000 else 0

    val finalScore =
      (cntOutConceptNodes * 1000) +
        (inOut * 10) +
        (outInDiffTopics * 400) +
        (outInDiffConcepts * 100) +
        levelScore +
        tfidfScorePenalty

    finalScore

  }

  case class NodeInfo(nodeId: Long, score: Double, level: Int, in: Int, out: Int, conceptIn: Int, conceptOut: Int, topicIn: Int, topicOut: Int, var sortScore: Double = 0.0) {
    override def toString(): String = {
      score + " " + level + "   in/out: " + in + "/" + out + "    concept in/out: " + conceptIn + "/" + conceptOut + "   topic in/out: " + topicIn + "/" + topicOut + "   sortScore: " + sortScore
    }
  }

  def extract(sgTfIdf: StoredGraph, minScore: Double = 0): Seq[(Long /*nodeId*/ , Double /*score*/ )] = {

    val nodeCntMap = new mutable.HashMap[Long, NodeInfo]()

    //   sgTfIdf.nodes.filter( n => n.score >= 0.15 && n.level != 1 ).foreach(n=>{
    sgTfIdf.nodes.foreach(n => {
      val inNodes = sgTfIdf.incomingNodes(n)
      val outNodes = sgTfIdf.outgoingNodes(n)

      val cntIn = inNodes.size
      val cntOut = outNodes.size

      val inTopicNodes = inNodes.filter(_.level == 100)
      val cntInTopicNodes = inTopicNodes.size
      val cntInConceptNodes = cntIn - cntInTopicNodes

      val outTopicNodes = outNodes.filter(_.level == 100)
      val cntOutTopicNodes = outTopicNodes.size
      val cntOutConceptNodes = cntOut - cntOutTopicNodes

      val sortScore = score(sgTfIdf, n)

      nodeCntMap(n.id) = NodeInfo(n.id, n.score, n.level, cntIn, cntOut, cntInConceptNodes, cntOutConceptNodes, cntInTopicNodes, cntOutTopicNodes, sortScore)
    })

    val sorted = nodeCntMap.toSeq.sortBy(-_._2.sortScore)
    val filtered = sorted.filter(e => e._2.sortScore >= minScore)

    //    filtered.foreach(entry => {
    //      println(sgTfIdf.nodeById(entry._1).uri + " " + entry._2)
    //    })

    filtered.map(entry => (entry._1, entry._2.sortScore))
  }
}
