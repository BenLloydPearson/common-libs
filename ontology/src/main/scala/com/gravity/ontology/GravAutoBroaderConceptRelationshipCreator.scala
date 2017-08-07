package com.gravity.ontology

import com.gravity.ontology.vocab.URIType
import org.neo4j.graphdb.Direction
import com.gravity.ontology.nodes.TopicRelationshipTypes
import scala.collection.mutable
import scala.collection.JavaConversions._

/**
 * Created with IntelliJ IDEA.
 * User: apatel
 * Date: 6/28/13
 * Time: 2:24 PM
 * To change this template use File | Settings | File Templates.
 */

class GravAutoBroaderConceptRelationshipCreator(dryRun: Boolean = false) extends NodeTypeValidator(Seq(URIType.WIKI_CATEGORY)) {

  println("dryRun = " + dryRun)

  var edgePromotionsCnt = 0
  var jumpCnt = 0

  override def processNode(node: OntologyNode) {

    val parents = collectDirectHigherLevelConcepts(node)
    if (parents.size > 0) {
      jumpCnt += 1

      val inRels = node.node.getRelationships(Direction.INCOMING,
        TopicRelationshipTypes.BROADER_CONCEPT,
        TopicRelationshipTypes.CONCEPT_OF_TOPIC).toList
      val eCnt = inRels.size
      edgePromotionsCnt += eCnt

      //      var line = parents.size + " " + node.uri
      //      parents foreach (parent => {
      //        line += "->" + parent.uri
      //      })
      //      line += " : " + eCnt + " edge promotions"
      //      println(line)

      inRels foreach (inRel => {

        val oStartNode = new OntologyNode(inRel.getStartNode)
        //println("   " + oStartNode.uri)

        if (!dryRun) {
          // create rel from lowerlevelnodes to highest level concept
          oStartNode.uriType match {
            case URIType.WIKI_CATEGORY =>
              OntologyNodeScore.createRelationship(oStartNode.node, parents.last.node, OntologyNodeScore.GRAV_AUTO_BROADER_CONCEPT, parents.size, OntologyNodeScore.GRAV_jumpScore)
            case URIType.TOPIC =>
              OntologyNodeScore.createRelationship(oStartNode.node, parents.last.node, OntologyNodeScore.GRAV_AUTO_CONCEPT_OF_TOPIC, parents.size, OntologyNodeScore.GRAV_jumpScore)
            case _ =>
          }
        }
        else {
          println("[dry run] create GRAV_AUTO rel from " + oStartNode.uri + " to " + parents.last.node.uri + " with " + OntologyNodeScore.GRAV_jumpScore + "=" + parents.size)
        }
      })

      //println("")
    }
  }

  private def collectDirectHigherLevelConcepts(node: OntologyNode): mutable.HashSet[OntologyNode] = {
    val parents = mutable.HashSet.empty[OntologyNode]
    collectDirectHighLevelConcepts(node, parents)
    parents
  }

  private def collectDirectHighLevelConcepts(node: OntologyNode, parents: mutable.HashSet[OntologyNode]) {
    val rels = node.node.getRelationships(Direction.OUTGOING, TopicRelationshipTypes.BROADER_CONCEPT).toList
    if (rels.size == 1) {
      val rel = rels(0)

      if (!parents.contains(rel.getEndNode)) {
        parents.add(rel.getEndNode)
        collectDirectHighLevelConcepts(rel.getEndNode, parents)
      }
    }
  }
}

object GravAutoBroaderConceptRelationshipCreatorScript extends App {
  val relCreator = new GravAutoBroaderConceptRelationshipCreator()
  OntologyNodesLoader.loadNodes(relCreator)
  println("Total Jumped Nodes: " + relCreator.jumpCnt)
  println("Total edge promotions: " + relCreator.edgePromotionsCnt)
}
