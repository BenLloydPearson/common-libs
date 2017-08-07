package com.gravity.ontology

import vocab.URIType
import org.neo4j.graphdb._
import scala.collection.JavaConversions._
import com.gravity.ontology.nodes.TopicRelationshipTypes


/**
 * Created with IntelliJ IDEA.
 * User: apatel
 * Date: 4/23/13
 * Time: 6:31 PM
 * To change this template use File | Settings | File Templates.
 */

//trait ConceptScoreValidator extends NodeProcessor {
//  abstract override def shouldProcess(node: OntologyNode): Boolean = {
//    super.shouldProcess(node) && node.node.hasProperty(ConceptHierarchy.PARENT_ATTRIBUTE_SCORE + 1)
//  }
//}

/*

For Reference:


  NodeType	      Node Count
	--------------	----------------
	Topics	 	      ~8M
	Concepts	      ~700K
  Total           ~12M

	*/

object OntologyNodeScore {

  // NEW attibutes
  val InEdges = "GRAV_InEdges"
  val OutEdges = "GRAV_OutEdges"

  //  val TOPIC_CLOSENESS_SCORE = "GRAV_TopicClosenessScore_Level"
  val TopicPrefix = "GRAV_TopicClosenessScore_Level"
  val TopicL1 = TopicPrefix + "1"
  val TopicL2 = TopicPrefix + "2"
  val TopicL3 = TopicPrefix + "3"
  val TopicL4 = TopicPrefix + "4"

  //  val PARENT_ATTRIBUTE_SCORE = "GRAV_ConceptParentScore_Level"
  val ConceptPrefix = "GRAV_ConceptParentScore_Level"
  val ConceptL1 = ConceptPrefix + "1"
  val ConceptL2 = ConceptPrefix + "2"
  val ConceptL3 = ConceptPrefix + "3"
  val ConceptL4 = ConceptPrefix + "4"


  //  val CHILD_ATTRIBUTE_SCORE = "GRAV_ConceptChildScore_Level"
  val ChildConceptPrefix = "GRAV_ConceptChildScore_Level"
  val ChildConceptL1 = ChildConceptPrefix + "1"
  val ChildConceptL2 = ChildConceptPrefix + "2"
  val ChildConceptL3 = ChildConceptPrefix + "3"
  val ChildConceptL4 = ChildConceptPrefix + "4"


  // Grav Auto Paths
  val GRAV_AUTO_BROADER_CONCEPT = new RelationshipType {
    def name(): String = "GRAV_AutoBroaderConcept"
  }
  val GRAV_AUTO_CONCEPT_OF_TOPIC = new RelationshipType {
    def name(): String = "GRAV_AutoConceptOfTopic"
  }

  val GRAV_jumpScore = "GRAV_JumpScore"


  private val gdb = OntologyGraph2.graph.graphDb

  def deleteNode(node: Node) {
    val tx = gdb.beginTx()
    try {
      node.delete()
      tx.success()
    }
    catch {
      case e: Exception => throw new Exception("Unable to delete node " + node.uri)
    }
    finally {
      tx.finish()
    }
  }

  def setScore(node: Node, score: Int, attributeName: String) {
    val tx = gdb.beginTx()
    try {
      node.setProperty(attributeName, score)
      //println("nodeId: " + node.getId + " set " + attributeName + " = " + score)
      //println("[dry run] nodeId: " + node.getId + " set " + attributeName + " = " + score)
      tx.success()
    }
    catch {
      case e: Exception => throw new Exception("Unable to set " + attributeName)
    }
    finally {
      tx.finish()
    }
  }

  def getScore(node: OntologyNode, attributeName: String): Int = {
    node.getProperty(attributeName, 0).asInstanceOf[Int]
  }

  def getScoreDouble(node: OntologyNode, attributeName: String): Double = {
    node.getProperty(attributeName, 0.0d).asInstanceOf[Double]
  }

  def getScoreLong(node: OntologyNode, attributeName: String): Long = {
    node.getProperty(attributeName, 0.0d).asInstanceOf[Long]
  }

  def createRelationship(s: Node, e: Node, relType: RelationshipType, score: Int, attributeName: String) {
    val tx = gdb.beginTx()
    try {
      val rel = s.createRelationshipTo(e, relType)
      rel.setProperty(attributeName, score)
      tx.success()
    }
    catch {
      case e: Exception => throw new Exception("Unable to create relationship " + e.toString)
    }
    finally {
      tx.finish()
    }
  }

  def removeProperty(n: Node, attributeName: String) {
    if (n.hasProperty(attributeName)) {
      val tx = gdb.beginTx()
      try {
        n.removeProperty(attributeName)
        tx.success()
      }
      catch {
        case e: Exception =>
      }
      finally {
        tx.finish()
      }
    }

  }


}

class EdgeCountScorer extends NodeTypeValidator(Seq(URIType.WIKI_CATEGORY)) {
  override def processNode(node: OntologyNode) {

    val relTypes = Seq(
      TopicRelationshipTypes.CONCEPT_OF_TOPIC,
      TopicRelationshipTypes.INTEREST_OF,
      TopicRelationshipTypes.BROADER_CONCEPT,
      TopicRelationshipTypes.REDIRECTS_TO,
      TopicRelationshipTypes.DISAMBIGUATES,
      OntologyNodeScore.GRAV_AUTO_BROADER_CONCEPT,
      OntologyNodeScore.GRAV_AUTO_CONCEPT_OF_TOPIC
    )

    val in = node.node.getRelationships(Direction.INCOMING, relTypes: _*).size
    val out = node.node.getRelationships(Direction.OUTGOING, relTypes: _*).size

    //    val in = node.node.getRelationships(Direction.INCOMING).size
    //    val out = node.node.getRelationships(Direction.OUTGOING).size

    OntologyNodeScore.setScore(node.node, in, OntologyNodeScore.InEdges)
    OntologyNodeScore.setScore(node.node, out, OntologyNodeScore.OutEdges)
  }
}

object OntologyNodesEdgeCountScript extends App {
  OntologyNodesLoader.loadNodes(new EdgeCountScorer())
}

object OntologyNodeCleanerScript extends App {
  OntologyNodesLoader.loadNodes(new ReachableScoreCleaner(), 10)
}

object OntologyNodeScorer {

  def main(startIndex: Int = 0) {
    // flatten the graph
    //    val relCreator = new GravAutoBroaderConceptRelationshipCreator()
    //    OntologyNodesLoader.loadNodes(relCreator)
    //    println("Total Jumped Nodes: " + relCreator.jumpCnt)
    //    println("Total edge promotions: " + relCreator.edgePromotionsCnt)

    OntologyNodesLoader.loadNodes(startIndex, Int.MaxValue, new EdgeCountScorer(), new ReachableTopicScorer(), new ReachableConceptScorer())
  }
}

object OntologyNodeScorerScript extends App {
  OntologyNodeScorer.main()
}

object OntologyNodesPrintAllScript extends App {
    OntologyNodesLoader.loadNodes(0, Int.MaxValue, new NodePrinter(Seq(URIType.WIKI_CATEGORY)))
//    OntologyNodesLoader.loadNodes(0, Int.MaxValue, new NodePrinter(Seq(URIType.WIKI_CATEGORY)), new NodePrinter(Seq(URIType.TOPIC)))
//    OntologyNodesLoader.loadNodes(new NodePrinter(Seq(URIType.WIKI_CATEGORY)), 10)
}
