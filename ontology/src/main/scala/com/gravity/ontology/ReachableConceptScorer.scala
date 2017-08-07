package com.gravity.ontology

import com.gravity.ontology.vocab.URIType
import scala.collection.JavaConversions._
import scala.collection.mutable
import com.gravity.ontology.TraversalFilters.NodeEval
import org.neo4j.graphdb.traversal.Evaluation
import org.neo4j.kernel.Traversal
import com.gravity.ontology.nodes.TopicRelationshipTypes
import org.neo4j.graphdb.Direction

/**
 * Created with IntelliJ IDEA.
 * User: apatel
 * Date: 6/28/13
 * Time: 4:44 PM
 * To change this template use File | Settings | File Templates.
 */

class ReachableConceptScoreCleaner extends NodeTypeValidator(Seq(URIType.WIKI_CATEGORY)) {
  override def processNode(node: OntologyNode) {

    OntologyNodeScore.removeProperty(node.node, OntologyNodeScore.ConceptL1)
    OntologyNodeScore.removeProperty(node.node, OntologyNodeScore.ConceptL2)
    OntologyNodeScore.removeProperty(node.node, OntologyNodeScore.ConceptL3)
    OntologyNodeScore.removeProperty(node.node, OntologyNodeScore.ConceptL4)

    OntologyNodeScore.removeProperty(node.node, OntologyNodeScore.ChildConceptL1)
    OntologyNodeScore.removeProperty(node.node, OntologyNodeScore.ChildConceptL2)
    OntologyNodeScore.removeProperty(node.node, OntologyNodeScore.ChildConceptL3)
    OntologyNodeScore.removeProperty(node.node, OntologyNodeScore.ChildConceptL4)
  }
}

class ReachableScoreCleaner extends NodeTypeValidator(Seq(URIType.WIKI_CATEGORY)) {
  override def processNode(node: OntologyNode) {

    OntologyNodeScore.removeProperty(node.node, OntologyNodeScore.ConceptL1)
    OntologyNodeScore.removeProperty(node.node, OntologyNodeScore.ConceptL2)
    OntologyNodeScore.removeProperty(node.node, OntologyNodeScore.ConceptL3)
    OntologyNodeScore.removeProperty(node.node, OntologyNodeScore.ConceptL4)

    OntologyNodeScore.removeProperty(node.node, OntologyNodeScore.ChildConceptL1)
    OntologyNodeScore.removeProperty(node.node, OntologyNodeScore.ChildConceptL2)
    OntologyNodeScore.removeProperty(node.node, OntologyNodeScore.ChildConceptL3)
    OntologyNodeScore.removeProperty(node.node, OntologyNodeScore.ChildConceptL4)

    OntologyNodeScore.removeProperty(node.node, OntologyNodeScore.TopicL1)
    OntologyNodeScore.removeProperty(node.node, OntologyNodeScore.TopicL2)
    OntologyNodeScore.removeProperty(node.node, OntologyNodeScore.TopicL3)
    OntologyNodeScore.removeProperty(node.node, OntologyNodeScore.TopicL4)

    OntologyNodeScore.removeProperty(node.node, OntologyNodeScore.OutEdges)
    OntologyNodeScore.removeProperty(node.node, OntologyNodeScore.InEdges)


  }
}

class ScoredConceptPrinter extends NodePrinter {

  var total = 0

  override def shouldProcess(node: OntologyNode): Boolean = {
    val c1 = OntologyNodeScore.getScore(node, OntologyNodeScore.ConceptPrefix + 1)
    val c2 = OntologyNodeScore.getScore(node, OntologyNodeScore.ConceptPrefix + 2)
    val c3 = OntologyNodeScore.getScore(node, OntologyNodeScore.ConceptPrefix + 3)
    val c4 = OntologyNodeScore.getScore(node, OntologyNodeScore.ConceptPrefix + 4)

    if (c1 + c2 + c3 + c4 > 0) {
      total += 1
      true
    }
    else {
      false
    }
  }
}

class NoNameConceptPrinter extends NodePrinter {
  override def shouldProcess(node: OntologyNode): Boolean = {
    node.name == "No name"
  }
}

class NoNameConceptScoreCleaner extends NodeTypeValidator(Seq(URIType.WIKI_CATEGORY)) {
  override def shouldProcess(node: OntologyNode): Boolean = {
    super.shouldProcess(node) && node.name == "No name"
  }

  override def processNode(node: OntologyNode) {

    OntologyNodeScore.setScore(node.node, 0, OntologyNodeScore.TopicL1)
    OntologyNodeScore.setScore(node.node, 0, OntologyNodeScore.TopicL2)
    OntologyNodeScore.setScore(node.node, 0, OntologyNodeScore.TopicL3)
    OntologyNodeScore.setScore(node.node, 0, OntologyNodeScore.TopicL4)

    OntologyNodeScore.setScore(node.node, 0, OntologyNodeScore.ConceptL1)
    OntologyNodeScore.setScore(node.node, 0, OntologyNodeScore.ConceptL2)
    OntologyNodeScore.setScore(node.node, 0, OntologyNodeScore.ConceptL3)
    OntologyNodeScore.setScore(node.node, 0, OntologyNodeScore.ConceptL4)

    OntologyNodeScore.setScore(node.node, 0, OntologyNodeScore.ChildConceptL1)
    OntologyNodeScore.setScore(node.node, 0, OntologyNodeScore.ChildConceptL2)
    OntologyNodeScore.setScore(node.node, 0, OntologyNodeScore.ChildConceptL3)
    OntologyNodeScore.setScore(node.node, 0, OntologyNodeScore.ChildConceptL4)

    OntologyNodeScore.setScore(node.node, 0, OntologyNodeScore.InEdges)
    OntologyNodeScore.setScore(node.node, 0, OntologyNodeScore.OutEdges)

  }
}

class NoNameConceptNodeDeleter extends NodeTypeValidator(Seq(URIType.WIKI_CATEGORY)) {
  override def shouldProcess(node: OntologyNode): Boolean = {
    super.shouldProcess(node) && node.name == "No name"
  }

  override def processNode(node: OntologyNode) {
    OntologyNodeScore.deleteNode(node.node)
  }
}


class ReachableConceptScorer(maxDepth: Int = 4, minTopicClosenessScore: Int = 5000, maxDegree: Int = 150, dryRun: Boolean = false)
  extends NodeTypeValidator(Seq(URIType.WIKI_CATEGORY)) {

  var validTopicScores1 = 0
  var validTopicScores2 = 0

  override def shouldProcess(node: OntologyNode): Boolean = {
    val isValid = hasValidTopicScore(node)
    if (isValid) {
      validTopicScores1 += 1
    }
    super.shouldProcess(node) && node.name != "No name" && isValid

  }

  override def processNode(node: OntologyNode) {
    // TBD
    //    println("[tbd] process node " + node.uri)
    scoreNode(node, maxDepth)

    // dump
    //    print(OntologyNodeScore.ConceptPrefix + ". " + node.uri)
    //    var parent = " parent("
    //    var child = " child("
    //
    //    (1 to maxDepth) foreach (d => {
    //      parent += node.getProperty(OntologyNodeScore.ConceptPrefix + d, 0) + ", "
    //      child += node.getProperty(OntologyNodeScore.ChildConceptPrefix + d, 0) + ", "
    //    })
    //    parent += ")"
    //    child += ")"
    //    println(parent + child)

  }

  private def hasValidTopicScore(node: OntologyNode): Boolean = {
    val p = OntologyNodeScore.getScore(node, OntologyNodeScore.OutEdges)

    val scoreAtDepth = new mutable.ListBuffer[Int]
    (1 to maxDepth) foreach (d => {
      scoreAtDepth.append(OntologyNodeScore.getScore(node, OntologyNodeScore.TopicPrefix + d))
    })

    if (scoreAtDepth.filter(s => s > 0).size == maxDepth &&
      p <= maxDegree &&
      scoreAtDepth.last >= minTopicClosenessScore) {
      //d2 >= 10 * d1 && d3 >= d2 * 4 && {

      // TBD: CUSTOM LOGIC FOR DEPTH 3 or GREATER
      if (maxDepth > 2) {
        val d3 = scoreAtDepth(3 - 1)
        val d2 = scoreAtDepth(2 - 1)
        val d1 = scoreAtDepth(1 - 1)

        (d2 >= 10 * d1 && d3 >= d2 * 4)
      }
      else {
        true
      }
    }
    else {
      false
    }

    //      // FILTER to get BEST high level concepts
    //      if (d1 > 0 && d2 > 0 && d3 > 0 &&
    //        d2 >= 10 * d1 && d3 >= d2 * 4 &&
    //        p <= maxDegree && d3 >= minTopicClosenessScore )
    //      //if (true)
    //      {
    //        filteredConceptNodes.add(node)
    //      }
  }

  private def scoreNode(startNode: OntologyNode, depth: Int) {
    val trav = Traversal.description.breadthFirst()
      .evaluator(conceptHierarchyEvaluator(depth + 1))
      .relationships(TopicRelationshipTypes.BROADER_CONCEPT, Direction.INCOMING)
      .relationships(OntologyNodeScore.GRAV_AUTO_BROADER_CONCEPT, Direction.INCOMING)
      .traverse(startNode.node)

    trav foreach (n => {})
  }


  private def conceptHierarchyEvaluator(stopDepth: Int) = NodeEval {
    path => {
      var evaluation = Evaluation.EXCLUDE_AND_CONTINUE
      val depth = path.length()

      if (depth >= stopDepth) {
        evaluation = Evaluation.EXCLUDE_AND_PRUNE
      }
      else if (path.nodes().size > 1) {
        val startNode = new OntologyNode(path.nodes().head)
        val endNode = new OntologyNode(path.endNode())

        if (hasValidTopicScore(endNode)) {
          validTopicScores2 += 1
          if (!dryRun) {
            incrementScore(startNode, depth, OntologyNodeScore.ConceptPrefix)
            incrementScore(endNode, depth, OntologyNodeScore.ChildConceptPrefix)
          }
          //            println("   Set " + OntologyNodeScore.ConceptPrefix + depth + "=" + OntologyNodeScore.getScore(startNode, OntologyNodeScore.ConceptPrefix + depth) + " for " + startNode.uri)
          //            println("       " + OntologyNodeScore.ChildConceptPrefix + depth + "=" + OntologyNodeScore.getScore(endNode, OntologyNodeScore.ChildConceptPrefix + depth) + " for " + endNode.uri)
        }
      }

      evaluation
    }

  }

  private def incrementScore(node: OntologyNode, depth: Int, attributeName: String) {
    val score = node.getProperty(attributeName + depth, 0).asInstanceOf[Int]
    OntologyNodeScore.setScore(node.node, score + 1, attributeName + depth)
  }

}

object ReachableConceptScorerScript extends App {
  val c = new ReachableConceptScorer(dryRun = false)
  OntologyNodesLoader.loadNodes(c)
  println("Valid Topic scores1: " + c.validTopicScores1)
  println("Valid Topic scores2: " + c.validTopicScores2)
}

object ReachableScoreCleanerScript extends App {
  OntologyNodesLoader.loadNodes(new ReachableScoreCleaner())
}

object ReachableConceptScoreCleanerScript extends App {
  OntologyNodesLoader.loadNodes(new ReachableConceptScoreCleaner())
}

object ReachableConceptReScoreScript extends App {
  OntologyNodesLoader.loadNodes(new ReachableScoreCleaner())
  val c = new ReachableConceptScorer(dryRun = false)
  OntologyNodesLoader.loadNodes(c)
  println("Valid Topic scores1: " + c.validTopicScores1)
  println("Valid Topic scores2: " + c.validTopicScores2)
}


// misc scripts
object PrintScoredConceptNodes extends App {
  val p = new ScoredConceptPrinter()
  OntologyNodesLoader.loadNodes(p)
  println("Total: " + p.total)
}


object NoNameConceptPrinterScript extends App {
  OntologyNodesLoader.loadNodes(new NoNameConceptPrinter())
}


object NoNameConceptScoreCleanerScript extends App {
  OntologyNodesLoader.loadNodes(new NoNameConceptScoreCleaner())
}

object NoNameConceptNodeDeleterScript extends App {
  OntologyNodesLoader.loadNodes(new NoNameConceptNodeDeleter())
}
