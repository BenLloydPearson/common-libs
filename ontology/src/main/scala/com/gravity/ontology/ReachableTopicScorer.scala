package com.gravity.ontology

import org.neo4j.kernel.Traversal
import org.neo4j.graphdb.Direction
import org.neo4j.graphdb.traversal.Evaluation
import com.gravity.ontology.vocab.URIType
import com.gravity.ontology.nodes.TopicRelationshipTypes
import com.gravity.ontology.TraversalFilters.NodeEval
import scala.collection.mutable
import scala.collection.JavaConversions._


/**
 * Created with IntelliJ IDEA.
 * User: apatel
 * Date: 6/28/13
 * Time: 3:37 PM
 * To change this template use File | Settings | File Templates.
 */

class ScoredTopicPrinter extends NodePrinter {

  override def shouldProcess(node: OntologyNode): Boolean = {
    val t1 = OntologyNodeScore.getScore(node, OntologyNodeScore.TopicL1)
    val t2 = OntologyNodeScore.getScore(node, OntologyNodeScore.TopicL2)
    val t3 = OntologyNodeScore.getScore(node, OntologyNodeScore.TopicL3)
    val t4 = OntologyNodeScore.getScore(node, OntologyNodeScore.TopicL4)
    (t1 + t2 + t3 + t4 > 0)
  }
}

class ReachableTopicScorer(maxDepth: Int = 4, dryRun: Boolean = false) extends NodeTypeValidator(Seq(URIType.WIKI_CATEGORY)) {
  var cnt = 0
  println("dryRun = " + dryRun)

  case class TopicClosenessResult(scoreMap: mutable.HashMap[Int, Int]) {
    def incrementDepth(depth: Int) {
      val curScore = scoreMap.getOrElse(depth, 0)
      scoreMap.put(depth, curScore + 1)
    }
  }

  override def shouldProcess(node: OntologyNode): Boolean = super.shouldProcess(node) && node.name != "No name"

  override def processNode(node: OntologyNode) {
    cnt += 1

    val topicClosenessScoreResult = searchForTopics(node, maxDepth)
    for ((depth, score) <- topicClosenessScoreResult.scoreMap) {
      if (dryRun) {
        println("[dry run] Set " + OntologyNodeScore.TopicPrefix + depth + "=" + score + " for " + node.uri)
      }
      else {
        if (score > 0) {
          OntologyNodeScore.setScore(node.node, score, OntologyNodeScore.TopicPrefix + depth)
          //          println("Set " + OntologyNodeScore.TopicPrefix + depth + "=" + score + " for " + node.uri)
        }
      }
    }

    // dump node
    //    print(cnt + ". " + node.uri + "   TopicScore(")
    //    (1 to maxDepth) foreach (d => {
    //      print(node.getProperty(OntologyNodeScore.TopicPrefix + d, 0) + ",")
    //    })
    //    println(")")

  }

  private def searchForTopics(startNode: OntologyNode, depth: Int): TopicClosenessResult = {
    val topicClosenessScoreResult = new TopicClosenessResult(new mutable.HashMap[Int, Int]())
    val trav = Traversal.description.breadthFirst()
      .evaluator(topicEvaluator(depth + 1, topicClosenessScoreResult))
      .relationships(TopicRelationshipTypes.CONCEPT_OF_TOPIC, Direction.INCOMING)
      .relationships(TopicRelationshipTypes.BROADER_CONCEPT, Direction.INCOMING)
      .relationships(TopicRelationshipTypes.INTEREST_OF, Direction.INCOMING)
      .relationships(TopicRelationshipTypes.REDIRECTS_TO, Direction.INCOMING)
      .relationships(TopicRelationshipTypes.DISAMBIGUATES, Direction.INCOMING)
      .relationships(OntologyNodeScore.GRAV_AUTO_BROADER_CONCEPT, Direction.INCOMING)
      .relationships(OntologyNodeScore.GRAV_AUTO_CONCEPT_OF_TOPIC, Direction.INCOMING)
      .traverse(startNode.node)

    trav foreach (n => {})
    topicClosenessScoreResult
  }

  private def topicEvaluator(stopDepth: Int, topicClosenessScoreResult: TopicClosenessResult) = NodeEval {
    path => {
      var evaluation = Evaluation.EXCLUDE_AND_CONTINUE
      if (path.length() >= stopDepth) {
        evaluation = Evaluation.EXCLUDE_AND_PRUNE
      }
      else {
        val ontEndNode = new OntologyNode(path.endNode())
        try {
          ontEndNode.uriType match {
            case URIType.TOPIC =>
              topicClosenessScoreResult.incrementDepth(path.length())
              evaluation = Evaluation.INCLUDE_AND_CONTINUE
            case _ =>
          }
        }
        catch {
          case _: Exception =>
        }
      }
      evaluation
    }
  }
}

object ReachableTopicScorerScript extends App {
  OntologyNodesLoader.loadNodes(new ReachableTopicScorer(dryRun = false))
}

object PrintScoredTopicNodes extends App {
  OntologyNodesLoader.loadNodes(new ScoredTopicPrinter())
}
