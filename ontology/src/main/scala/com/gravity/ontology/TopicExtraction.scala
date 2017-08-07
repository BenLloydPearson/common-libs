package com.gravity.ontology

import java.io.{BufferedReader, InputStreamReader}

import org.neo4j.graphdb.Direction
import com.gravity.ontology.nodes.TopicRelationshipTypes
import com.gravity.utilities.Settings

import scala.collection._
import scala.collection.JavaConversions._
import scala.Some

/**
 * Created with IntelliJ IDEA.
 * User: apatel
 * Date: 11/15/12
 * Time: 3:31 PM
 * To change this template use File | Settings | File Templates.
 */

object TopicExtraction_ConsoleApp extends App {
  val defaultUrl = "http://www.cnn.com/2012/07/26/politics/electoral-college-tie/index.html"

  implicit val ogName = new OntologyGraphName(Settings.ONTOLOGY_DEFAULT_GRAPH_NAME)

  var continue = true
  val in = new BufferedReader(new InputStreamReader(java.lang.System.in))
  print(">")
  while(continue){
    val ln = in.readLine()
    ln match {
      case "q" =>
        continue = false
        println("")
        println("exiting...")
      case str =>
        val url = if (ln.startsWith("http")) ln else defaultUrl
        val result = ConceptGraph.processContent(url)
        println("fetching: " + url + "...")

        val r3 = TopicExtraction.extractUsingReductiveAlgo(result.grapher.allTopics, Int.MaxValue, 4)

        println("Available Topics: " + result.grapher.allTopics.size )
        r3.print()
        print(">")
    }
  }
}

case class TopicExtractionResult(keep: Set[String], discard: Set[String]) {
  def print() {
    var i = 0
    println("Kept Nodes: " + keep.size)
    keep.foreach(uri=> {i+=1;println("   " + uri)})

    i=0
    println("Discarded Nodes: " + discard.size)
    discard.foreach(uri=> {i+=1;println("   " + uri)})
  }
}

object TopicExtraction {

  def extractUsingReductiveAlgo(allTopics: Set[String], maxNodesToVisit: Int = Int.MaxValue, maxHops: Int = 4, minMatch: Option[Int] = None)(implicit ogName: OntologyGraphName) : TopicExtractionResult = {

    val matchCount =
      minMatch match {
        case Some(x) => x
        case None => if (allTopics.size >100)  (allTopics.size/15) else (allTopics.size/20).max(1).min(5)
      }
    //println("match count = " + matchCount)

    val reachableConceptCountForConcept = new mutable.HashMap[String, Int]()
    val reachableConceptsForTopic = new mutable.HashMap[String, Set[String]]()

    for (topic <- allTopics)
    {
      val reachableConcepts = getAllReachableConcepts(topic, maxNodesToVisit, maxHops)
      reachableConceptsForTopic += (topic -> reachableConcepts)

      for (concept <- reachableConcepts){
        reachableConceptCountForConcept += (concept -> (reachableConceptCountForConcept.getOrElse(concept, 0) + 1))
      }
    }

    val keep = new mutable.HashSet[String]()
    val discard = new mutable.HashSet[String]()

    for (topic <- allTopics){

      val concepts = reachableConceptsForTopic.get(topic).get.toList
      var found = false
      var i=0

      while (!found && i<concepts.size) {
        val concept = concepts.get(i)
        val cnt = reachableConceptCountForConcept.getOrElse(concept, 0)
        found = (cnt-1 >= matchCount )
        i+=1
      }

      if (found) {
        keep += topic
      }
      else {
        discard += topic
      }
    }

    new TopicExtractionResult(keep, discard)
  }

  private def getAllReachableConcepts(uri: String, maxNodesToVisit: Int, maxHops: Int)(implicit ogName: OntologyGraphName) : Set[String] = {

//    println("og: " + ogName)
//    println("uri: " + uri)
    ConceptGraph.getNode(uri) match {
      case Some(startNode) =>{
        val dirRelCol= List(new RelationshipDirection(TopicRelationshipTypes.CONCEPT_OF_TOPIC, Direction.OUTGOING),
          new RelationshipDirection(TopicRelationshipTypes.BROADER_CONCEPT, Direction.OUTGOING))

        val res = ShortestHopAlgo.traverse(
          startNode,
          dirRelCol,
          maxHops,
          maxNodesToVisit,
          destinationNodes=List.empty[OntologyNode],
          collectAllPossiblePaths = false,
          discardNonDestinationNodes = false)

        res.nonDestinationNodes.map(n=>n.uri).filter(ConceptGraph.passBlackListConceptFilter(_))
      }
      case None =>
        println("Error fetching node for uri: " + uri)
        Set.empty[String]

    }

  }

}
