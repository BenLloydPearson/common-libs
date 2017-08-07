package com.gravity.interests.jobs.intelligence

import com.gravity.ontology._
import vocab.URIType

import scala.collection._
import scala.collection.JavaConversions._
import com.gravity.ontology.OntologyNode
import com.gravity.ontology.ConceptGraphResult
import org.neo4j.graphdb.{Direction, Path, Relationship}
import java.io.{BufferedReader, InputStreamReader}

import com.gravity.interests.graphs.graphing.{Grapher, ScoredTerm}
import com.gravity.utilities.Settings


/**
 * Created with IntelliJ IDEA.
 * User: apatel
 * Date: 9/4/12
 * Time: 6:02 PM
 * To change this template use File | Settings | File Templates.
 */

object BuildFreeTextGraph extends App {
  implicit val ogName = new OntologyGraphName(Settings.ONTOLOGY_DEFAULT_GRAPH_NAME)

  val in: BufferedReader = new BufferedReader(new InputStreamReader(java.lang.System.in))
  print(">")
  val freeTxt: String = in.readLine()
  val res: ConceptGraphResult = ConceptGraph.processFreeText(freeTxt)
  val sg: StoredGraph = StoredGraphHelper.buildGraph(res)
  sg.prettyPrint()
}

object StoredGraphCleaner_ConsoleApp extends App {
  val defaultUserId: Long = -3982116502516043204L

  var continue = true
  val in: BufferedReader = new BufferedReader(new InputStreamReader(java.lang.System.in))
  print(">")
  while (continue) {
    val ln = in.readLine()
    ln match {
      case "q" =>
        continue = false
        println("")
        println("exiting...")
      case str =>
        val userId =
          try {
            ln.toLong
          }
          catch {
            case _: Exception => defaultUserId
          }
        println("fetching userId: " + userId + " ...")
        //val uk = UserKey()



        println("")
        print(">")
    }
  }
}

object StoredGraphHelper_ConsoleApp extends App {
  implicit val ogName = new OntologyGraphName(Settings.ONTOLOGY_DEFAULT_GRAPH_NAME)

  //  val defaultUrl = "http://www.cnn.com/2012/07/26/politics/electoral-college-tie/index.html"
  //val defaultUrl = "http://www.cnn.com/2012/08/29/showbiz/tv/charlie-sheen-anger-management-renewal-ew/index.html?hpt=en_c1"
  val defaultUrl = "http://espn.go.com/los-angeles/story/_/id/9806144/coach-ed-orgeron-brings-fun-back-usc-trojans"

  var continue = true
  val in: BufferedReader = new BufferedReader(new InputStreamReader(java.lang.System.in))
  print(">")
  while (continue) {
    val ln = in.readLine()
    ln match {
      case "q" =>
        continue = false
        println("")
        println("exiting..."
        )
      case str =>
        val url = if (ln.startsWith("http")) ln else defaultUrl
        println("fetching: " + url + " ...")

        val result = ConceptGraph.processContent(url)
        val sg = StoredGraphHelper.buildGraph(result)
        sg.prettyPrintNodes(true)
        //        StoredGraphHelper.prettyPrint(sg)
        println("")
        print(">")
    }
  }
}

object StoredGraphHelper {

  implicit val ogName = new OntologyGraphName(Settings.ONTOLOGY_DEFAULT_GRAPH_NAME)

  def getNodeType(on: OntologyNode): com.gravity.interests.jobs.intelligence.NodeType = {
    try {
      if (on.uriType == URIType.TOPIC) com.gravity.interests.jobs.intelligence.NodeType.Topic
      else com.gravity.interests.jobs.intelligence.NodeType.Interest
    }

    catch {
      case _: Exception => com.gravity.interests.jobs.intelligence.NodeType.Interest
    }
  }

  def getLevel(nType: com.gravity.interests.jobs.intelligence.NodeType): Int = {
    if (nType == com.gravity.interests.jobs.intelligence.NodeType.Topic) 100
    else 3
  }

  def buildGraphAroundNode(startNodeUri: String, depth: Int = 2, direction: Direction = Direction.BOTH): StoredGraph = {

    def getEdgeType(r: Relationship): EdgeType = {
      // tbd
      EdgeType.BroaderThan
    }


    val paths = ReachabilityAlgo.traverseAroundNode(startNodeUri, depth, direction)
    val sgBuilder = StoredGraph.make

    paths.foreach(path => {

      val r = path.lastRelationship()
      if (r != null && r.getStartNode != null && r.getEndNode != null) {
        //        val startNode = new OntologyNode(path.startNode()) // (r.getStartNode)
        val endNode = new OntologyNode(path.endNode()) //(r.getEndNode)

        val endNodeType = getNodeType(endNode)
        val endNodeLevel = getLevel(endNodeType)
        val endNodeDistance = path.length()


        // add end node
        sgBuilder.addNode(endNode.uri, endNode.name, endNodeType, endNodeLevel, score = (1d / endNodeDistance.toDouble))

        //        val edgeType = getEdgeType(r)
        //      //add edge
        //        sgBuilder.relate(
        //          startNode.uri,
        //          endNode.uri,
        //          edgeType,
        //          path.length(),
        //          count = 1,
        //          score = 1.0)
      }


    })

    sgBuilder.build


  }

  def buildGraphAroundNodeById(startNodeId: Long, depth: Int = 2, direction: Direction = Direction.BOTH): StoredGraph = {

    def getEdgeType(r: Relationship): EdgeType = {
      // tbd
      EdgeType.BroaderThan
    }


    val paths = ReachabilityAlgo.traverseAroundNode(startNodeId, depth, direction)
    val sgBuilder = StoredGraph.make

    paths.foreach(path => {

      val r = path.lastRelationship()
      if (r != null && r.getStartNode != null && r.getEndNode != null) {
        //        val startNode = new OntologyNode(path.startNode()) // (r.getStartNode)
        val endNode = new OntologyNode(path.endNode()) //(r.getEndNode)

        val endNodeType = getNodeType(endNode)
        val endNodeLevel = getLevel(endNodeType)
        val endNodeDistance = path.length()


        // add end node
        sgBuilder.addNode(endNode.uri, endNode.name, endNodeType, endNodeLevel, score = (1d / endNodeDistance.toDouble))

        //        val edgeType = getEdgeType(r)
        //      //add edge
        //        sgBuilder.relate(
        //          startNode.uri,
        //          endNode.uri,
        //          edgeType,
        //          path.length(),
        //          count = 1,
        //          score = 1.0)
      }


    })

    sgBuilder.build


  }


  def prettyPrint(sg: StoredGraph) {

    println("Nodes: " + sg.nodes.size)
    print("Topics by Level: ")
    sg.nodes.groupBy(n => n.level).toList.sortBy(entry => -entry._1).foreach(entry => print("L" + entry._1 + "=" + entry._2.size + ", "))
    println("")
    sg.nodes.sortBy(n => n.score * -1) foreach (n => {
      println("\t" + n.name + ": type: " + n.nodeType.name + " : score: " + n.score + " : count: " + n.count + " : level: " + n.level)
    })

    println("")
    println("Edges: " + sg.edges.size)
    sg.edges.sortBy(e => e.score * -1) foreach (edge => {
      println("\t" + sg.nodeById(edge.start).name + " to " + sg.nodeById(edge.end).name + " : type: " + edge.relType.name + " count: " + edge.count + " : score: " + edge.score + " : distance: " + edge.distance)
    })
  }

  def buildGraphFromAllTopics(grapher: Grapher, depth: Int = 3, minFrequency: Int = 1)(implicit ogName: OntologyGraphName): StoredGraph = {
    val result = ConceptGraph.getRelatedConcepts(depth, grapher, useAllTopics = true)
    buildGraph(result, maxDepth = depth, minFrequency = minFrequency)
  }


  def build(grapher: Grapher,
            maxTopics: Int = 100,
            maxDepth: Int = 4,
            highLevel: Int = 10,
            lowLevel: Int = 15,
            otherLevel: Int = 30,
            extractCount: Int = 25)(implicit ogName: OntologyGraphName): StoredGraph = {
    val result = ConceptGraph.getRelatedConcepts(maxDepth, grapher)
    buildGraph(result, highLevel, lowLevel, otherLevel, extractCount, maxDepth)
  }

  def buildGraph(conceptGraphResult: ConceptGraphResult,
                 highLevel: Int = 10,
                 lowLevel: Int = 15,
                 otherLevel: Int = 30,
                 extractCount: Int = 25,
                 maxDepth: Int = 4,
                 minFrequency: Int = 2)(implicit ogName: OntologyGraphName): StoredGraph = {

    val sgBuilder = StoredGraph.make
    val conceptStatList = conceptGraphResult.getFinalResults(highLevel, lowLevel, otherLevel, extractCount, minFrequency)


    val topicCount = new mutable.HashMap[OntologyNode, Int]()
    val conceptCount = new mutable.HashMap[OntologyNode, Int]()
    for (cs <- conceptStatList) {

      // track concept counts
      conceptCount.put(cs.conceptNode, cs.frequency)

      // add concept nodes to stored graph
      sgBuilder.addNode(
        cs.conceptNode.uri,
        cs.conceptNode.name,
        NodeType.Interest,
        calculateLevel(cs),
        count = cs.frequency,
        score = cs.frequency) // TBD: score = count

      // extract topics from paths
      cs.pathSet foreach (path => {
        val on = new OntologyNode(path.startNode())
        if (on.uriType == URIType.TOPIC) {
          // accumulate topic frequency
          topicCount.put(on, topicCount.getOrElse(on, 0) + 1)
        }
      })
    }

    // add topic nodes to stored graph
    topicCount foreach (entry => {
      val on = entry._1
      val count = (for {st <- conceptGraphResult.grapher.scoredTopics.find(_.topic.uri == on.uri)} yield st.frequency).getOrElse(0)
      val score = (for {s <- conceptGraphResult.topicScoreMap.get(on.uri)} yield s).getOrElse(0D)
      val level = 100


      sgBuilder.addNode(on.uri, on.name, NodeType.Topic, level, count, score)
    })

    // add relationships from topics to concepts
    for (cs <- conceptStatList) {

      cs.pathSet foreach (path => {
        val startTopicNode = path.startNode()
        val endConceptNode = path.endNode()

        sgBuilder.relate(
          startTopicNode.uri,
          endConceptNode.uri,
          EdgeType.InterestOf,
          path.length(),
          count = 1,
          score = getEdgeScore(path, conceptGraphResult, 1.0))

        if (path.nodes().size > 2) {
          // there are intermediate concept nodes - relate them if necessary
          relateConcepts(sgBuilder, path, conceptGraphResult, conceptCount)
        }
      })
    }

    // add terms nodes to stored graph
    addTermsToGraph(conceptGraphResult.grapher.phraseVectorKea, sgBuilder, 0.0D)
    addTermsToGraph(conceptGraphResult.grapher.termVector1, sgBuilder, 1.5D)
    //addTermsToGraph(conceptGraphResult.grapher.termVector2, sgBuilder)
    //addTermsToGraph(conceptGraphResult.grapher.termVector3, sgBuilder)
    addTermsToGraph(conceptGraphResult.grapher.termVectorG, sgBuilder, 0.00)

    //println("Total Concepts: " + conceptStatList.size)
    //println("Total Topics: " + topicCount.size)

    // reduce duplicate edges
    sgBuilder.build + StoredGraph.EmptyGraph
  }

  private def addTermsToGraph(terms: Seq[ScoredTerm], sgBuilder: StoredGraphBuilder, minScore: Double) {
    if (terms != null) {
      for (term <- terms if term.score >= minScore) {
        sgBuilder.addNode(term.term, term.term, NodeType.Term, 100, 1, term.score)
      }
    }
  }

  private def relateConcepts(sgBuilder: StoredGraphBuilder, path: Path, conceptGraphResult: ConceptGraphResult,
                             conceptCountMap: mutable.HashMap[OntologyNode, Int]) {
    var index = 0
    var startNode: OntologyNode = null
    var endNode: OntologyNode = null
    var distance = 0
    var tempON: OntologyNode = null

    //println("processing path: " + ReachabilityAlgo.getReadablePath(path))

    path.nodes foreach (node => {
      if (index > 0) {

        tempON = new OntologyNode(node)

        if (conceptCountMap.contains(tempON)) {
          if (startNode == null) {
            startNode = tempON
            distance = 0
          }
          else {
            endNode = tempON
            sgBuilder.relate(
              startNode.uri,
              endNode.uri,
              EdgeType.BroaderThan,
              distance,
              count = 1,
              score = getEdgeScore(startNode, endNode, conceptGraphResult))

            //println("\tcreating edge from " + startNode.uri + " to " + endNode.uri)

            startNode = endNode
            endNode = null
            distance = 0
          }

        }
        distance += 1
      }
      index += 1
    })
  }

  private def getEdgeScore(start: OntologyNode, end: OntologyNode, conceptGraphResult: ConceptGraphResult): Double = {
    getNodeScore(start, conceptGraphResult) + getNodeScore(end, conceptGraphResult)
  }

  private def getEdgeScore(path: Path, conceptGraphResult: ConceptGraphResult, bias: Double = 0.0): Double = {
    val start = new OntologyNode(path.startNode())
    val end = new OntologyNode(path.endNode())
    getEdgeScore(start, end, conceptGraphResult) + bias
  }

  private def getNodeScore(node: OntologyNode, conceptGraphResult: ConceptGraphResult): Double = {
    node.uriType match {
      case URIType.TOPIC => conceptGraphResult.topicScoreMap.getOrElse(node.uri, 0.0)
      case URIType.WIKI_CATEGORY => conceptGraphResult.getConceptStats(node).topicScores.avg
      case _ => 0.0
    }
  }

  private def calculateLevel(cs: ConceptStats): Int = {
    cs.cType match {
      case "H" => 1
      case "L" => 2
      case _ => 3
    }
  }
}
