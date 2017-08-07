package com.gravity.ontology

import java.util.Calendar
import java.io.InputStreamReader
import java.io.BufferedReader
import java.io.FileWriter

import scala.io.Source
import scala.collection._
import scala.Some
import org.joda.time.DateTime
import org.neo4j.kernel.Traversal
import org.neo4j.tooling.GlobalGraphOperations
import org.neo4j.graphdb._
import org.neo4j.graphdb.traversal.Evaluation
import com.gravity.ontology.vocab.URIType
import com.gravity.ontology.nodes.TopicRelationshipTypes
import com.gravity.ontology.TraversalFilters.NodeEval
import com.gravity.interests.graphs.graphing._
import com.gravity.interests.graphs.graphing.ContentToGraph
import com.gravity.interests.graphs.graphing.ScoredTopic
import com.gravity.utilities.web.ContentUtils
import com.gravity.ontology.Implicits.dummyPhrase
import com.gravity.ontology.importingV2.NodeProperties

import scala.collection.JavaConversions._
import com.gravity.ontology.vocab.NS
import com.gravity.utilities.{MurmurHash, Settings}


/**
 * Created with IntelliJ IDEA.
 * User: apatel
 * Date: 7/27/12
 * Time: 1:14 PM
 * To change this template use File | Settings | File Templates.
 */


object Reachability_ConsoleApp extends App {
  val defaultUrl = "http://www.cnn.com/2012/07/26/politics/electoral-college-tie/index.html"
  //val defaultUrl = "http://www.cnn.com/2012/08/29/showbiz/tv/charlie-sheen-anger-management-renewal-ew/index.html?hpt=en_c1"

  var continue = true
  val in = new BufferedReader(new InputStreamReader(java.lang.System.in))
  print(">")
  while (continue) {
    val ln = in.readLine()
    ln match {
      case "q" =>
        continue = false
        println("")
        println("exiting...")
      case str =>
        val url = if (ln.startsWith("http")) ln else defaultUrl
        println("fetching: " + url + "...")
        ConceptReport.extractConcepts(url)
        print(">")
    }
  }
}

object ConceptReport {

  implicit val ogName = new OntologyGraphName(Settings.ONTOLOGY_DEFAULT_GRAPH_NAME)

  // TBD: resolve bug in final phase of leveling
  def performLevelGrouping(concepts: List[OntologyNode], groupDepth: Int, levels: Int) {
    val groupLevels = new mutable.ListBuffer[mutable.HashMap[String, mutable.HashSet[OntologyNode]]]()
    groupLevels.append(performGrouping(concepts, groupDepth))
    (1 to levels - 1).foreach(i => {
      groupLevels.append(
        performGrouping(
          groupLevels.get(i - 1).filter(entry => entry._1 != "Other").map(entry => ConceptGraph.getNode(entry._1).get).toList,
          groupDepth
        ))
    })

    val flatMap = new mutable.HashMap[String, mutable.HashSet[OntologyNode]]()
    groupLevels foreach (level => {
      level.foreach(groupMap => {

        val groupNode = groupMap._1
        val groupChildren = groupMap._2

        val existingChildren = flatMap.getOrElse(groupNode, new mutable.HashSet[OntologyNode]())
        groupChildren foreach (child => {
          existingChildren.add(child)
        })

        flatMap.put(groupNode, existingChildren)
      })
    })

    println("")
    println("Final Frequent Concepts in Path:")
    groupLevels.last foreach (entry => {
      println(entry._1)
      entry._2 foreach (child => {
        printChild(child.uri, flatMap)
      })
    })

    // TBD fix other printing
    //
    //    flatMap foreach (entry=>
    //    {
    //      println(entry._1)
    //      entry._2 foreach (child=>{
    //        printChild(child.uri, flatMap)
    //      })
    //    })

  }

  private def printChild(nodeUri: String, flatMap: mutable.HashMap[String, mutable.HashSet[OntologyNode]], level: Int = 0) {
    def tab(levelNum: Int) {
      (1 to levelNum) foreach (ln => {
        print("\t")
      })
    }

    tab(level)
    println(nodeUri)
    if (flatMap.contains(nodeUri)) {
      flatMap.get(nodeUri).get foreach (childNode => {
        printChild(childNode.uri, flatMap, level + 1)
      })
    }
    flatMap.remove(nodeUri)
  }

  def performGrouping(concepts: List[OntologyNode], groupDepth: Int):
  mutable.HashMap[String, mutable.HashSet[OntologyNode]] = {

    val group = ConceptGrouper.group(concepts, groupDepth)
    var i = 0
    println("Groups: " + group.size)
    group.foreach(entry => {
      val parent = entry._1
      val set = entry._2

      if (set.size > 0) {
        i += 1
        println(i + ". " + parent.name)
        var j = 0
        set.foreach(child => {
          j += 1;
          println("   " + j + ". " + child.name)
        })
      }
    })


    val groupSummary = ConceptGrouper.groupSummary(group)
    println("")
    println("Summary groups: " + groupSummary.size + " from " + group.size)
    i = 0
    groupSummary.toList.sortBy(entry => entry._2.size * -1) foreach (entry => {
      i += 1
      println(i + ") " + entry._1)
      entry._2 foreach (c => {
        println("\t" + c.uri)
      })
    })
    groupSummary
  }

  def printTopics(result: ConceptGraphResult) {

    val allTopics = result.grapher.allTopics
    val usedTopics = result.grapher.bestTopics

    val unusedTopics = allTopics -- usedTopics

    println("Topics Available: " + allTopics.size)
    allTopics.foreach(t => println("   " + t))

    println("Topics Considered: " + usedTopics.size)
    usedTopics.foreach(t => println("   " + t))

    println("Topics Not Considered: " + unusedTopics.size)
    unusedTopics.foreach(t => println("   " + t))

  }

  def extractConcepts(url: String,
                      depth: Int = 4,
                      groupDepth: Int = 3,
                      highLevel: Int = 15,
                      lowLevel: Int = 15,
                      otherLevel: Int = 30,
                      extractCount: Int = 25,
                      maxTopics: Int = 100) {
    val result = ConceptGraph.processContent(url, depth, maxTopics)
    val finalConceptStatList = result.getFinalResults(highLevel, lowLevel, otherLevel, extractCount)


    println("")
    println("Topics Available: " + result.grapher.allTopics.size)
    println("Topics Considered: " + result.grapher.bestTopics.size)
    println("Total Concepts Visited: " + result.conceptsVisited)

    println("Selected Concepts: " + finalConceptStatList.count(cs => !cs.cType.contains("E")))
    println("   High, Low, Other: "
      + finalConceptStatList.count(cs => cs.cType == "H") + ", "
      + finalConceptStatList.count(cs => cs.cType == "L") + ", "
      + finalConceptStatList.count(cs => cs.cType == "O"))

    println("Extracted Concepts: " + finalConceptStatList.count(cs => cs.cType.contains("E")))
    println("   High, Low, Other: "
      + finalConceptStatList.count(cs => cs.cType == "EH") + ", "
      + finalConceptStatList.count(cs => cs.cType == "EL") + ", "
      + finalConceptStatList.count(cs => cs.cType == "EO"))

    println("Total Final Concepts: " + finalConceptStatList.size)

    if (finalConceptStatList.size > 0) {
      println("")
      finalConceptStatList.sortBy(cs => cs.frequency * -1) foreach (stats => {
        println(stats.conceptNode.uri + ": " + stats.frequency + " Node Type: " + stats.cType + " avg: " + stats.topicScores.avg + " Topic Level Scores: (" + stats.topicLevelScore.L1 + ", " + stats.topicLevelScore.L2 + ", " + stats.topicLevelScore.L3 + ", " + stats.topicLevelScore.L4 + ") Concept Level Scores: (" + stats.conceptLevelScore.L1 + ", " + stats.conceptLevelScore.L2 + ", " + stats.conceptLevelScore.L3 + ", " + stats.conceptLevelScore.L4 + ")")
      })

      val concepts = finalConceptStatList.map(cs => cs.conceptNode).toList
      performGrouping(concepts, groupDepth)
    }
    else {
      println("No final concepts")
    }

    //    printTopics(result)

    //val conceptStatList = result.getSelectedResults(highLevel, lowLevel, otherLevel)

    //
    //    if (conceptStatList.size > 0){
    //      println("Total Reduced Concepts: " + conceptStatList.size)
    //      conceptStatList.sortBy(cs=>cs.topicScores.avg * -1)  foreach (stats  => {
    //        println(stats.conceptNode.uri + ": " + stats.frequency + " avg: " + stats.topicScores.avg  + " sum: " + stats.topicScores.sum + " max: " + stats.topicScores.max)
    //      })
    //
    //      val concepts = conceptStatList.map(cs => cs.conceptNode).toList
    //      performGrouping(concepts, groupDepth)

    //
    //      val conceptCountMap = new mutable.HashMap[OntologyNode, Int]()
    //      for (cs <- conceptStatList;
    //           p <- cs.pathSet;
    //           n <- p.nodes if n.uriType == URIType.WIKI_CATEGORY) {
    //        conceptCountMap.put(n, conceptCountMap.getOrElse(n, 0) + 1)
    //      }

    //
    //      val filteredConceptCountMap = conceptCountMap.filter(entry => entry._2 > 1)
    //      println("")
    //      println("Frequent Concepts in Paths: " + filteredConceptCountMap.size)
    //      var i=0
    //      filteredConceptCountMap.toSeq.sortBy(entry=>entry._2 * -1)  foreach (entry=>{
    //        //println(entry._1.uri + ": " + entry._2)
    //        val node = entry._1
    //        val freq = entry._2
    //        val stats = result.getConceptStats(node)
    //
    //        println(node.uri + ": " + freq + " Node Type: " + stats.cType + " avg: " + stats.topicScores.avg  +  " Topic Level Scores: (" + stats.topicLevelScore.L1 + ", " + stats.topicLevelScore.L2 + ", " + stats.topicLevelScore.L3 + ", " + stats.topicLevelScore.L4 +") Concept Level Scores: (" + stats.conceptLevelScore.L1 + ", " + stats.conceptLevelScore.L2 + ", " + stats.conceptLevelScore.L3 + ", " + stats.conceptLevelScore.L4 +")" )
    //
    //      })

    //LevelGrouping(conceptCountMap.filter(entry=>entry._2 > 1).keySet.toList, groupDepth, 3)


    //
    //      val llc = result.getSortedLowLevelConcepts()
    //      println("")
    //      println("Low Level Concepts: " + llc.size)
    //      i=0
    //      llc foreach (cs=>{
    //        i+=1
    //        println(i + ") " + cs.conceptNode.uri + " freq: " + cs.frequency)
    //      })

    //      val allTopics = result.grapher.topScoredTopics(Int.MaxValue)
    //      println("")
    //      println("Topics: " +  allTopics.size )
    //      i =0
    //      allTopics foreach (topic=>{
    //        i+=1
    //        println(i + ") " + topic.topic.uri + ": " + topic.score)
    //      })


    //
    //      val unusedConcepts = result.getUnusedConcepts()
    //      println("")
    //      println("Unused Concepts: " +  unusedConcepts.size )
    //      i =0
    //      unusedConcepts foreach (c=>{
    //        i+=1
    //        println(i + ") " + c.conceptNode.uri + " freq : " + c.frequency)
    //      })
    //
    //    }
    //    else {
    //      println("No Concepts found")
    //    }


  }

}

object ConceptGrouper {

  def topConcepts(conceptNodesStatList: List[ConceptStats], groupDepth: Int = 3): List[(String, List[OntologyNode])] = {
    val conceptNodesList = conceptNodesStatList.map(entry => entry.conceptNode).toList
    val summary = groupSummary(group(conceptNodesList, groupDepth))
    val ll = summary.map(entry => (entry._1, entry._2.toList)).toList
    ll.sortBy(entry => entry._2.size * -1)
  }

  def groupSummary(groupMap: mutable.HashMap[OntologyNode, mutable.HashSet[OntologyNode]]):
  mutable.HashMap[String, mutable.HashSet[OntologyNode]] = {

    val pool = new mutable.HashSet() ++= groupMap.keys
    val groupMapRes = new mutable.HashMap[String, mutable.HashSet[OntologyNode]]()

    groupMap.toList.sortBy(entry => entry._2.size * -1) foreach (entry => {

      val parent = entry._1
      val children = entry._2

      if (pool.size > 0 && entry._2.size > 0) {

        children foreach (nodeToRemove => {
          var nodeRemoved = false
          if (pool.remove(nodeToRemove)) {
            nodeRemoved = true
            val s = groupMapRes.getOrElse(parent.uri, new mutable.HashSet[OntologyNode]())
            s.add(nodeToRemove)
            groupMapRes.put(parent.uri, s)
          }

          if (nodeRemoved) {
            pool.remove(parent)
            //groupMapRes.put(parent.uri, children)
          }
        })
      }
    })

    pool foreach (n => {
      val children = groupMapRes.getOrElse("Other", new mutable.HashSet[OntologyNode]())
      children.add(n)
      groupMapRes.put("Other", children)
    })
    groupMapRes
  }


  def group(conceptNodesList: List[OntologyNode], groupDepth: Int = 3):
  mutable.HashMap[OntologyNode, mutable.HashSet[OntologyNode]] = {

    val conceptNodes = new mutable.HashSet() ++= conceptNodesList
    val groupMap = new mutable.HashMap[OntologyNode, mutable.HashSet[OntologyNode]]()
    conceptNodes foreach (node => {
      val lowerConcepts = collectLowerConcepts(node, groupDepth, conceptNodes)
      val filteredConcepts = lowerConcepts.filter(on => ConceptGraph.passBlackListConceptFilter(on.uri))
      groupMap.put(node, filteredConcepts)
    })
    groupMap
  }

  def collectLowerConcepts(startNode: OntologyNode,
                           depth: Int,
                           destinationNodes: mutable.HashSet[OntologyNode]): mutable.HashSet[OntologyNode] = {
    val reachableConceptNodes = new mutable.HashSet[OntologyNode]()

    val trav = Traversal.description.breadthFirst()
      .evaluator(reachableEvaluator(depth + 1, destinationNodes, reachableConceptNodes))
      .relationships(TopicRelationshipTypes.BROADER_CONCEPT, Direction.INCOMING)
      .relationships(ConceptNodes.GRAV_AUTO_BROADER_CONCEPT, Direction.INCOMING)
      .traverse(startNode.node)

    trav foreach (n => {})
    reachableConceptNodes

  }


  private def reachableEvaluator(stopDepth: Int,
                                 destinationNodes: mutable.HashSet[OntologyNode],
                                 result: mutable.HashSet[OntologyNode]) = NodeEval {
    path => {
      var evaluation = Evaluation.EXCLUDE_AND_CONTINUE

      if (path.length() >= stopDepth) {
        evaluation = Evaluation.EXCLUDE_AND_PRUNE
      }
      else if (path.length() > 0) {
        val ontEndNode = new OntologyNode(path.endNode())
        try {
          ontEndNode.uriType match {
            case URIType.WIKI_CATEGORY =>
              if (destinationNodes.contains(ontEndNode)) {
                evaluation = Evaluation.INCLUDE_AND_CONTINUE
                result.add(ontEndNode)
              }
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


case class TopicScore(avg: Double, sum: Double, max: Double)

case class ConceptLevelScore(L1: Int, L2: Int, L3: Int, L4: Int)

case class TopicLevelScore(L1: Int, L2: Int, L3: Int, L4: Int)


case class ConceptStats(conceptNode: OntologyNode,
                        var frequency: Int,
                        topicUriSet: mutable.HashSet[String],
                        pathSet: mutable.HashSet[Path]) {
  def this(cnode: OntologyNode) = this(cnode, 0, new mutable.HashSet[String](), new mutable.HashSet[Path]())

  var topicScores = new TopicScore(0.0, 0.0, 0.0)
  var conceptLevelScore = new ConceptLevelScore(0, 0, 0, 0)
  var topicLevelScore = new TopicLevelScore(0, 0, 0, 0)
  var cType = "O"
  lazy val pgRank = OntologyNodeScore.getScoreDouble(conceptNode, NodeProperties.PageRank)

}

case class ConceptGraphResult(grapher: Grapher) {

  private val highLevelConceptMap = new mutable.HashMap[OntologyNode, ConceptStats]()
  private val lowLevelConceptMap = new mutable.HashMap[OntologyNode, ConceptStats]()
  private val unusedConceptMap = new mutable.HashMap[OntologyNode, ConceptStats]()
  var conceptsVisited = 0
  val topicScoreMap = getTopicScore(grapher.scoredTopics)

  def getConceptStats(conceptNode: OntologyNode): ConceptStats = {
    highLevelConceptMap.getOrElse(conceptNode,
      lowLevelConceptMap.getOrElse(conceptNode,
        unusedConceptMap.getOrElse(conceptNode, new ConceptStats(conceptNode))))
  }

  private def getTopicScores(conceptNode: OntologyNode): TopicScore = {
    val cStat = getConceptStats(conceptNode)
    var maxVal = 0.0
    var sum = 0.0
    cStat.topicUriSet foreach {
      topicUri => {
        val score = topicScoreMap.getOrElse(topicUri, 0.0)
        maxVal = scala.math.max(maxVal, score)
        sum += score
      }
    }

    new TopicScore(sum / cStat.topicUriSet.size, sum, maxVal)
  }

  def calculateScores(implicit ogName: OntologyGraphName) {
    highLevelConceptMap.toList ::: lowLevelConceptMap.toList ::: unusedConceptMap.toList foreach (entry => {
      val conceptNode = entry._1
      val conceptStats = entry._2

      conceptStats.topicScores = this.getTopicScores(conceptNode)

      if (ConceptGraph.isNewOntology(ogName)) {
        conceptStats.conceptLevelScore = new ConceptLevelScore(
          ConceptNodes.getScore(conceptNode, NodeProperties.ConceptL1),
          ConceptNodes.getScore(conceptNode, NodeProperties.ConceptL2Sum),
          ConceptNodes.getScore(conceptNode, NodeProperties.ConceptL3Sum),
          ConceptNodes.getScore(conceptNode, NodeProperties.ConceptL4Sum)
        )
        conceptStats.topicLevelScore = new TopicLevelScore(
          ConceptNodes.getScore(conceptNode, NodeProperties.TopicL1),
          ConceptNodes.getScore(conceptNode, NodeProperties.TopicL2Sum),
          ConceptNodes.getScore(conceptNode, NodeProperties.TopicL3Sum),
          ConceptNodes.getScore(conceptNode, NodeProperties.TopicL4Sum)
        )
      }
      else {
        conceptStats.conceptLevelScore = new ConceptLevelScore(
          ConceptNodes.getScore(conceptNode, ConceptHierarchy.PARENT_ATTRIBUTE_SCORE + 1),
          ConceptNodes.getScore(conceptNode, ConceptHierarchy.PARENT_ATTRIBUTE_SCORE + 2),
          ConceptNodes.getScore(conceptNode, ConceptHierarchy.PARENT_ATTRIBUTE_SCORE + 3),
          ConceptNodes.getScore(conceptNode, ConceptHierarchy.PARENT_ATTRIBUTE_SCORE + 4)
        )
        conceptStats.topicLevelScore = new TopicLevelScore(
          ConceptNodes.getScore(conceptNode, TopicClosenessScore.TOPIC_CLOSENESS_SCORE + 1),
          ConceptNodes.getScore(conceptNode, TopicClosenessScore.TOPIC_CLOSENESS_SCORE + 2),
          ConceptNodes.getScore(conceptNode, TopicClosenessScore.TOPIC_CLOSENESS_SCORE + 3),
          ConceptNodes.getScore(conceptNode, TopicClosenessScore.TOPIC_CLOSENESS_SCORE + 4)
        )
      }

    })
  }

  def addHighLevelConceptNode(concept: OntologyNode, topicUri: String, path: Path) {
    addToMap(highLevelConceptMap, concept, topicUri, "H", path)
  }

  def addLowLevelConceptNode(concept: OntologyNode, topicUri: String, path: Path) {
    addToMap(lowLevelConceptMap, concept, topicUri, "L", path)
  }

  def addUnsedConceptNode(concept: OntologyNode, topicUri: String, path: Path) {
    addToMap(unusedConceptMap, concept, topicUri, "O", path)
  }

  def getSortedHighlLevelResults(): List[ConceptStats] = {
    highLevelConceptMap.values.toList.sortBy(_.frequency * -1)
  }

  def getSortedLowLevelConcepts(): List[ConceptStats] = {
    lowLevelConceptMap.values.toList.sortBy(_.frequency * -1)
  }

  def getUnusedConcepts(): List[ConceptStats] = {
    unusedConceptMap.values.toList.sortBy(_.frequency * -1)
  }

  def getFinalResults(highLevel: Int, lowLevel: Int, otherLevel: Int, extractCount: Int, minFrequency: Int = 2)(implicit ogName: OntologyGraphName): List[ConceptStats] = {

    val useNewOntologyThresholds = ConceptGraph.isNewOntology(ogName)
    val conceptStatList =
      if (useNewOntologyThresholds){
        getSelectedResults_New(highLevel, lowLevel, otherLevel, minFrequency)
      }
      else {
        getSelectedResults(highLevel, lowLevel, otherLevel, minFrequency)
      }
    //    println("Selected Concepts: " + conceptStatList.size)
    //    conceptStatList.sortBy(cs => cs.frequency * -1).foreach(cs=> {
    //      println(cs.conceptNode.uri + " " + cs.frequency + " " + cs.topicScores.avg)
    //    })

    val selectedConceptSet = new mutable.HashSet[OntologyNode]() ++ conceptStatList.map(cs => cs.conceptNode)
    val conceptCountMap = new mutable.HashMap[OntologyNode, Int]()
    for (cs <- conceptStatList;
         p <- cs.pathSet;
         n <- p.nodes
         if n.uriType == URIType.WIKI_CATEGORY && n != p.startNode() && n != p.endNode() && !selectedConceptSet.contains(n)) {
      conceptCountMap.put(n, conceptCountMap.getOrElse(n, 0) + 1)
    }

    val filteredConceptCountMap = {
      if (useNewOntologyThresholds) {
        // New thresholds for New Ontology
        conceptCountMap.filter(entry => entry._2 > 1 && getConceptStats(entry._1).topicLevelScore.L1 > 0 /* && getConceptStats(entry._1).topicLevelScore.L3 <= 6000*/)
      }
      else {
        // Old thresholds for Old Ontology
        conceptCountMap.filter(entry => entry._2 > 1 && getConceptStats(entry._1).topicLevelScore.L4 <= 25000)
      }
    }

    val extractedConceptsAll = filteredConceptCountMap.map(entry => {
      val cs = getConceptStats(entry._1)
      cs.cType = "E" + cs.cType
      //cs.frequency = entry._2
      cs
    }).toList
      .filter(cs => cs.frequency > 1 && ConceptGraph.passBlackListConceptFilter(cs.conceptNode.uri))
      .sortBy(cs => cs.frequency * -1)

    val extractedConcepts = extractedConceptsAll.take(extractCount)

    //    println("")
    //    println("Extracted additional " + (extractedConcepts.size) + " concepts from available " + extractedConceptsAll.size)
    //    extractedConcepts.sortBy(cs => cs.frequency * -1).foreach(cs=> {
    //        println(cs.conceptNode.uri + " " + cs.frequency + " " + cs.topicScores.avg)
    //    })

    conceptStatList ++ extractedConcepts
  }

  private def getSelectedResults_New(highLevel: Int, lowLevel: Int, otherLevel: Int, minFrequency: Int): List[ConceptStats] = {
    val highLevelResults = getSortedHighlLevelResults()
      .filter(stat => stat.topicLevelScore.L1 > 0 && stat.frequency >= minFrequency && stat.conceptLevelScore.L3 >0) // && stat.topicLevelScore.L4 <= 100000)
      .take(highLevel)

    val lowLevelResults = getSortedLowLevelConcepts()
      .filter(stat => stat.topicLevelScore.L1 > 0 && stat.frequency >= minFrequency)
      .take(lowLevel)

    if (otherLevel > 0) {
      val unusedButGoodConcepts = getUnusedConcepts()
        .filter(stat =>
          stat.pgRank < 10.0d && stat.topicLevelScore.L1 > 0 &&
            (
              (stat.frequency >= minFrequency + 1 )// keep concepts from very frequent topics
              //|| ( stat.topicScores.avg > 0.02 && stat.topicLevelScore.L2 < 150) // keep concepts from really good topics
            )
        )
        .sortBy(stat => stat.frequency * -1)
        .take(otherLevel)

      highLevelResults ++ lowLevelResults ++ unusedButGoodConcepts
    }
    else {
      highLevelResults ++ lowLevelResults
    }
  }

  private def getSelectedResults(highLevel: Int, lowLevel: Int, otherLevel: Int, minFrequency: Int): List[ConceptStats] = {
    val highLevelResults = getSortedHighlLevelResults()
      .filter(stat => stat.frequency >= minFrequency && stat.conceptLevelScore.L4 <= 200) // && stat.topicLevelScore.L4 <= 100000)
      .take(highLevel)

    val lowLevelResults = getSortedLowLevelConcepts()
      .filter(stat => stat.frequency >= minFrequency)
      .take(lowLevel)

    if (otherLevel > 0) {
      val unusedButGoodConcepts = getUnusedConcepts()
        .filter(stat =>
          (stat.frequency >= minFrequency + 1 &&
            stat.topicLevelScore.L1 + stat.topicLevelScore.L2 + stat.topicLevelScore.L3 + stat.topicLevelScore.L4 > 0 &&
            stat.topicLevelScore.L4 <= 1000)
            ||
            (stat.topicScores.avg > 0.02 && stat.topicLevelScore.L2 < 150)) // keep concepts from really good topics
        .sortBy(stat => stat.frequency * -1)
        .take(otherLevel)

      highLevelResults ++ lowLevelResults ++ unusedButGoodConcepts
    }
    else {
      highLevelResults ++ lowLevelResults
    }
  }

  private def getTopicScore(ts: Iterable[ScoredTopic]): mutable.HashMap[String, Double] = {
    val res = new mutable.HashMap[String, Double]()
    ts foreach (entry => {
      res.put(entry.topic.uri, entry.score)
    })
    res
  }


  private def addToMap(map: mutable.HashMap[OntologyNode, ConceptStats],
                       concept: OntologyNode,
                       topicUri: String,
                       cType: String,
                       path: Path) {
    val stat = map.getOrElse(concept, new ConceptStats(concept))
    stat.cType = cType
    stat.frequency += 1
    stat.topicUriSet.add(topicUri)
    stat.pathSet.add(path)
    map.put(concept, stat)
  }
}

object ConceptGraph{

  // MAGIC NUMBERS are generally associated with a given ontology.
  // Calibration of these magic numbers may be required when moving to a new ontology - look for usages of this method
  def isNewOntology(ogName: OntologyGraphName) = {
    ogName.graphName == "graph.2015.04"
  }

  private val graphMap = new mutable.HashMap[String, OntologyGraph2]()

  private val conceptContainsBlackList = immutable.HashSet(
    "Geography_of",
    "Counties_of",
    "areas_of",
    "Countries_bordering",
    "Wikipedia_",
    "Categories_",
    "Genres_by_medium",
    "by_state",
    "people_by",
    "by_country",
    "by_artist",
    "by_type",
    "by_genre"
  )

  def passBlackListConceptFilter(uri: String): Boolean = {
    val badConcept = OntologyNodesBlackLists.concepts.contains(uri) || conceptContainsBlackList.exists(item => uri.contains(item))
    !badConcept
  }

  private def conceptEvaluator(stopDepth: Int, partialResult: ConceptGraphResult, topicUri: String)(implicit ogName: OntologyGraphName) = NodeEval {
    path => {
      var evaluation = Evaluation.EXCLUDE_AND_CONTINUE

      if (path.length() >= stopDepth) {
        evaluation = Evaluation.EXCLUDE_AND_PRUNE
      }
      else {
        val ontEndNode = new OntologyNode(path.endNode())
        ontEndNode.uriType match {
          case URIType.WIKI_CATEGORY =>
            partialResult.conceptsVisited += 1

            //            println(partialResult.conceptsVisited + ".  evaluating topic: " + topicUri +  " concept: " + ontEndNode.uri)
            if (isNewOntology(ogName)) {

              val topicClosenessScoreL1 = OntologyNodeScore.getScore(ontEndNode, NodeProperties.TopicL1)
              val conceptClosenessSumScoreL3 = OntologyNodeScore.getScore(ontEndNode, NodeProperties.ConceptL3Sum)
              val conceptClosenessSumScoreL2 = OntologyNodeScore.getScore(ontEndNode, NodeProperties.ConceptL2Sum)

              val pgRank = OntologyNodeScore.getScoreDouble(ontEndNode, NodeProperties.PageRank)

              if (ontEndNode.name != "No name" &&
                  passBlackListConceptFilter(ontEndNode.uri) &&
                pgRank > 1.0d &&
                pgRank < 40.0d &&
                OntologyNodeScore.getScore(ontEndNode, NodeProperties.OutEdges ) > 1 &&
                topicClosenessScoreL1 > 0) {

                if (OntologyNodeScore.getScore(ontEndNode, NodeProperties.ConceptL3) > 0 //&&
//                  pgRank > 5.0d &&
//                  pgRank < 10.0d
//                  &&
//                  conceptClosenessSumScoreL3 < 250
                ){
                  partialResult.addHighLevelConceptNode(ontEndNode, topicUri, path)
                  evaluation = Evaluation.INCLUDE_AND_CONTINUE
                }
                else if (
//                  pgRank <= 5.0d &&
//                  topicClosenessScoreL1 > 75 && topicClosenessScoreL1 < 150 &&
//                  conceptClosenessSumScoreL2 < 50 &&
                  path.length() == 1) {
                  partialResult.addLowLevelConceptNode(ontEndNode, topicUri, path)
                  Evaluation.INCLUDE_AND_CONTINUE
                }
                else {
                  partialResult.addUnsedConceptNode(ontEndNode, topicUri, path)
                }

              }
            }
            else {
              // Old thresholds for Old ontology
              if (ontEndNode.name != "No name" && passBlackListConceptFilter(ontEndNode.uri) &&
                OntologyNodeScore.getScore(ontEndNode, OntologyNodeScore.OutEdges) + OntologyNodeScore.getScore(ontEndNode, DegreeScore.OUT_DEGREE) > 1 &&
                OntologyNodeScore.getScore(ontEndNode, OntologyNodeScore.TopicL1) > 0) {

                if ((ontEndNode.node.hasProperty(OntologyNodeScore.ConceptL3) || ontEndNode.node.hasProperty(OntologyNodeScore.ConceptL4)) &&
                  OntologyNodeScore.getScore(ontEndNode, OntologyNodeScore.TopicL4) <= 40000 &&
                  OntologyNodeScore.getScore(ontEndNode, OntologyNodeScore.ConceptL4) <= 115 &&
                  OntologyNodeScore.getScore(ontEndNode, OntologyNodeScore.ChildConceptL4) <= 10) {
                  partialResult.addHighLevelConceptNode(ontEndNode, topicUri, path)
                  evaluation = Evaluation.INCLUDE_AND_CONTINUE
                }
                else {

                  val topicClosenessScoreL1 = OntologyNodeScore.getScore(ontEndNode, OntologyNodeScore.TopicL1)
                  val topicClosenessScoreL4 = OntologyNodeScore.getScore(ontEndNode, OntologyNodeScore.TopicL4)

                  if (topicClosenessScoreL1 > 75 && topicClosenessScoreL1 < 150 && topicClosenessScoreL4 >= 15000 && path.length() == 1) {
                    partialResult.addLowLevelConceptNode(ontEndNode, topicUri, path)
                    Evaluation.INCLUDE_AND_CONTINUE
                  }
                  else {
                    partialResult.addUnsedConceptNode(ontEndNode, topicUri, path)
                  }
                }
              }

            }

          case _ =>
        }
      }
      evaluation
    }
  }

  def getRelatedConcepts(depth: Int, grapher: Grapher, useAllTopics: Boolean = false)(implicit ogName: OntologyGraphName): ConceptGraphResult = {
    val result = new ConceptGraphResult(grapher)
    val topics = if (useAllTopics) grapher.allTopics else grapher.bestTopics
    topics foreach (topicUri => {
      collectConcepts(topicUri, depth, result)
    })
    result.calculateScores
    result
  }

  def collectConcepts(topic: String, depth: Int, partialResult: ConceptGraphResult)(implicit ogName: OntologyGraphName) {
    getNode(topic) match {
      case Some(startNode) =>

        val trav = Traversal.description.breadthFirst()
          .evaluator(conceptEvaluator(depth + 1, partialResult, topic))
          .relationships(TopicRelationshipTypes.CONCEPT_OF_TOPIC, Direction.OUTGOING)
          .relationships(TopicRelationshipTypes.BROADER_CONCEPT, Direction.OUTGOING)
          .relationships(TopicRelationshipTypes.REDIRECTS_TO, Direction.OUTGOING)
          .relationships(TopicRelationshipTypes.DISAMBIGUATES, Direction.OUTGOING)
          .relationships(ConceptNodes.GRAV_AUTO_BROADER_CONCEPT, Direction.OUTGOING)
          .relationships(ConceptNodes.GRAV_AUTO_CONCEPT_OF_TOPIC, Direction.OUTGOING)
          .traverse(startNode.node)

        trav foreach (_ => {})

      case None =>
        println("Invalid Topic: " + topic)
    }
  }


  def withGraph(ogName: OntologyGraphName) = {
    graphMap.getOrElseUpdate(ogName.graphName, {
      val gr = OntologyGraphMgr.getInstance().getGraph(ogName.graphName)
      new OntologyGraph2(gr)
    })
  }

  def getNode(id: Long)(implicit ogName: OntologyGraphName): Option[OntologyNode] = {
    withGraph(ogName).node(id)
  }

  def getNode(uri: String)(implicit ogName: OntologyGraphName): Option[OntologyNode] = {
    withGraph(ogName).node(NS.fromString(uri))
  }

  def getNodeRichly(term: String)(implicit ogName: OntologyGraphName): Option[OntologyNode] = {
    withGraph(ogName).searchForTopicRichly(term) match {
      case Some(scoredNode) => Some(scoredNode.node)
      case _ => None
    }
  }

  def processFreeText(freeTxt: String, maxTopics: Int = 100, depth: Int = 3)(implicit ogName: OntologyGraphName): ConceptGraphResult = {
    val content = ContentToGraph(
      "",
      new DateTime(),
      freeTxt,
      Weight.High,
      ContentType.Article)

    val grapher = new Grapher(content :: Nil)
    getRelatedConcepts(depth, grapher, useAllTopics = true)
  }

  def processArticle(url: String, articleContent: String, articleTitle: String, maxTopics: Int = 100, depth: Int = 4)(implicit ogName: OntologyGraphName): ConceptGraphResult = {

    println("article Text: " + articleContent.size)
    val content = ContentToGraph(
      url,
      new DateTime(),
      articleContent,
      Weight.Low,
      ContentType.Article)

    val title = ContentToGraph(
      url,
      new DateTime(),
      articleTitle,
      Weight.High,
      ContentType.Title)

    val grapher = new Grapher(title :: content :: Nil)

    //    println("phrases")
    //    grapher.phrasesWithTopics.foreach(pt => println("   " + pt.phrase.text))

    getRelatedConcepts(depth, grapher)
  }

  def processContent(url: String, depth: Int = 4, maxTopics: Int = 100)(implicit ogName: OntologyGraphName): ConceptGraphResult = {
    val articleContent = ContentUtils.fetchAndExtractMetadataSafe(url)
    processArticle(url, articleContent.text, articleContent.title, maxTopics, depth)
  }
}


object FileProcessor {
  //promoNodes.txt, promoNodes_all.txt   --> sorted_end_promoted_concepts.txt -> compacted_sorted_and_promoted_concepts.txt
  def compactPromotedEndConceptNodes(fname: String) {
    val SPLIT = " : "
    val map = new mutable.HashMap[String /*concept*/ , Int /*cnt*/ ]()

    for (line <- Source.fromFile(fname).getLines()) {
      val parts = line.split(SPLIT)
      val concept = parts(0)
      val cnt = parts(1).toInt

      val c = map.getOrElse(concept, 0)
      map.put(concept, c + cnt)
    }

    map.toList sortBy {
      _._2 * -1
    } foreach (entry => {
      println(entry._1 + " : " + entry._2)
    })
  }
}

object BroaderConceptWeights {
  val WEIGHT = "GRAV_BroaderConceptWeight"
  private val graph = OntologyGraph2.graph


  def printRpt(fname: String, maxNodes: Int = Int.MaxValue) {
    val data = readFile(fname, 0)
    val edges = data._1

    // startNode -> weighedEdgeCount
    val uniqueStartNodesWithEdges = getWeightedEdges(maxNodes, edges)

    uniqueStartNodesWithEdges foreach (entry => {
      val startNodeUri = entry._1
      val endNodeWithWeightList = entry._2

      val ttlEdgeCnt = getNode(startNodeUri) match {
        case Some(on) =>
          on.node.getRelationships(Direction.OUTGOING, TopicRelationshipTypes.BROADER_CONCEPT).size
        case scala.None =>
          0
      }

      // start node
      println()
      println(startNodeUri + ": " + endNodeWithWeightList.size + "/" + ttlEdgeCnt)

      var c = 1
      // weighted edges
      endNodeWithWeightList foreach (endNodeWithWeight => {
        println("   " + c + ". " + endNodeWithWeight._1 + ": " + endNodeWithWeight._2)
        c += 1
      })

      // non-weighted edges
      getNode(startNodeUri) match {
        case Some(on) =>
          on.node.getRelationships(Direction.OUTGOING, TopicRelationshipTypes.BROADER_CONCEPT) foreach (rel => {

            var found = false
            endNodeWithWeightList foreach (entry => {
              if (entry._1 == rel.getEndNode.uri)
                found = true
            })

            if (!found) {
              println("   " + c + ". " + rel.getEndNode.uri + ": (No Weight)")
              c += 1
            }

          })
        case scala.None =>
      }
    })

    println("Total: " + uniqueStartNodesWithEdges.size)
  }

  private def getWeightedEdges(maxNodes: Int, edges: Iterable[(String /*StartNode*/ , String /*EndNode*/ , Int /*Weight*/ )]): mutable.HashMap[String, mutable.ListBuffer[(String, Int)]] = {
    val result = new mutable.HashMap[String /*startNode*/ , mutable.ListBuffer[(String /*endNode*/ , Int /*weight*/ )]]()
    val iterator = edges.iterator
    while (result.size < maxNodes && iterator.hasNext) {
      val edge = iterator.next()
      val startNodeUri = edge._1
      val endNodeUri = edge._2
      val weight = edge._3

      // skip portals
      if (!endNodeUri.contains("Portal:")) {
        val endNodes = result.getOrElse(startNodeUri, new mutable.ListBuffer[(String /*endNode*/ , Int /*weight*/ )])
        endNodes.append((endNodeUri, weight))
        result.put(startNodeUri, endNodes)
      }
    }
    result
  }

  def importWeights(fname: String) {
    val data = readFile(fname, 5)
    writeToGraph(data)
  }


  def dumpRelProperties(startUri: String, endUri: String, relType: TopicRelationshipTypes = TopicRelationshipTypes.BROADER_CONCEPT) {
    getRelationship(startUri, endUri, relType) match {
      case Some(r) =>
        r.getPropertyKeys foreach (k => {
          println(k + ": " + r.getProperty(k))
        })
      case None =>
        println("Rel NOT found")
    }
  }


  private def writeToGraph(data: (Iterable[(String /*StartNode*/ , String /*EndNode*/ , Int /*Weight*/ )], Int /*MaxScore*/ )) {
    val edgeCollection = data._1

    var processedCnt = 0
    var ttlEdgeCnt = 0

    edgeCollection foreach (edgeData => {

      ttlEdgeCnt += 1
      val edgeScore = edgeData._3

      //val maxScore = data._2
      //val normalizedScore = edgeScore.toFloat / maxScore.toFloat

      getRelationship(edgeData._1, edgeData._2) match {
        case Some(r) =>
          println(r.getStartNode.uri + " -> " + r.getEndNode.uri + " : " + edgeScore)
          setWeight(r, WEIGHT, edgeScore)
          processedCnt += 1

        case None =>
          println("Skipped: " + edgeData._1 + " -> " + edgeData._2 + " : " + edgeScore)
      }
    })

    println("Total Edges: " + ttlEdgeCnt)
    println("Edges Processed: " + processedCnt)
    println("Skipped Edges: " + (ttlEdgeCnt - processedCnt))
  }

  private def getRelationship(startUri: String,
                              endUri: String,
                              relType: TopicRelationshipTypes = TopicRelationshipTypes.BROADER_CONCEPT): Option[Relationship] = {
    val startNode = getNode(startUri)
    val endNode = getNode(endUri)

    for (nS <- startNode; nE <- endNode) {
      // start & end concept nodes exist
      val rels = nS.node.getRelationships(Direction.OUTGOING, TopicRelationshipTypes.BROADER_CONCEPT)
      val rel = rels.find(r => r.getEndNode.getId == nE.node.getId)
      for (r <- rel) {
        // found relationship
        return Some(r)
      }
    }
    None
  }

  private def readFile(fname: String, maxLinesToDump: Int = Int.MaxValue):
  (Iterable[(String /*StartNode*/ , String /*EndNode*/ , Int /*Weight*/ )], Int /*MaxScore*/ ) = {
    val SPLIT = """","""
    var cnt = 0
    val map = new mutable.ListBuffer[(String, String, Int)]()

    var maxScore = 0
    for (line <- Source.fromFile(fname).getLines()) {
      val cols = line.split(SPLIT)

      try {
        val cStart = cols(0).replace( """"""", """""")
        val cEnd = cols(1).replace( """"""", """""")
        val score = cols(2).toInt

        if (cnt < maxLinesToDump) {
          println(cStart + " | " + cEnd + " | " + score)
        }
        cnt += 1
        if (score > maxScore) {
          maxScore = score
        }
        map.append((cStart, cEnd, score))
      }
      catch {
        case _: Exception => println("cannot parse: " + line)
      }
    }

    println("Max score " + maxScore)
    (map, maxScore)
  }

  private def setWeight[T](rel: Relationship, key: String, score: T) {
    val tx = graph.graphDb.beginTx()
    try {
      rel.setProperty(key, score)
      tx.success()
    }
    catch {
      case e: Exception => throw new Exception("Unable to set property " + key)
    }
    finally {
      tx.finish()
    }
  }

  private def getNode(uri: String): Option[OntologyNode] = {
    graph.node(NS.fromString(uri))
  }
}


object TopicNodes {
  private val gdb = OntologyGraph2.graph.graphDb

  def dumpNodes(maxNodesToDump: Int = Int.MaxValue) {
    getNodes(maxNodesToDump) foreach (n => {
      println(n.uri)
    })
  }

  def getNodes(maxNodes: Int = Int.MaxValue): mutable.HashSet[OntologyNode] = {
    val iterator = GlobalGraphOperations.at(gdb).getAllNodes.iterator()
    val nodes = new mutable.HashSet[OntologyNode]()
    var cnt = 0

    while (cnt < maxNodes && iterator.hasNext) {
      val node = new OntologyNode(iterator.next())

      try {
        node.uriType match {
          case URIType.TOPIC =>
            nodes.add(node)
            cnt += 1
          case _ =>
        }
      }
      catch {
        case _: Exception =>
      }
    }
    nodes
  }
}

object ConceptNodes {
  private val gdb = OntologyGraph2.graph.graphDb

  val GRAV_AUTO_BROADER_CONCEPT = new RelationshipType {
    def name(): String = "GRAV_AutoBroaderConcept"
  }
  val GRAV_AUTO_CONCEPT_OF_TOPIC = new RelationshipType {
    def name(): String = "GRAV_AutoConceptOfTopic"
  }

  val GRAV_jumpScore = "GRAV_JumpScore"

  def clearScores(maxDepth: Int = 4, nodesToScan: Int = Int.MaxValue) {
    ConceptHierarchy.clearScores(ConceptNodes.getConceptNodes(nodesToScan), maxDepth)
  }

  def runAnalysis(maxDepth: Int = 4) {

    println("StartTime: " + Calendar.getInstance.getTime)
    ConceptNodes.flattenGraph()
    DegreeScore.scoreGraph()
    TopicClosenessScore.scoreGraph(depth = maxDepth)
    ConceptNodes.scoreGraph()

    // print report
    //ConceptNodes.dumpHighConceptNodes()

    // TBD: point to correct source for more weights
    //BroaderConceptWeights.importWeights("/Users/apatel/junk/categoryScore.csv")

    println("EndTime: " + Calendar.getInstance.getTime)

  }

  def collectDirectHigherLevelConcepts(node: OntologyNode): mutable.HashSet[OntologyNode] = {
    val parents = mutable.HashSet.empty[OntologyNode]
    collectDirectHighLevelConcepts(node, parents)
    parents
  }


  def collectDirectHighLevelConcepts(node: OntologyNode, parents: mutable.HashSet[OntologyNode]) {
    val rels = node.node.getRelationships(Direction.OUTGOING, TopicRelationshipTypes.BROADER_CONCEPT).toList
    if (rels.size == 1) {
      val rel = rels(0)

      if (!parents.contains(rel.getEndNode)) {
        parents.add(rel.getEndNode)
        collectDirectHighLevelConcepts(rel.getEndNode, parents)
      }

    }

  }

  def flattenGraph(maxNodesToScan: Int = Int.MaxValue, dryRun: Boolean = false) {
    val iterator = GlobalGraphOperations.at(gdb).getAllNodes.iterator()
    var scannedNodes = 0
    var conceptNodes = 0

    var cnt = 0
    var edgePromotionsCnt = 0

    while (scannedNodes < maxNodesToScan && iterator.hasNext) {
      val node = new OntologyNode(iterator.next())
      scannedNodes += 1
      try {
        node.uriType match {
          case URIType.WIKI_CATEGORY =>
            conceptNodes += 1
            val parents = collectDirectHigherLevelConcepts(node)
            if (parents.size > 0) {
              cnt += 1

              val inRels = node.node.getRelationships(Direction.INCOMING,
                TopicRelationshipTypes.BROADER_CONCEPT,
                TopicRelationshipTypes.CONCEPT_OF_TOPIC).toList
              val eCnt = inRels.size
              edgePromotionsCnt += eCnt

              var line = parents.size + " " + node.uri
              parents foreach (parent => {
                line += "->" + parent.uri
              })
              line += " : " + eCnt
              println(line)

              inRels foreach (inRel => {

                val oStartNode = new OntologyNode(inRel.getStartNode)
                println("   " + oStartNode.uri)

                if (!dryRun) {
                  // create rel from lowerlevelnodes to highest level concept
                  oStartNode.uriType match {
                    case URIType.WIKI_CATEGORY =>
                      createAutoRelationship(oStartNode.node, parents.last.node, GRAV_AUTO_BROADER_CONCEPT, parents.size)
                    case URIType.TOPIC =>
                      createAutoRelationship(oStartNode.node, parents.last.node, GRAV_AUTO_CONCEPT_OF_TOPIC, parents.size)
                    case _ =>
                  }
                }
              })
            }
          case _ =>
        }
      }
      catch {
        case e: Exception => println(e.toString)
      }
    }

    println("Total Irrelevant Nodes: " + cnt)
    println("Total edge promotions: " + edgePromotionsCnt)
  }


  private def createAutoRelationship(s: Node, e: Node, relType: RelationshipType, w: Int) {
    val tx = gdb.beginTx()
    try {
      val rel = s.createRelationshipTo(e, relType)
      rel.setProperty(GRAV_jumpScore, w)
      tx.success()
    }
    catch {
      case e: Exception => throw new Exception("Unable to create auto relationship " + e.toString)
    }
    finally {
      tx.finish()
    }
  }

  def generateReport(maxNodesToDump: Int = Int.MaxValue,
                     maxNodesToScan: Int = Int.MaxValue,
                     minTopicClosenessDepth3Score: Int = 5000,
                     maxDegree: Int = 150) {
    var tStart = new java.util.Date().getTime
    val concepts = get(maxNodesToScan, minTopicClosenessDepth3Score, maxDegree)
    var tEnd = new java.util.Date().getTime
    println("Search Time: " + (tEnd - tStart) / 1000 + " [sec]")
    dumpScores(concepts.take(maxNodesToDump))

    tStart = new java.util.Date().getTime
    val topics = getReachableTopicCountForConcepts(concepts)
    tEnd = new java.util.Date().getTime
    println("Reachable Calculation Time: " + (tEnd - tStart) / 1000 + " [sec]")
    println("Total Rachable Topics: " + topics.size)

    // get all topics
    // TBD

    // get unreachable topics
    // TBD

  }

  def scoreGraph(maxNodesToScan: Int = Int.MaxValue,
                 minTopicClosenessScore: Int = 5000,
                 maxDegree: Int = 150,
                 depth: Int = 3) {
    val conceptNodes = getConceptNodes(maxNodesToScan)
    println("Initial Concept Nodes: " + conceptNodes.size)
    val nodesToScore = filterByTopicScore(conceptNodes, minTopicClosenessScore, maxDegree, depth)
    println("Filtered Concept Nodes To Score: " + nodesToScore.size)
    ConceptHierarchy.scoreNodes(nodesToScore, depth)

  }

  def get(maxNodesToScan: Int = Int.MaxValue,
          minTopicClosenessDepth3Score: Int = 5000,
          maxDegree: Int = 150): Iterable[OntologyNode] = {


    val nodes = getConceptNodes(maxNodesToScan)
    val f1 = filterByTopicScore(nodes, minTopicClosenessDepth3Score, maxDegree)
    val f2 = filterByConceptScore(f1)
    // sort by concept score 3
    val sortedConceptNodes = f2.toBuffer.sortWith(
      (l, r) =>
        getScore(l, ConceptHierarchy.PARENT_ATTRIBUTE_SCORE + 3) > getScore(r, ConceptHierarchy.PARENT_ATTRIBUTE_SCORE + 3))

    println("initial concepts: " + nodes.size)
    println("Post Topic filter: " + f1.size)
    println("Post Concept filter: " + f2.size)

    sortedConceptNodes
  }


  private def getReachableTopicCountForConcepts(conceptNodes: Iterable[OntologyNode]): mutable.HashSet[OntologyNode] = {
    val reachableTopicsSet = new mutable.HashSet[OntologyNode]()
    conceptNodes foreach (node => {
      collectReachableTopics(node, reachableTopicsSet)
    })
    reachableTopicsSet
  }

  private def collectReachableTopics(startNode: OntologyNode,
                                     reachableTopics: mutable.HashSet[OntologyNode],
                                     depth: Int = 3) {
    val trav = Traversal.description.breadthFirst()
      .evaluator(topicEvaluator(depth + 1))
      .relationships(TopicRelationshipTypes.CONCEPT_OF_TOPIC, Direction.INCOMING)
      .relationships(TopicRelationshipTypes.BROADER_CONCEPT, Direction.INCOMING)
      .relationships(TopicRelationshipTypes.INTEREST_OF, Direction.INCOMING)
      .relationships(TopicRelationshipTypes.REDIRECTS_TO, Direction.INCOMING)
      .relationships(TopicRelationshipTypes.DISAMBIGUATES, Direction.INCOMING)
      .relationships(ConceptNodes.GRAV_AUTO_BROADER_CONCEPT, Direction.INCOMING)
      .relationships(ConceptNodes.GRAV_AUTO_CONCEPT_OF_TOPIC, Direction.INCOMING)
      .traverse(startNode.node)

    trav foreach (p => {
      reachableTopics.add(new OntologyNode(p.endNode()))
    })

  }


  private def topicEvaluator(stopDepth: Int) = NodeEval {
    path => {
      var evaluation = Evaluation.EXCLUDE_AND_CONTINUE

      if (path.length() >= stopDepth) {
        evaluation = Evaluation.EXCLUDE_AND_PRUNE
      }
      else {
        val ontEndNode = new OntologyNode(path.endNode())
        if (ontEndNode.uriType == URIType.TOPIC) {
          evaluation = Evaluation.INCLUDE_AND_CONTINUE
        }
      }
      evaluation
    }
  }

  def filterByTopicScore(nodes: mutable.HashSet[OntologyNode],
                         minTopicClosenessScore: Int = 5000,
                         maxDegree: Int = 150,
                         depth: Int = 3): mutable.HashSet[OntologyNode] = {
    val filteredConceptNodes = new mutable.HashSet[OntologyNode]()

    nodes foreach (node => {
      val p = getScore(node, DegreeScore.OUT_DEGREE) + getScore(node, OntologyNodeScore.OutEdges)


      val scoreAtDepth = new mutable.ListBuffer[Int]
      (1 to depth) foreach (d => {
        scoreAtDepth.append(getScore(node, TopicClosenessScore.TOPIC_CLOSENESS_SCORE + d))
      })

      if (scoreAtDepth.filter(s => s > 0).size == depth &&
        p <= maxDegree &&
        scoreAtDepth.last >= minTopicClosenessScore) {
        //d2 >= 10 * d1 && d3 >= d2 * 4 && {

        // TBD: CUSTOM LOGIC FOR DEPTH 3 or GREATER
        if (depth > 2) {
          val d3 = scoreAtDepth(3 - 1)
          val d2 = scoreAtDepth(2 - 1)
          val d1 = scoreAtDepth(1 - 1)

          if (d2 >= 10 * d1 && d3 >= d2 * 4) {
            filteredConceptNodes.add(node)
          }
        }
        else {
          filteredConceptNodes.add(node)
        }
      }

      //      // FILTER to get BEST high level concepts
      //      if (d1 > 0 && d2 > 0 && d3 > 0 &&
      //        d2 >= 10 * d1 && d3 >= d2 * 4 &&
      //        p <= maxDegree && d3 >= minTopicClosenessScore )
      //      //if (true)
      //      {
      //        filteredConceptNodes.add(node)
      //      }
    })

    filteredConceptNodes
  }

  private def filterByConceptScore(nodes: mutable.HashSet[OntologyNode]): mutable.HashSet[OntologyNode] = {
    ConceptHierarchy.clearScores(nodes)
    ConceptHierarchy.scoreNodes(nodes)

    val filteredNodes = new mutable.HashSet[OntologyNode]
    nodes.foreach(node => {

      val l1 = getScore(node, ConceptHierarchy.PARENT_ATTRIBUTE_SCORE + 1)
      val l2 = getScore(node, ConceptHierarchy.PARENT_ATTRIBUTE_SCORE + 2)
      val l3 = getScore(node, ConceptHierarchy.PARENT_ATTRIBUTE_SCORE + 3)

      if (l1 + l2 + l3 > 0) {
        filteredNodes.add(node)
      }
    })
    filteredNodes
  }


  def dumpSelctedConceptNodes(maxDepth: Int = 4, maxNodesToFetch: Int = Int.MaxValue, fname: Option[String] = None) {

    val result = new mutable.ArrayBuffer[OntologyNode]()

    getConceptNodes(maxNodesToFetch) foreach {
      node => {

        val parentScores = new mutable.ListBuffer[Int]
        val childScores = new mutable.ListBuffer[Int]
        val topicScores = new mutable.ListBuffer[Int]
        var outEdges = 0
        var inMinusOutEdges = 0


        (1 to maxDepth) foreach (d => {
          parentScores.append(getScore(node, ConceptHierarchy.PARENT_ATTRIBUTE_SCORE + d))
          childScores.append(getScore(node, ConceptHierarchy.CHILD_ATTRIBUTE_SCORE + d))
          topicScores.append(getScore(node, TopicClosenessScore.TOPIC_CLOSENESS_SCORE + d))

          outEdges = getScore(node, DegreeScore.OUT_DEGREE) + getScore(node, OntologyNodeScore.OutEdges)
          //          outEdges = getScore(node, DegreeScore.OUT_DEGREE + d) + getScore(node, OntologyNodeScore.OutEdges)
          //          inMinusOutEdges = getScore(node, DegreeScore.IN_MINUS_OUT_DEGREE + d)
        })

        val parentSum = parentScores.foldRight(0)(_ + _)
        val childSum = childScores.foldRight(0)(_ + _)

        val topicScore = topicScores.last
        if (topicScore >= 5000 && topicScore <= 25000 && outEdges <= 150) {
          result.append(node)
        }
      }
    }

    val sortedResults = result.sortBy(on => -1 * on.getProperty(ConceptHierarchy.PARENT_ATTRIBUTE_SCORE + maxDepth, 0).asInstanceOf[Int])
    dumpScores(sortedResults, maxDepth, fname)
    println("Total Results: " + sortedResults.size)
  }

  def dumpHighConceptNodes(maxDepth: Int = 4, maxNodesToFetch: Int = Int.MaxValue, fname: Option[String] = None) {

    val result = new mutable.ArrayBuffer[OntologyNode]()

    getConceptNodes(maxNodesToFetch) foreach {
      node => {

        val parentScores = new mutable.ListBuffer[Int]
        val childScores = new mutable.ListBuffer[Int]

        (1 to maxDepth) foreach (d => {
          parentScores.append(getScore(node, ConceptHierarchy.PARENT_ATTRIBUTE_SCORE + d))
          childScores.append(getScore(node, ConceptHierarchy.CHILD_ATTRIBUTE_SCORE + d))
        })

        val parentSum = parentScores.foldRight(0)(_ + _)
        val childSum = childScores.foldRight(0)(_ + _)

        if (parentSum > 0 && childSum > 0 && childScores.filter(s => s > 0).size == maxDepth) {
          result.append(node)
        }
      }
    }

    val sortedResults = result.sortBy(on => -1 * on.getProperty(ConceptHierarchy.PARENT_ATTRIBUTE_SCORE + maxDepth, 0).asInstanceOf[Int])
    dumpScores(sortedResults, maxDepth, fname)
    println("Total Results: " + sortedResults.size)
  }

  private def toString[T](ary: Iterable[T], prefixStr: String, postfixStr: String): String = {
    var s = ""
    ary.foreach(i => {
      s += prefixStr + i.toString + postfixStr
    })
    s
  }

  def dumpAll(maxDepth: Int = 4, fname: Option[String] = None) {
    ConceptNodes.dumpScores(ConceptNodes.getConceptNodes(), maxDepth, fname)
  }

  def dumpScores(nodes: Iterable[OntologyNode], maxDepth: Int = 4, fname: Option[String] = None) {
    val TAB = "\t"
    val ds = toString((1 to maxDepth), "topicD", TAB)
    val ls = toString((1 to maxDepth), "conceptD", TAB)
    val cs = toString((1 to maxDepth), "childConceptD", TAB)

    val out = fname match {
      case Some(f) =>
        println("file " + f + " generated at " + Calendar.getInstance().getTime)
        new Some(new FileWriter(f))
      case None =>
        None
    }

    try {

      wlineFile("id\turi\tOutEdges\tInMinusOutEdges\t" + ds + "topicMaxDepth\t" + ls + "conceptMaxDepth\t" + cs + "\t")
      nodes foreach {
        node => {

          val parentScores = new mutable.ListBuffer[Int]
          val childScores = new mutable.ListBuffer[Int]
          val topicClosenessScores = new mutable.ListBuffer[Int]

          (1 to maxDepth) foreach (d => {
            parentScores.append(getScore(node, ConceptHierarchy.PARENT_ATTRIBUTE_SCORE + d))
            childScores.append(getScore(node, ConceptHierarchy.CHILD_ATTRIBUTE_SCORE + d))
            topicClosenessScores.append(getScore(node, TopicClosenessScore.TOPIC_CLOSENESS_SCORE + d))
          })

          val id = node.node.getId
          val uri = node.uri
          val p = getScore(node, DegreeScore.OUT_DEGREE) + getScore(node, OntologyNodeScore.OutEdges)
          val f = getScore(node, DegreeScore.IN_MINUS_OUT_DEGREE)

          val topicSum = topicClosenessScores.foldLeft(0)(_ + _)
          val topicMaxDepth = topicClosenessScores.indexOf(topicClosenessScores.max) + 1
          val topicDepth = if (topicSum == 0) 0 else topicMaxDepth

          val parentSum = parentScores.foldLeft(0)(_ + _)
          val parentMaxDepth = parentScores.indexOf(parentScores.max) + 1
          val parentDepth = if (parentSum == 0) 0 else parentMaxDepth

          val parentStr = toString(parentScores, "", TAB)
          val childStr = toString(childScores, "", TAB)
          val tcStr = toString(topicClosenessScores, "", TAB)

          wFile(id + TAB)
          wFile(uri + TAB)
          wFile(p + TAB)
          wFile(f + TAB)
          wFile(tcStr)
          wFile(topicDepth + TAB)
          wFile(parentStr)
          wFile(parentDepth + TAB)
          wlineFile(childStr)
        }
      }

    }
    finally {
      out match {
        case Some(o) => o.close()
        case None =>
      }
    }

    def wlineFile(s: String) {
      wFile(s + "\n")
    }

    def wFile(s: String) {
      print(s)
      out match {
        case Some(w) => w.write(s)
        case None =>
      }
    }
  }

  def getScore(node: OntologyNode, attributeName: String): Int = {
    node.getProperty(attributeName, 0).asInstanceOf[Int]
  }

  def getConceptNodes(maxNodesToScan: Int = Int.MaxValue): mutable.HashSet[OntologyNode] = {
    val iterator = GlobalGraphOperations.at(gdb).getAllNodes.iterator()
    val conceptNodes = new mutable.HashSet[OntologyNode]()
    var cnt = 0

    while (cnt < maxNodesToScan && iterator.hasNext) {
      val node = new OntologyNode(iterator.next())
      cnt += 1
      try {
        node.uriType match {
          case URIType.WIKI_CATEGORY =>
            conceptNodes.add(node)
          case _ =>
        }
      }
      catch {
        case _: Exception =>
      }
    }
    conceptNodes
  }

}

object ConceptHierarchy {
  private val gdb = OntologyGraph2.graph.graphDb
  val PARENT_ATTRIBUTE_SCORE = "GRAV_ConceptParentScore_Level"
  val CHILD_ATTRIBUTE_SCORE = "GRAV_ConceptChildScore_Level"

  def scoreNodes(conceptNodes: mutable.HashSet[OntologyNode], depth: Int = 3) {

    var cnt = 0
    conceptNodes foreach (node => {
      cnt += 1
      scoreNode(node, conceptNodes, depth)

      print(cnt + ". " + node.uri)
      var parent = " parent("
      var child = " child("

      (1 to depth) foreach (d => {
        parent += node.getProperty(PARENT_ATTRIBUTE_SCORE + d, 0) + ", "
        child += node.getProperty(CHILD_ATTRIBUTE_SCORE + d, 0) + ", "
      })
      parent += ")"
      child += ")"
      println(parent + child)
    })
  }

  def clearScores(conceptNodes: mutable.HashSet[OntologyNode], maxDepth: Int = 4) {
    println("conceptNodes to process: " + conceptNodes.size)
    var cnt = 0

    for (depth <- (1 to maxDepth); node <- conceptNodes) {

      removeProperty(node.node, PARENT_ATTRIBUTE_SCORE + depth)
      removeProperty(node.node, CHILD_ATTRIBUTE_SCORE + depth)
      if (depth == 1) {
        cnt += 1
        println(cnt + ". " + node.uri)
      }
    }
  }


  private def scoreNode(startNode: OntologyNode, conceptNodes: mutable.HashSet[OntologyNode], depth: Int) {
    val trav = Traversal.description.breadthFirst()
      .evaluator(conceptHierarchyEvaluator(depth + 1, conceptNodes))
      .relationships(TopicRelationshipTypes.BROADER_CONCEPT, Direction.INCOMING)
      .relationships(ConceptNodes.GRAV_AUTO_BROADER_CONCEPT, Direction.INCOMING)
      .traverse(startNode.node)

    trav foreach (n => {})
  }

  private def conceptHierarchyEvaluator(stopDepth: Int, conceptNodes: mutable.HashSet[OntologyNode]) = NodeEval {
    path => {
      var evaluation = Evaluation.EXCLUDE_AND_CONTINUE
      val depth = path.length()

      if (depth >= stopDepth) {
        evaluation = Evaluation.EXCLUDE_AND_PRUNE
      }
      else if (path.nodes().size > 1) {
        val startNode = new OntologyNode(path.nodes().head)
        val endNode = new OntologyNode(path.endNode())

        if (conceptNodes.contains(endNode)) {
          incrementScore(startNode, depth, PARENT_ATTRIBUTE_SCORE)
          incrementScore(endNode, depth, CHILD_ATTRIBUTE_SCORE)
        }
      }

      evaluation
    }

  }

  private def incrementScore(node: OntologyNode, depth: Int, attributeName: String) {
    val score = node.getProperty(attributeName + depth, 0).asInstanceOf[Int]
    setScore(node.node, attributeName + depth, score + 1)
  }


  def removeProperty(node: Node, key: String) {
    val tx = gdb.beginTx()
    try {
      node.removeProperty(key)
      tx.success()
    }
    catch {
      case e: Exception => throw new Exception("Unable to remove property")
    }
    finally {
      tx.finish()
    }

  }

  private def setScore(node: Node, key: String, score: Int) {
    val tx = gdb.beginTx()
    try {
      node.setProperty(key, score)
      tx.success()
    }
    catch {
      case e: Exception => throw new Exception("Unable to set property " + key)
    }
    finally {
      tx.finish()
    }
  }
}

object TopicClosenessScore {
  val TOPIC_CLOSENESS_SCORE = "GRAV_TopicClosenessScore_Level"
  private val gdb = OntologyGraph2.graph.graphDb
  private val graph = OntologyGraph2.graph


  def scoreGraph(maxNodesToScore: Int = Int.MaxValue, depth: Int = 3) {

    val iterator = GlobalGraphOperations.at(gdb).getAllNodes.iterator()
    var cnt = 0
    while (cnt < maxNodesToScore && iterator.hasNext) {
      val node = new OntologyNode(iterator.next())

      try {
        node.uriType match {
          case URIType.WIKI_CATEGORY =>
            cnt += 1
            scoreNode(node, depth)
            print(cnt + ". " + node.uri + "   (")
            (1 to depth) foreach (d => {
              print(node.getProperty(TOPIC_CLOSENESS_SCORE + d, 0) + ",")
            })
            println(")")
          case _ =>
        }
      }
      catch {
        case _: Exception =>
      }
    }

    println("Topic Closeness Scored Nodes: " + cnt)
  }

  def printScores() {
    val nodes = GlobalGraphOperations.at(gdb).getAllNodes
    nodes foreach (node => {
      node.getPropertyKeys foreach (key => {
        if (key.startsWith(TOPIC_CLOSENESS_SCORE)) {
          val score = node.getProperty(key).asInstanceOf[Int]
          val on = new OntologyNode(node)
          println(on.uri + " " + key + ": " + score)
        }

      })
    })
  }

  def printScoreRange(depth: Int = 2, printUri: Boolean = false, lowScorePrint: Int = 0, highScorePrint: Int = Int.MaxValue) {
    val scoreMap = new mutable.HashMap[Int, Int]()
    val nodes = GlobalGraphOperations.at(gdb).getAllNodes

    val interestingNodes = new mutable.HashMap[OntologyNode, Double]()

    var totalScoredNodes = 0
    nodes foreach (n => {
      if (n.hasProperty(TOPIC_CLOSENESS_SCORE + depth)) {
        totalScoredNodes += 1
        val score = n.getProperty(TOPIC_CLOSENESS_SCORE + depth).asInstanceOf[Int]
        val count = scoreMap.getOrElse(score, 0)
        scoreMap.put(score, count + 1)

        if (score > lowScorePrint && score < highScorePrint && printUri) {
          val o = new OntologyNode(n)
          interestingNodes.put(o, score)
        }
      }

    })

    var checkCount = 0
    scoreMap.toList sortBy {
      _._1
    } foreach (kv => {
      println("Score Key: " + kv._1 + " %: " + (100D * kv._2 / totalScoredNodes) + " Value: " + kv._2)
      checkCount += kv._2
    })

    println("Total Scored Nodes: " + totalScoredNodes)
    if (checkCount != totalScoredNodes) {
      println("!! Sum of counts: " + checkCount + " does NOT match total scored nodes !!")
    }

    println("")
    println("Interesting Nodes")
    interestingNodes.toList sortBy {
      _._2
    } foreach (entry => {
      println("score: " + entry._2 + "  Id: " + entry._1.node.getId + "  uri: " + entry._1.uri)
    })

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

  private def searchForTopics(startNode: OntologyNode, depth: Int): TopicClosenessResult = {
    val topicClosenessScoreResult = new TopicClosenessResult(new mutable.HashMap[Int, Int]())
    val trav = Traversal.description.breadthFirst()
      .evaluator(topicEvaluator(depth + 1, topicClosenessScoreResult))
      .relationships(TopicRelationshipTypes.CONCEPT_OF_TOPIC, Direction.INCOMING)
      .relationships(TopicRelationshipTypes.BROADER_CONCEPT, Direction.INCOMING)
      .relationships(TopicRelationshipTypes.INTEREST_OF, Direction.INCOMING)
      .relationships(TopicRelationshipTypes.REDIRECTS_TO, Direction.INCOMING)
      .relationships(TopicRelationshipTypes.DISAMBIGUATES, Direction.INCOMING)
      .relationships(ConceptNodes.GRAV_AUTO_BROADER_CONCEPT, Direction.INCOMING)
      .relationships(ConceptNodes.GRAV_AUTO_CONCEPT_OF_TOPIC, Direction.INCOMING)
      .traverse(startNode.node)

    trav foreach (n => {})
    topicClosenessScoreResult
  }

  private def scoreNode(node: OntologyNode, depth: Int) {
    val topicClosenessScoreResult = searchForTopics(node, depth)
    topicClosenessScoreResult.scoreMap.iterator foreach (entry => {
      setScore(node.node, TOPIC_CLOSENESS_SCORE + entry._1, entry._2)
    })
  }

  private def setScore(node: Node, key: String, score: Int) {
    val tx = gdb.beginTx()
    try {
      node.setProperty(key, score)
      tx.success()
    }
    catch {
      case e: Exception => throw new Exception("Unable to set score property")
    }
    finally {
      tx.finish()
    }
  }


}

case class TopicClosenessResult(scoreMap: mutable.HashMap[Int, Int]) {
  def incrementDepth(depth: Int) {
    val curScore = scoreMap.getOrElse(depth, 0)
    scoreMap.put(depth, curScore + 1)
  }
}


object DegreeScore {
  // New attribute name = OntologyNodeScore.OutEdges
  // TBD: remove references to OUT_DEGREE once we have transitioned
  val OUT_DEGREE = "GRAV_OutDegree"
  val IN_MINUS_OUT_DEGREE = "GRAV_InMinusOutDegree"

  private val gdb = OntologyGraph2.graph.graphDb

  def scoreGraph(maxNodesToScore: Int = Int.MaxValue) {

    val iterator = GlobalGraphOperations.at(gdb).getAllNodes.iterator()
    var cnt = 0
    var scored = 0
    while (cnt < maxNodesToScore && iterator.hasNext) {
      cnt += 1
      val node = new OntologyNode(iterator.next())
      try {
        node.uriType match {
          case URIType.WIKI_CATEGORY =>
            scoreNode(node)
            scored += 1
          case _ =>
        }
      }
      catch {
        case _: Exception =>
      }
    }

    println("Degree Scored Concept Nodes: " + scored)
  }

  def printScores() {
    val nodes = GlobalGraphOperations.at(gdb).getAllNodes

    var scoredNodes = 0
    nodes foreach (node => {
      if (node.hasProperty(OUT_DEGREE) || node.hasProperty(OntologyNodeScore.OutEdges)) {
        scoredNodes += 1
        val score = ConceptNodes.getScore(node, OUT_DEGREE) + ConceptNodes.getScore(node, OntologyNodeScore.OutEdges)
        val on = new OntologyNode(node)
        println(on.uri + " " + score)
      }
    })

    println("Scored Nodes: " + scoredNodes)
  }

  def printScoreRange(printUri: Boolean = false, lowScorePrint: Int = 0, highScorePrint: Int = Int.MaxValue) {
    val scoreMap = new mutable.HashMap[Int, Int]()
    val nodes = GlobalGraphOperations.at(gdb).getAllNodes

    val interestingNodes = new mutable.HashMap[OntologyNode, Double]()

    var totalScoredNodes = 0
    nodes foreach (n => {

      if (n.hasProperty(OUT_DEGREE) || n.hasProperty(OntologyNodeScore.OutEdges)) {
        totalScoredNodes += 1
        val score = ConceptNodes.getScore(n, OUT_DEGREE) + ConceptNodes.getScore(n, OntologyNodeScore.OutEdges)
        val count = scoreMap.getOrElse(score, 0)
        scoreMap.put(score, count + 1)

        if (score > lowScorePrint && score < highScorePrint && printUri) {
          val o = new OntologyNode(n)
          interestingNodes.put(o, score)
        }
      }

    })

    var checkCount = 0
    scoreMap.toList sortBy {
      _._1
    } foreach (kv => {
      println("Score Key: " + kv._1 + " %: " + (100D * kv._2 / totalScoredNodes) + " Value: " + kv._2)
      checkCount += kv._2
    })

    println("Total Scored Nodes: " + totalScoredNodes)
    if (checkCount != totalScoredNodes) {
      println("!! Sum of counts: " + checkCount + " does NOT match total scored nodes !!")
    }

    println("")
    println("Interesting Nodes")
    interestingNodes.toList sortBy {
      _._2
    } foreach (entry => {
      println("score: " + entry._2 + "  Id: " + entry._1.node.getId + "  uri: " + entry._1.uri)
    })
  }

  private def scoreNode(node: OntologyNode) {
    val in = node.node.getRelationships(Direction.INCOMING).size
    val out = node.node.getRelationships(Direction.OUTGOING).size

    setScore(node.node, in + out, OUT_DEGREE)
    setScore(node.node, in - out, IN_MINUS_OUT_DEGREE)
  }

  private def setScore(node: Node, score: Int, attributeName: String) {
    val tx = gdb.beginTx()
    try {
      node.setProperty(attributeName, score)
      tx.success()
    }
    catch {
      case e: Exception => throw new Exception("Unable to set " + attributeName)
    }
    finally {
      tx.finish()
    }
  }


}

object ReachabilityAlgo {
  val KNOWN_MAX_TOPICS = 678
  val INTEREST_SCORE = "GRAV_InterestClosenessScore"
  private val gdb = OntologyGraph2.graph.graphDb
  private val graph = OntologyGraph2.graph

  val debugPrint = true

  private def println(str: String) {
    if (debugPrint)
      Console.println(str)
  }

  private def print(str: String) {
    if (debugPrint)
      Console.print(str)
  }

  def dumpSelectedNodeProperties(node: OntologyNode, properties: Seq[String] =
  Seq(TopicClosenessScore.TOPIC_CLOSENESS_SCORE + 1,
    TopicClosenessScore.TOPIC_CLOSENESS_SCORE + 2,
    TopicClosenessScore.TOPIC_CLOSENESS_SCORE + 3,
    TopicClosenessScore.TOPIC_CLOSENESS_SCORE + 4,
    ConceptHierarchy.PARENT_ATTRIBUTE_SCORE + 1,
    ConceptHierarchy.PARENT_ATTRIBUTE_SCORE + 2,
    ConceptHierarchy.PARENT_ATTRIBUTE_SCORE + 3,
    ConceptHierarchy.PARENT_ATTRIBUTE_SCORE + 4,
    ConceptHierarchy.CHILD_ATTRIBUTE_SCORE + 1,
    ConceptHierarchy.CHILD_ATTRIBUTE_SCORE + 2,
    ConceptHierarchy.CHILD_ATTRIBUTE_SCORE + 3,
    ConceptHierarchy.CHILD_ATTRIBUTE_SCORE + 4)) {

    properties.foreach(key => {
      dumpNodeProperty(node, key, " ")
    })


  }


  def dumpSelectedNodePropertiesByUri(uri: String, properties: Seq[String] =
  Seq(TopicClosenessScore.TOPIC_CLOSENESS_SCORE + 1,
    TopicClosenessScore.TOPIC_CLOSENESS_SCORE + 2,
    TopicClosenessScore.TOPIC_CLOSENESS_SCORE + 3,
    TopicClosenessScore.TOPIC_CLOSENESS_SCORE + 4,
    ConceptHierarchy.PARENT_ATTRIBUTE_SCORE + 1,
    ConceptHierarchy.PARENT_ATTRIBUTE_SCORE + 2,
    ConceptHierarchy.PARENT_ATTRIBUTE_SCORE + 3,
    ConceptHierarchy.PARENT_ATTRIBUTE_SCORE + 4,
    ConceptHierarchy.CHILD_ATTRIBUTE_SCORE + 1,
    ConceptHierarchy.CHILD_ATTRIBUTE_SCORE + 2,
    ConceptHierarchy.CHILD_ATTRIBUTE_SCORE + 3,
    ConceptHierarchy.CHILD_ATTRIBUTE_SCORE + 4)) {
    val u = NS.fromString(uri)
    graph.node(u) match {
      case Some(on) =>
        dumpSelectedNodeProperties(on, properties)
      case None =>

    }
  }

  def dumpNodeProperties(uri: String) {
    val u = NS.fromString(uri)
    graph.node(u) match {
      case Some(on) =>
        on.node.getPropertyKeys foreach (key => {
          dumpNodeProperty(on, key)
        })
      case None =>
        println("Node not found")
    }
  }

  def dumpNodeProperty(on: OntologyNode, key: String, suffix: String = "\n") {

    if (on.node.hasProperty(key)) {
      try {
        print(key + ": " + on.node.getProperty(key).asInstanceOf[Int] + suffix)
      }
      catch {
        case _: Exception =>
          try {
            print(key + ": " + on.node.getProperty(key).asInstanceOf[Double] + suffix)
          }
          catch {
            case _: Exception =>
              try {
                print(key + ": " + on.node.getProperty(key).asInstanceOf[String] + suffix)
              }
              catch {
                case _: Exception =>
              }
          }
      }
    }
    else {
      print(key + ": 0" + suffix)
    }

  }

  def quickDump(graphName: String,
                nodeUri: String,
                depth: Int = 2,
                travMoreGeneral: Boolean = true,
                allRelationships: Boolean = false,
                showPaths: Boolean = false) {

    val g = OntologyGraphMgr.getInstance().getGraph(graphName)
    val og = new OntologyGraph2(g)
    for (n <- og.node(MurmurHash.hash64(nodeUri))) {
      dumpNode(n, depth, false, travMoreGeneral, allRelationships, showPaths)
    }

  }

  def dumpNodeByUri(nodeUri: String,
                    depth: Int = 2,
                    dumpOnlyInterestNodes: Boolean = true,
                    travMoreGeneral: Boolean = true,
                    allRelationships: Boolean = false,
                    showPaths: Boolean = false
                     ) {
    val u = NS.fromString(nodeUri)
    graph.node(u) match {
      case Some(on) =>
        dumpNode(on, depth, dumpOnlyInterestNodes, travMoreGeneral, allRelationships, showPaths)
      case None =>
        println("Node not found")
    }
  }

  def dumpNodeById(nodeId: Long,
                   depth: Int = 2,
                   dumpOnlyInterestNodes: Boolean = false,
                   travMoreGeneral: Boolean = true,
                   allRelationships: Boolean = false,
                   showPaths: Boolean = false) {
    val og = new OntologyGraph2()
    val ontNode = og.getNodeByNodeId(nodeId)
    ontNode match {
      case Some(on) =>
        dumpNode(on, depth, dumpOnlyInterestNodes, travMoreGeneral, allRelationships, showPaths)
      case _ =>
        println("Node not found")
    }
  }


  def traverseAroundNode(startNodeUri: String, depth: Int, direction: Direction): Set[Path] = {
    traverseAroundNode(graph.node(NS.fromString(startNodeUri)), depth, direction)
  }

  def traverseAroundNode(startNodeId: Long, depth: Int, direction: Direction): Set[Path] = {
    traverseAroundNode(graph.node(startNodeId), depth, direction)
  }

  def traverseAroundNode(startOntNode: Option[OntologyNode], depth: Int, direction: Direction): Set[Path] = {
    val pathSet = new mutable.HashSet[Path]()
    startOntNode match {
      case Some(startNode) =>
        val t = Traversal.description.breadthFirst()
          .evaluator(TraversalFilters.DepthEval(depth + 1))
          .relationships(TopicRelationshipTypes.CONCEPT_OF_TOPIC, direction)
          .relationships(TopicRelationshipTypes.BROADER_CONCEPT, direction)
          .relationships(ConceptNodes.GRAV_AUTO_BROADER_CONCEPT, direction)
          .relationships(ConceptNodes.GRAV_AUTO_CONCEPT_OF_TOPIC, direction)
        //          .relationships(TopicRelationshipTypes.INTEREST_OF, dir)
        //          .relationships(TopicRelationshipTypes.REDIRECTS_TO, dir)
        //          .relationships(TopicRelationshipTypes.DISAMBIGUATES, dir)

        val trav = t.traverse(startNode.node)
        trav foreach (p => {
          pathSet.add(p)
        })
      case None =>
      //println("Node not found")
    }
    pathSet
  }


  def dumpNode(ontNode: OntologyNode,
               depth: Int = 2,
               dumpOnlyInterestNodes: Boolean = false,
               travMoreGeneral: Boolean = true,
               allRelationships: Boolean = false,
               showPaths: Boolean = false) {

    println("NodeId: " + ontNode.node.getId + " uri: " + ontNode.uri)

    val dir = if (travMoreGeneral) Direction.OUTGOING else Direction.INCOMING

    var t = Traversal.description.breadthFirst()
      .evaluator(TraversalFilters.DepthEval(depth + 1))

    if (!allRelationships) {
      t = t
        .relationships(TopicRelationshipTypes.CONCEPT_OF_TOPIC, dir)
        .relationships(TopicRelationshipTypes.BROADER_CONCEPT, dir)
        .relationships(TopicRelationshipTypes.INTEREST_OF, dir)
        .relationships(TopicRelationshipTypes.REDIRECTS_TO, dir)
        .relationships(TopicRelationshipTypes.DISAMBIGUATES, dir)
        .relationships(ConceptNodes.GRAV_AUTO_BROADER_CONCEPT, dir)
        .relationships(ConceptNodes.GRAV_AUTO_CONCEPT_OF_TOPIC, dir)
    }

    val nodeGroup = new mutable.HashMap[String, mutable.ListBuffer[(OntologyNode, Path)]]()

    val trav = t.traverse(ontNode.node)

    var prevDepth = 0
    var interestCount = 0
    var maxDepth = 0

    trav foreach (p => {
      val o = new OntologyNode(p.endNode())

      if (p.length() != prevDepth) {
        println("")
        println("Depth = " + p.length())
        prevDepth = p.length()
      }

      val parentScoreKeys = o.node.getPropertyKeys.filter(key => key.startsWith(ConceptHierarchy.PARENT_ATTRIBUTE_SCORE)).toList

      val parentDepth =
        if (parentScoreKeys.size > 0)
          parentScoreKeys.map(key => key.replace(ConceptHierarchy.PARENT_ATTRIBUTE_SCORE, "").toInt).sortBy(i => i).last
        else
          0

      maxDepth = scala.math.max(parentDepth, maxDepth)

      // print super node
      var nodePrinted = false
      if (parentDepth > 0) {
        val h = "   L" + parentDepth + " (" + o.node.getProperty(ConceptHierarchy.PARENT_ATTRIBUTE_SCORE + parentDepth) + ")"
        print(h + "   Depth: " + p.length() + " NodeId: " + o.node.getId + " uri: " + o.uri)
        nodePrinted = true
        val list = nodeGroup.getOrElse("L" + parentDepth, new mutable.ListBuffer[(OntologyNode, Path)]())
        list.append((o, p))
        nodeGroup.put("L" + parentDepth, list)
      }

      // print interest node
      if (o.uriType == URIType.GRAVITY_INTEREST) {
        print("   I  Depth: " + p.length() + " NodeId: " + o.node.getId + " uri: " + o.uri)
        interestCount += 1
        nodePrinted = true
        val list = nodeGroup.getOrElse("I", new mutable.ListBuffer[(OntologyNode, Path)]())
        list.append((o, p))
        nodeGroup.put("I", list)
      }

      if (nodePrinted && showPaths) {
        println("      Path:" + p)
        printPath(p)
      }


      if (!nodePrinted && !dumpOnlyInterestNodes) {
        val r = p.lastRelationship()
        val rName = if (r == null) "null" else r.getType.name()
        print("      Depth: " + p.length() + " NodeId: " + o.node.getId + " uri: " + o.uri + " via " + rName)
        val list = nodeGroup.getOrElse("O", new mutable.ListBuffer[(OntologyNode, Path)]())
        list.append((o, p))
        nodeGroup.put("O", list)

      }

      print(" ")
      dumpSelectedNodeProperties(o)
      //      dumpSelectedNodePropertiesByUri(o.uri)
      println("")
    })

    println("Reachable Interests: " + interestCount)


    println("")

    val LL = (for (l <- (1 to maxDepth)) yield "L" + l).toList

    "I" :: LL foreach (g => {
      println(g)
      printHelper(nodeGroup.getOrElse(g, new mutable.ListBuffer[(OntologyNode, Path)]()))
    })


  }

  private def printHelper(nodePathCol: mutable.ListBuffer[(OntologyNode, Path)]) {
    val sortedCol = nodePathCol.sortBy(op => (10000 * op._2.length()) +
      (-1 * op._1.node.getProperty(ConceptHierarchy.PARENT_ATTRIBUTE_SCORE + op._2.length(), 0).asInstanceOf[Int])
    )

    sortedCol foreach (np => {
      val depth = np._2.length()
      val score = np._1.node.getProperty(ConceptHierarchy.PARENT_ATTRIBUTE_SCORE + depth, 0)
      val uri = np._1.uri
      println("\t" + score + "\t" + depth + "\t" + uri)
    })
  }

  def getReadablePath(path: Path): String = {
    path.nodes().foldRight("")(new OntologyNode(_).uri + "->" + _)
  }

  def printPath(p: Path) {
    p.relationships() foreach (rel => {
      val oEnd = new OntologyNode(rel.getEndNode)
      var weight = ""
      if (rel.hasProperty(BroaderConceptWeights.WEIGHT)) {
        weight = "  [weight: " + rel.getProperty(BroaderConceptWeights.WEIGHT).asInstanceOf[Float] + "]"
      }
      println("         id: " + oEnd.node.getId + " = " + oEnd.uri + weight)
    })
  }

  def scoreGraphForNode(uri: String, depth: Int = Int.MaxValue) {
    val u = NS.fromString(uri)

    graph.node(u) match {
      case Some(node) =>
        val iNodes = new mutable.ArrayBuffer[OntologyNode](1)
        iNodes.append(node)
        ReachabilityAlgo.traverseAndSetScore(new ReachabilityAlgoInput(iNodes, depth))
      case None =>
        println("Node not found: " + uri)
    }
  }

  def scoreGraph(depth: Int, maxInterestNodesToScore: Int = KNOWN_MAX_TOPICS) {
    val iNodes = ReachabilityAlgo.getInterestNodes(maxInterestNodesToScore)
    ReachabilityAlgo.traverseAndSetScore(new ReachabilityAlgoInput(iNodes, depth))
  }

  def printScoreRange(printUri: Boolean = false, lowScorePrint: Double = 2.0, highScorePrint: Double = Double.MaxValue) {
    val scoreMap = new mutable.HashMap[Int, Int]()
    val nodes = GlobalGraphOperations.at(gdb).getAllNodes

    val interestingNodes = new mutable.HashMap[OntologyNode, Double]()

    var totalScoredNodes = 0
    nodes foreach (n => {
      if (n.hasProperty(ReachabilityAlgo.INTEREST_SCORE)) {
        totalScoredNodes += 1
        val score = n.getProperty(ReachabilityAlgo.INTEREST_SCORE).asInstanceOf[Double]
        val count = scoreMap.getOrElse(score.toInt, 0)
        scoreMap.put(score.toInt, count + 1)

        if (score > lowScorePrint && score < highScorePrint && printUri) {
          val o = new OntologyNode(n)
          interestingNodes.put(o, score)
        }
      }

    })

    var checkCount = 0
    scoreMap foreach (kv => {
      println("Score Key: " + kv._1 + " %: " + (100D * kv._2 / totalScoredNodes) + " Value: " + kv._2)
      checkCount += kv._2
    })

    println("Total Scored Nodes: " + totalScoredNodes)
    if (checkCount != totalScoredNodes) {
      println("!! Sum of counts: " + checkCount + " does NOT match total scored nodes !!")
    }

    println("")
    println("Interesting Nodes")
    interestingNodes.toList sortBy {
      _._2
    } foreach (entry => {
      println("score: " + entry._2 + "  Id: " + entry._1.node.getId + "  uri: " + entry._1.uri)
    })
  }

  def printReachableScoredOrInterestNodes(depth: Int, maxTopicNodes: Int = Int.MaxValue, printUnreachable: Boolean = false) {
    val iterator = GlobalGraphOperations.at(gdb).getAllNodes.iterator()
    var reachableCnt = 0
    var topicNodeCnt = 0

    while (topicNodeCnt < maxTopicNodes && iterator.hasNext) {
      try {
        val o = new OntologyNode(iterator.next())
        val uriType = o.uriType
        uriType match {
          case URIType.TOPIC =>
            topicNodeCnt += 1

            if (topicNodeCnt % 1000000 == 0) {
              println("Processed " + topicNodeCnt + " topics")
            }

            val isReachable = isReachableToScoredOrInterestNode(o, depth)
            if (isReachable) {
              reachableCnt += 1
            }
            else if (printUnreachable) {
              println("Unreachable Topic: " + o.uri)
            }
          case _ =>
        }
      }
      catch {
        case _: Exception =>
      }
    }

    System.out.println("Total Topic Nodes: " + topicNodeCnt)
    System.out.println("Reachable Topic Nodes: " + reachableCnt)
  }


  private def reachableEvaluator(stopDepth: Int, topicReachable: TopicReachableResult) = NodeEval {
    path => {
      var evaluation = Evaluation.EXCLUDE_AND_CONTINUE

      if (path.length() >= stopDepth) {
        evaluation = Evaluation.INCLUDE_AND_PRUNE
      }
      else {
        val ontEndNode = new OntologyNode(path.endNode())

        try {
          ontEndNode.uriType match {
            case URIType.WIKI_CATEGORY =>
              if (ontEndNode.node.hasProperty(INTEREST_SCORE)) {
                topicReachable.reachable = true
                evaluation = Evaluation.INCLUDE_AND_PRUNE
              }
            case URIType.GRAVITY_INTEREST =>
              topicReachable.reachable = true
              evaluation = Evaluation.INCLUDE_AND_PRUNE
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

  private def isReachableToScoredOrInterestNode(startNode: OntologyNode, depth: Int): Boolean = {
    val topicReachableResult = new TopicReachableResult()
    val trav = Traversal.description.breadthFirst()
      .evaluator(reachableEvaluator(depth + 1, topicReachableResult))
      .relationships(TopicRelationshipTypes.CONCEPT_OF_TOPIC, Direction.OUTGOING)
      .relationships(TopicRelationshipTypes.BROADER_CONCEPT, Direction.OUTGOING)
      .relationships(TopicRelationshipTypes.INTEREST_OF, Direction.OUTGOING)
      .relationships(TopicRelationshipTypes.REDIRECTS_TO, Direction.OUTGOING)
      .relationships(TopicRelationshipTypes.DISAMBIGUATES, Direction.OUTGOING)
      .relationships(ConceptNodes.GRAV_AUTO_BROADER_CONCEPT, Direction.OUTGOING)
      .relationships(ConceptNodes.GRAV_AUTO_CONCEPT_OF_TOPIC, Direction.OUTGOING)
      .traverse(startNode.node)

    trav foreach (n => {})
    topicReachableResult.reachable
  }


  def traverseAndSetScore(input: ReachabilityAlgoInput) {
    var result = new ReachabilityAlgoResult(input)
    var nodeCount = 1
    input.startInterestNodes foreach (interestNode => {
      println(Calendar.getInstance.getTime + " Traversing Node " + nodeCount + ": " + interestNode.uri)
      val before = result.scoredNodeCount
      scoreReachableNodes(interestNode, input.maxDepth, result)
      if (debugPrint) {
        println("Scored Node Count (New): " + (result.scoredNodeCount - before))
        result.print()
      }


      nodeCount += 1
    })

    result
  }

  def getCountOfScoredNodes(): Int = {
    val nodes = GlobalGraphOperations.at(gdb).getAllNodes
    nodes.count(n => n.hasProperty(INTEREST_SCORE))
  }

  def printScores(scoreName: String = INTEREST_SCORE) {
    val nodes = GlobalGraphOperations.at(gdb).getAllNodes

    var maxScore = 0D
    var maxNodeUri = ""
    nodes foreach (n => {
      if (n.hasProperty(scoreName)) {
        val o = new OntologyNode(n)
        val score = n.getProperty(scoreName).asInstanceOf[Double]
        System.out.println(o.toString + " : " + score)
        if (score > maxScore) {
          maxNodeUri = o.uri
          maxScore = score
        }
      }
    })
    System.out.println("Max Score: " + maxScore)
    System.out.println("Uri: " + maxNodeUri)

  }

  def clearReachableScore() {
    val nodes = GlobalGraphOperations.at(gdb).getAllNodes

    nodes foreach (n => {
      if (n.hasProperty(INTEREST_SCORE)) {
        val tx = gdb.beginTx()
        try {
          n.removeProperty(INTEREST_SCORE)
          tx.success()
        }
        catch {
          case e: Exception =>
        }
        finally {
          tx.finish()
        }
      }
    })
  }

  private def scoreReachableNodes(startNode: OntologyNode, depth: Int, partialResult: ReachabilityAlgoResult) {
    val trav = Traversal.description.breadthFirst()
      .evaluator(TraversalFilters.DepthEval(depth + 1))
      .evaluator(scoreEvaluator(partialResult))
      .relationships(TopicRelationshipTypes.CONCEPT_OF_TOPIC, Direction.INCOMING)
      .relationships(TopicRelationshipTypes.BROADER_CONCEPT, Direction.INCOMING)
      .relationships(TopicRelationshipTypes.INTEREST_OF, Direction.INCOMING)
      .relationships(TopicRelationshipTypes.REDIRECTS_TO, Direction.INCOMING)
      .relationships(TopicRelationshipTypes.DISAMBIGUATES, Direction.INCOMING)
      .relationships(ConceptNodes.GRAV_AUTO_BROADER_CONCEPT, Direction.INCOMING)
      .relationships(ConceptNodes.GRAV_AUTO_CONCEPT_OF_TOPIC, Direction.INCOMING)
      .traverse(startNode.node)

    trav foreach (_ => {})
  }


  def traverse(input: ReachabilityAlgoInput): ReachabilityAlgoResult = {
    val result = new ReachabilityAlgoResult(input)

    var nodeCount = 1
    input.startInterestNodes foreach (interestNode => {
      println(Calendar.getInstance.getTime + " Traversing Node " + nodeCount + ": " + interestNode.uri)
      //result = new ReachabilityAlgoResult(input)
      collectReachableNodes(interestNode, input.maxDepth, result)
      if (debugPrint) result.print()

      nodeCount += 1
    })

    result
  }

  def getInterestNodes(maxInterestNodes: Int = Int.MaxValue): mutable.ArrayBuffer[OntologyNode] = {
    val iterator = GlobalGraphOperations.at(gdb).getAllNodes.iterator()
    val nodeList = new mutable.ArrayBuffer[OntologyNode]()

    while (nodeList.size < maxInterestNodes && iterator.hasNext) {
      val node = new OntologyNode(iterator.next())
      if (node.uriType == URIType.GRAVITY_INTEREST) {
        nodeList.append(node)
      }
    }
    nodeList
  }

  private def collectReachableNodes(startNode: OntologyNode, depth: Int, partialResult: ReachabilityAlgoResult) {

    val trav = Traversal.description.breadthFirst()
      .evaluator(TraversalFilters.DepthEval(depth + 1))
      .evaluator(alreadyVisitedNodes(partialResult))
      .relationships(TopicRelationshipTypes.CONCEPT_OF_TOPIC, Direction.INCOMING)
      .relationships(TopicRelationshipTypes.BROADER_CONCEPT, Direction.INCOMING)
      .relationships(TopicRelationshipTypes.INTEREST_OF, Direction.INCOMING)
      .relationships(TopicRelationshipTypes.REDIRECTS_TO, Direction.INCOMING)
      .relationships(TopicRelationshipTypes.DISAMBIGUATES, Direction.INCOMING)
      .relationships(ConceptNodes.GRAV_AUTO_BROADER_CONCEPT, Direction.INCOMING)
      .relationships(ConceptNodes.GRAV_AUTO_CONCEPT_OF_TOPIC, Direction.INCOMING)
      .traverse(startNode.node)

    val n = partialResult.newNodeDiscovered
    val v = partialResult.alreadyVisitedNode
    val s = partialResult.smallerPathPromotionCount

    //    val it = trav.nodes().iterator()
    //    while(it.hasNext) it.next()


    trav foreach (_ => {})

    //    trav foreach (path => {
    //      //if (dot % 10000 == 0) print(".")
    //      //dot +=1
    //    })

    //    if (debugPrint)
    //      partialResult.print()

    val dn = partialResult.newNodeDiscovered - n
    val dv = partialResult.alreadyVisitedNode - v
    val ds = partialResult.smallerPathPromotionCount - s
    //
    //    println("")
    //    println("traveresed " + (dn+dv+ds) + " nodes")
  }

  private def alreadyVisitedNodes(partialResult: ReachabilityAlgoResult) = NodeEval {
    path => {
      var evaluation = Evaluation.INCLUDE_AND_CONTINUE

      if (path.nodes().size == 1) {
        // root node
        evaluation = Evaluation.EXCLUDE_AND_CONTINUE
      }
      else {
        val node = new OntologyNode(path.endNode())
        val discoveredPathLen = path.length()

        partialResult.reachableSet.get(node) match {

          case None =>
            // new node discovered
            partialResult.incrementMetrics(discoveredPathLen, node)
            partialResult.reachableSet.put(node, path)
            partialResult.newNodeDiscovered += 1
            if (partialResult.newNodeDiscovered % 10000 == 0)
              print(".")

          case Some(existingPath) =>
            // node has already been visited
            evaluation = Evaluation.EXCLUDE_AND_CONTINUE

            partialResult.collisionCount += 1
            val existingDepth = existingPath.length()

            if (discoveredPathLen < existingDepth) {
              // smaller path found

              partialResult.decrementMetrics(existingDepth, node)
              partialResult.incrementMetrics(discoveredPathLen, node)

              // update reachable set with new path
              partialResult.reachableSet.put(node, path)
              partialResult.smallerPathPromotionCount += 1

              if (partialResult.smallerPathPromotionCount % 10000 == 0)
                print("S")
            }
            else {
              // already visited node
              partialResult.alreadyVisitedNode += 1

              if (partialResult.alreadyVisitedNode % 10000 == 0)
                print("V")
            }
        }
      }
      evaluation
    }
  }

  private def updateScore(path: Path, result: ReachabilityAlgoResult) {
    val endNode = path.endNode()
    var curScore = 0.0

    if (endNode.hasProperty(INTEREST_SCORE)) {
      try {
        curScore = endNode.getProperty(INTEREST_SCORE).asInstanceOf[Double]
      }
      catch {
        case e: Exception =>
      }
    }
    else {
      result.newNodeDiscovered += 1
    }

    val depth = path.length()
    val newScore = curScore + getScore(depth)

    val tx = gdb.beginTx()
    try {
      endNode.setProperty(INTEREST_SCORE, newScore)
      tx.success()
      result.scoredNodeCount += 1
    }
    catch {
      case e: Exception => throw new Exception("Unable to set score property")
    }
    finally {
      tx.finish()
    }

  }

  private def getScore(depth: Int): Double = {
    1 / scala.math.pow(depth, 2)
  }


  private def scoreEvaluator(result: ReachabilityAlgoResult) = NodeEval {
    path => {
      var evaluation = Evaluation.EXCLUDE_AND_CONTINUE
      val ontEndNode = new OntologyNode(path.endNode())

      try {
        ontEndNode.uriType match {
          case URIType.WIKI_CATEGORY =>
            evaluation = Evaluation.INCLUDE_AND_CONTINUE
            updateScore(path, result)

          case _ =>
        }
      }
      catch {
        case _: Exception =>
      }
      evaluation
    }
  }
}

case class ReachabilityAlgoInput(startInterestNodes: Iterable[OntologyNode], maxDepth: Int)

case class DepthMetrics(var reachableCategoryCount: Int = 0, var reachableTopicCount: Int = 0, var unknownCount: Int = 0) {
  def incrementMetrics(node: OntologyNode) {
    getUriType(node) match {
      case URIType.WIKI_CATEGORY => reachableCategoryCount += 1
      case URIType.TOPIC => reachableTopicCount += 1
      case _ => unknownCount += 1
    }
  }

  def decrementMetrics(node: OntologyNode) {
    getUriType(node) match {
      case URIType.WIKI_CATEGORY => reachableCategoryCount -= 1
      case URIType.TOPIC => reachableTopicCount -= 1
      case _ => unknownCount -= 1
    }
  }

  private def getUriType(node: OntologyNode): URIType = {
    var uriType = URIType.UNKNOWN
    try {
      val nut = node.uriType
      if (nut == URIType.WIKI_CATEGORY || nut == URIType.TOPIC) {
        uriType = nut
      }
    }
    catch {
      case e: Exception =>
    }
    uriType
  }

}

case class ReachabilityAlgoResult(algoInput: ReachabilityAlgoInput,
                                  reachableSet: mutable.HashMap[OntologyNode, Path] = new mutable.HashMap[OntologyNode, Path]()) {
  var collisionCount = 0
  var smallerPathPromotionCount = 0
  val metricsMap = new mutable.HashMap[Int /*depth*/ , DepthMetrics]()
  var alreadyVisitedNode = 0
  var newNodeDiscovered = 0
  var scoredNodeCount = 0

  def print() {

    var totalNodes = 0
    metricsMap.keys foreach (depthKey => {
      val metrics = metricsMap.getOrElse(depthKey, new DepthMetrics())

      System.out.println("")
      System.out.println("  Depth " + depthKey)
      System.out.println("     Cat: " + metrics.reachableCategoryCount)
      System.out.println("     Topic: " + metrics.reachableTopicCount)
      System.out.println("     Unknown: " + metrics.unknownCount)
      totalNodes += metrics.unknownCount + metrics.reachableCategoryCount + metrics.reachableTopicCount
      System.out.println("     *Total at depth " + depthKey + ": " + (metrics.unknownCount + metrics.reachableCategoryCount + metrics.reachableTopicCount))
      System.out.println("")
    })

    //System.out.println("Total collisions: " + collisionCount)
    //System.out.println("Total smaller path promotions: " + smallerPathPromotionCount)
    //System.out.println("*Total reachable nodes: " + reachableSet.size)
    System.out.println("Scored Node Count: " + scoredNodeCount)
    System.out.println("Newly Discovered Nodes: " + newNodeDiscovered)
    System.out.println("")

  }

  def incrementMetrics(depth: Int, node: OntologyNode) {
    metricsMap.get(depth) match {
      case Some(metrics) =>
        metrics.incrementMetrics(node)
      case None =>
        val newMetrics = new DepthMetrics()
        newMetrics.incrementMetrics(node)
        metricsMap.put(depth, newMetrics)
    }
  }

  def decrementMetrics(depth: Int, node: OntologyNode) {
    metricsMap.get(depth) match {
      case Some(metrics) =>
        metrics.decrementMetrics(node)
      case None =>
        throw new Exception("attempt to decrement from nonexisting metrics")
    }
  }
}

case class TopicReachableResult(var reachable: Boolean = false)


object NearestTopicScore {

  private val gdb = OntologyGraph2.graph.graphDb

  def getNodeCount(): Int = {
    val nodes = GlobalGraphOperations.at(gdb).getAllNodes
    nodes.size
  }

  def getCountByNodeType(): mutable.HashMap[String, Int] = {
    var topicCnt = 0
    var interestCnt = 0
    var categoryCnt = 0
    var unknownCnt = 0

    val nodes = GlobalGraphOperations.at(gdb).getAllNodes

    nodes foreach (n => {
      try {
        val uriType = new OntologyNode(n).uriType
        uriType match {
          case URIType.WIKI_CATEGORY => categoryCnt += 1
          case URIType.TOPIC => topicCnt += 1
          case URIType.GRAVITY_INTEREST => interestCnt += 1
          case _ => unknownCnt += 1
        }
      }
      catch {
        case _: Exception => unknownCnt += 1
      }
    })

    val result = new mutable.HashMap[String, Int]()
    result.put("Category", categoryCnt)
    result.put("Topic", topicCnt)
    result.put("Interest", interestCnt)
    result.put("Unknown", unknownCnt)
    result.put("Total", categoryCnt + topicCnt + interestCnt + unknownCnt)
    result
  }

  def writeToFile(filename: String, nodes: Iterable[OntologyNode]) {
    lazy val out = new FileWriter(filename)

    try {
      wline("InterestNodes generated " + new DateTime())
      wline("size " + nodes.size)
      var i = 0
      nodes foreach (n => {
        wline((i + 1) + " " + n.node.getId + " " + getUri(n))
        i += 1
      })

    }
    finally {
      out.close()
    }

    def getUri(node: OntologyNode): String = {
      try {
        node.uri
      }
      catch {
        case _: Exception => return ""
      }
    }

    def wline(s: String) {
      out.write(s + "\n")
    }
  }
}
