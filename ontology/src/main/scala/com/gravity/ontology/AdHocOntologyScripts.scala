package com.gravity.ontology


import java.io.FileWriter
import java.util.Calendar

import com.gravity.ontology.TraversalFilters.NodeEval
import com.gravity.ontology.nodes.TopicRelationshipTypes
import com.gravity.ontology.vocab.{NS, URIType}
import com.gravity.utilities.Settings
import org.neo4j.graphdb._
import org.neo4j.graphdb.traversal.Evaluation
import org.neo4j.kernel.Traversal

import scala.Predef._
import scala.collection.JavaConversions._
import scala.collection._
import scala.io.Source


/**
 * Created with IntelliJ IDEA.
 * User: apatel
 * Date: 12/17/12
 * Time: 1:26 PM
 * To change this template use File | Settings | File Templates.
 */

object AdTaxonomyFileGenerator extends App {
  implicit val ogName = new OntologyGraphName(Settings.ONTOLOGY_DEFAULT_GRAPH_NAME)

  // inputs
  val taxonomyInputFile = "/Users/apatel/junk/openXDictionary/openXDictionary.txt"
  val taxonomyMapInputFile = "/Users/apatel/junk/openXDictionary/webAdMapping.txt"

  //outputs
  val taxonomyOutputFile = "/Users/apatel/repo/gravity/interests-sbt/jobs/src/main/resources/com/gravity/interests/jobs/intelligence/algorithms/graphing/openx_adword_taxonomy.txt"
  val highToLowConceptOutputFilename = "/Users/apatel/repo/gravity/interests-sbt/jobs/src/main/resources/com/gravity/interests/jobs/intelligence/algorithms/graphing/openx_higher_concept_to_lower_concept_mapping.txt"
  val lowToHighConceptOutputFilename = "/Users/apatel/repo/gravity/interests-sbt/jobs/src/main/resources/com/gravity/interests/jobs/intelligence/algorithms/graphing/openx_lower_concept_to_higher_concept_mapping.txt"

  OpenXDictionaryGenerateOutput.generateAdTaxonomy(taxonomyInputFile, taxonomyMapInputFile, taxonomyOutputFile)
  OpenXLowerConcepts.generateHigherAndLowerConceptMapping(taxonomyOutputFile, highToLowConceptOutputFilename, lowToHighConceptOutputFilename)
}

object OpenXLowerConcepts {

  def generateHigherAndLowerConceptMapping(taxonomyInputFile: String, highToLowConceptOutputFilename: String, lowToHighConceptOutputFilename: String)(implicit ogName: OntologyGraphName) {

    val rawEntries = OpenXToOntologyGenerateNodeMapping.getRawEntries(taxonomyInputFile)
    val UriToAdTerm = rawEntries.map(e => (e.uri, (e.webAdToken, e.id, e.parentId))).toMap
    val adTermIdToAdTerm = rawEntries.map(e => (e.id, e.webAdToken)).toMap

    val uriList = rawEntries.map(e => e.uri)

    // lower concept -> higher concepts
    val lowerConceptToHigherConceptsMap = new mutable.HashMap[String, mutable.HashSet[String]]()

    // higher concept -> lower concepts
    val higherConceptToLowerConceptsMap = new mutable.HashMap[String, mutable.HashSet[String]]()

    // lower concept -> adWords
    val lowerConceptsToAdWords = new mutable.HashMap[String, mutable.HashSet[String]]()

    println("UriList: " + uriList.size)
    uriList.foreach(uri => {
      print(".")
      var lowerConceptUris = getLowerConcepts(uri, 2)
      if (lowerConceptUris.size > 75) {
        //println("Re-traversing with depth 1 for: " + uri)
        lowerConceptUris = getLowerConcepts(uri, 1)
      }

      higherConceptToLowerConceptsMap(uri) = lowerConceptUris

      //println("lower concepts for " + uri + ": " + lowerConceptUris.size)
      lowerConceptUris.foreach(lowerUri => {
        val set = lowerConceptToHigherConceptsMap.getOrElse(lowerUri, new mutable.HashSet[String]())
        set.add(uri)
        lowerConceptToHigherConceptsMap(lowerUri) = set
        //println("   " + lowerUri)
      })
    })
    println("")
    println("lower topics: " + lowerConceptToHigherConceptsMap.size)

    lowerConceptToHigherConceptsMap.toList.sortBy(e => -e._2.size).foreach(i => {
      val lowerConcept = i._1
      val higherConcepts = i._2

      //println(lowerConcept + ": " + higherConcepts.size)
      higherConcepts.foreach(higherUri => {
        //println("   " + higherUri)

        val set = lowerConceptsToAdWords.getOrElse(lowerConcept, new mutable.HashSet[String]())

        val adWordEntry = UriToAdTerm(higherUri)
        val adWord = adWordEntry._1
        set.add(adWord)
        for (parentId <- adWordEntry._3;
             parentWord <- adTermIdToAdTerm.get(parentId)) {
          set.add(parentWord)
        }

        lowerConceptsToAdWords(lowerConcept) = set

        //      set.foreach(adWord=> {
        //        println("      " + adWord)
        //      })
      })
    })

    lowerConceptsToAdWords.toList.sortBy(i => -i._2.size).foreach(e => {
      val lowerConcept = e._1
      val adWords = e._2

      //println(lowerConcept)
      adWords.foreach(adWord => {
        //println("   " + adWord)
      })
    })


    writeToFile(highToLowConceptOutputFilename, higherConceptToLowerConceptsMap)
    writeToFile(lowToHighConceptOutputFilename, lowerConceptToHigherConceptsMap)


    println("Wrote: " + highToLowConceptOutputFilename)
    println("Wrote: " + lowToHighConceptOutputFilename)
  }


  private def writeToFile(fname: String, map: mutable.HashMap[String, mutable.HashSet[String]]) {
    val SPLIT = "\t"
    val out = new FileWriter(fname)
    wline("file generated: " + Calendar.getInstance().getTime)

    try {
      wline("concept" + SPLIT + "associatedConcepts")
      map.foreach(e => {
        val higherConcept = e._1
        val lowerConcepts = e._2
        val lc = lowerConcepts.foldLeft("")(_ + _ + ",")
        wline(higherConcept + SPLIT + lc)
      })
    }
    finally {
      out.close()
    }

    def wline(s: String) {
      out.write(s + "\n")
    }
  }

  private def getLowerConcepts(startUri: String, depth: Int)(implicit ogName: OntologyGraphName): mutable.HashSet[String] = {
    var res = new mutable.HashSet[String]()
    try {
      for (startNode <- ConceptGraph.getNode(startUri);
           reachableConceptNodes = collectLowerConcepts(startNode, depth)) {
        res = reachableConceptNodes.map(n => n.uri)
      }
    }
    catch {
      case ex: Exception => //println("Unale to fetch lower concepts for: " + startUri)
    }
    res
  }


  private def collectLowerConcepts(startNode: OntologyNode,
                                   depth: Int): mutable.HashSet[OntologyNode] = {
    val reachableConceptNodes = new mutable.HashSet[OntologyNode]()

    val trav = Traversal.description.breadthFirst()
      .evaluator(reachableEvaluator(depth + 1, reachableConceptNodes))
      .relationships(TopicRelationshipTypes.BROADER_CONCEPT, Direction.INCOMING)
      .relationships(ConceptNodes.GRAV_AUTO_BROADER_CONCEPT, Direction.INCOMING)
      .traverse(startNode.node)

    trav foreach (n => {})
    reachableConceptNodes
    //ConceptNodes.filterByTopicScore(reachableConceptNodes, 1, Int.MaxValue)

  }

  private def reachableEvaluator(stopDepth: Int,
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
              evaluation = Evaluation.INCLUDE_AND_CONTINUE
              result.add(ontEndNode)
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

object OpenXDictionaryGenerateOutput {
  implicit val ogName = new OntologyGraphName(Settings.ONTOLOGY_DEFAULT_GRAPH_NAME)

  def generateAdTaxonomy(taxonomyInputFile: String, taxonomyMapInputFile: String, taxonomyOutputFile: String) {
    //    val taxonomyInputFile = "/Users/apatel/junk/openXDictionary/openXDictionary.txt"
    //    val taxonomyMapInputFile = "/Users/apatel/junk/openXDictionary/webAdMapping.txt"
    //
    //    val taxonomyOutputFile = "/Users/apatel/repo/gravity/interests-sbt/jobs/src/main/resources/com/gravity/interests/jobs/intelligence/algorithms/graphing/openx_adword_taxonomy.txt"
    //val outputFile = fname + ".out"

    val openXEntriesWithConcepts = Source.fromFile(taxonomyInputFile).getLines().map(openXEntryFromLine).toList.flatMap(n => n)
    val openXEntriesWithConceptsMap = openXEntriesWithConcepts.map(e => (e._1.id, (e._1, e._2, e._3))).toMap

    val webAdEntries = Source.fromFile(taxonomyMapInputFile).getLines().map(webAdEntryFromLine).toList.flatMap(n => n)


    println("Read " + openXEntriesWithConcepts.size + " lines from " + taxonomyInputFile)
    println("Read " + webAdEntries.size + " lines from " + taxonomyMapInputFile)


    writeToFile(taxonomyOutputFile, openXEntriesWithConcepts, openXEntriesWithConceptsMap, webAdEntries)
    println("outputFile: " + taxonomyOutputFile)

  }

  private def getMappedOntNode(openXEntriesWithConceptsMap: collection.Map[Int, (OpenXEntry, Option[OntologyNode], String)], webAdEntries: List[WebAdEntry], webXId: Int): Option[OntologyNode] = {
    val openXItem = openXEntriesWithConceptsMap(webXId)
    val openXEntry = openXItem._1

    // try mapping it using webAdMapping
    val searchToken = openXEntry.name.toLowerCase


    // there really has to be a better way to do this
    // logic:
    //  - find all entries where the token exists in l1, l2 or l3
    //  - prefer entry with lowest found score (ie. l1 preferred over l2 over l3 IFF lx matches token and lx+1 is empty
    val possibleMatches = webAdEntries.filter(e => e.CatL1.toLowerCase == searchToken || e.CatL2.toLowerCase == searchToken || e.CatL3.toLowerCase == searchToken)

    // determine best match
    var foundScore = Int.MaxValue
    var uri = ""
    possibleMatches.foreach(e => {
      uri = e.wikiUri
      if (foundScore > 0 && e.CatL1.nonEmpty && e.CatL2.isEmpty) {
        foundScore = 0
      }
      else if (foundScore > 1 && e.CatL2.nonEmpty && e.CatL3.isEmpty) {
        foundScore = 1
      }
      else if (foundScore > 2 && e.CatL3.nonEmpty) {
        foundScore = 2
      }
    })

    //if (uri.startsWith("http")) ConceptGraph.getNode(uri) else None
    if (uri.startsWith("http")) OntologyNodeExtractionHelper.getNodeViaUri(uri) else None
  }

  private def writeToFile(fname: String, entries: List[(OpenXEntry, Option[OntologyNode], String)], openXEntriesWithConceptsMap: collection.Map[Int, (OpenXEntry, Option[OntologyNode], String)], webAdEntries: List[WebAdEntry]) {
    val SPLIT = "\t"
    val out = new FileWriter(fname)
    wline("file generated: " + Calendar.getInstance().getTime)

    try {
      wline("Id" + SPLIT + "Name" + SPLIT + "ParentCategoryId" + SPLIT + "URI" + SPLIT + "Method")

      var count = 0
      entries.foreach(e => {
        count += 1
        val openXEntry = e._1
        var status = "unknown"

        val node =
          getMappedOntNode(openXEntriesWithConceptsMap, webAdEntries, openXEntry.id) match {
            case Some(n) =>
              status = "mapped"
              Some(n)
            case _ =>
              status = e._3
              e._2
          }

        wline(openXEntry.id + SPLIT + openXEntry.name + SPLIT + prettyPrintInt(openXEntry.parentId) + SPLIT + prettyPrint(node) + SPLIT + status)
      })
    }
    finally {
      out.close()
    }

    def wline(s: String) {
      out.write(s + "\n")
    }
  }

  private def webAdEntryFromLine(line: String): Option[WebAdEntry] = {
    val SPLIT = "\t"
    val parts = line.split(SPLIT)

    if (parts.size == 5) {
      val catTree = parts(0)
      val L1 = parts(1)
      val L2 = parts(2)
      val L3 = parts(3)
      val uri = parts(4)

      //println(catTree + SPLIT + L1 + SPLIT + L2 + SPLIT + L3 + SPLIT + uri)
      Some(new WebAdEntry(catTree, L1, L2, L3, uri))
    }
    else {
      //println("cannot process: "  + line + " from WebAdMapFile")
      None
    }
  }

  private def openXEntryFromLine(line: String): Option[(OpenXEntry, Option[OntologyNode], String)] = {
    val SPLIT = "\t"
    val parts = line.split(SPLIT)

    if (parts.size >= 2) {
      try {
        val id = parts(0).toInt
        val name = parts(1)
        val parentId = {
          if (parts.size == 3 && parts(2).nonEmpty) {
            Some(parts(2).toInt)
          }
          else {
            None
          }
        }

        var status = "unknown"
        var node = OntologyNodeExtractionHelper.getConceptNode(name)
        node = node match {
          case None =>
            val tmp = OntologyNodeExtractionHelper.deriveConceptNode(name)
            if (tmp.isDefined) status = "derived"
            tmp
          case n =>
            status = "matched"
            n
        }

        //println(id + SPLIT + name + SPLIT + parentId + SPLIT + prettyPrint(node) + SPLIT + status)

        Some(new OpenXEntry(id, name, parentId), node, status)
      }
      catch {
        case e: Exception =>
          //println("cannot process: "  + line + " from OpenXFile: " + e.getStackTrace.mkString("", "\n", "\n"))
          None
      }
    }
    else {
      //println("too many columns: "  + line + " from OpenXFile")
      None
    }
  }

  private def prettyPrintInt(someInt: Option[Int]): String = {
    someInt match {
      case Some(i) => i.toString
      case None => "none"
    }
  }

  private def prettyPrint(node: Option[OntologyNode]): String = {
    node match {
      case Some(n) => n.uri
      case _ => "none"
    }
  }

}


object OntologyNodeExtractionHelper {

  val graph = OntologyGraph2.graph

  def getNodeViaUri(uri: String): Option[OntologyNode] = {
    val parts = uri.split("Category:")
    if (parts.size == 2) {
      val concept = parts(1)
      graph.node(NS.CONCEPT(concept))
    }
    else {
      None
    }
  }

  def getTopicNodeRichly(term: String)(implicit ogName: OntologyGraphName): Option[OntologyNode] = {

    //graph.searchForTopicRichly(term)
    ConceptGraph.getNodeRichly(term)
  }

  def getConceptNode(term: String)(implicit ogName: OntologyGraphName): Option[OntologyNode] = {
    ConceptGraph.getNode(NS.CONCEPT(term).toString)
  }

  def deriveConceptNode(term: String)(implicit ogName: OntologyGraphName): Option[OntologyNode] = {
    getTopicNodeRichly(term) match {
      case Some(topicNode) =>
        val topicTokens = term.split(Array(' ', '_', '&'))

        val derivedConcepts = topicNode.getRelatedNodes(TopicRelationshipTypes.CONCEPT_OF_TOPIC)

        val filteredConcepts = derivedConcepts.filter(n => {
          topicTokens.exists(token => n.name.toLowerCase.contains(token.toLowerCase) || n.name.toLowerCase.contains(token.toLowerCase + "s"))
        })

        if (filteredConcepts.size > 0) {
          val firstConcept = filteredConcepts.toList.head
          Some(firstConcept)
        }
        else if (derivedConcepts.nonEmpty) {
          val firstConcept = derivedConcepts.toList.head
          Some(firstConcept)
        }
        else {
          //println("no derivations for topic: " + topic)
          None
        }
      case _ =>
        None
    }
  }
}

case class OpenXEntry(id: Int, name: String, parentId: Option[Int])

case class WebAdEntry(catTree: String, CatL1: String, CatL2: String, CatL3: String, wikiUri: String)

case class WebAdTokenAndUri(webAdToken: String, uri: String, id: Int, parentId: Option[Int])

object OpenXToOntologyGenerateNodeMapping extends App {

  val SPLIT = "\t"
  val fname = "/Users/apatel/junk/openXDictionary/openXDictionary.txt.out"
  val rawEntries = getRawEntries(fname)

  val adTermToUri = rawEntries.map(e => (e.webAdToken, e.uri)).toMap
  val UriToAdTerm = rawEntries.map(e => (e.uri, (e.webAdToken, e.id, e.parentId))).toMap
  val adTermIdToAdTerm = rawEntries.map(e => (e.id, e.webAdToken)).toMap

  println("adTermToUri: " + adTermToUri.size)
  println("UriToAdTerm: " + UriToAdTerm.size)

  val outputFile = "/Users/apatel/junk/openXDictionary/uriToAdTerm.txt"
  writeToFile(outputFile, UriToAdTerm, adTermIdToAdTerm)
  println("wrote to file: " + outputFile)


  def getRawEntries(fname: String): List[WebAdTokenAndUri] = {
    Source.fromFile(fname).getLines().map(rawWebAdConceptsFromLine).toList.flatMap(n => n)
  }


  def writeToFile(fname: String, entries: collection.Map[String, (String, Int, Option[Int])], adTermIdToAdTerm: collection.Map[Int, String]) {
    val SPLIT = "\t"
    val out = new FileWriter(fname)

    try {
      wline("Uri" + SPLIT + "WebAdTerm" + SPLIT + "WebAdParentTerm")
      UriToAdTerm.foreach(e => {
        val uri = e._1
        val adEntry = e._2

        val adTerm = adEntry._1
        val id = adEntry._2
        val parentId = adEntry._3

        val parentAdTerm =
          parentId match {
            case Some(i) =>
              adTermIdToAdTerm.getOrElse(i, "Cannot resolve Parent")
            case _ => ""

          }

        wline(uri + SPLIT + adTerm + SPLIT + parentAdTerm)
      })
    }
    finally {
      out.close()
    }

    def wline(s: String) {
      out.write(s + "\n")
    }
  }

  private def rawWebAdConceptsFromLine(line: String): Option[WebAdTokenAndUri] = {
    val SPLIT = "\t"
    val parts = line.split(SPLIT)

    try {
      if (parts.size == 5) {
        val id = parts(0).toInt
        val name = parts(1)
        val parentId = if (parts(2) == "none") None else Some(parts(2).toInt)
        val uri = parts(3)
        val method = parts(4)

        //println(name + SPLIT + uri)
        Some(new WebAdTokenAndUri(name, uri, id, parentId))
      }
      else {
        //println("cannot process: "  + line + " from output file")
        None
      }
    }
    catch {
      case _: Exception =>
        None
    }

  }

}

case class TopicEntry(node: OntologyNode, count: Int, inputLine: String)

object CommonTopicsGroupingReport extends App {
  implicit val ogName = new OntologyGraphName(Settings.ONTOLOGY_DEFAULT_GRAPH_NAME)

  val fname = "/Users/apatel/junk/popular topics/TopicCounts.csv"
  val SPLIT = ","

  val unmatchedLines = new mutable.ListBuffer[(String, Int)]()
  //  dumpFile()

  printFileSummary()

  val topicMap = Source.fromFile(fname).getLines().map(lineToTopicEntry).toList.flatMap(n => n).toMap
  val nodes = topicMap.keys.toList

  println("Matched topics: " + nodes.size)
  println("Unmatched topics: " + unmatchedLines.size)

  //unmatchedLines.foreach(e=> println(e._1 + " : " + e._2))


  (1 to 1).foreach(depth => {
    println("Grouping of Lower Concepts (depth = " + depth + ")")
    ConceptReport.performGrouping(nodes, depth)
    println("")
    println("-----------------------------------------------")
    println("")
  })


  println("Level Grouping")
  ConceptReport.performLevelGrouping(nodes, 3, 3)

  private def lineToTopicEntry(line: String): Option[(OntologyNode, TopicEntry)] = {
    val parts = line.split(SPLIT)
    if (parts.size == 2) {
      val topic = parts(0)
      try {
        val count = parts(1).toInt
        // try concept
        ConceptGraph.getNode(NS.CONCEPT(topic).toString) match {
          case Some(conceptNode) => Some(conceptNode, TopicEntry(conceptNode, count, line))
          case _ =>
            // try Topic
            ConceptGraph.getNode(NS.TOPIC(topic).toString) match {
              case Some(topicNode) =>
                val topicTokens = topic.split(Array(' ', '_', '&'))

                //                val derivedConcepts = node.concepts
                val derivedConcepts = topicNode.getRelatedNodes(TopicRelationshipTypes.CONCEPT_OF_TOPIC).filter(n => {
                  topicTokens.exists(token => n.name.toLowerCase.contains(token.toLowerCase) || n.name.toLowerCase.contains(token.toLowerCase + "s"))
                })

                if (derivedConcepts.size > 0) {
                  val firstConcept = derivedConcepts.toList(1)
                  Some(firstConcept, TopicEntry(firstConcept, count, line))
                }
                else {
                  //println("no derivations for topic: " + topic)
                  //derivedConcepts.foreach(i=> println("   derived concept: " + i.name + " " + i.inferredNodeType.name()))
                  unmatchedLines.append((topic, count))
                  None
                }
              case _ =>
                unmatchedLines.append((topic, count))
                None
            }
        }
      }
      catch {
        case _: Exception =>
          unmatchedLines.append((topic, -1))
          None
      }
    }
    else {
      unmatchedLines.append((line, -1))
      None
    }
  }


  private def printFileSummary() {
    println("terms: " + (Source.fromFile(fname).getLines().size - 1))
  }

  private def dumpFile() {

    var lineCount = 0


    for (line <- Source.fromFile(fname).getLines()) {
      val parts = line.split(SPLIT)
      if (parts.size == 2) {
        val topic = parts(0)
        try {
          val count = parts(1).toInt
          lineCount += 1
          println(lineCount + "." + topic + ": " + count)
        }
        catch {
          case _: Exception => println("cannot parse: " + line)
        }
      }
      else {
        println("cannot parse: " + line)
      }
    }
  }

}

//object GenderDetectionScripts {
//
//}
//
//case class GenderNode(uri: String, score: Double) {
//  lazy val node = ConceptGraph.getNode(uri)
//}
//
//trait GenderDetectionTrait {
//  def getScore(genderNodeVector: Seq[GenderNode]): Double
//}
//
//class GenderDetectionBasic(graphNodeUriList: Seq[String]) extends GenderDetectionTrait {
//  def getScore(genderNodeVector: Seq[GenderNode]): Double = {
//    0.0
//
//    //    val destinationNodes =
//    //      for (genderNode <- genderNodeVector;
//    //           ontNode <- genderNode.node) yield ontNode
//
//
//    // graphNodeUriList reachable to topicVector
//  }
//}
