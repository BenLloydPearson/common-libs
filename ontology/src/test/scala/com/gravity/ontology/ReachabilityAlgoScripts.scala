package com.gravity.ontology

import org.junit.{Assert, Test}
import org.neo4j.graphdb._

import scala.collection._
import scala.collection.JavaConversions._
import scala.Some
import java.util.Calendar

import org.neo4j.tooling.GlobalGraphOperations
import com.gravity.ontology.vocab.{NS, URIType}
import com.gravity.interests.graphs.graphing.{ContentToGraph, ContentType, Grapher, Weight}
import org.joda.time.DateTime
import com.gravity.utilities.{MurmurHash, Settings, Stopwatch}
import com.gravity.ontology.nodes.{NodeBase, NodeType}
import org.neo4j.graphdb.index.IndexHits

import scala.io.Source
import java.io.FileWriter

/**
 * Created with IntelliJ IDEA.
 * User: apatel
 * Date: 7/27/12
 * Time: 1:29 PM
 * To change this template use File | Settings | File Templates.
 */

object testGenerateNewMasterGraph extends App {
  println("StartTime: " + Calendar.getInstance.getTime)
  ConceptNodes.clearScores(4)

  //ConceptNodes.flattenGraph()
  DegreeScore.scoreGraph()
  TopicClosenessScore.scoreGraph(depth = 4)
  ConceptNodes.scoreGraph(depth = 4)


  // print report
  val fname1 = "/Users/apatel/junk/ontNodeScores/HighConcepts_scores_" + Calendar.getInstance().getTime.toString.replace(" ", "") + ".txt"
  val fname2 = "/Users/apatel/junk/ontNodeScores/AllConcepts_scores_" + Calendar.getInstance().getTime.toString.replace(" ", "") + ".txt"

  ConceptNodes.dumpHighConceptNodes(4, fname = Some(fname1))
  ConceptNodes.dumpScores(ConceptNodes.getConceptNodes(), fname = Some(fname2))


  // TBD: point to correct source for more weights
  //BroaderConceptWeights.importWeights("/Users/apatel/junk/categoryScore.csv")

  println("EndTime: " + Calendar.getInstance.getTime)
}

object testDumpAllConceptNodes2 extends App {
  println("StartTime: " + Calendar.getInstance.getTime)

  //val fname1 = "/Users/apatel/junk/ontNodeScores/HighConcepts_scores_" + Calendar.getInstance().getTime.toString.replace(" ", "") + ".txt"
  val fname2 = "/Users/apatel/junk/ontNodeScores/OldOntAllConceptsScores_" + Calendar.getInstance().getTime.toString.replace(" ", "") + ".txt"

  //ConceptNodes.dumpHighConceptNodes(4, fname = Some(fname1))
  ConceptNodes.dumpScores(ConceptNodes.getConceptNodes(), fname = Some(fname2))
  println("EndTime: " + Calendar.getInstance.getTime)
}

object testNewGraph extends App {
  val graph = OntologyGraphMgr.getInstance().getGraph("graph_concept")
  val gdb = graph.getDb

  val nodes = GlobalGraphOperations.at(gdb).getAllNodes
  val it = nodes.iterator()
  var i = 0
  while (i < 10 && it.hasNext) {
    val on = new OntologyNode(it.next())
    try {
      if (on.uriType == URIType.WIKI_CATEGORY || on.uriType == URIType.TOPIC) {
        println(on.uri)
        i += 1
      }
    }
    catch {
      case ex: Exception =>
    }
  }
}

object testConceptScoring extends App {
  ConceptNodes.scoreGraph(depth = 4)
}

object testMultipleSideBySideGraphs extends App {
  val graph = OntologyGraphMgr.getInstance().getGraph("graph_concept")
  val graph2 = OntologyGraphMgr.getInstance().getGraph("graph_concept.backup5.latestBrokenOnt")

  println("Graph: " + graph.getLocation)
  printFirstFewNodes(graph)

  println("Graph2: " + graph2.getLocation)
  printFirstFewNodes(graph2)


  def printFirstFewNodes(graph: OntologyGraph) {
    val gdb = graph.getDb

    val nodes = GlobalGraphOperations.at(gdb).getAllNodes
    val it = nodes.iterator()
    var i = 0
    while (i < 10 && it.hasNext) {
      val on = new OntologyNode(it.next())
      try {
        if (on.uriType == URIType.WIKI_CATEGORY || on.uriType == URIType.TOPIC) {
          println(on.uri)
          i += 1
        }
      }
      catch {
        case ex: Exception =>
      }
    }
  }
}

object testDumpAllConceptNodes extends App {
  val fname = "/Users/apatel/junk/ontNodeScores/scores_" + Calendar.getInstance().getTime.toString.replace(" ", "") + ".txt"
  println("File: " + fname)
  ConceptNodes.dumpAll(4, Some(fname))
}

object testDumpSelectedConceptNodes extends App {
  val fname = "/Users/apatel/junk/ontNodeScores/scores_" + Calendar.getInstance().getTime.toString.replace(" ", "") + ".txt"
  println("File: " + fname)
  ConceptNodes.dumpSelctedConceptNodes(4, Int.MaxValue, Some(fname))
}

object testTopicExtraction extends App {
  implicit val ogName = new OntologyGraphName(Settings.ONTOLOGY_DEFAULT_GRAPH_NAME)

  val urls = List(
    "http://finance.yahoo.com/news/how-race-slipped-away-from-romney.html",
    "http://money.cnn.com/2012/11/08/technology/priceline-buys-kayak/index.html",
    "http://money.cnn.com/2012/11/08/technology/amazon-wine/index.html",
    "http://techcrunch.com/2012/11/09/kickstarter-release-your-inner-james-bond-with-these-high-tech-linear-watches/",
    "http://techcrunch.com/2012/11/09/china-blocks-virtually-all-of-googles-web-services-as-18th-party-congress-gets-underway/",
    "http://eatocracy.cnn.com/2012/11/06/craving-pumpkin-flavors-the-marketing-is-working/?hpt=li_mid",
    "http://www.cnn.com/2012/07/26/politics/electoral-college-tie/index.html"
  )

  for (url <- urls) {
    print("Url: " + url + "   ")
    val result = ConceptGraph.processContent(url)
    println("   Total Topics: " + result.grapher.allTopics.size)

    val sw = new Stopwatch()
    sw.start()
    println("   Running Topic Extraction Algo ...")
    val res1 = TopicExtraction.extractUsingReductiveAlgo(result.grapher.allTopics, Int.MaxValue, 4)
    sw.stop()
    val duration1 = sw.getDuration()
    println("   Time: " + duration1 + "   Keeps " + res1.keep.size + " Discards " + res1.discard.size)
    //res1.print()
  }
}

object testTopConcepts extends App {
  implicit val ogName = new OntologyGraphName(Settings.ONTOLOGY_DEFAULT_GRAPH_NAME)

  val url = "http://www.cnn.com/2012/07/26/politics/electoral-college-tie/index.html"
  val result = ConceptGraph.processContent(url, 4)
  val resultList = result.getFinalResults(30, 30, 30, 30)
  val topConcepts = ConceptGrouper.topConcepts(resultList, 4)

  println("url: " + url)
  println("Top Concepts: " + topConcepts.size)
  var i = 0
  topConcepts foreach (entry => {
    i += 1
    println(i + ") " + entry._1)
    entry._2 foreach (c => {
      println("\t" + c.uri)
    })
  })
}

object testConceptReport extends App {
  //val url = "http://www.cnn.com/2012/07/26/politics/electoral-college-tie/index.html"
  val url = "http://www.cnn.com/2012/08/28/justice/georgia-soldiers-plot/index.html?hpt=ju_c1"
  ConceptReport.extractConcepts(url, 4, 3, 30, 30, 100)
}

object testProcessContent extends App {
  implicit val ogName = new OntologyGraphName(Settings.ONTOLOGY_DEFAULT_GRAPH_NAME)

  //val url = "http://money.cnn.com/2012/08/27/technology/apple-samsung-phone/index.html?iid=HP_LN&hpt=te_t1"

  val url = "http://www.cnn.com/2012/07/26/politics/electoral-college-tie/index.html"
  val result = ConceptGraph.processContent(url, 4)
  Assert.assertNotNull(result)

  val resultList = result.getFinalResults(15, 15, 15, 15)

  Assert.assertNotNull(resultList)
  Assert.assertTrue(resultList.size > 0)
  println("Total concepts: " + resultList.size)

  resultList.sortBy(cs => cs.topicScores.avg * -1) foreach (stats => {
    Assert.assertTrue(stats.frequency > 0)
    println(stats.conceptNode.uri + ": " + stats.frequency + " avg: " + stats.topicScores.avg + " sum: " + stats.topicScores.sum + " max: " + stats.topicScores.max)
  })
}

object testBadUriGetNode extends App {
  val topic = "http://dbpedia.org/resource/Lexus"
  OntologyGraph2.graph.node(NS.fromString(topic)) match {
    case Some(on) =>
      println("topic is valid: " + topic)
    case None =>
      println("topic is not valid: " + topic)
  }
}

object testConceptGraph extends App {
  implicit val ogName = new OntologyGraphName(Settings.ONTOLOGY_DEFAULT_GRAPH_NAME)

  val depth = 3
  val title = ContentToGraph("http://blah.com", new DateTime(2011, 1, 1, 1, 1, 1, 1), "Cars", Weight.High, ContentType.Title)
  val content = ContentToGraph("http://blah.com", new DateTime(2011, 1, 1, 1, 1, 1, 1), "I like Honda and Toyota but not Ford.  I like Japanese cars.  BMW and Lexus are also good vehicles", Weight.Low, ContentType.Article)
  val grapher = new Grapher(title :: content :: Nil)

  println("Title to Parser: " + title.text)
  println("Content To Parse: " + content.text)
  println("Total Topics extracted: " + grapher.totalTopics)

  var cnt = 0
  grapher.topics foreach (entry => {
    cnt += 1

    val topicUri = entry._1
    println("\ttopic " + cnt + ": " + topicUri)
    val phraseAndTopicCol = entry._2
    println("\tNum PhraseAndTopics: " + phraseAndTopicCol.size)

    phraseAndTopicCol foreach (pt => {
      println("\t\t" + pt.topic.node.name)
      println("\t\t" + pt.topic.score)
    })
  })

  val result = ConceptGraph.getRelatedConcepts(depth, grapher)

  println("High Level:")
  result.getSortedHighlLevelResults().take(10) foreach (stat => {
    println(stat.conceptNode.uri + ": " + stat.frequency)
  })

  println("Low Level:")
  result.getSortedLowLevelConcepts().take(10) foreach (stat => {
    println(stat.conceptNode.uri + ": " + stat.frequency)
  })
}

object testDumpNodeByUri extends App {
  ReachabilityAlgo.dumpNodeByUri("http://dbpedia.org/resource/Pelias", 1, false, true, false)
}

object testBroaderConceptWeights extends App {
  BroaderConceptWeights.printRpt("/Users/apatel/junk/categoryScore.csv")
}

object testDumpConceptScores extends App {
  ConceptNodes.generateReport()
}

object testDumpIrrelevantNodes extends App {
  ConceptNodes.flattenGraph()
}

object testGetNodeByUri extends App {
  val uri = "http://data.nytimes.com/68581771646200356283"
  val u = NS.fromString(uri)

  OntologyGraph2.graph.node(u) match {
    case Some(node) =>
      Assert.assertEquals(node.node.uri, uri)
    case None =>
      Assert.fail()
  }
}

object testWebMdTopicNodes extends App {
  val graph = OntologyGraphMgr.getInstance().getGraph("graph_concept")

  val terms = Seq("video", "diabetes health", "diabetes", "type 1 diabetes", "thyroid disease",
    "allergies", "weight loss", "diets",
    "alkaline diets", "pain medication addiction", "heart disease")

  for (t <- terms;
       r = graph.findTopicNode(t)) {
    println("Term:" + t)
    if (r != null) {
      //println("  Topic Node Name: " + r.node.getName)
      println("  Topic Uri: " + r.node.getURI)
      println("  Topic Node Id: " + MurmurHash.hash64(r.node.getURI))
    }
    else {
      println("   No Topic found")
    }

  }
  println("")
}

object testNodeSearch extends App {
  val terms = Seq("diabetes", "type 1 diabetes", "thyroid disease",
    "allergies", "weight loss", "diets",
    "alkaline diets", "pain medication addiction", "heart disease")

  for (t <- terms) {
    testSearchIndex(t)
  }

  def testSearchIndex(term: String) {
    val graph = OntologyGraphMgr.getInstance().getGraph("graph_concept")

    try {
      val hits = graph.getNameIndex.get(NodeBase.NAME_PROPERTY, term)
      println("Term: " + term)

      for (node <- hits.iterator()) {
        if (node.inferredNodeType == NodeType.TOPIC || node.inferredNodeType == NodeType.CONCEPT) {
          println("  NodeType: " + node.inferredNodeType)
          println("  Topic Uri: " + node.uri)
          println("  Topic Node Id: " + MurmurHash.hash64(node.uri))
          println("")
        }

      }
    }
    catch {
      case e: Exception => {
        println("   Exception while querying fulltext index for term: " + term + "  " + e)
      }
    }
  }
}

object testDumpNode extends App {
  (1 to 10) foreach (nodeId => {
    ReachabilityAlgo.dumpNodeById(nodeId)
  })
}

object testConnectedTopics extends App {
  ReachabilityAlgo.printReachableScoredOrInterestNodes(2, 100)
}

object testScoreRange extends App {
  ReachabilityAlgo.printScoreRange(true)
}

object testMaxScore extends App {
  val gdb = OntologyGraph2.graph.graphDb
  val nodes = GlobalGraphOperations.at(gdb).getAllNodes

  var maxScore = 0D
  var maxNodeUri = ""
  nodes foreach (n => {
    if (n.hasProperty(ReachabilityAlgo.INTEREST_SCORE)) {
      val score = n.getProperty(ReachabilityAlgo.INTEREST_SCORE).asInstanceOf[Double]
      if (score > maxScore) {
        val o = new OntologyNode(n)
        maxNodeUri = o.uri
        maxScore = score
      }
    }
  })

  System.out.println("Max Score: " + maxScore)
  System.out.println("Uri: " + maxNodeUri)
}

object testScoreWithMultipleInterests extends App {
  val depth = 2
  ReachabilityAlgo.scoreGraph(depth)
}

object testGetScoredNodeCount extends App {
  println("Scored Nodes = " + ReachabilityAlgo.getCountOfScoredNodes())
}

object testShowScores extends App {
  ReachabilityAlgo.printScores()
}

object testShowScoresForTopics extends App {
  ReachabilityAlgo.printScores(ConceptHierarchy.PARENT_ATTRIBUTE_SCORE)
}

object testClearScore extends App {
  ReachabilityAlgo.clearReachableScore()
}

object testNodesWithCustomProperty extends App {
  val gdb = OntologyGraph2.graph.graphDb
  val nodes = GlobalGraphOperations.at(gdb).getAllNodes
  val c = nodes.count(n => n.hasProperty("akash"))

  Console.println("Nodes with akash property = " + c)
}

object testSetNodeAttribute extends App {
  val interesetNodes = ReachabilityAlgo.getInterestNodes(2) //KNOWN_MAX_TOPICS)

  interesetNodes foreach (n => {
    if (n.node.hasProperty("akash")) {
      println(n.toString + " has property akash = " + n.node.getProperty("akash").asInstanceOf[String])
    }
    else {
      println("akash property does not exist for " + n.toString)
    }

    val gdb = OntologyGraph2.graph.graphDb

    val tx = gdb.beginTx()
    try {
      println("setting akash property for " + n.toString)
      n.node.setProperty("akash", Calendar.getInstance().getTime.toString)
      tx.success()
    }
    catch {
      case e: Exception => println(e.toString)
    }
    finally {
      tx.finish()
    }

  })
}

object testReachableNodesForMultipleTopics extends App {
  val KNOWN_MAX_TOPICS = 678

  val interestNodes = ReachabilityAlgo.getInterestNodes(KNOWN_MAX_TOPICS)
  val depth = 2

  val algoResult = ReachabilityAlgo.traverse(new ReachabilityAlgoInput(interestNodes, depth))

  //    if (debugPrint)
  //      algoResult.print()

  //assertEquals(reachableSet.size, depth1 + depth2 + depth3)
}

object testReachableNodesFromOneInterest extends App {
  val iNodes = ReachabilityAlgo.getInterestNodes(1)

  val node = iNodes.get(0)
  ReachabilityAlgoIT.printNode(node, 0, true)

  val depth = 1
  val algoResult = ReachabilityAlgo.traverse(new ReachabilityAlgoInput(iNodes, depth))


  println("reachable node count = " + algoResult.reachableSet.size)

  // ouput counters
  ReachabilityAlgoIT.printNodesViaPaths(algoResult.reachableSet.values)
}

object testCountByNodeType extends App {
  val res = NearestTopicScore.getCountByNodeType()
  res.keySet foreach (k => {
    println(k + " : " + res.get(k))
  })
}

object testNodeCount extends App {
  println("Total Nodes in Graph: " + NearestTopicScore.getNodeCount())
}

object testWriteOutInterestNodes extends App {
  val KNOWN_MAX_TOPICS = 678
  val interestNodes = ReachabilityAlgo.getInterestNodes(KNOWN_MAX_TOPICS)
  val fname = """/Users/apatel/junk/testScalaOutput.txt"""
  NearestTopicScore.writeToFile(fname, interestNodes)
}

object compareWebMdWithAll extends App {
  val SPLIT = "\t"

  def loadAll(f: String, m: mutable.HashMap[String, Int]) {
    for (line <- Source.fromFile(f).getLines()) {
      val cols = line.split(SPLIT)
      if (cols.size == 3) {
        val uri = cols(0)
        if (!uri.contains("/Category:")) {
          val parts = uri.split("/")
          val name = parts(parts.size - 1).toLowerCase()
          val freq = cols(2).toInt
          m(name) = freq
        }

      }
    }

  }


  val webMdFile = "/Users/apatel/junk/webMDUsers/webmdTermFreq.clean.txt"
  val allFile = "/Users/apatel/junk/webMDUsers/all.clean.txt"

  val frequentWebMdNodes = new mutable.HashMap[String, Double]()

  val webMdMap = new mutable.HashMap[String, Int]()
  val allMap = new mutable.HashMap[String, Int]()

  ReachabilityAlgoIT.loadWemMd(webMdFile, webMdMap)
  loadAll(allFile, allMap)

  println("webMd: " + webMdMap.size)
  println("all: " + allMap.size)

  var overlap = 0
  var totalR = 0.0D
  for ((n, freq) <- webMdMap if overlap < 100000) {
    if (allMap.containsKey(n)) {
      overlap += 1
      val allFreq = allMap(n)
      val ratio = freq.toDouble / allFreq.toDouble

      if (ratio >.15 * 5) {
        frequentWebMdNodes(n) = ratio
        //          println(overlap + ":" + n + ":" + freq + ":" + allFreq + ":" + ratio)
      }
      totalR += ratio
    }
    else {
      if (freq > 100) {
        println(n)
        //          println("no match in all: " + n)
      }
    }
  }

  println("overlap: " + overlap)
  println("avg r: " + totalR / overlap.toDouble)
}

object generateWebMdFreqCountsWithUri extends App {
  val webMdFile = "/Users/apatel/junk/webMDUsers/webmdTermFreq.clean.txt"
  val targetFile = "/Users/apatel/junk/webMDUsers/webmdUriFreq.txt"

  val SPLIT = "\t"
  val out = new FileWriter(targetFile)

  val webMdMap = new mutable.HashMap[String, Int]()
  ReachabilityAlgoIT.loadWemMd(webMdFile, webMdMap)
  val g = OntologyGraphMgr.getInstance.getDefaultGraph

  var foundCount = 0
  var notFoundCount = 0
  var totalCount = 0

  for ((n, freq) <- webMdMap) {
    val name = n.replace("_", " ").replace( """"""", "").replace(",", " ").replace("(", "").replace(")", "")
    val res = g.findTopicNode(name)

    if (res == null) {
      notFoundCount += 1
      println("Not Found: " + name)
    }
    else {
      foundCount += 1
      val uri = res.node.getURI
      val output = uri + SPLIT + freq
      //println(output)
      out.write(output + "\n")
    }

    totalCount += 1
  }

  out.close()
  println("Total Topics: " + totalCount)
  println("Found: " + foundCount)
  println("Not Found:" + notFoundCount)
}

object printTfidfScores extends App {
  val sourceFile = "/Users/apatel/junk/webMDUsers/user-gravity-reports-graph-node-frequency.txt"
  val targetFile = "/Users/apatel/junk/webMDUsers/user-gravity-reports-graph-node-frequency.out.txt"
  val SPLIT = "\t"
  val out = new FileWriter(targetFile)
  val g = OntologyGraphMgr.getInstance.getDefaultGraph
  for (line <- Source.fromFile(sourceFile).getLines()) {
    val cols = line.split(SPLIT)
    if (cols.size == 2) {
      try {
        val id = cols(0).toLong
        val n = g.getNode(id)
        val uri = n.uri
        val output = uri + SPLIT + cols(0) + SPLIT + cols(1) + SPLIT + MurmurHash.hash64(uri).toString
        println(output)
        out.write(output + "\n")
      }
      catch {
        case _: Exception => "Node Not Found: " + cols(0)
      }
      //        println(uri + SPLIT + cols(0) + SPLIT + cols(1) + SPLIT + MurmurHash.hash64(uri).toString)
    }
  }
  out.close()
}

object ReachabilityAlgoIT {
  val debugPrint = true

  private def println(str: String) {
    if (debugPrint)
      Console.println(str)
  }

  val KNOWN_MAX_TOPICS = 678

  def printNodesViaPaths(paths: Iterable[Path]) {
    var i = 1
    paths foreach (p => {
      printNode(new OntologyNode(p.endNode()), i)
      i += 1
    })
  }

  def printNodes(nodes: Iterable[OntologyNode]) {
    var i = 1
    nodes foreach (n => {
      printNode(n, i)
      i += 1
    })
  }

  def printNode(node: OntologyNode, num: Int, detailPrint: Boolean = false) {
    if (!debugPrint) return




    if (detailPrint) {
      println("Node " + num + ": ")

      try {
        println("\tnode.node.dbpediaClass.get.name = " + node.node.dbpediaClass.get.name)
      }
      catch {
        case e: Exception =>
      }

      try {
        println("\tnode.node.toString = " + node.node.toString)
      }
      catch {
        case e: Exception =>
      }

      try {
        println("\tnode.node.uriType = " + node.node.uriType)
      }
      catch {
        case e: Exception =>
      }

      printNodeProperties(node)
    }
    else {
      println(num + ": " + node.toString)
    }
  }

  def printNodeProperties(node: OntologyNode) {
    if (!debugPrint) return

    println("\tNode Properties")
    for (p <- node.node.getPropertyKeys)
      println("\t\t" + p + " : " + node.node.getProperty(p))
  }


  def loadWemMd(f: String, m: mutable.HashMap[String, Int]) {
    val SPLIT = "\t"

    for (line <- Source.fromFile(f).getLines()) {
      val cols = line.split(SPLIT)
      if (cols.size == 3) {
        val name = cols(1)
        val newName = name.replace(" ", "_").toLowerCase()
        val freq = cols(2).toInt
        m(newName) = freq
      }
    }
  }
}
