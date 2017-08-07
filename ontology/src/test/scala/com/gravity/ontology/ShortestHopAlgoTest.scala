package com.gravity.ontology

import nodes.TopicRelationshipTypes
import org.junit.{Ignore, Test}
import org.neo4j.graphdb._
import com.gravity.ontology.Implicits.dummyPhrase
import scala.collection._
import scala.collection.JavaConversions._
import org.junit.Assert._
import scala.Some
import org.openrdf.model.impl.URIImpl

/**
 * Created with IntelliJ IDEA.
 * User: apatel
 * Date: 7/18/12
 * Time: 5:26 PM
 * To change this template use File | Settings | File Templates.
 */

class ShortestHopAlgoTest {

  lazy val og = OntologyGraph2.graph
  val debugPrint = false

  private def println(str: String)
  {
    if (debugPrint)
      Console.println(str)
  }

  @Test def testFrequentNodes2()
  {
    val result = performShortestHopAlgo("Cat", 4, Int.MaxValue, getRelsAndDirsToTraverse)
    val freqNodes = FrequentNodes.getSorted(result.visitedPathMap.values)

    assertNotNull(freqNodes)

    println("Trav for Cat. Total visited Nodes: " + result.totalVisitedNodes)

    freqNodes foreach {nc => {

      println(nc.node.toString)
      println("\tFreqInPath Count: " + nc.count)
      println("\tPathCount: " + result.getNumPaths(nc.node))
      println("\tDepth: " + result.getMinDepth(nc.node)
        + " / " + result.getMaxDepth(nc.node) )

      println("\tTotal Edge Count: " + nc.node.node.getRelationships.size)
    }}


  }
  @Test def testFrequentNodes()
  {
    val result = performShortestHopAlgo("Megan Fox", 4, Int.MaxValue, getRelsAndDirsToTraverse)
    val freqNodes = FrequentNodes.getSorted(result.visitedPathMap.values)

    assertNotNull(freqNodes)

    println("Trav for Megan Fox. Total visited Nodes: " + result.totalVisitedNodes)

    freqNodes foreach {nc => {

      println(nc.node.toString)
      println("\tFreqInPath Count: " + nc.count)
      println("\tPathCount: " + result.getNumPaths(nc.node))
      println("\tDepth: " + result.getMinDepth(nc.node)
        + " / " + result.getMaxDepth(nc.node) )

      println("\tTotal Edge Count: " + nc.node.node.getRelationships.size)
    }}


  }

  @Test def testNodesByDepth()
  {
    val maxNodesToVisit = 1000
    val result = performShortestHopAlgo("Megan Fox", Int.MaxValue, maxNodesToVisit, getRelsAndDirsToTraverse)

    assertNotNull(result)
    assertNotNull(result.getAllNodes())
    //assertNotNull(result.getNonDestinationNodes())

    val nodesAtDepth1 =   NodesByDepth.getReachableDestinationNodesByDepth(1, result)
    assertEquals(0, nodesAtDepth1.size)


    println("Start Node: ")
    printNode(result.startNode, 0, mutable.HashSet.empty[Path])

    (1 to 4).foreach{depth => {

      var i = 1
      println("At Depth " + depth)

      val nodesAtDepth = NodesByDepth.getAllNodesByDepth(depth, result)

      // verify nodes are of the correct depth
      nodesAtDepth foreach {npc => {
        assertTrue(result.getMinDepth(npc._1) == depth)
      }}

      // verify nodes are sorted
      val sortedNodes = NodesByDepth.sortByCount(nodesAtDepth)
      var pathCount = Int.MaxValue
      sortedNodes foreach {sortedNode =>{
        assertTrue(pathCount >= sortedNode.pathCount)
        pathCount = sortedNode.pathCount
        printNode(sortedNode.node, i, result.getPaths(sortedNode.node) )
        i += 1

      }}

    }
    }
  }


  @Test def testTraversalWithSinglePathSetting()
  {
    val maxNodesToVisit = 500
    val result = performShortestHopAlgo("Megan Fox", Int.MaxValue, maxNodesToVisit, getRelsAndDirsToTraverse, false)

    result.getAllNodes() foreach( node => {
      val paths = result.getPaths(node)
      assertEquals(1, paths.size)
    })
  }

  @Test def testLargeTraversal()
  {
    val maxNodesToVisit = 500
    val result = performShortestHopAlgo("Megan Fox", Int.MaxValue, maxNodesToVisit, getRelsAndDirsToTraverse)

    assertNotNull(result)
    assertNotNull(result.getAllNodes())

    println("total visits = " + result.totalVisitedNodes)
    println("all nodes count = " + result.getAllNodes().size)

    assertEquals(maxNodesToVisit, result.totalVisitedNodes)
    assertTrue(result.totalVisitedNodes > result.getAllNodes().size )

    var i = 1
    result.getAllNodes() foreach(node => {

      val paths = result.getPaths(node)

      if (paths.size > 1)
      {
        assertTrue(result.getMaxDepth(node) >= result.getMinDepth(node))

        println("-----")
        println("max: " + result.getMaxDepth(node))
        println("min: " + result.getMinDepth(node))
        paths foreach(path => {
          println(path.length() + ":" + path.toString)
        })
      }

      printNode(node, i, paths)
      i += 1
    })


  }

  private def getNodeByUri(nodeUri: String): OntologyNode = {
    og.node(new URIImpl(nodeUri)) match {
      case Some(n) => n
      case _ => throw new Exception("Node NOT found: " + nodeUri)
    }
  }

  private def getNodeById(nodeId: Long)  : OntologyNode =
  {
    og.getNodeByNodeId(nodeId)  match {
      case Some(x) => x
      case _ => throw new Exception("Node NOT found: " + nodeId)
    }
  }

  @Test def testDiscardNonDestinationNodes() {

    val nodeUri1 = "http://dbpedia.org/resource/Category:Animals"
    val nodeUri2 = "http://dbpedia.org/resource/Category:Felines"

    val destinationNodes = new mutable.HashSet[OntologyNode]
    destinationNodes.add(getNodeByUri(nodeUri1))
    destinationNodes.add(getNodeByUri(nodeUri2))

    val result = performShortestHopAlgo("Cat", 3, 1000, getRelsAndDirsToTraverse, true, destinationNodes, true)

    assertNotNull(result)
    assertNotNull(result.getReachableDestinationNodes())
    assertEquals(0, result.getNonDestinationNodes().size)
    assertEquals(2, result.getAllNodes().size)
    assertEquals(2, result.getReachableDestinationNodes().size)
    assertTrue(result.getReachableDestinationNodes().exists(n => n.node.uri == nodeUri1))
    assertTrue(result.getReachableDestinationNodes().exists(n => n.node.uri == nodeUri2))
    assertEquals(0, result.getUnreachableDestinationNodes().size)
  }


  @Test def testStopConditionDestinationNodesReached()
  {
    val nodeUri1 = "http://dbpedia.org/resource/Category:Animals"
    val nodeUri2 = "http://dbpedia.org/resource/Category:Felines"

    val destinationNodes = new mutable.HashSet[OntologyNode]
    destinationNodes.add(getNodeByUri(nodeUri1))
    destinationNodes.add(getNodeByUri(nodeUri2))

    val resultStopEarly = performShortestHopAlgo("Cat", 3, 1000, getRelsAndDirsToTraverse, true, destinationNodes, false, destinationNodes.size)
    val resultFullRun = performShortestHopAlgo("Cat", 3, 1000, getRelsAndDirsToTraverse, true, destinationNodes, false)

    assertEquals(2, resultStopEarly.getReachableDestinationNodes().size)
    assertTrue(resultStopEarly.getReachableDestinationNodes().exists(n => n.node.uri == nodeUri1))
    assertTrue(resultStopEarly.getReachableDestinationNodes().exists(n => n.node.uri == nodeUri2))
    assertEquals(0, resultStopEarly.getUnreachableDestinationNodes().size)

    assertEquals(2, resultFullRun.getReachableDestinationNodes().size)

    assertTrue(resultFullRun.getReachableDestinationNodes().exists(n => n.node.uri == nodeUri1))
    assertTrue(resultFullRun.getReachableDestinationNodes().exists(n => n.node.uri == nodeUri2))

    assertEquals(0, resultFullRun.getUnreachableDestinationNodes().size)

    assertTrue(resultFullRun.totalVisitedNodes > resultStopEarly.totalVisitedNodes)
  }


  @Test def testStopConditionMaxNodesVisited() {

    val maxNodesToVisit = 5
    val result = performShortestHopAlgo("Cat", 2, maxNodesToVisit, getRelsAndDirsToTraverse)

    assertNotNull(result)
    assertNotNull(result.getAllNodes())
    assertEquals(maxNodesToVisit, result.getAllNodes().size)
    assertEquals(maxNodesToVisit, result.totalVisitedNodes)

    printNodes(result.getAllNodes, result)
  }




  @Test def testStopConditionMaxDepth() {

    val maxDepth = 2
    val result = performShortestHopAlgo("Cat", maxDepth, 1000, getRelsAndDirsToTraverse)

    assertNotNull(result)
    assertNotNull(result.getAllNodes())

    result.getAllNodes() foreach( node =>
    {
      val paths = result.getPaths(node)
      paths foreach
        {
          p => assertTrue(maxDepth >= p.length())
        }
    } )

    printNodes(result.getAllNodes, result)
  }

  @Test def testSingleDestinationNode()
  {
    val nodeUri1 = "http://dbpedia.org/resource/Category:Animals"


    val destinationNodes = new mutable.HashSet[OntologyNode]
    val destNode = getNodeByUri(nodeUri1)
    destinationNodes.add( destNode )

    val result = performShortestHopAlgo("Cat", 3, 1000, getRelsAndDirsToTraverse, true,  destinationNodes)

    assertNotNull(result)
    assertNotNull(result.getReachableDestinationNodes())
    assertEquals(1, result.getReachableDestinationNodes().size)
    assertTrue(result.getReachableDestinationNodes().exists(n => n.node.uri == nodeUri1))
    assertTrue(result.isReachable(destNode))
    assertTrue(result.getAllNodes().size > 1)
    assertEquals(0, result.getUnreachableDestinationNodes().size)
    assertTrue(result.getNonDestinationNodes().size > 0)
    assertTrue(result.getNumPaths(destNode) > 0)
//    assertEquals(result.getMaxDepth(destNode), result.getMinDepth(destNode))
  }

  @Test def testMultipleDestinationNodes()
  {
    val nodeUri1 = "http://dbpedia.org/resource/Category:Animals"
    val nodeUri2 = "http://dbpedia.org/resource/Category:Felines"

    val destinationNodes = new mutable.HashSet[OntologyNode]
    destinationNodes.add(getNodeByUri(nodeUri1))
    destinationNodes.add(getNodeByUri(nodeUri2))

    val result = performShortestHopAlgo("Cat", 3, 1000, getRelsAndDirsToTraverse, true, destinationNodes)

    assertNotNull(result)
    assertNotNull(result.getReachableDestinationNodes())
    assertEquals(2, result.getReachableDestinationNodes().size)
    assertTrue(result.getReachableDestinationNodes().exists(n => n.node.uri == nodeUri1))
    assertTrue(result.getReachableDestinationNodes().exists(n => n.node.uri == nodeUri2))
    assertTrue(result.getAllNodes().size > 2)
    assertEquals(0, result.getUnreachableDestinationNodes().size)
    assertTrue(result.getNonDestinationNodes().size > 0)
  }

  @Test def testUnreachableDestinationNode()
  {
    val nodeId = 5849281

    val destinationNodes = new mutable.HashSet[OntologyNode]
    val destNode =  getNodeById(nodeId)
    destinationNodes.add(destNode)

    val result = performShortestHopAlgo("Cat", 2, 1000, getRelsAndDirsToTraverse, true, destinationNodes)

    assertNotNull(result)
    assertNotNull(result.getReachableDestinationNodes())
    assertEquals(0, result.getReachableDestinationNodes().size)
    assertFalse(result.isReachable(destNode))
    assertTrue(result.getAllNodes().size > 1)
    assertEquals(1, result.getUnreachableDestinationNodes().size)
    assertTrue(result.getNonDestinationNodes().size > 0)
  }

  @Test def testReachableAndUnreachableDestinationNode()
  {
    val unreachableNodeUri = "http://dbpedia.org/resource/Category:Sports"

    val nodeUri1 = "http://dbpedia.org/resource/Category:Animals"
    val nodeUri2 = "http://dbpedia.org/resource/Category:Felines"

    val destinationNodes = new mutable.HashSet[OntologyNode]
    destinationNodes.add(getNodeByUri(unreachableNodeUri))
    destinationNodes.add(getNodeByUri(nodeUri1))
    destinationNodes.add(getNodeByUri(nodeUri2))

    val result = performShortestHopAlgo("Cat", 3, 1000, getRelsAndDirsToTraverse, true, destinationNodes)

    assertNotNull(result)
    assertNotNull(result.getReachableDestinationNodes())
    assertTrue(result.getAllNodes().size > 3)
    assertEquals(2, result.getReachableDestinationNodes().size)
    assertTrue(result.getReachableDestinationNodes().exists(n => n.node.uri == nodeUri1))
    assertTrue(result.getReachableDestinationNodes().exists(n => n.node.uri == nodeUri2))

    assertEquals(1, result.getUnreachableDestinationNodes().size)
    assertTrue(result.getUnreachableDestinationNodes().exists(n => n.node.uri == unreachableNodeUri))
    assertTrue(result.getNonDestinationNodes().size > 0)
  }

  // Helper methods
  private def performShortestHopAlgo(searchString: String,
                                     maxDepth: Int,
                                     maxNodesToVisit: Int,
                                     relDirCol : Iterable[RelationshipDirection],
                                     collectAllPossiblePaths : Boolean = true,
                                     destinationNodes : Iterable[OntologyNode] = mutable.HashSet.empty[OntologyNode],
                                     discardNonDestNodes : Boolean = false,
                                     stopWhenMaxDestinationNodesReached : Int = Int.MaxValue) : ShortestHopAlgoResult =
  {
    val startNode = og.searchForTopicRichly(searchString).get.node
    ShortestHopAlgo.traverse(
      startNode,
      relDirCol,
      maxDepth,
      maxNodesToVisit,
      collectAllPossiblePaths,
      destinationNodes,
      discardNonDestNodes,
      stopWhenMaxDestinationNodesReached)
  }

  private def getRelsAndDirsToTraverse : Iterable[RelationshipDirection] =
  {
    val relDirCol = new mutable.HashSet[RelationshipDirection]
    relDirCol.add( new RelationshipDirection(TopicRelationshipTypes.CONCEPT_OF_TOPIC))
    relDirCol.add( new RelationshipDirection(TopicRelationshipTypes.BROADER_CONCEPT))
    relDirCol.add( new RelationshipDirection(TopicRelationshipTypes.INTEREST_OF))
    relDirCol
  }

  def printNodeProperties(node: OntologyNode)
  {
    if (!debugPrint) return

    println("\tNode Properties")
    for (p <- node.node.getPropertyKeys)
      println("\t\t" + p + " : " + node.node.getProperty(p))
  }

  def printNode(node: OntologyNode, num: Int, paths: Iterable[Path])
  {
    if (!debugPrint) return

    println("Node " + num + ": ")
    println("Total Paths:" + paths.size)
    for (path <- paths)
    {
      println("\tDepth: " + path.length)
      println("\tPath: " + path.toString)
    }
    try {
      println("\tnode.node.dbpediaClass.get.name = " + node.node.dbpediaClass.get.name)
    }
    catch{
      case e: Exception =>
    }

    try {
      println("\tnode.node.toString = " + node.node.toString)
    }
    catch{
      case e: Exception =>
    }

    try {
      println("\tnode.node.inferredNodeTyp = " + node.node.inferredNodeType  )
    }
    catch{
      case e: Exception =>
    }

    printNodeProperties(node)
  }

  def printNodes(nodes : Iterable[OntologyNode], result: ShortestHopAlgoResult)
  {
    if (!debugPrint) return

    var i = 1
    nodes foreach( node =>
    {
      val paths = result.getPaths(node)
      printNode(node, i, paths)
      i += 1
    })
  }

}
