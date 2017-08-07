package com.gravity.ontology

import org.neo4j.graphdb._
import scala.collection.mutable.HashMap
import collection._
import scala.collection.JavaConversions._
import traversal.Evaluation
import traversal.TraversalDescription
import org.neo4j.kernel.Uniqueness
import org.neo4j.kernel.Traversal

import com.gravity.ontology.TraversalFilters.NodeEval


/**
 * Created with IntelliJ IDEA.
 * User: apatel
 * Date: 7/18/12
 * Time: 2:06 PM
 * To change this template use File | Settings | File Templates.
 */

case class RelationshipDirection(relationshipType: RelationshipType, direction: Direction = Direction.OUTGOING)

object ShortestHopAlgo
{
  // stop when either max threshold is reached
  private case class StopCondition(maxDepth: Int, maxNodeVisits: Int = 10000, maxDestinationNodesReached: Int = Int.MaxValue)

  def traverse(startNode: OntologyNode,
               relationshipDirCol: Iterable[RelationshipDirection],
               maxDepth: Int,
               maxNodesToVisit: Int = 1000,
               collectAllPossiblePaths : Boolean = true,
               destinationNodes: Iterable[OntologyNode] = mutable.HashSet.empty[OntologyNode],
               discardNonDestinationNodes : Boolean = false,
               stopWhenMaxDestinationNodesReached: Int = Int.MaxValue) : ShortestHopAlgoResult =
  {
    val result = new ShortestHopAlgoResult(startNode, destinationNodes, discardNonDestinationNodes)

    var td = Traversal.description.breadthFirst()
      .evaluator(allButStartNodeFilter)
      .evaluator(stopConditionFilter(new StopCondition(maxDepth, maxNodesToVisit, stopWhenMaxDestinationNodesReached), result))

    if (collectAllPossiblePaths)
    {
      td = td.uniqueness(Uniqueness.NODE_PATH)
    }

    val trav = addRelationshipDirection(td, relationshipDirCol)
      .traverse(startNode.node)

    trav foreach {nodes => }

    result
  }


  private def stopConditionFilter(stopCondition: StopCondition, result: ShortestHopAlgoResult) = NodeEval
  {
    path =>
    {
      // assume node will be kept
      var evaluation =  Evaluation.INCLUDE_AND_CONTINUE

      // check exit condition
      if (path.length() > stopCondition.maxDepth ||
        result.totalVisitedNodes >= stopCondition.maxNodeVisits ||
        result.reachableDestinationNodes.size >= stopCondition.maxDestinationNodesReached)
      {
        // exit condition reached
        evaluation = Evaluation.EXCLUDE_AND_PRUNE
      }
      else
      {

        val ontEndNode = new OntologyNode(path.endNode())

        // count visit
        result.totalVisitedNodes += 1

        if (result.destinationNodes.contains(ontEndNode))
        {
          // this is a destination node
          val pathList = result.visitedPathMap.getOrElseUpdate(ontEndNode, mutable.HashSet.empty[Path])
          pathList.add(path)

          // node is reachable
          result.reachableDestinationNodes.add(ontEndNode)
          result.unreachableDestinationNodes.remove(ontEndNode)
        }
        else if (!result.discardNonDestinationNodes)
        {
          // keep non dest node
          val pathList = result.visitedPathMap.getOrElseUpdate(ontEndNode, mutable.HashSet.empty[Path])
          pathList.add(path)

          // add to nonDestNodes collection
          result.nonDestinationNodes.add(ontEndNode)
        }
        else
        {
          evaluation = Evaluation.EXCLUDE_AND_CONTINUE
        }
      }

      evaluation
    }
  }

  private def allButStartNodeFilter = NodeEval
  {
    path =>
      if(path.startNode() == path.endNode()) Evaluation.EXCLUDE_AND_CONTINUE else Evaluation.INCLUDE_AND_CONTINUE
  }

  private def addRelationshipDirection(td: TraversalDescription, relationshipDirCol: Iterable[RelationshipDirection]) : TraversalDescription =
  {
    var localTd = td
    for (relDir <- relationshipDirCol)
      localTd = localTd.relationships(relDir.relationshipType, relDir.direction)
    localTd
  }
}

class ShortestHopAlgoResult(startTravNode: OntologyNode ,destNodes: Iterable[OntologyNode] = mutable.HashSet.empty[OntologyNode], discardNonDestNodes : Boolean = false)
{
  val startNode = startTravNode
  val discardNonDestinationNodes = discardNonDestNodes

  // target destination nodes
  val destinationNodes = mutable.HashSet.empty[OntologyNode]
  destNodes foreach {i => destinationNodes.add(i)}

  // all visited nodes live here (not just destination nodes)
  val visitedPathMap = HashMap.empty[OntologyNode, mutable.HashSet[Path]]

  // all visited destination nodes live here
  val reachableDestinationNodes = mutable.HashSet.empty[OntologyNode]

  // all unvisited destination nodes live here
  val unreachableDestinationNodes = mutable.HashSet.empty[OntologyNode]
  destNodes foreach {i => unreachableDestinationNodes.add(i)}

  // all other nodes
  val nonDestinationNodes = mutable.HashSet.empty[OntologyNode]

  var totalVisitedNodes = 0

  def isReachable(node: OntologyNode) =  visitedPathMap.contains(node)

  def getReachableDestinationNodes()  = reachableDestinationNodes.toIterable

  def getUnreachableDestinationNodes() = unreachableDestinationNodes.toIterable

  def getNonDestinationNodes() = nonDestinationNodes.toIterable

  def getAllNodes() = visitedPathMap.keys

  def getNumPaths(node : OntologyNode) = visitedPathMap.getOrElse(node, mutable.HashSet.empty[OntologyNode]).size

  // return all possible paths to this endNode
  def getPaths(endNode: OntologyNode) = visitedPathMap.getOrElse(endNode, mutable.HashSet.empty[Path]).toIterable

  // total nodes visited
  def getTotalVisitedNodes() = totalVisitedNodes

  def getMaxDepth(endNode: OntologyNode) : Int =
  {
    val intSeq = getPaths(endNode).map( path => path.length())
    intSeq.reduceRight(_ max _)
  }

  def getMinDepth(endNode: OntologyNode) : Int =
  {
    val intSeq = getPaths(endNode).map( path => path.length())
    intSeq.reduceRight(_ min _)
  }

}

case class NodeFreqCount(node: OntologyNode, count: Int)

object FrequentNodes
{
  def get(pathListCollection: Iterable[Iterable[Path]]) : Map[OntologyNode, Int]   =
  {
    val nodes = for (pathList <- pathListCollection; path<-pathList; node <- path.nodes()) yield new OntologyNode(node)
    nodes.groupBy(n => n).mapValues(_.size)
  }
  def getSorted(pathListCollection: Iterable[Iterable[Path]]) : Iterable[NodeFreqCount]   =
  {
    sort(get(pathListCollection))
  }
  private def sort(map : Map[OntologyNode,Int]) : Iterable[NodeFreqCount]  =
  {
    map.toList.sortWith(_._2 > _._2).map(t => new NodeFreqCount(t._1,t._2) )
  }
}


case class NodePathCount(node: OntologyNode, pathCount: Int)

object NodesByDepth
{
  def getAllNodesByDepth(depth : Int, result : ShortestHopAlgoResult) : Map[OntologyNode,Int] =
  {
    getNodesByDepth(result.getAllNodes(), depth, result)
  }
  def getReachableDestinationNodesByDepth(depth : Int, result : ShortestHopAlgoResult) : Map[OntologyNode,Int] =
  {
    getNodesByDepth(result.getReachableDestinationNodes(), depth, result)
  }
  def getNonDestinationNodesByDepth(depth : Int, result : ShortestHopAlgoResult) : Map[OntologyNode,Int] =
  {
    getNodesByDepth(result.getNonDestinationNodes(), depth, result)
  }
  def sortByCount(map : Map[OntologyNode,Int]) : Iterable[NodePathCount]  =
  {
    map.toList.sortWith(_._2 > _._2).map(t => new NodePathCount(t._1,t._2) )
  }

  private def getNodesByDepth(nodes : Iterable[OntologyNode], depth: Int, result : ShortestHopAlgoResult) : Map[OntologyNode, Int] =
  {
    val nodeToPathCountMap = HashMap.empty[OntologyNode, Int]
    val pathsForDepthNodes = nodes.filter(n => result.getMinDepth(n) == depth).map(n => result.getPaths(n))
    pathsForDepthNodes foreach {pathCol => {
      val path = pathCol.head
      nodeToPathCountMap.put(path.endNode(), path.length())
    }}
    nodeToPathCountMap
  }

}