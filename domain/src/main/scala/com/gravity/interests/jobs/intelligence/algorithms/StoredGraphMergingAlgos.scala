package com.gravity.interests.jobs.intelligence.algorithms

import scala.collection._
import com.gravity.interests.jobs.intelligence._
import com.gravity.utilities.MurmurHash
import scala.collection.mutable.{ArrayBuffer, Buffer}

/*             )\._.,--....,'``.
 .b--.        /;   _.. \   _\  (`._ ,.
`=,-,-'~~~   `----(,_..'--(,_..'`-.;.'  */


/** Ways to merge graphs
  *
  */
object StoredGraphMergingAlgos {

  object StandardMerger extends AdditiveTrimmingStoredGraphMerger

  object CompactionBasedMerger extends CompactionBasedStoredGraphMerger

}

/** Ways to coalesce graphs around a unifying topic.
  */
object StoredGraphCoalescingAlgos {

  object Standard extends PrivilegedNodeDistanceMerger

}

/**
 * A stateful coalescer that can be used inside of a large iterator.
 * @param algo
 * @param privilegedNodeId
 */
class GraphCoalescer(algo: StoredGraphCoalescingAlgo, privilegedNodeId: Int, maxNodesInMemory: Int = 20000) {
  val buffer: ArrayBuffer[StoredGraph] = mutable.ArrayBuffer[StoredGraph]()
  val graphBuffer: ArrayBuffer[StoredGraph] = mutable.ArrayBuffer[StoredGraph]()
  var graphBufferSize = 0
  private var graph = StoredGraph.EmptyGraph

  def coalesceBuffer() {
    graphBuffer += graph
    algo.coalesce(privilegedNodeId, graphBuffer, maximumSize = 2000) match {
      case Some(mergedGraph) =>
        graph = mergedGraph
      case None => println("Merge failed")
    }
    graphBuffer.clear()
    graphBufferSize = 0
  }

  def addGraph(thatGraph: StoredGraph) {
    graphBuffer += thatGraph
    graphBufferSize += thatGraph.size

    if (graphBufferSize >= maxNodesInMemory) {
      coalesceBuffer()
    }
  }

  def finalGraph: StoredGraph = {
    coalesceBuffer()
    graph
  }
}

/**
 * A stateful graph merger that can follow a reducer that's doing other things.
 * @param maxNodesInMemory
 */
class GraphMerger(maxNodesInMemory: Int = 20000) {
  val buffer: ArrayBuffer[StoredGraph] = mutable.ArrayBuffer[StoredGraph]()
  val graphBuffer: ArrayBuffer[StoredGraph] = mutable.ArrayBuffer[StoredGraph]()
  var graphBufferSize = 0
  private var graph = StoredGraph.EmptyGraph

  def mergeBuffer() {
    graphBuffer += graph
    graph = StoredGraph.addMulti(graphBuffer)
    graphBuffer.clear()
    graphBufferSize = 0
  }

  def addGraph(thatGraph: StoredGraph) {
    graphBuffer += thatGraph
    graphBufferSize += thatGraph.size

    if (graphBufferSize >= maxNodesInMemory) {
      mergeBuffer()
    }
  }

  def finalGraph: StoredGraph = {
    mergeBuffer()
    graph
  }
}

trait StoredGraphMergingAlgo {
  def merge(those: Iterable[StoredGraph]): Option[StoredGraph]
}

trait StoredGraphCoalescingAlgo {
  def coalesce(centralNodeUri: String, those: Iterable[StoredGraph], maximumSize: Int): Option[StoredGraph] = coalesce(MurmurHash.hash64(centralNodeUri), those, maximumSize)

  def coalesce(centralNodeID: Long, those: Iterable[StoredGraph], maximumSize: Int): Option[StoredGraph]
}

/** Attempt to express the aggregated graph as a constellation of relevant topics and concepts surrounding the central node.
  *
  */
class PrivilegedNodeDistanceMerger extends StoredGraphCoalescingAlgo {
 import com.gravity.logging.Logging._
  override def coalesce(centralNodeID: Long, those: Iterable[StoredGraph], maximumSize: Int = 200): Option[StoredGraph] = {
    if (those.isEmpty) {
      None
    } else {
      val central = StoredGraph.addMulti(those)
      if (central.size > maximumSize) {
        try {
          Some(central.subGraph(maximumSize, centralNodeID))
        }
        catch {
          case e: Exception => {
            // tbd - determine why subGraph operation failed
            warn(e, "Coalesce operation failed for Node Id [" + centralNodeID + "] for " + those.size + " graphs")
            None
          }
        }
      }
      else {
        Some(central)
      }

    }

  }

}


trait MergeAndTrimGraphAlgo extends StoredGraphMergingAlgo {

  def maxNodes: Int

  def trimSize: Int = 20000

  override def merge(those: Iterable[StoredGraph]): Option[StoredGraph] = {

    if (those.isEmpty) {
      None
    } else if (those.size == 1) {
      Some(those.head)
    } else {
      var mergedGraph = StoredGraph.makeEmptyGraph
      val graphBuffer = Buffer[StoredGraph]()
      var graphBufferSize = 0
      for (graph <- those) {
        graphBuffer += graph
        graphBufferSize += graph.size
        if (graphBufferSize > trimSize) {
          graphBuffer += mergedGraph
          mergedGraph = mergeAndTrim(graphBuffer, maxNodes)
          graphBufferSize = 0
          graphBuffer.clear()
        }
      }

      if (graphBuffer.length > 0) {
        graphBuffer += mergedGraph
        mergedGraph = mergeAndTrim(graphBuffer, maxNodes)
      }
      Some(mergedGraph)
    }
  }

  def mergeAndTrim(graphsToMerge: Buffer[StoredGraph], maxNodes: Int): StoredGraph
}

class CompactionBasedStoredGraphMerger extends MergeAndTrimGraphAlgo {
  val maxNodes = 2000

  def mergeAndTrim(graphsToMerge: Buffer[StoredGraph], maxNodes: Int): StoredGraph = {
    val tempGraph = StoredGraph.addMulti(graphsToMerge)
    new GraphCompactorRatioBased(tempGraph).compact(maxNodes)
  }
}

class AdditiveTrimmingStoredGraphMerger extends MergeAndTrimGraphAlgo {
  val maxNodes = 200

  def mergeAndTrim(graphsToMerge: Buffer[StoredGraph], maxNodes: Int): StoredGraph = {
    StoredGraph.addMulti(graphsToMerge).subGraph(maxNodes)
  }
}
