package com.gravity.interests.jobs.intelligence.operations

import com.gravity.interests.graphs.graphing.PhraseAnalysisService
import com.gravity.interests.jobs.intelligence.helpers.grvhadoop
import com.gravity.interests.jobs.intelligence.{NodeType, StoredGraph, Node}
import gnu.trove.map.TLongLongMap
import com.gravity.utilities.cache.PermaCacher
import com.gravity.interests.jobs.hbase.HBaseConfProvider
import com.gravity.utilities.{grvio, Settings}
import gnu.trove.map.hash.TLongLongHashMap
import akka.actor.ActorSystem
import com.gravity.utilities.grvakka.Configuration._
import com.gravity.interests.jobs.intelligence.Node
import org.apache.hadoop.security.AccessControlException
import scala.concurrent.duration._
import java.io.FileNotFoundException

/*             )\._.,--....,'``.
.b--.        /;   _.. \   _\  (`._ ,.
`=,-,-'~~~   `----(,_..'--(,_..'`-.;.'  */
object GraphAnalysisService {
 import com.gravity.logging.Logging._
  implicit val system = ActorSystem("SiteService", defaultConf)

  import system.dispatcher

  val useFrequencyMap = Settings.getProperty("ontology.nodefrequencymap.utilize", "true").toBoolean

  val emptyMap = new TLongLongHashMap()

  val nodeFreqMapPath = "/user/gravity/reports/graph-node-frequency.csv"
  var nodeFrequencyMap: TLongLongMap = nodeFrequencyMapFactory
  system.scheduler.schedule(6.hours, 6.hours)(nodeFrequencyMap = nodeFrequencyMapFactory)

  /**
   * Builds and caches a map of nodeId to node frequency, which is how many documents the node appeared in.
   * @return
   */
  //  def getNodeFrequencyMap: TLongLongMap = {
  //    if (true) {
  //
  //      try {
  //        PermaCacher.getOrRegister("node-frequency-map", nodeFrequencyMapFactory, 60 * 60 * 6)
  //      } catch {
  //        case ex:Exception =>
  //          emptyMap
  //      }
  //    } else {
  //      emptyMap
  //    }
  //  }


  private def nodeFrequencyMapFactory = {
    val troveMap = new gnu.trove.map.hash.TLongLongHashMap(1200000, 0.5f, 0l, 1l)

    try {
      grvhadoop.perHdfsLine(HBaseConfProvider.getConf.fs, nodeFreqMapPath) {
        line =>
          line.split("\t") match {
            case Array(nodeId, count) => {
              troveMap.put(nodeId.toLong, count.toLong)
            }
            case _ =>
          }
      }

    } catch {
      case fnf: FileNotFoundException => warn("Node Frequency Map does not exist in HDFS at: {0}", nodeFreqMapPath)
      case ace: AccessControlException => warn("Node Frequency Map access denied from HDFS at: {0}", nodeFreqMapPath)
      case ex: Exception =>
        throw new RuntimeException("Unknown exception while trying to read in Node Frequency Map at: " + nodeFrequencyMap, ex)
    }

    troveMap.compact()
    info("Node Frequency Map: Loaded {0} nodes", troveMap.size())
    troveMap
  }

  val defaultTotalDocuments = 8000000L

  /**
   * Gets the total number of documents in the corpus (~330M) used to generate the frequency map
   * Returns a constant if data is not available
   */
  lazy val documentsInCorpus = {
    val totalDocumentNodeId = 0L
    if (nodeFrequencyMap.containsKey(totalDocumentNodeId)) frequencyForNode(totalDocumentNodeId)
    else defaultTotalDocuments
  }

  /**
   * Upper thresholds for calculating tfidf scores for topics and concepts
   * ie. if topic or concept is in more than x% of all documents, ignore it
   */
  // TBD - calibrate
  val maxFrequencyForConcepts =.09 * documentsInCorpus // original .009
  //(~3M)
  val maxFrequencyForTopics =.009 * documentsInCorpus //(~300K)  original .0009

  /**
   * Gets the frequency for a particular node (murmurhash64 of a node's URI).  Returns 0 if the node was never discovered.
   * @param nodeId
   * @return
   */
  def frequencyForNode(nodeId: Long, frequencyMap: TLongLongMap = nodeFrequencyMap): Long = {
    frequencyMap.get(nodeId)
  }

  /**
   * Copies an existing graph and returns an identical graph with the scores replaced by tf/idf scores.
   * @param graph
   * @return
   */
  def copyGraphToTfIdfGraph(graph: StoredGraph): StoredGraph = {
    val totalTopics = graph.topics.size
    val totalConcepts = graph.interests.size
    val totalTerms = graph.terms.size

    val gb = StoredGraph.make

    val currentFrequencyMap = nodeFrequencyMap //this means we hit permacacher once per copy instead of once per node

    graph.nodes.foreach {
      node =>
        val newScore =
          node.nodeType match {
            case NodeType.Interest => nodeTfIdf(node, totalConcepts, currentFrequencyMap)
            case NodeType.Topic => nodeTfIdf(node, totalTopics, currentFrequencyMap)
            case NodeType.Term => PhraseAnalysisService.phraseTfIdf(node.name, 1, totalTerms)
            case _ => node.score
          }
        gb.nodes += node.copy(score = newScore)
    }
    graph.edges.foreach {
      edge =>
        gb.edges += edge.copy()
    }
    gb.build
  }

  /**
   * Convert an existing graph to a tf-idf graph.  Will mutuate the graph -- consider using copyGraphToTfidfGraph
   * @param graph
   */
  def mutateGraphWithTfIdfs(graph: StoredGraph) {
    val totalNodes = graph.nodes.size
    val currentFrequencyMap = nodeFrequencyMap
    graph.nodes.foreach {
      node =>
        node.score = nodeTfIdf(node, totalNodes, currentFrequencyMap)
    }
  }


  def nodeTfIdf(node: Node, totalNodesInDocument: Long, frequencyMap: TLongLongMap) = {
    // TBD: replace defaultTotalDocuments with documentsInCorpus after sufficient testing
    //  This change may impact filters elsewhere
    node.nodeType match {
      case NodeType.Interest =>
        if (frequencyForNode(node.id, frequencyMap) > maxFrequencyForConcepts) {
          0.0
        } else {
          tfidf(1, totalNodesInDocument, defaultTotalDocuments, frequencyForNode(node.id, frequencyMap))
        }
      case NodeType.Topic =>
        if (frequencyForNode(node.id, frequencyMap) > maxFrequencyForTopics) {
          0.0
        } else {
          tfidf(1, totalNodesInDocument, defaultTotalDocuments, frequencyForNode(node.id, frequencyMap))
        }
      case _ =>
        0.0
    }
  }

  private def tfidf(nodeCountInDocument: Long, nodesInDocument: Long, totalDocuments: Long, totalDocumentsWithNode: Long) = {
    val score = (nodeCountInDocument.toDouble / nodesInDocument.toDouble) * math.log(totalDocuments.toDouble / totalDocumentsWithNode.toDouble)
    if (score.isInfinity || score.isNaN) {
      0.0
    } else
      score
  }

}
