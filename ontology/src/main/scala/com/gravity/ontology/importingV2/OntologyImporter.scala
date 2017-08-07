package com.gravity.ontology.importingV2

import java.io.File

import com.gravity.utilities.MurmurHash
import org.apache.commons.io.FileUtils
import org.neo4j.graphdb.RelationshipType
import org.neo4j.index.impl.lucene.LuceneBatchInserterIndexProvider
import org.neo4j.kernel.impl.batchinsert.BatchInserterImpl
import org.openrdf.model.URI

import scala.collection.JavaConversions._
import scala.collection.mutable
import com.gravity.domain.ontology.{EdgeInfo, EnrichedOntologyNode, GraphFileInfo, NodeType}
import com.gravity.interests.jobs.hbase.HBaseConfProvider
import com.gravity.interests.jobs.intelligence.helpers
import org.neo4j.graphdb.index.BatchInserterIndex


/**
  * Created by apatel on 7/19/16.
  */
class OntologyImporter(fileInfo: GraphFileInfo, tsNode: Long, tsEdge: Long){

  val params = mutable.Map("neostore.nodestore.db.mapped_memory" -> "200M",
    "neostore.propertystore.db.strings.mapped_memory" -> "3G",
    "neostore.propertystore.db.mapped_memory" -> "800M",
    "neostore.relationshipstore.db.mapped_memory" -> "800M"
  )

  //  lazy val batchInserter = new BatchInserterImpl(fileInfo.neo4JGraphDir, params)
  //  lazy val batchIndex = new LuceneBatchInserterIndexProvider(batchInserter)
  //
  //  lazy val nameIndex = batchIndex.nodeIndex("names", mutable.Map("type" -> "exact"))
  //  lazy val urisIndex = batchIndex.nodeIndex("uris", mutable.Map("type" -> "exact"))

  //  cleanOutputDir()


  var batchInserter: BatchInserterImpl = null //new BatchInserterImpl(fileInfo.neo4JGraphDir, params)
  var batchIndex: LuceneBatchInserterIndexProvider = null // new LuceneBatchInserterIndexProvider(batchInserter)

  var nameIndex : BatchInserterIndex = null//batchIndex.nodeIndex("names", mutable.Map("type" -> "exact"))
  var urisIndex : BatchInserterIndex = null//batchIndex.nodeIndex("uris", mutable.Map("type" -> "exact"))


  val nodeIdMap = new mutable.HashMap[URI, Long]()
  val counts = mutable.HashMap[String, Long]()

  val printFreq = 100000
  val saveFreq = 1000000

  def reopenIndexes() = {
    batchInserter = new BatchInserterImpl(fileInfo.neo4JGraphDir, params)
    batchIndex = new LuceneBatchInserterIndexProvider(batchInserter)

    nameIndex = batchIndex.nodeIndex("names", mutable.Map("type" -> "exact"))
    urisIndex = batchIndex.nodeIndex("uris", mutable.Map("type" -> "exact"))
  }

  def doImport(): Unit ={

    cleanOutputDir()
    reopenIndexes()

    importNodes()
    importEdges()
    shutdown()
  }


  def shutdown() {
    urisIndex.flush()
    nameIndex.flush()

    batchIndex.shutdown()
    batchInserter.shutdown()
  }

  def cleanOutputDir(): Unit ={
    try {
      println("deleting " + fileInfo.neo4JGraphDir)
      FileUtils.deleteDirectory(new File(fileInfo.neo4JGraphDir))
    }
    catch {
      case ex: Exception =>
        ex.printStackTrace()
        throw ex
    }

  }

  def importNodes() {
    // read file
    var lineCnt = 0

    val nodeFilePath = fileInfo.getFinalNodeImportFile(tsNode)

    println("Importing Nodes from: " + nodeFilePath)

    helpers.grvhadoop.perHdfsLineToSeq(HBaseConfProvider.getConf.fs, nodeFilePath) { line =>
      for {reachabilityNode <- EnrichedOntologyNode.fromFileFormat(line)}{
        lineCnt +=1

        addCount("Create Node")
        addCount("Create Node Type " + NodeType.toString(reachabilityNode.nodeInfo.nodeType))

        val propMap = getPropMap(reachabilityNode)
        nodeId( new org.openrdf.model.impl.URIImpl(reachabilityNode.nodeInfo.uri), propMap)

        if (lineCnt % printFreq == 0){
          showCounters()

        }

        if (lineCnt % saveFreq == 0){
          checkpoint()
        }
      }
    }
  }


  def checkpoint(): Unit = {
    println("saving neo4j")
    shutdown()
    reopenIndexes()
    println("reopening neo4j")
  }

  def getPropMap(rn: EnrichedOntologyNode) = {
    val map = mutable.Map[String, AnyRef]()

    // TBD: for NY Times people nodes, replace: "jones, bob" with "bob jones" or do this while generating the ontology!
    map(NodeProperties.NAME_PROPERTY) = rn.nodeInfo.name

    map(NodeProperties.TopicL1) = Int.box(rn.topics.totalDepth1)
    map(NodeProperties.TopicL2) = Int.box(rn.topics.onlyDepth2)
    map(NodeProperties.TopicL3) = Int.box(rn.topics.onlyDepth3)
    map(NodeProperties.TopicL4) = Int.box(rn.topics.onlyDepth4)

    map(NodeProperties.TopicL2Sum) = Int.box(rn.topics.totalDepth2)
    map(NodeProperties.TopicL3Sum) = Int.box(rn.topics.totalDepth3)
    map(NodeProperties.TopicL4Sum) = Int.box(rn.topics.totalDepth4)

    map(NodeProperties.ConceptL1) = Int.box(rn.concepts.totalDepth1)
    map(NodeProperties.ConceptL2) = Int.box(rn.concepts.onlyDepth2)
    map(NodeProperties.ConceptL3) = Int.box(rn.concepts.onlyDepth3)
    map(NodeProperties.ConceptL4) = Int.box(rn.concepts.onlyDepth4)

    map(NodeProperties.ConceptL2Sum) = Int.box(rn.concepts.totalDepth2)
    map(NodeProperties.ConceptL3Sum) = Int.box(rn.concepts.totalDepth3)
    map(NodeProperties.ConceptL4Sum) = Int.box(rn.concepts.totalDepth4)

    map(NodeProperties.InEdges) = Int.box(rn.inDegree)
    map(NodeProperties.OutEdges) = Int.box(rn.outDegree)

    map(NodeProperties.PageRank) = Double.box(rn.pageRank)
    map(NodeProperties.TriangleCount) = Int.box(rn.triangleCnt)
    map(NodeProperties.ConnectedComponenetId) = Long.box(rn.connectedComponentId)

    map
  }

  def importEdges() {

    val edgeFilePath = fileInfo.getFinalEdgeImportFile(tsEdge)

    // for each line add edge
    var printCnt = 0

    println("Importing Edges from: " + edgeFilePath)

    helpers.grvhadoop.perHdfsLineToSeq(HBaseConfProvider.getConf.fs, edgeFilePath) { line =>

      for {edge <- EdgeInfo.fromFileFormat(line)
           relType <- RelationshiptTypeConverter.getRelationshipType(edge.edgeType)} {

        printCnt +=1

        addCount("Create Edge")
        addCount("Create Edge Type " + relType.name())

        val uriFrom = new org.openrdf.model.impl.URIImpl(edge.source)
        val uriTo = new org.openrdf.model.impl.URIImpl(edge.target)

        relate(uriFrom, uriTo, relType)

        if (printCnt % printFreq == 0){
          showCounters()
        }

      }

    }

  }

  /**
    * Relates two nodes, lazily creating the nodes if they don't exist.
    */
  def relate(uri: URI, target: URI, relationshipType: RelationshipType, properties: mutable.Map[String, AnyRef] =  mutable.Map[String, AnyRef]() ) = {
    val uriId = nodeId(uri)
    val targetId = nodeId(target)
    if (uriId != targetId) {
      batchInserter.createRelationship(uriId, targetId, relationshipType, properties)
    }
    else {
      addCount("Colliding Nodes")
    }
  }


  def nodeId(uri: URI, properties: mutable.Map[String, AnyRef] = null) = {
    nodeIdMap.getOrElseUpdate(uri, {
      create(uri = uri, properties = properties)
    })
  }


  /**
    * Creates a node and adds the given properties.  These properties, by convention, should be keyed using the constants in the
    * NodeBase class (e.g. NodeBase.NAME_PROPERTY)
    */
  def create(uri: URI, name: String = null, properties: mutable.Map[String, AnyRef]): Long = {

    val props = if (properties != null) properties else new mutable.HashMap[String, AnyRef]()

    props += (NodeProperties.URI_PROPERTY -> uri.stringValue)
    if (name != null) props += (NodeProperties.NAME_PROPERTY -> name)
    val nodeId = batchInserter.createNode(props)
    indexUri(nodeId, uri)

    if (name != null) nameIndex.add(nodeId, mutable.Map[String, AnyRef](NodeProperties.NAME_PROPERTY -> name.toLowerCase))

    else if (props.contains(NodeProperties.NAME_PROPERTY)) {
      val propName = props(NodeProperties.NAME_PROPERTY).asInstanceOf[String].toLowerCase
      nameIndex.add(nodeId, mutable.Map[String, AnyRef](NodeProperties.NAME_PROPERTY -> propName))
    }

    nodeId
  }

  /**
    * Search indexes a URI to nodeID pair.
    */
  def indexUri(nodeId: Long, uri: URI) {
    addCount("Uris Indexed")
    urisIndex.add(nodeId, mutable.Map[String, AnyRef](NodeProperties.URI_PROPERTY -> getHashKey(uri).asInstanceOf[AnyRef]))
  }

  /**
    * Adds a point to a counter (which is lazily created)
    */
  def addCount(name: String) {
    val count = counts.getOrElseUpdate(name, 0l)
    counts += (name -> (count + 1l))
  }

  def getHashKey(uri: URI): Long = {
    return MurmurHash.hash64(uri.stringValue)
  }

  /**
    * Will display the performance counters.
    */
  def showCounters() {
    println("==COUNTERS==")
    for ((name, count) <- counts.toSeq.sortBy(_._1)) {
      println("Counter: " + name + " : Count : " + count)
    }
  }

}
