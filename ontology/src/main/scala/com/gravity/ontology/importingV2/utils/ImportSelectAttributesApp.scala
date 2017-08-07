package com.gravity.ontology.importingV2.utils

import com.gravity.domain.ontology.EnrichedOntologyNode
import com.gravity.ontology.importingV2.{NodeProperties, OntologyImporterFileInfo}
import com.gravity.ontology.{OntologyGraph2, OntologyNode}
import org.neo4j.graphdb.{GraphDatabaseService, Node}
import org.neo4j.tooling.GlobalGraphOperations

import scala.collection.mutable

/**
  * Created by apatel on 7/19/16.
  */
object ImportSelectAttributesApp extends App {
  val milRange = 1

  val mil = 1000000

  val startLine = 0 //milRange * mil
  val endLine =  Int.MaxValue //startLine + mil

  println("Start at " + startLine)
  println("End at " + endLine)


  val printFreq = 10000
  val checkPoint = 1000
  val counts = mutable.HashMap[String, Long]()
  //  val maxNodes = None //Some(1000000)
  val updateEnabled = true

  val map = loadAttributes(startLine, endLine)
  updateNoe4j(map, updateEnabled)


  def updateNoe4j(attributeMap : mutable.HashMap[String, SelectNodeAttributes], updateEnabled: Boolean) = {

    val gdb = OntologyGraph2.graph.graphDb
    val iterator = GlobalGraphOperations.at(gdb).getAllNodes.iterator()

    var nodeCnt = 0
    var tx = gdb.beginTx()

    while (iterator.hasNext && attributeMap.size > 0){
      val node = iterator.next()
      val on = OntologyNode(node)

      try {
        val uri = on.uri

        attributeMap.get(uri) match {
          case Some(selectNodeAttributes) =>

            if (updateEnabled) {

              setProperty(gdb, tx, node, NodeProperties.PageRank, selectNodeAttributes.pgRank)
              setProperty(gdb, tx, node, NodeProperties.ConnectedComponenetId, selectNodeAttributes.ccId)

              tx = checkPointTx(gdb, tx, nodeCnt)

              attributeMap.remove(uri)

              addCount("Updated Node Attributes")
            }
            else {
              addCount("Updated Node Attributes [dry run]")
            }

          case None =>
            addCount("uri NOT found in loaded attributes map")
        }

      }
      catch {
        case ex: Exception =>
          addCount("exception in updating")
      }

      if (nodeCnt % printFreq == 0) {
        showCounters()
      }

      nodeCnt +=1
    }

  }

  def checkPointTx(gdb: GraphDatabaseService, tx: org.neo4j.graphdb.Transaction, nodeCnt: Int) = {
    if (nodeCnt % checkPoint == 0){
      addCount("tx complete")
      tx.finish()
      gdb.beginTx()
    }
    else {
      tx
    }
  }

  def setProperty(gdb: GraphDatabaseService, tx: org.neo4j.graphdb.Transaction, node: Node, propName: String, propValue: Any): Unit ={
    try {
      node.setProperty(propName, propValue)
      tx.success()
    }
    catch {
      case e: Exception =>
        addCount("Unable to set property")
        throw new Exception("Unable to set score property")
    }
  }

  def loadAttributes(startLine: Int, endLine: Int)  = {
    val fileInfo = new OntologyImporterFileInfo(useTestFiles = false)

    val attributeMap = new scala.collection.mutable.HashMap[String, SelectNodeAttributes]()


    // read file
    var lineCnt = 0
    for {line <- scala.io.Source.fromFile(fileInfo.nodeFile).getLines}{

      for{reachabilityNode <- EnrichedOntologyNode.fromFileFormat(line) if  (lineCnt >= startLine && lineCnt <= endLine) } {

        val uri = reachabilityNode.nodeInfo.uri
        attributeMap.put(uri, SelectNodeAttributes(reachabilityNode.pageRank, reachabilityNode.connectedComponentId))
        addCount("Loaded Select Node Attributes")

        if (lineCnt % printFreq == 0){
          showCounters()
        }

      }

      lineCnt +=1


    }

    println("Loaded "  + attributeMap.size + " node attributes.")

    attributeMap
  }

  def showCounters() {
    println("==COUNTERS==")
    for ((name, count) <- counts.toSeq.sortBy(_._1)) {
      println("Counter: " + name + " : Count : " + count)
    }
  }

  def addCount(name: String) {
    val count = counts.getOrElseUpdate(name, 0l)
    counts += (name -> (count + 1l))
  }


}

