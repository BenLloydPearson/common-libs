package com.gravity.interests.jobs.intelligence

import com.gravity.interests.jobs.intelligence.operations.graphing.SiteConceptMetrics
import com.gravity.ontology.{ConceptGraph, ConceptGrouper, OntologyGraphName, OntologyNode}

import scala.collection.JavaConversions._

/**
 * Created by apatel on 12/23/13.
 */

object SiteConcepts {

  case class GroupRec(groupConcept: String, childConcepts: collection.mutable.HashSet[OntologyNode], nodeCountSum: Int)

  def getTopConcepts(siteGraph: StoredGraph)(implicit ogName: OntologyGraphName): Seq[SiteConceptMetrics] = {

    val maxNodes = 300
    val maxL3Nodes = 100

//    println("siteGraph: " + siteGraph.nodes.size)
    val ontNodesLevel2 = siteGraph.nodes.filter(n => n.level == 2).sortBy(n => -n.count).take(maxNodes).map(n => ConceptGraph.getNode(n.id)).flatten
//    println("L2 nodes: " + ontNodesLevel2.size)

    val nodes = {
      if (ontNodesLevel2.size < maxNodes) {
        // backfill with L3 Nodes respecting both maxNodes and maxL3Nodes
        val ontNodesLevel3 = siteGraph.nodes.filter(n => n.level == 3).sortBy(n => -n.count).take((maxNodes - ontNodesLevel2.size).min(maxL3Nodes)).map(n => ConceptGraph.getNode(n.id)).flatten
//        println("L3 nodes: " + ontNodesLevel3.size)
        (ontNodesLevel2.toList ::: ontNodesLevel3.toList)
      }
      else {
        ontNodesLevel2.toList
      }
    }

    getTopConcepts(nodes, 2, siteGraph)
  }

  private def getTopConcepts(nodes: List[OntologyNode], groupDepth: Int, graph: StoredGraph): Seq[SiteConceptMetrics] = {
    val groups = groupConcepts(nodes, groupDepth, graph)
//    println("")
//    println("Summary groups: " + groups.size)

    var i = 0
    //val sgBuilder = StoredGraph.make
    val siteConcepts = new collection.mutable.ListBuffer[SiteConceptMetrics]()

    groups.foreach(group => {
      //sgBuilder.addNode(group.groupConcept, "", NodeType.Interest, 2, getCount(graph, group.groupConcept), 1)
      siteConcepts.append(
        new SiteConceptMetrics(group.groupConcept,
          getCount(graph, group.groupConcept),
          group.nodeCountSum))
      i += 1
//      println(i + ") " + group.groupConcept + "\tcount: " + getCount(graph, group.groupConcept) + "\tsum:" + group.nodeCountSum)
      group.childConcepts.toSeq.sortBy(n => -getCount(graph, n.uri)).foreach(c => {
//        println("\t" + c.uri + "\tcount: " + getCount(graph, c.uri))
        //sgBuilder.addNode(c.uri, "", NodeType.Interest, 2, getCount(graph, c.uri), 1)

        siteConcepts.append(
          new SiteConceptMetrics(c.uri,
            getCount(graph, c.uri),
            group.nodeCountSum))
      })
    })

    siteConcepts.toSeq
  }

  private def groupConcepts(nodes: List[OntologyNode], groupDepth: Int, graph: StoredGraph): collection.mutable.ArrayBuffer[GroupRec] = {
    val group = ConceptGrouper.group(nodes, groupDepth)
    val groupSummary = ConceptGrouper.groupSummary(group)

    val finalList = new collection.mutable.ArrayBuffer[GroupRec]()
    groupSummary.filter(gs => gs._1 != "Other").foreach(entry => {
      val uri = entry._1
      val children = entry._2
      val groupNodes = uri :: children.map(c => c.uri).toList
      val groupSum = groupNodes.map(uri => getCount(graph, uri)).sum

      finalList.add(GroupRec(uri, children, groupSum))
    })
    finalList
  }

  private def getCount(graph: StoredGraph, uri: String): Int = {
    try {
      graph.nodeByUri(uri).count
    }
    catch {
      case e: Exception => println(e)
        0
    }
  }

}
