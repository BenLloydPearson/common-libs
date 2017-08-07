package com.gravity.interests.jobs.intelligence.operations

import com.gravity.interests.jobs.intelligence.{MutableGraph, StoredGraph}
import com.gravity.ontology.OntologyNodesBlackLists

/**
 * Created with IntelliJ IDEA.
 * User: apatel
 * Date: 12/2/13
 * Time: 7:01 PM
 * To change this template use File | Settings | File Templates.
 */
object FilteredGraphViewService {

  lazy val filterUris = OntologyNodesBlackLists.renderBlacklist ++ OntologyNodesBlackLists.concepts

  def toFilteredGraph(sg: StoredGraph): StoredGraph = {
    sg.populateUriAndName()

    println("filtered nodes: " + filterUris.size)

    val m = new MutableGraph(sg)
    for (nodesToRemove <- sg.nodes.filter(n => filterUris.contains(n.uri))) {
      m.removeAndConnectNode(nodesToRemove)
    }
    m.toStoredGraph
  }
}
