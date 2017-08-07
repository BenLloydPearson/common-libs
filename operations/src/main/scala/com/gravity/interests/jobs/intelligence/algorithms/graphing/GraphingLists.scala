package com.gravity.interests.jobs.intelligence.algorithms.graphing

import scala.io.Source
import com.gravity.interests.jobs.intelligence.{StoredGraphBuilder, StoredGraph}

/*             )\._.,--....,'``.
.b--.        /;   _.. \   _\  (`._ ,.
`=,-,-'~~~   `----(,_..'--(,_..'`-.;.'  */
object GraphingLists {
  private def resourceToSet(name: String) = Source.fromInputStream(getClass.getResourceAsStream(name)).getLines().toSet

  val topicUriExclusionSet = resourceToSet("topic_uri_blacklist.txt")
  val conceptUriExclusionSet = resourceToSet("concept_uri_blacklist.txt")

  def scrubGraph(graph: StoredGraph): StoredGraph = {
    graph.populateUriAndName()
    val b = new StoredGraphBuilder()
    b.nodes.appendAll(graph.nodes.filter(node => !topicUriExclusionSet.contains(node.uri) && !conceptUriExclusionSet.contains(node.uri)))
    b.edges.appendAll(graph.edges)
    val newGraph = b.build

    newGraph
  }

}
