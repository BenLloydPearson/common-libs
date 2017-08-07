package com.gravity.ontology

import scala.io.Source

/**
 * Created with IntelliJ IDEA.
 * User: apatel
 * Date: 10/14/13
 * Time: 4:34 PM
 * To change this template use File | Settings | File Templates.
 */
object OntologyNodesBlackLists {
  private def resourceToSet(name: String) = Source.fromInputStream(getClass.getResourceAsStream(name)).getLines().toSet

  val concepts = resourceToSet("concept_uri_blacklist.txt")
  val renderBlacklist = resourceToSet("render_node_blacklist.txt")
  //  val topics = resourceToSet("/com/gravity/interests/jobs/intelligence/algorithms/graphing/topic_uri_blacklist.txt")
}
