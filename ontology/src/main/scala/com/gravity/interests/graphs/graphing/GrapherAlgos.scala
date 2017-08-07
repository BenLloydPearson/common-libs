package com.gravity.interests.graphs.graphing

import com.gravity.ontology.OntologyGraphName

import collection.mutable.Buffer

/*             )\._.,--....,'``.
 .b--.        /;   _.. \   _\  (`._ ,.
`=,-,-'~~~   `----(,_..'--(,_..'`-.;.'  */

object GrapherAlgo {
  object EverythingAlgo extends EverythingAlgo
  object NothingAlgo extends NothingAlgo
  object LayeredAlgo extends LayeredAlgo
}


abstract class GrapherAlgo(id:Int,version:Int) {
  def apply(grapher:Grapher)(implicit ogName: OntologyGraphName) : InterestGraph

}

class EverythingAlgo() extends GrapherAlgo(1,1) {
  override   def apply(grapher:Grapher)(implicit ogName: OntologyGraphName) : InterestGraph = new InterestGraph(grapher.scoredConcepts)
}

class NothingAlgo() extends GrapherAlgo(2,1) {
  override   def apply(grapher:Grapher)(implicit ogName: OntologyGraphName) : InterestGraph = new InterestGraph(Nil)
}

class LayeredAlgo() extends GrapherAlgo(3,1) {
  def apply(grapher:Grapher)(implicit ogName: OntologyGraphName) : InterestGraph = {
    val results = Buffer[ScoredConcept]()
    val annotatedInterests = new InterestGraph(grapher.copy(followNonAnnotated = false).scoredConcepts)
    val nonAnnotatedInterests = new InterestGraph(grapher.copy(followNonAnnotated = true).scoredConcepts)

    annotatedInterests.topInterestOfLevelByScore(1) match {
      case Some(topL1) => {
        results.append(topL1)
        val topInterestsUnder = nonAnnotatedInterests.topInterestsUnderInterest(topL1)

        results.append(topInterestsUnder:_*)
      }
      case None =>
    }
    new InterestGraph(results)
  }
}
/*
class KAlgo() extends GrapherAlgo(4,1) {
  def apply(grapher:Grapher) : InterestGraph = {

    val graph = new InterestGraph(grapher.scoredConcepts)
    graph.topics.foreach {
      topic =>
        //val topicid = GrapherOntology.instance.getTopic(topic._1.uri,true,10)
        //println ("topic id is:" + topicid)
        //println(topic._1.uri)
      val og = OntologyGraph2.graph
      og.node(23423) match {
        case Some(n) =>
        n.
      }

    }

    graph
    //new InterestGraph(allTopics)
  }

}
*/
