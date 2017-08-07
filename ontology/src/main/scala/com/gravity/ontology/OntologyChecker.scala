package com.gravity.ontology

import vocab.NS
import com.gravity.utilities.ScalaMagic
import org.joda.time.DateTime
import com.gravity.interests.graphs.graphing.{ContentType, Weight, ContentToGraph, Phrase}

/*             )\._.,--....,'``.
 .b--.        /;   _.. \   _\  (`._ ,.
`=,-,-'~~~   `----(,_..'--(,_..'`-.;.'  */


object OntologyChecker extends App {
  try {
    val topic = OntologyGraph2.graph.topic(NS.getTopicURI("Cats"))
    println(topic.get.name + " (got us a topic!)")

    val search = OntologyGraph2.graph.searchForTopicRichly(Phrase("Barack Obama",2,1,ContentToGraph("http://hi.com",new DateTime(),"Yep",Weight.High,ContentType.Article)))
    println(search)
  }catch {
    case ex:Exception => {
      ScalaMagic.printException("Couldn't do it",ex)
    }
  }

}