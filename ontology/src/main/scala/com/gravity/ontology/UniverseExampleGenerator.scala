package com.gravity.ontology

import importing.OntologySource
import org.openrdf.rio.{RDFFormat, Rio}

/*             )\._.,--....,'``.
 .b--.        /;   _.. \   _\  (`._ ,.
`=,-,-'~~~   `----(,_..'--(,_..'`-.;.'  */


/**
 * Uses the example nodes in the universe ontology file to create triples that can be used as examples for how to generate various datasets.
 */
object UniverseExampleGenerator extends App{

  val writer = Rio.createWriter(RDFFormat.NTRIPLES, System.out)
  writer.startRDF()
  OntologySource.rdfFile(getClass.getResourceAsStream("universe_2010_9.owl"), RDFFormat.TURTLE) {
    itm=>
      writer.handleStatement(itm)
  }

  writer.endRDF()

}
