package com.gravity.ontology.importing

import com.gravity.ontology.vocab.NS
import org.openrdf.model.impl.StatementImpl
import org.openrdf.model.vocabulary.{OWL, RDF}
import org.openrdf.rio.{RDFFormat, Rio}

/**
 * User: chris
 * Date: Dec 6, 2010
 * Time: 9:51:11 PM
 */

object makeDyingScene extends App {
  RdfGenerator.generateDyingScene
}

object exportDbpediaToTriples extends App {
  val writer = Rio.createWriter(RDFFormat.NTRIPLES,System.out)

  writer.startRDF()

  OntologySource.rdfFile(getClass.getResourceAsStream("../dbpedia_3.6.owl"), RDFFormat.RDFXML) {
    statement =>
      if(statement.getPredicate == RDF.TYPE && statement.getObject == OWL.CLASS) {
        writer.handleStatement(new StatementImpl(statement.getSubject, RDF.TYPE, NS.DBPEDIA_CLASS))
      }


  }

  writer.endRDF()
}

object exportOntologyToTriples extends App {
  val writer = Rio.createWriter(RDFFormat.NTRIPLES, System.out)
  writer.startRDF()
  OntologySource.rdfFile(getClass.getResourceAsStream("../universe_2010_9.owl"), RDFFormat.TURTLE) {
    itm=>
      writer.handleStatement(itm)
  }

  writer.endRDF()
}