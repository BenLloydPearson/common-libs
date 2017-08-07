package com.gravity.ontology.importing

import org.openrdf.rio.{RDFHandler, RDFFormat, Rio}
import java.io.{InputStreamReader, InputStream, BufferedReader}
import org.openrdf.model.{Literal, URI, Statement}

/*             )\._.,--....,'``.
.b--.        /;   _.. \   _\  (`._ ,.
`=,-,-'~~~   `----(,_..'--(,_..'`-.;.'  */

class StatementWrapper(statement:Statement) {
  def meow = "Woof"

  def subjectURI = statement.getSubject.asInstanceOf[URI]

  def predicateURI = statement.getPredicate.asInstanceOf[URI]

  def objectURI = statement.getObject.asInstanceOf[URI]

  def objectString = statement.getObject.stringValue

  def objectInt = statement.getObject.asInstanceOf[Literal].intValue

  def SPO = (subjectURI, statement.getPredicate, statement.getObject)
}

object OntologySource {

  implicit def statementWrapper(statement:Statement) = new StatementWrapper(statement)

  def rdfFile(path:InputStream, format:RDFFormat = RDFFormat.NTRIPLES)(handler:(Statement) => Any) = {
    val parser = Rio.createParser(format)
    parser.setRDFHandler(new RDFHandler() {
      def startRDF(){}
      def handleStatement(st: Statement) {

        handler(st)
      }
      def handleNamespace(prefix: String, uri: String) {}
      def handleComment(comment: String) {}
      def endRDF(){}
    })
    parser.parse(new BufferedReader(new InputStreamReader(path)),"")
    path.close()
  }

}