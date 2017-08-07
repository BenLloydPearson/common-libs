package com.gravity.ontology.annotation

import org.openrdf.rio.{RDFFormat, Rio}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{Path, FileSystem}
import org.openrdf.model.Statement
import scalaz.NonEmptyList

object Annotation2OntologyWriter extends App with AnnotationQuery {
  def getAnnotationStatements: Iterable[Statement] = {
    Seq(
      getRenameNodes(None, None, None, None),
      getBadTopics(NonEmptyList(BannedMatch, HiddenMatch), None, None, None, None),
      getBadConceptLinks(None, None, None, None)
    ) flatMap (_.toOntologyStatements)
  }

  def writeToHDFS(filename: String = "/user/gravity/dbpedia/gravity-annotations/user-annotations.nt") {
    val stmts = getAnnotationStatements

    val hdfs = FileSystem.get(new java.net.URI("hdfs://grv-hadoopm01.lax1.gravity.com:9000"), new Configuration())
    val out = hdfs.create(new Path(filename), true)
    try {
      val rdfWriter = Rio.createWriter(RDFFormat.NTRIPLES, out)
      rdfWriter.startRDF()
      stmts foreach (rdfWriter.handleStatement(_))
      rdfWriter.endRDF()
    }
    finally {
      out.close()
    }
  }

  writeToHDFS()
}