package com.gravity.ontology.annotation

import org.openrdf.rio.RDFWriter
import com.google.common.collect.ForwardingObject
import org.openrdf.model.Statement

class ForwardingRDFWriter(writer: RDFWriter) extends ForwardingObject with RDFWriter {
  override def delegate: RDFWriter = writer

  override def startRDF() { delegate.startRDF() }
  override def endRDF() { delegate.endRDF() }
  override def handleNamespace(prefix: String, uri: String) { delegate.handleNamespace(prefix, uri) }
  override def handleStatement(st: Statement) { delegate.handleStatement(st) }
  override def handleComment(comment: String) { delegate.handleComment(comment) }
  override def getRDFFormat = delegate.getRDFFormat
}