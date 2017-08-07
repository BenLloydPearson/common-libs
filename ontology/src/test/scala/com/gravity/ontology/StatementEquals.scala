package com.gravity.ontology

import org.mockito.{Matchers => m}
import org.mockito.{AdditionalMatchers => addlm}
import org.mockito.ArgumentMatcher
import org.openrdf.model.{Statement, Resource, Value, URI}
import org.hamcrest.Description
import org.openrdf.model.impl.StatementImpl

case class StatementEquals(subject: Resource, predicate: URI, `object`: Value) extends ArgumentMatcher[Statement] {
  override def matches(argument: Object) = {
    val stmt = argument.asInstanceOf[Statement]
    (subject == stmt.getSubject) && (predicate == stmt.getPredicate) && (`object` == stmt.getObject)
  }

  override def describeTo(description: Description) {
    description appendText (new StatementImpl(subject, predicate, `object`).toString)
  }
}

object StatementEquals {
  def stmtEq(subject: Resource, predicate: URI, `object`: Value): Statement = {
    m.argThat(StatementEquals(subject, predicate, `object`))
  }
}

object AdditionalMatchers {
  def notEq[T](value: T): T = {
    addlm.not (m.eq (value))
  }
}