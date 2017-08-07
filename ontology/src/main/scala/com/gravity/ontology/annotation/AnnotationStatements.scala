package com.gravity.ontology.annotation

import org.openrdf.model.{Value, URI, Statement}
import org.openrdf.model.impl.{LiteralImpl, BooleanLiteralImpl, URIImpl, StatementImpl}
import com.gravity.ontology.vocab.NS
import play.api.libs.json.Json

sealed trait AnnotationStatements {
  val toOntologyStatements: Iterable[Statement]
}

class BadTopics(badTopicSuggestions: Set[(String, AnnotatorSuggestion)]) extends AnnotationStatements {
  lazy val badTopics = badTopicSuggestions.unzip._1
  override lazy val toOntologyStatements = for ((badTopic, suggestion) <- badTopicSuggestions) yield new StatementImpl(new URIImpl(badTopic), suggestion.ontologyPredicate, BooleanLiteralImpl.TRUE)
}

case class ConceptLink(narrowerConcept: String, broaderConcept: String)

object ConceptLink {
  implicit val jsonFormat = Json.format[ConceptLink]
}

case class BadConcepts(badLinks: Set[ConceptLink], badNodes: Set[String]) extends AnnotationStatements {
  lazy val isEmpty = badLinks.isEmpty && badNodes.isEmpty
  override lazy val toOntologyStatements = {
    val badNodeStatements = badNodes map (badNode => new StatementImpl(new URIImpl(badNode), NS.DO_NOT_GRAPH, BooleanLiteralImpl.TRUE))
    val badLinkStatements = badLinks map (badLink => new StatementImpl(new URIImpl(badLink.narrowerConcept), NS.DO_NOT_FOLLOW_BROADER_CONCEPT, new URIImpl(badLink.broaderConcept)))
    badNodeStatements ++ badLinkStatements
  }
}
object BadConcepts {
  val empty = BadConcepts(Set.empty, Set.empty)

  implicit val jsonFormat = Json.format[BadConcepts]
}

case class RenameNodes(nodeRenames: Map[String, String]) extends AnnotationStatements {
  override lazy val toOntologyStatements = nodeRenames map { case (uri, rename) => new StatementImpl(new URIImpl(uri), NS.RENAME, new LiteralImpl(rename)) }
}

case class AnnotatedPhrase(phrase: String, atIndex: Int)

sealed trait AnnotatorSuggestion {
  val annotationPredicate: URI
  val annotationObject: Value
  val ontologyPredicate: URI
}
object BannedMatch extends AnnotatorSuggestion {
  override val annotationPredicate = NS.SHOULD_MATCH_TOPIC
  override val annotationObject = NS.NEVER_MATCH
  override val ontologyPredicate = NS.DO_NOT_GRAPH
}
object HiddenMatch extends AnnotatorSuggestion {
  override val annotationPredicate = NS.SHOULD_HIDE_MATCHED
  override val annotationObject = BooleanLiteralImpl.TRUE
  override val ontologyPredicate = NS.DO_NOT_SHOW
}