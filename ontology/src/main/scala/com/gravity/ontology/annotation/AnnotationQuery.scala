package com.gravity.ontology.annotation

import scalaz._
import Scalaz._
import org.joda.time.DateTime
import org.openrdf.model.Value
import com.gravity.ontology._
import com.gravity.ontology.VirtuosoOntology2._
import com.gravity.ontology.vocab.NS
import javax.xml.datatype.DatatypeFactory

/**
 * Methods for querying user ontology annotations.
 */
trait AnnotationQuery extends VirtuosoSparqlQuery {
  implicit def dateTime2XMLGregorianCalendar(dateTime: DateTime) = DatatypeFactory.newInstance.newXMLGregorianCalendar(dateTime.toGregorianCalendar)

  val vf = RdfGraphFactory.getVirtuosoGraph.getRepository.getValueFactory

  protected lazy val ontologyCreationDate = OntologyGraph2.graph.ontologyCreationDate

  private def isByUserIfDefined(annotation: RowWrapper, byUser: Option[String]) = {
    byUser map (annotation.getString("byUser").endsWith(_)) getOrElse (true)
  }

  private def isAdminUser(annotation: RowWrapper): Boolean = {
    isAdminUser(annotation.getString("byUser"))
  }

  private def isAdminUser(user: String): Boolean = {
    !user.matches(".*\\btest\\b.*") && user.matches(".*/(igi_admin|gravity):.*")
  }

  private def baseAnnotationQuery(sparql: String, bindings: Map[String, Value] = Map.empty, byUser: Option[String] = None) = {
    sparqlQuery(sparql, bindings) filter (isByUserIfDefined(_, byUser)) filter (isAdminUser)
  }

  /**
   * Fetches the topic URIs that annotators suggest should be banned or hidden,
   * optionally filtering annotations made by one user.
   *
   * @param byUser a user identifier to filter annotations by, e.g. "jdoe@gravity.com"
   * @param alsoGetBadTopicsForUrl if provided, also get topic matches from the url marked as bad (not just universally banned topics)
   */
  def getBadTopics(filterSuggestions: NonEmptyList[AnnotatorSuggestion], byUser: Option[String] = None, alsoGetBadTopicsForUrl: Option[String] = None, startDate: Option[DateTime] = None, endDate: Option[DateTime] = None): BadTopics = {
    val sparql = """
      PREFIX go: <http://insights.gravity.com/2010/9/universe#>
      SELECT DISTINCT ?topicURI ?shouldMatchTopic ?shouldHideMatched ?byUser ?url FROM <http://insights.gravity.com/annotations> WHERE {
        ?anno go:matchesTopic ?topicURI .
        OPTIONAL {
          ?anno go:shouldMatchTopic ?shouldMatchTopic .
        }
        OPTIONAL {
          ?anno go:shouldHideMatched ?shouldHideMatched .
        }

        ?anno go:byUser ?byUser .
        ?anno go:url ?url .
        ?anno go:atTime ?atTime

        FILTER ( bound(?shouldMatchTopic) || bound(?shouldHideMatched) )

        %s
        %s
      }
    """ format (
      if (startDate.isDefined) "FILTER ( ?atTime >= ?startDate )" else "",
      if (endDate.isDefined) "FILTER ( ?atTime < ?endDate )" else ""
    )
    val bindings = makeBindings(
      startDate map ("startDate" -> vf.createLiteral(_)),
      endDate map ("endDate" -> vf.createLiteral(_))
    )

    val (filterBanned, filterHidden) = {
      val filterSuggestionsList = filterSuggestions.list
      (
        filterSuggestionsList contains (BannedMatch),
        filterSuggestionsList contains (HiddenMatch)
      )
    }

    new BadTopics((for {
      anno <- baseAnnotationQuery(sparql, bindings, byUser)

      shouldMatchTopic = Option(anno.getURI("shouldMatchTopic"))
      badMatchJustForArticleUrl = anno.getString("url") === (alsoGetBadTopicsForUrl getOrElse ("")) && (shouldMatchTopic some (_ === NS.NO_SUGGESTION) none (false))

      shouldHideMatched = anno.getBool("shouldHideMatched") getOrElse (false)
      isBanned = filterBanned && ((shouldMatchTopic some (_ === NS.NEVER_MATCH) none (false)) || badMatchJustForArticleUrl)
      isHidden = filterHidden && shouldHideMatched

      if (isBanned || isHidden)

      suggestion = if (isBanned) BannedMatch else HiddenMatch
    } yield anno.getURI("topicURI").toString -> suggestion).toSet)
  }

  def getBannedTopicsInCurrentOntology(byUser: Option[String] = None, alsoGetBadTopicsForUrl: Option[String] = None) = getBadTopics(NonEmptyList(BannedMatch), byUser, alsoGetBadTopicsForUrl, Some(ontologyCreationDate))

  /**
   * Don't show none of them topics to nobody.
   */
  def getBannedAndHiddenTopicsInCurrentOntology = getBadTopics(NonEmptyList(BannedMatch, HiddenMatch), startDate = Some(ontologyCreationDate))

  def getBadConceptLinks(byUser: Option[String] = None, url: Option[String] = None, startDate: Option[DateTime] = None, endDate: Option[DateTime] = None): BadConcepts = {
    val sparql = """
      PREFIX go: <http://insights.gravity.com/2010/9/universe#>
      SELECT ?atTime ?byUser ?matchesConcept ?shouldMatchConcept ?path FROM <http://insights.gravity.com/annotations>
      WHERE {
        ?anno go:byUser ?byUser .
        ?anno go:url ?url .
        ?anno go:atTime ?atTime .

        {
          SELECT ?s ?o WHERE {
            ?s go:broaderProposedConcept ?o .
          }
        } OPTION (TRANSITIVE, t_distinct, t_in(?s), t_out(?o),
            t_step (?s) as ?link, t_step('path_id') as ?path, t_step('step_no') as ?step, t_direction 3) .
        FILTER (?s = ?anno)

        ?link go:matchesConcept ?matchesConcept .
        OPTIONAL {
          ?link go:shouldMatchConcept ?shouldMatchConcept .
        }
      }
    """
    val bindings = makeBindings(
      url map ("url" -> vf.createURI(_))
    )

    def nodeHasCorrectedConcept(row: RowWrapper) = Option(row.getURI("shouldMatchConcept")).isDefined

    val (pathsWithBannedNode, pathsWithBadLink) = baseAnnotationQuery(sparql, bindings, byUser) filter { row =>
      lazy val atTime = row.getDateTime("atTime")
      (startDate some (_ isBefore (atTime)) none (true)) && (endDate some (_ isAfter (atTime)) none (true))
    } groupBy {
      _.getInteger("path")
    } filter { case (_, path) =>
      path exists (nodeHasCorrectedConcept)
    } partition { case (_, path) =>
      path.find(nodeHasCorrectedConcept).get.getURI("shouldMatchConcept") == NS.NEVER_MATCH
    }

    val badLinks = pathsWithBadLink.map{ case (_, path) =>
      val badNodeIdx = path indexWhere (nodeHasCorrectedConcept)
      ConceptLink(path(badNodeIdx-1).getURI("matchesConcept").toString, path(badNodeIdx).getURI("matchesConcept").toString)
    }.toSet

    val badNodes = pathsWithBannedNode.map{ case (_, path) =>
      path.find(nodeHasCorrectedConcept).get.getURI("matchesConcept").toString
    }.toSet

    BadConcepts(badLinks, badNodes)
  }

  def getBadConceptLinksInCurrentOntology(byUser: Option[String] = None, url: Option[String] = None) = getBadConceptLinks(byUser, url, Some(ontologyCreationDate))

  def getRenameNodes(byUser: Option[String] = None, url: Option[String] = None, startDate: Option[DateTime] = None, endDate: Option[DateTime] = None): RenameNodes = {
    val sparql = """
      PREFIX go: <http://insights.gravity.com/2010/9/universe#>
      SELECT DISTINCT ?uri ?shouldRename ?byUser FROM <http://insights.gravity.com/annotations> WHERE {
        ?anno go:matchesTopic ?uri .
        ?anno go:shouldRename ?shouldRename .

        ?anno go:byUser ?byUser .
        ?anno go:url ?url .
        ?anno go:atTime ?atTime .

        %s
        %s
      }
    """ format (
      if (startDate.isDefined) "FILTER ( ?atTime >= ?startDate )" else "",
      if (endDate.isDefined) "FILTER ( ?atTime < ?endDate )" else ""
    )
    val bindings = makeBindings(
      startDate map ("startDate" -> vf.createLiteral(_)),
      endDate map ("endDate" -> vf.createLiteral(_)),
      url map ("url" -> vf.createURI(_))
    )

    val renames = baseAnnotationQuery(sparql, bindings, byUser) map (anno => anno.getString("uri") -> anno.getString("shouldRename"))
    RenameNodes(renames.toMap)
  }

  def getRenameNodesInCurrentOntology(byUser: Option[String] = None, url: Option[String] = None) = getRenameNodes(byUser, url, Some(ontologyCreationDate))
}