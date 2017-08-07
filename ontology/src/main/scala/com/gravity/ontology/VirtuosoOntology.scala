package com.gravity.ontology

import scala.collection.JavaConversions._
import org.slf4j.LoggerFactory
import vocab.{URIType, NS}
import collection.mutable.ListBuffer
import org.openrdf.model.impl.URIImpl
import java.sql.{DriverManager, ResultSet}
import org.openrdf.rio.{RDFHandler, RDFFormat, Rio}
import com.gravity.ontology.VirtuosoOntology.TopicIterator
import org.openrdf.query.{QueryLanguage, BindingSet}
import org.openrdf.model._
import org.joda.time.DateTime
import java.io.{FileReader, BufferedReader}
import org.openrdf.repository.RepositoryException
import com.gravity.utilities.{Settings, Streams}
import scalaz.{Value =>_, _}
import Scalaz._

/**
 * User: chris
 * Date: 12/14/10
 * Time: 3:24 PM
 */

object VirtuosoOntology2 extends VirtuosoSparqlQuery {
  private implicit def bindingSetToRowWrapper(x: BindingSet) = new RowWrapper(x)

  implicit def nativeToResultSetWrapper(x: ResultSet) = new ResultSetWrapper(x)

  private val logger = LoggerFactory.getLogger(getClass)

  def getQuery(resName: String) = Streams.getResource("queries/" + resName, getClass)

  def alternateLabels(labelIterator: (URI, String) => Unit) = {
    query("Get alternate labels", "getAlternateLabels.query", (row, count) => {
      labelIterator(row.getURI("topicURI"), row.getString("alternateLabel"))
    })
  }

  def contextualPhrases(phraseIterator: (String, URI, Boolean) => Unit) = {
    query("Get contextual phrases", "getContextualPhrases.query", (row, count) => {
      val phraseRequired = row.getBool("phraseRequired")
      phraseIterator(row.getString("phraseText"), row.getURI("indicatedNode"), row.getBool("phraseRequired").get)
    })
  }

  def lowercaseBlacklist(blacklistIterator: (String) => Unit) = {
    query("Get lowercase blacklist", "getBlacklistLowercase.query", (row, count) => {
      blacklistIterator(row.getValue("blacklistedText").stringValue)
    })
  }

  def gravityOntology(rdfIterator: (URI, URI, Value) => Unit) = {
    query("Get gravity ontology", "getOntology.query", (row, count) => {
      rdfIterator(row.getURI("subject"), row.getURI("predicate"), row.getValue("object"))
    })
  }

  def topicTemplates(templateIterator: (URI, URI) => Unit) = {
    query("Get topic templates", "getTemplates.query", (row, count) => {
      templateIterator(row.getURI("topicId"), row.getURI("template"))
    })
  }

  def redirects(redirectIterator: (URI, URI) => Unit) = {
    query("Get redirects", "getRedirects.query", (row, count) => {
      redirectIterator(row.getURI("subject"), row.getURI("redirect"))
    })
  }

  def removals(removalIterator: (URI) => Unit) = {
    query("Get removals", "getRemovals.query", (row, count) => {
      removalIterator(row.getURI("topicId"))
    })
  }

  def renames(renameIterator: (URI, String) => Unit) = {
    query("Get renames", "getRenames.query", (row, count) => {
      renameIterator(row.getURI("topicId"), row.getString("renameTo"))
    })
  }

  def disambiguations(disIterator: (URI, URI) => Unit) = {
    query("Get disambiguations", "getDisambiguations.query", (row, count) => {
      disIterator(row.getURI("subject"), row.getURI("disambiguates"))
    })
  }

  def dbpediaOntology(ontologyIterator: (URI, String, Int, URI) => Unit) = {
    query("Get dbpedia ontology", "getDbPediaOntology.query", (row, count) => {
      if (!row.getString("name").contains("Aktivit")) {
        ontologyIterator(row.getURI("classId"), row.getString("name"), row.getInteger("level"), row.getURI("superClass"))
      }
    })
  }

  def topicConcepts(topicConceptIterator: (URI, URI) => Unit) = {
    query("Get topics to concepts", "getTopicConcepts.query", (row, count) => {
      if (NS.getType(row.getURI("topicId")) == URIType.TOPIC) {
        topicConceptIterator(row.getURI("topicId"), row.getURI("categoryId"))
      }
    })
  }

  def conceptHierarchy(conceptIterator: (URI, URI) => Unit) = {
    query("Get concept hierarchy", "getConceptHierarchy.query", (row, count) => {
      conceptIterator(row.getURI("conceptId"), row.getURI("targetId"))
    })
  }

  def concepts(conceptIterator: (URI, String) => Unit) = {
    query("Get concepts", "getAllConcepts.query", (row, count) => {
      conceptIterator(row.getURI("conceptId"), row.getString("conceptName"))
    })
  }

  def viewCounts(viewCountIterator: (URI, Int) => Unit) = {
    query("Get view counts", "getViewCounts.query", (row, count) => {
      viewCountIterator(row.getURI("subject"), row.getInteger("viewCount"))
    })
  }

  def topics(topicIterator: (URI, String) => Unit) = {
    nativequery("Get topics","getAllTopics.query",(row,count)=>{
      if (NS.getType(row.getURI("topicId")) == URIType.TOPIC) {
        topicIterator(row.getURI("topicId"), row.getString("topicName"))
      }
    })
  }

  def topicsFile(topicIterator:TopicIterator) = {
    val path = "/usr/local/virtuoso-opensource/var/lib/virtuoso/ontology_db/rdf/ontology/dbp36/labels_en.nt"
    rdfFile(path,st=>{
      topicIterator.row(st.getSubject.asInstanceOf[URI],st.getObject().stringValue,null,null)
    })
  }

  def rdfFile(path:String, handler:(Statement) => Unit) = {
    val parser = Rio.createParser(RDFFormat.NTRIPLES)
    parser.setRDFHandler(new RDFHandler() {
      def startRDF(){}
      def handleStatement(st: Statement) {handler(st)}
      def handleNamespace(prefix: String, uri: String) {}
      def handleComment(comment: String) {}
      def endRDF(){}
    })
    parser.parse(new BufferedReader(new FileReader(path)),"")
  }

  def topicsToClasses(ttcIterator: (URI, URI) => Unit) = {
    query("Topics to classes","getTopicTypeInfo.query", (row,count)=>{
      val topicId = row.getURI("topicId")
      val typeId = row.getURI("typeId")
      ttcIterator(topicId,typeId)
    })
  }

  def makeBindings(pairs: Option[(String, Value)]*): Map[String, Value] = {
    pairs.flatten.toMap
  }

  def query(queryName: String, queryFile: String, row: (BindingSet, Int) => Unit) {
    val connection = graph.getConnection
    try {
      val tupleQuery = connection.prepareTupleQuery(QueryLanguage.SPARQL, getQuery(queryFile))
      val queryResult = tupleQuery.evaluate
      var count = 0
      while (queryResult.hasNext) {
        count = count + 1
        row(queryResult.next, count)
        if (count % 10000 == 0) {
          logger.info(count + " rows through " + queryName)
        }
      }
      logger.info("Completed " + queryName + " with " + count + " rows.")
    } catch {
      case e: Exception => logger.error("Exception whilst issuing query", e)
    } finally {
      connection.close
    }
  }

  def withNativeConnection(stmtHandler: (java.sql.Statement) => Any)(implicit errorHandler: (Throwable) => Any = throw _) {
    Class.forName("virtuoso.jdbc3.Driver").newInstance
    val connection = DriverManager.getConnection(Settings.VIRTUOSO_LOCATION, Settings.VIRTUOSO_USERNAME, Settings.VIRTUOSO_PASSWORD)
    try {
      val stmt = connection.createStatement()
      stmtHandler(stmt)
    }
    catch {
      case ex: Exception => errorHandler(ex)
    }
    finally {
      connection.close()
    }
  }

  def nativequery(queryName: String, queryFile: String, row: (ResultSet, Int) => Unit) {
    var rowCount = 0
    //implicit val errorHandler = (ex: Throwable) => logger.error("Got exception at row " + rowCount, ex)
    withNativeConnection { stmt =>
      val more = stmt.execute("sparql " + getQuery(queryFile))
      if (more) {
        val resultSet = stmt.getResultSet
        while (resultSet.next) {
          rowCount = rowCount + 1
          row(resultSet, rowCount)
          if (rowCount % 10000 == 0) {
            logger.info(rowCount + " rows via " + queryName)
          }
        }
      }
    }
  }

  def main(args: Array[String]) = {

    this.topics((uri, name) => assert(uri != null && name != null))
  }

}

class RowWrapper(original: BindingSet) {
  def getURI(row: String): URI = {
    return original.getValue(row).asInstanceOf[URI]
  }

  def getBool(row: String) = Option(original.getValue(row)) map (_.asInstanceOf[Literal].booleanValue)

  def getString(row: String): String = {
    return original.getValue(row).stringValue
  }

  def getInteger(row: String): Int = {
    return original.getValue(row).asInstanceOf[Literal].intValue
  }

  def getDateTime(row: String): DateTime = {
    new DateTime(original.getValue(row).asInstanceOf[Literal].calendarValue().toGregorianCalendar)
  }

  def yomama() = {

  }

  override def toString = {
    original.getBindingNames map (name => name + "=" + original.getValue(name)) mkString (", ")
  }
}

class ResultSetWrapper(original: ResultSet) {
  def getURI(row: String): URI = {
    return new URIImpl(original.getString(row))
  }
}

trait SparqlQuery {
  def sparqlQuery(query: String, bindings: Map[String, Value] = Map.empty): Seq[RowWrapper]
}

trait IgnoreConnectionError extends SparqlQuery {
 import com.gravity.logging.Logging._
  abstract override def sparqlQuery(query: String, bindings: Map[String, Value] = Map.empty) = {
    try {
      super.sparqlQuery(query, bindings)
    }
    catch {
      case rex: RepositoryException if Option(rex.getCause) some (_.getMessage contains ("Connection refused")) none (false) => {
        warn("Ignoring connection error; returning empty result set. " + rex.getMessage)
        Seq.empty
      }
    }
  }
}

trait VirtuosoSparqlQuery extends SparqlQuery {
  protected val graph = RdfGraphFactory.getVirtuosoGraph

  private implicit def bindingSetToRowWrapper(x: BindingSet) = new RowWrapper(x)

  override def sparqlQuery(query: String, bindings: Map[String, Value] = Map.empty): Seq[RowWrapper] = {
    val connection = graph.getRepository.getConnection
    try {
      val tupleQuery = connection.prepareTupleQuery(QueryLanguage.SPARQL, query)
      bindings foreach { case (k, v) => tupleQuery.setBinding(k, v) }

      val queryResult = tupleQuery.evaluate
      val rows = new ListBuffer[RowWrapper]
      while (queryResult.hasNext) {
        rows += queryResult.next
      }

      rows
    }
    finally {
      connection.close()
    }
  }
}
