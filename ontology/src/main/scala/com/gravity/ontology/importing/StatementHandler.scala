package com.gravity.ontology.importing

import org.openrdf.model.{Statement, URI}
import collection.mutable.ListBuffer
import collection.mutable
import com.gravity.ontology.nodes.{TopicRelationshipTypes, NodeBase}
import org.openrdf.model.vocabulary.RDF
import com.gravity.utilities.grvmath
import com.gravity.ontology.vocab.{URIType, NS}
import org.openrdf.model.impl.BooleanLiteralImpl


/*             )\._.,--....,'``.
.b--.        /;   _.. \   _\  (`._ ,.
`=,-,-'~~~   `----(,_..'--(,_..'`-.;.'  */


abstract class StatementHandler(uri: URI, buffer: ListBuffer[Statement], firstPass: Boolean, ogp: OntologyGraphPopulator3) {
  implicit def toBufferWrapper(statementBuffer: ListBuffer[Statement]): StatementBuffer = new StatementBuffer(statementBuffer)

  implicit def statementWrapper(statement: Statement) = new StatementWrapper(statement)

  lazy val timedViews = buffer.list(NS.VIEWCOUNT_AND_DAY)

  lazy val totalViews = {
    timedViews.foldLeft(0)((total, stmt) => {
      val (viewCount, dateString) = wikiPageView.getTupleOfCountDate(stmt.objectString)
      viewCount + total
    })
  }

  lazy val recentViews = {
    timedViews.foldLeft(-1, -1, -1)((thatTotal, thisStatement) => {
      def countYearDay(str: String) = {
        val arr = str.split("\\|")
        val count = arr(0).toInt
        val year = arr(1).take(4).toInt
        val day = arr(1).drop(5).toInt
        (count, year, day)
      }
      val thisTotal = countYearDay(thisStatement.objectString)
      val (thatCount, thatYear, thatDay) = thatTotal
      val (thisCount, thisYear, thisDay) = thisTotal

      if (thatYear > thisYear) {
        thatTotal
      }
      else if (thatYear < thisYear) {
        thisTotal
      }
      else if (thatDay >= thisDay) {
        thatTotal
      }
      else {
        thisTotal
      }
    })
  }


  def saveNodes()

  def saveRelations()

  def hasOnlyViews = {
    /**
     * if we have view counts where we don't have any matching topics let's log those in case there is
     * any gold showing topics that are getting popular that we don't have in our ontolgy yet.
     * to view the data by number of pageviews use the following cmd
     * sort -n -r -k2 viewsWithNoTopics.log
     */
    if (timedViews.size == buffer.size) {
      ogp.addCount("Topics with only viewcounts")

      if (timedViews.size > 0) {
        ogp.viewsWithNoTopicsOutfile match {
          case Some(outfile) => outfile.write("%s\t%s\n".format(uri, totalViews))
          case None =>
        }
      }
      true
    } else {
      false
    }
  }

  def work() {
    if (firstPass) {
      if (hasOnlyViews) {
        ogp.addCount("Items with only views")
      } else {
        saveNodes()
      }
    }
    else {
      saveRelations()
    }
  }
}

class ClassHandler(uri: URI, buffer: ListBuffer[Statement], firstPass: Boolean, ogp: OntologyGraphPopulator3) extends StatementHandler(uri, buffer, firstPass, ogp) {
  override def saveNodes() {

  }

  override def saveRelations() {
    buffer.list(RDF.TYPE) foreach {
      statement =>
        if (NS.getType(statement.objectURI) == URIType.GRAVITY_INTEREST) {
          ogp.relateNoCreate(statement.subjectURI, statement.objectURI, TopicRelationshipTypes.INTEREST_OF)
          ogp.addCount("Classes Related to Interests")
        }
    }
  }
}

trait FilterBadBroaderRelationships {
  this: StatementHandler =>

  val unfilteredBuffer: ListBuffer[Statement]

  private val RELEVANT_RELATIONSHIPS = Set(NS.SKOS_BROADER, NS.BROADER_ANNOTATED_CONCEPT, NS.DBPEDIA_WIKIPAGE_REDIRECT, NS.PURL_SUBJECT, RDF.TYPE)

  private def isRelevantBroaderRelationship(stmt: Statement) = {
    RELEVANT_RELATIONSHIPS.contains(stmt.predicateURI) && (stmt.predicateURI != RDF.TYPE || NS.getType(stmt.objectURI) == URIType.GRAVITY_INTEREST)
  }

  val buffer = {
    val badConceptLinks = unfilteredBuffer.list(NS.DO_NOT_FOLLOW_BROADER_CONCEPT).map(_.objectURI).toSet
    unfilteredBuffer filter { stmt =>
      if (isRelevantBroaderRelationship(stmt)) {
        !badConceptLinks.contains(stmt.objectURI)
      }
      else {
        true
      }
    }
  }
}

class CategoryHandler(uri: URI, override val unfilteredBuffer: ListBuffer[Statement], firstPass: Boolean, ogp: OntologyGraphPopulator3) extends StatementHandler(uri, unfilteredBuffer, firstPass, ogp) with FilterBadBroaderRelationships {
  lazy val shouldExclude = buffer.exists(stmt => stmt.predicateURI == NS.DO_NOT_GRAPH && stmt.getObject == BooleanLiteralImpl.TRUE)

  override def saveNodes() {
    if (shouldExclude) {
      ogp.addCount("Concepts filtered from removal list")
      return
    }
    
    ogp.addCount("Categories created")


    val name = buffer.stringVal(NS.SKOS_PREFLABEL).orElse(Some(uri.getLocalName.replace("Category:", ""))).get
    val viewCount = totalViews

        val googleSearchCount = buffer.intVal(NS.GOOGLE_SEARCH_CNT).orElse(Some(0)).get
    ogp.nodeId(uri, mutable.Map[String, AnyRef](NodeBase.NAME_PROPERTY -> name, NodeBase.VIEWCOUNT_PROPERTY -> viewCount.asInstanceOf[AnyRef], NodeBase.GOOGLE_SEARCH_COUNT_PROPERTY -> googleSearchCount.asInstanceOf[AnyRef]))

  }

  override def saveRelations() {
    if (shouldExclude) {
      return
    }
    
    buffer.list(NS.SKOS_BROADER).foreach {
      statement =>
        ogp.relate(uri, statement.objectURI, TopicRelationshipTypes.BROADER_CONCEPT)
        ogp.addCount("Broader Concepts related")
    }

    buffer.list(NS.BROADER_ANNOTATED_CONCEPT).foreach {
      statement =>
      ogp.relateNoCreate(uri,statement.objectURI, TopicRelationshipTypes.BROADER_ANNOTATED_CONCEPT)
      ogp.addCount("Broader Annotated Concepts related")
    }

    buffer.list(RDF.TYPE).foreach {
      statement =>
        if (NS.getType(statement.objectURI) == URIType.GRAVITY_INTEREST) {
          ogp.relateNoCreate(statement.subjectURI, statement.objectURI, TopicRelationshipTypes.INTEREST_OF)
          ogp.addCount("Categories related to interests")
        }
    }

  }
}

class ColloquialGramHandler(uri: URI, buffer: ListBuffer[Statement], firstPass: Boolean, ogp: OntologyGraphPopulator3) extends StatementHandler(uri, buffer, firstPass, ogp) {
  override def saveNodes() {
    buffer.find(NS.NGRAM).foreach {
      case statement: Statement =>
        if (buffer.hasPredicate(NS.INDICATED_NODE) || buffer.hasPredicate(NS.INDICATED_NODE_NEW)) {
          val name = statement.objectString
          ogp.nodeId(uri, mutable.Map[String, AnyRef](NodeBase.NAME_PROPERTY -> name))
          ogp.addCount("Colloquial Grams from " + uri.getNamespace)
        }
    }
  }

  override def saveRelations() {
    buffer.find(NS.INDICATED_NODE).foreach {
      case statement: Statement =>
        ogp.relateNoCreate(statement.subjectURI, statement.objectURI, TopicRelationshipTypes.COLLOQUIAL_PHRASE_OF)
        ogp.addCount("Colloquial Grams from " + uri.getNamespace + " added to topics")
    }
  }
}

class TagHandler(uri: URI, buffer: ListBuffer[Statement], firstPass: Boolean, ogp: OntologyGraphPopulator3) extends StatementHandler(uri, buffer, firstPass, ogp) {
  override def saveNodes() {

    for {
      nameStatement <- buffer.find(NS.NGRAM, NS.NGRAM_NEW)
      extractedFromDomainStatement <- buffer.list(NS.EXTRACTED_FROM_DOMAIN)

    } {
      val name = nameStatement.objectString
      val indicatedDomain = extractedFromDomainStatement.objectString

      ogp.nodeId(uri, mutable.Map[String, AnyRef](NodeBase.NAME_PROPERTY -> name, NodeBase.VALID_DOMAINS -> indicatedDomain))
      ogp.addCount("Tags from " + uri.getNamespace)

    }

  }

  override def saveRelations() {
    buffer.find(NS.INDICATED_NODE, NS.INDICATED_NODE_NEW).foreach {
      case statement: Statement =>
        ogp.relateNoCreate(statement.subjectURI, statement.objectURI, TopicRelationshipTypes.TAG_INDICATES_NODE)
        ogp.addCount("Tags from " + uri.getNamespace + " added to topics")
    }
  }
}


class TopicHandler(uri: URI, override val unfilteredBuffer: ListBuffer[Statement], firstPass: Boolean, ogp: OntologyGraphPopulator3) extends StatementHandler(uri, unfilteredBuffer, firstPass, ogp) with FilterBadBroaderRelationships {


  def shouldBeFiltered(uri: URI, name: String = null, buffer: ListBuffer[Statement]): Boolean = {
    if (buffer.exists(stmt => stmt.predicateURI == NS.DO_NOT_GRAPH && stmt.getObject == BooleanLiteralImpl.TRUE)) {
      ogp.addCount("Topics filtered from removal list")
      return true
    }


    //Remove topics whose concepts are in a filtration list because those concepts are typically indicative of not usefulness.

  //Temporary removal to create unfiltered ontology
    buffer.list(NS.PURL_SUBJECT).foreach(stmt => {
      if (ogp.badConcepts.contains(stmt.objectURI)) {
        ogp.addCount("Topics removed because concepts in Bad Concept List -- temporarily not done")
//        return true
      }
    })


    if (name != null && ogp.lowercaseBlacklist.contains(name.toLowerCase)) {
      ogp.addCount("Topics filtered from blacklist -- temporarily not done")
//      return true
    }
    buffer.find(NS.DBPEDIA_WIKIPAGE_USESTEMPLATE) match {
      case Some(stmt) => {
        if (ogp.badTemplates.contains(stmt.objectURI)) {
          ogp.addCount("Topics filtered from template list -- temporarily not done")
//          return true
        }
      }
      case None => return false
    }
    return false
  }


  override def saveNodes() {
    val name = buffer.nameProp.orElse(Some(uri.getLocalName)).get

    if (shouldBeFiltered(uri, name, buffer)) {
      return
    }


    val (recentViewCount, viewYear, viewDay) = recentViews

    val properties = mutable.Map[String, AnyRef]()

    if (recentViewCount > 0) {
      properties += (NodeBase.VIEWCOUNT_RECENT_PROPERTY -> recentViewCount.asInstanceOf[AnyRef])
    }

    val viewCount = totalViews

    if (viewCount > 0) {
      properties += (NodeBase.VIEWCOUNT_PROPERTY -> viewCount.asInstanceOf[AnyRef])

      val trendingScore = wikiPageView.getTrendingScore(timedViews)
      properties += (NodeBase.TRENDING_VIEW_SCORE -> trendingScore.asInstanceOf[AnyRef])
    } else {
      properties += (NodeBase.TRENDING_VIEW_SCORE -> 0.00.asInstanceOf[AnyRef])
    }

    properties += (NodeBase.NAME_PROPERTY -> name)
    ogp.nodeId(uri, properties)

    ogp.addCount("Topic nodes created")

  }

  override def saveRelations() {

    for (categoryRel <- buffer.list(NS.PURL_SUBJECT)) {
      ogp.relateNoCreate(uri, categoryRel.objectURI, TopicRelationshipTypes.CONCEPT_OF_TOPIC)
      ogp.addCount("Topics to Concepts Related")
    }

    val classes = buffer.list(RDF.TYPE)

    if (classes.size > 0) {
      val topClass = classes.map(stmt => {
        (stmt.objectURI, ogp.levelMap.getOrElse(stmt.objectURI, -1))
      }).sortBy(-_._2).headOption.orNull

      if (topClass != null) {
        ogp.relateNoCreate(uri, topClass._1, TopicRelationshipTypes.DBPEDIA_CLASS_OF_TOPIC)
        ogp.addCount("Topics to Classes Related")
      }
    }

    buffer.list(NS.DBPEDIA_WIKIPAGE_PAGELINK).foreach(stmt =>{
      if(NS.getType(stmt.objectURI) == URIType.TOPIC) {
        ogp.relateNoCreate(stmt.subjectURI, stmt.objectURI, TopicRelationshipTypes.TOPIC_LINK)
        ogp.addCount("Topics to Topics Related")
      }else {
        ogp.addCount("Topics Pagelinks to non-topics avoided")
      }
    })


    buffer.find(NS.DBPEDIA_WIKIPAGE_REDIRECT) match {
      case Some(stmt) => {
        ogp.relateNoCreate(uri, stmt.objectURI, TopicRelationshipTypes.REDIRECTS_TO)
        ogp.addCount("Wiki Redirects Found")
      }
      case None =>
    }

    for (stmt <- buffer.list(NS.DBPEDIA_WIKIPAGE_DISAMBIGUATES)) {
      ogp.relateNoCreate(uri, stmt.objectURI, TopicRelationshipTypes.DISAMBIGUATES)
      ogp.addCount("Disambiguations Found")
    }

  }
}



