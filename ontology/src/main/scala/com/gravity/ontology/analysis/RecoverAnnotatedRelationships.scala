package com.gravity.ontology.analysis

import com.gravity.ontology.vocab.NS
import org.openrdf.model.impl.ValueFactoryImpl
import com.gravity.ontology.{OntologyNode, OntologyGraph2}
import com.gravity.ontology.nodes.{ConceptNode, NodeBase}
import com.gravity.utilities.grvio
import scala.collection.mutable

/**
 * Created by apatel on 1/7/14.
 */

object RecoverAnnotatedRelationships extends App {

  // Annotated Relationship types
  val relMap = Map(
    // relates Concept to Interest
    "INTEREST_OF" -> "http://www.w3.org/1999/02/22-rdf-syntax-ns#type",

    // relates Concept to Concept
    "BROADER_ANNOTATED_CONCEPT" -> NS.BROADER_ANNOTATED_CONCEPT.toString //  "http://insights.gravity.com/2010/9/universe#broaderAnnotatedConcept",

    // relates Interest to Interest
    //"BROADER_INTEREST" -> "http://www.w3.org/2000/01/rdf-schema#subClassOf"
  )

  val annotatedRels = relMap.keys.toSet

  val oldOnt = "graph_concept_gold"
  val fileName = "/Users/apatel/junk/AnnotatedRelationships/rels.txt"
  val rdfFileName = "/Users/apatel/junk/AnnotatedRelationships/ontology2.nt"

  val newOnt = "graph_concept"

  //dumpToFile(fileName)
  //findNode()
  dumpToRDFile(rdfFileName)

  def findNode() = {
    val nodeUri = "http://insights.gravity.com/interest/Art"
    val name = "Art"

    val oi = new OntologyInfo(newOnt)
    val on = oi.graph.node(ValueFactoryImpl.getInstance().createURI(nodeUri))

    var found = false
    for (n <- on) {
      println(n.uri + " exists")
      found = true
    }

    if (!found) println("node not found")


    // add node
    addNode(nodeUri, name)

  }

  def addNode(uri: String, name: String) {
    val gdb = OntologyGraph2.graph.graphDb


    val tx = gdb.beginTx()
    try {
      val newNode = gdb.createNode()
      // add to name index
      // add to uri index

      newNode.setProperty(NodeBase.URI_PROPERTY, uri)
      if (name != null) newNode.setProperty(NodeBase.NAME_PROPERTY, name)
      tx.success()
    }
    catch {
      case e: Exception => throw new Exception("Unable to create node " + uri)
    }
    finally {
      tx.finish()
    }





    //    val graphDir = "/opt/interests/data2/graph_concept"
    //    val populator = new OntologyGraphPopulator3(directory = graphDir, filePaths = Map.empty[String, InputStream], debug = false)
    //    val uri = new URIImpl(nodeUri)
    //    populator.create( uri , name=name, properties= null)

  }

  def dumpToFile(fileName: String) = {
    val oi = new OntologyInfo(oldOnt)
    grvio.appendToFile(fileName)("startNode\tendNode\trelType")
    for (rel <- oi.edgeIterator();
         relName = rel.getType.name() if (annotatedRels.contains(relName))) {
      val sNode = OntologyNode(rel.getStartNode())
      val eNode = OntologyNode(rel.getEndNode())
      grvio.appendToFile(fileName)(safeUri(sNode) + "\t" + safeUri(eNode) + "\t" + relName)
    }

  }

  def dumpToRDFile(fileName: String) = {
    val oi = new OntologyInfo(oldOnt)

    val interestUriSet = mutable.Set.empty[String]

    // example
    // <http://dbpedia.org/resource/Category:Community_organizers> <http://insights.gravity.com/2010/9/universe#broaderAnnotatedConcept> <http://dbpedia.org/resource/Category:Activists_by_type> .

    for (rel <- oi.edgeIterator();
         relName = rel.getType.name() if (annotatedRels.contains(relName))) {
      val sNode = OntologyNode(rel.getStartNode())
      val eNode = OntologyNode(rel.getEndNode())

      //      processNode(sNode, fileName, interestUriSet)
      //      processNode(eNode, fileName, interestUriSet)

      val startUri = safeUri(sNode)
      val endUri = safeUri(eNode)

      grvio.appendToFile(fileName)("<" + startUri + "> <" + relMap(relName) + "> <" + endUri + ">")
    }

  }

  private def processNode(node: OntologyNode, fileName: String, interestUriSet: mutable.Set[String]) {
    val prefix = NS.GRAVITY_RESOURCE_INTEREST_NAMESPACE //"http://insights.gravity.com/interest/"
    val rdfLabel = "<http://www.w3.org/2000/01/rdf-schema#label>"


    val uri = safeUri(node)

    if (uri.contains(prefix) && !interestUriSet.contains(uri)) {

      // process node only once
      interestUriSet.add(uri)

      // output interest label
      // <http://insights.gravity.com/interest/Home> <http://www.w3.org/2000/01/rdf-schema#label> "Home" .
      //val name = node.name
      //val nameToUse =  if (name.size > 0) name else deriveNameFromUri(uri, prefix)
      val nameToUse = deriveNameFromUri(uri, prefix)
      grvio.appendToFile(fileName)("<" + uri + "> " + rdfLabel + " \"" + nameToUse + "\"@en .")

      // output interest level
      //<http://insights.gravity.com/interest/Cooking> <http://insights.gravity.com/2010/9/universe#level> "2"^^<http://www.w3.org/2001/XMLSchema#int> .

      val rdfLevel = "<" + NS.LEVEL + ">"
      val rdfInt = "<http://www.w3.org/2001/XMLSchema#int>"
      val level = node.getProperty(ConceptNode.LEVEL_PROPERTY, -1).asInstanceOf[Int]
      if (level != -1) {
        grvio.appendToFile(fileName)("<" + uri + "> " + rdfLevel + " \"" + level + "\"^^" + rdfInt + " .")
      }

    }
  }

  private def deriveNameFromUri(uri: String, prefix: String) = {
    val parts = uri.split(prefix)
    if (parts.size == 2) {
      val rawName = parts(1)
      val name = convertToAlphaNumeric(rawName)
      name
    }
    else {
      ""
    }

  }

  private def convertToAlphaNumeric(s: String) = {
    val stb = new StringBuilder()
    var space = false
    val safeChars = Set('\'', '-', '(', ')', '!', '&')
    for (ch <- s;
         chAsInt = ch.toInt) {

      if (safeChars.contains(ch) ||
        (chAsInt >= 48 && chAsInt <= 57) /* digit */ ||
        (chAsInt >= 65 && chAsInt <= 90) /* capital letter */ ||
        (chAsInt >= 97 && chAsInt <= 122) /* lowercase letter */ ) {
        stb.append(ch) // safe character
        space = false
      }
      else {
        // unsafe char
        if (!space) {
          stb.append(' ') // replace with space
          space = true
        }
      }
    }
    stb.toString()
  }


  private def safeUri(node: OntologyNode) = {
    try {
      node.uri
    }
    catch {
      case _: Exception => ""
    }
  }
}
