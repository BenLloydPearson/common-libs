package com.gravity.ontology.importing

import org.neo4j.kernel.impl.batchinsert.BatchInserterImpl
import scala.collection.JavaConversions._
import scala.collection._
import mutable.ListBuffer
import com.gravity.ontology.importing.OntologySource._
import scala.io.Source
import com.gravity.ontology.VirtuosoOntology2
import com.gravity.ontology.vocab.{URIType, NS}
import com.gravity.ontology.nodes.{ConceptNode, TopicRelationshipTypes, NodeBase}
import org.openrdf.model.{Statement, URI}
import java.io._
import org.openrdf.model.vocabulary.RDFS
import org.openrdf.model.impl.URIImpl
import org.neo4j.index.impl.lucene.LuceneBatchInserterIndexProvider
import org.neo4j.graphdb.RelationshipType
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{Path, FileSystem}
import com.gravity.hadoop.Implicits._
import com.gravity.utilities.{Stopwatch, grvmath, Directory}
import org.apache.commons.io.FilenameUtils
import org.joda.time.format.DateTimeFormat
import org.joda.time.{DateTimeZone, DateTime}
import com.gravity.ontology.annotation.Annotation2OntologyWriter

/*             )\._.,--....,'``.
.b--.        /;   _.. \   _\  (`._ ,.
`=,-,-'~~~   `----(,_..'--(,_..'`-.;.'  */

object OntologyBuilder extends App {
  OntologyGraphPopulator3.main(Array.ofDim[String](0))
}


/**
 * Sequel-itis anyone?
 * The main method of this singleton will run the ontology in production from grv-graph01.
 * If you want to build a test ontology to play around with, use OntologyGraphPopulator3IT.
 */
object OntologyGraphPopulator3 {
  lazy val fs = FileSystem.get(new java.net.URI("hdfs://sjc1-hadoopm01.prod.grv:8020"), new Configuration())

  val graphBaseDir = "/opt/interests/graph-staging/"
  var graphDirectory = graphBaseDir + "populatedgraph"
  val versionFile = "version.txt"
  val versionFileSalutation = "Graph created on: "
  val versionDateFormatter = DateTimeFormat.forPattern("dd MMM yyyy HH:mm:ss Z").withZone(DateTimeZone.UTC)

  def getStream(name: String, basePath: String = "/user/gravity/dbpedia/") = {
    val path = new Path(basePath + name)
    if (fs.getFileStatus(path).isDirectory) {
      fs.openParts(path)
    }
    else {

      fs.open(path)
    }
  }

  def main(args: Array[String]) {
    //MAVEN_OPTS="-Xms12000m -Xmx12000m" mvn -P production -pl ontology exec:java -Dexec.mainClass=com.gravity.ontology.importing.OntologyGraphPopulator3 -e

    if (args.size > 0) {
      graphDirectory = graphBaseDir + args(0)
    }

    println("GraphDirectory is now: " + graphDirectory)

    //Annotation2OntologyWriter.writeToHDFS()

    val testStreams = Map(
      "sorted-dbpedia" -> getStream("3.8-mushed", "/user/gravity/dbpedia/"),
      "sorted-dbpedia2" -> getStream("3.8-mushed", "/user/gravity/dbpedia/"),
      "removed-infoboxes" -> getClass.getResourceAsStream("removed_infoboxes.csv"),
      //"lowercase-blacklist" -> getStream("blacklist.n3"),
      "gravity-ontology" -> getClass.getResourceAsStream("../ontology2.n3")
      //"classes-to-ontology" -> getClass.getResourceAsStream("../class_to_interests.nt")
    )

    val populator = new OntologyGraphPopulator3(directory = graphDirectory, filePaths = testStreams, debug = false)

    var running = true

    new Thread(new Runnable() {
      override def run() {
        while (running) {
          Thread.sleep(5000)
          populator.showCounters()
        }
      }
    }).start()


    val sw = new Stopwatch()
    sw.start()
    try {
      populator.populate()
      populator.optimize()
      populator.shutdown()

      sw.stop()

      println("Ontology built in " + sw.getFormattedDuration)
      running = false

      try {
        val osw = new OutputStreamWriter(new FileOutputStream(FilenameUtils.concat(graphDirectory, versionFile)))
        osw.write(versionFileSalutation + versionDateFormatter.print(new DateTime) + "\n")
        osw.close()
        println("Version file written")
      } catch {
        case ex: IOException => {
          println("Error whilst writing version file, this still means the graph is done.")
          ex.printStackTrace()
        }
      }
    } catch {
      case ex: Exception => {
        ex.printStackTrace()

        running = false
      }
    }

  }
}

class OntologyGraphPopulator3(directory: String, deleteExistingOutput: Boolean = true, filePaths: Map[String, InputStream], debug: Boolean = true) {

  Directory.delete(directory)

  val params = mutable.Map("neostore.nodestore.db.mapped_memory" -> "200M",
    "neostore.propertystore.db.strings.mapped_memory" -> "3G",
    "neostore.propertystore.db.mapped_memory" -> "800M",
    "neostore.relationshipstore.db.mapped_memory" -> "800M"
  )

  val batchInserter = new BatchInserterImpl(directory, params)
  val batchIndex = new LuceneBatchInserterIndexProvider(batchInserter)

  val nameIndex = batchIndex.nodeIndex("names", mutable.Map("type" -> "exact"))
  val urisIndex = batchIndex.nodeIndex("uris", mutable.Map("type" -> "exact"))


  val badTemplates = new mutable.HashSet[URI]()
  val lowercaseBlacklist = new mutable.HashSet[String]()
  val levelMap = new mutable.HashMap[URI, Int]()

  val viewsWithNoTopicsOutfile: Option[BufferedWriter] = createViewsOutputFile()

  val badConcepts = Set("Surname_stubs", "Given_names", "Waste", "Feces").map(NS.getConceptURI(_))
  val badWords = Set("names", "disambiguation", "defunct", "porn", "abuse", "child", "pedo").map(NS.getConceptURI(_)) // lowercaseBlacklist terms


  val nodeIdMap = new mutable.HashMap[URI, Long]()

  def createViewsOutputFile(): Option[BufferedWriter] = {
    // clear out the old viewsWithNoTopics File that holds view counts where we don't have matching topics
    println("Cleaning up local resources")
    val filename = "/mnt/disk1/gravity/viewsWithNoTopics.log"
    try {
      val f = new File(filename)
      if (f.exists()) f.delete()
      try {
        Some(new BufferedWriter(new FileWriter(filename)));
      } catch {
        case e: java.io.IOException => println("Unabel to create BufferedWriter for viewsWithNoTopics: " + e.toString); None
      }
    } catch {
      case e: java.io.IOException => println("Unabel to remove viewsWithNoTopics: " + e.toString); None
    }
  }

  /**
   * Performs all the tasks necessary to create the graph.  You need to call shutdown.
   */
  def populate() {
    makeFilters() //Makes blacklist filteres and such
    //makeDbpediaClassHierarchy() //Make the dbpedia class hierarchy first, so other topics can fall into place
    makeGravityOntology() //Make the gravity ontology, which is a scaffold on top of the dbpedia hierarchy
    //linkClassesToOntology() //Link dbpedia classes to gravity interests
    bufferedTriples(true) //Roll through the triples and build the nodes
    println("Tuples done, moving on to relationships")
    optimize()
    bufferedTriples(false) //Roll through the triples a second time and relate nodes to one another

    showCounters()
  }

  /**
   * Helper method that will only print a line if the debug parameter is true
   */
  def print(message: => String) {
    if (debug) println(message)
  }

  /**
   * Will display the performance counters.
   */
  def showCounters() {
    println("==COUNTERS==")
    for ((name, count) <- counts) {
      println("Counter: " + name + " : Count : " + count)
    }
  }

  val counts = mutable.HashMap[String, Long]()

  /**
   * Adds a point to a counter (which is lazily created)
   */
  def addCount(name: String) {
    val count = counts.getOrElseUpdate(name, 0l)
    counts += (name -> (count + 1l))
  }


  /**
   * Creates an empty mutable property map with the types preferred by Neo4j
   */
  def newPropMap() = {
    new mutable.HashMap[String, AnyRef]()
  }


  /**
   * Lazily creates a new node and caches its id for later use.
   * Node IDs are cached so we don't have to hit the persistent index as it's being built.
   * MEMORY LEAK WARNING: If the ontology gets way huge, this will exceed the amount of memory on the machine and cause an out of memory exception and/or swapping.
   */
  def nodeId(uri: URI, properties: mutable.Map[String, AnyRef] = null) = {
    nodeIdMap.getOrElseUpdate(uri, {
      create(uri = uri, properties = properties)
    })
  }

  /**
   * Will get a node ID without lazily creating the node.  This is useful when you're creating relationships.
   */
  def nodeIdNoCreate(uri: URI) = {
    nodeIdMap.get(uri)
  }

  /**
   * Add a property to a node (if it's the NAME property then use the addName call, because that needs to be search indexed)
   */
  def addProp(uri: URI, key: String, anyVal: AnyRef): Long = {
    val nnodeId = nodeId(uri)
    val props = batchInserter.getNodeProperties(nnodeId)
    props.put(key, anyVal)
    batchInserter.setNodeProperties(nnodeId, props)
    nnodeId
  }

  /**
   * Add a name to a node, this will search index it.
   */
  def addName(uri: URI, name: String) {
    val nodeId = addProp(uri, NodeBase.NAME_PROPERTY, name)
    nameIndex.add(nodeId, mutable.Map[String, AnyRef](NodeBase.NAME_PROPERTY -> name.toLowerCase))
    addCount("Names indexed")
  }

  /**
   * Search indexes a URI to nodeID pair.
   */
  def indexUri(nodeId: Long, uri: URI) {
    addCount("Uris Indexed")
    urisIndex.add(nodeId, mutable.Map[String, AnyRef](NodeBase.URI_PROPERTY -> NS.getHashKey(uri).asInstanceOf[AnyRef]))
  }

  /**
   * Creates a node and adds the given properties.  These properties, by convention, should be keyed using the constants in the
   * NodeBase class (e.g. NodeBase.NAME_PROPERTY)
   */
  def create(uri: URI, name: String = null, properties: mutable.Map[String, AnyRef]): Long = {

    val props = if (properties != null) properties else newPropMap()

    props += (NodeBase.URI_PROPERTY -> uri.stringValue)
    if (name != null) props += (NodeBase.NAME_PROPERTY -> name)
    val nodeId = batchInserter.createNode(props)
    indexUri(nodeId, uri)

    if (name != null) nameIndex.add(nodeId, mutable.Map[String, AnyRef](NodeBase.NAME_PROPERTY -> name.toLowerCase))

    else if (props.contains(NodeBase.NAME_PROPERTY)) {
      val propName = props(NodeBase.NAME_PROPERTY).asInstanceOf[String].toLowerCase
      nameIndex.add(nodeId, mutable.Map[String, AnyRef](NodeBase.NAME_PROPERTY -> propName))
    }

    nodeId
  }

  /**
   * Relates two nodes, lazily creating the nodes if they don't exist.
   */
  def relate(uri: URI, target: URI, relationshipType: RelationshipType, properties: mutable.Map[String, AnyRef] = newPropMap()) = {
    val uriId = nodeId(uri)
    val targetId = nodeId(target)
    if (uriId != targetId) {
      batchInserter.createRelationship(uriId, targetId, relationshipType, properties)
    }
    else {
      addCount("Colliding Nodes")
    }
  }

  /**
   * Relates two nodes, but silently does nothing if either of the nodes doesn't exist.
   */
  def relateNoCreate(uri: URI, target: URI, relationshipType: RelationshipType, properties: mutable.Map[String, AnyRef] = newPropMap()) = {
    nodeIdNoCreate(uri).foreach(uriId => {
      nodeIdNoCreate(target).foreach(targetId => {
        if (uriId != targetId) {
          batchInserter.createRelationship(uriId, targetId, relationshipType, properties)
        }
      })
    })
  }

  /**
   * Makes an index of dbpedia class URI to what level it's on.  Can be used to determine the lowest level a topic should be attached to.
   */
  def makeDbpediaClassHierarchy() {

    val alreadyRelated = mutable.ArrayBuffer[(URI, URI)]()

    VirtuosoOntology2.dbpediaOntology((typeId, className, level, superClassId) => {
      val propMap = newPropMap()
      propMap += (NodeBase.NAME_PROPERTY -> className)
      propMap += (ConceptNode.LEVEL_PROPERTY -> level.asInstanceOf[AnyRef])
      nodeId(typeId, propMap)
      levelMap += (typeId -> level.intValue)
    })

    VirtuosoOntology2.dbpediaOntology((typeId, className, level, superClassId) => {
      val comparison = (typeId, superClassId)

      if (alreadyRelated.exists(itm => itm == comparison)) {

      } else {
        relateNoCreate(typeId, superClassId, TopicRelationshipTypes.BROADER_CONCEPT)
        alreadyRelated += comparison

      }
    })


    if (debug) levelMap.foreach {
      case (key, value) => println("Class: " + key.stringValue + " : Level : " + value)
    }
  }

  /**
   * Blacklists, removal lists, templates, etc.  A series of indices that filter things later on.
   */
  def makeFilters() {
    //Populate an index of bad templates--will use string matching without case sensitivity to determine this.
    Source.fromInputStream(filePaths("removed-infoboxes")).getLines.foreach(itm => badTemplates += new URIImpl(itm))
    //Will check to see if each topic has <http://dbpedia.org/property/wikiPageUsesTemplate> and kill if in the badTemplates index

    //    rdfFile(filePaths("lowercase-blacklist")) {
    //      statement =>
    //        lowercaseBlacklist += statement.objectString
    //    }
  }

  def linkClassesToOntology() {
    rdfFile(filePaths("classes-to-ontology")) {
      statement =>
        val (subject, predicate, obj) = statement.SPO
        NS.getType(subject) match {
          case URIType.DBPEDIA_CLASS => {
            if (!statement.objectURI.getLocalName.contains("DbPediaClass")) {
              //All dbpedia classes subclass DbPediaClass, we get rid of that because it's unnecessary
              addCount("Classes to Interests Related")
              relate(statement.subjectURI, statement.objectURI, TopicRelationshipTypes.INTEREST_OF)
            }
          }
          case _ => // ignore
        }

    }
  }

  /**
   * Iterates the Gravity Ontology, adding nodes and linking Wikipedia Concepts to them.
   */
  def makeGravityOntology() {
    rdfFile(filePaths("gravity-ontology")) {
      statement =>
        val (subject, predicate, obj) = statement.SPO

        NS.getType(subject) match {
          //          case URIType.WIKI_CATEGORY => {
          //            if (NS.getType(statement.objectURI) == URIType.GRAVITY_INTEREST) {
          //              relate(statement.subjectURI, statement.objectURI, TopicRelationshipTypes.INTEREST_OF)
          //              addName(statement.subjectURI, statement.subjectURI.getLocalName.replace("Category:", ""))
          //            }
          //          }
          //          case URIType.DBPEDIA_CLASS => {
          //            if (!statement.objectURI.getLocalName.contains("DbPediaClass")) {
          //              //All dbpedia classes subclass DbPediaClass, we get rid of that because it's unnecessary
          //              relate(statement.subjectURI, statement.objectURI, TopicRelationshipTypes.INTEREST_OF)
          //            }
          //          }
          case URIType.GRAVITY_INTEREST => {
            predicate match {
              case NS.LEVEL => addProp(subject, ConceptNode.LEVEL_PROPERTY, statement.objectInt.asInstanceOf[AnyRef])
              case RDFS.LABEL => addName(subject, statement.objectString)
              case RDFS.SUBCLASSOF => relate(statement.subjectURI, statement.objectURI, TopicRelationshipTypes.BROADER_INTEREST)
            }
          }
          case unknown => {
            print("Found uri type " + unknown + " in " + statement.toString)
          }
        }
    }
  }

  /**
   * The dbpedia triples are pre-sorted by Subject in hadoop.  That way we can buffer them up and work with then in batches by key.
   */
  def bufferedTriples(firstPass: Boolean) {
    val buffer = new ListBuffer[Statement]()
    var mark: URI = null

    val path = if (firstPass) "sorted-dbpedia" else "sorted-dbpedia2"

    rdfFile(filePaths(path)) {
      statement =>
        if (mark == null) {
          mark = statement.subjectURI
        }
        else if (mark == statement.subjectURI) {
          buffer += statement
        }
        else if (mark != statement.subjectURI) {
          workWithBuffer(mark, buffer, firstPass)
          buffer.clear()
          mark = statement.subjectURI
          buffer += statement
        }
    }

    if (mark != null) {
      workWithBuffer(mark, buffer, firstPass)
    }
    buffer.clear()
  }

  /**
   * First pass--save nodes and properties.
   * Second pass--save relationships
   */
  def workWithBuffer(uri: URI, buffer: ListBuffer[Statement], firstPass: Boolean) {
    print("BUFFER for--->" + uri)

    NS.getType(uri) match {
      case URIType.WIKI_CATEGORY => {
        new CategoryHandler(uri, buffer, firstPass, this).work
        print("IS CATEGORY")
      }
      case URIType.TOPIC => {
        new TopicHandler(uri, buffer, firstPass, this).work
      }
      case URIType.DBPEDIA_CLASS => {
        new ClassHandler(uri, buffer, firstPass, this).work
      }
      case _ => {
        //At times we will get nodes with different
        if (buffer.hasObject(NS.COLLOQUIALGRAM)) {
          new ColloquialGramHandler(uri, buffer, firstPass, this).work
        }

        if (buffer.hasObject(NS.TAG)) {
          new TagHandler(uri, buffer, firstPass, this).work()
        }
        addCount("Other Nodes Encountered")
      }
    }

    print("ENDBUFFER-->" + uri)
  }


  def optimize() {
    urisIndex.flush()
    nameIndex.flush()
  }

  def shutdown() {
    batchIndex.shutdown()
    batchInserter.shutdown()

    viewsWithNoTopicsOutfile match {
      case Some(outfile) => outfile.close()
      case None =>
    }
  }

  implicit def toBufferWrapper(statementBuffer: ListBuffer[Statement]): StatementBuffer = new StatementBuffer(statementBuffer)

}

/**
 * Nodes are handled as lists of triples.  This class provides convenience methods for getting at what is needed, so that the actual methods handling them
 * look more business-logic-y
 */
class StatementBuffer(buffer: ListBuffer[Statement]) {

  def stringVal(uri: URI): Option[String] = {
    find(uri) match {
      case Some(statement) => Some(statement.objectString)
      case None => None
    }
  }

  def nameProp = {
    stringVal(NS.RENAME) match {
      case Some(rename) => Some(rename)
      case None => stringVal(RDFS.LABEL)
    }
  }

  def hasPredicate(uri: URI) = buffer.exists(_.predicateURI == uri)

  def hasObject(uri: URI) = buffer.exists {
    case statement: Statement =>
      if (statement.getObject.isInstanceOf[URI] && statement.objectURI == uri) true else false
  }

  def list(uri: URI) = buffer.filter(_.predicateURI == uri)

  def find(possiblePredicates: URI*): Option[Statement] = buffer.find(stmt => possiblePredicates.contains(stmt.predicateURI))

  def intVal(uri: URI): Option[Int] = {
    find(uri) match {
      case Some(statement) => Some(statement.objectInt)
      case None => None
    }
  }
}


object wikiPageView {

  def getTupleOfCountDate(str: String) = {
    str.split("\\|") match {
      case Array(viewCount, dateString) => {
        (viewCount.toInt, dateString)
      }
      case _ => (0, "")
    }
  }


  def sortViewByDate(views: List[String]): List[String] = {

    val sortedViews = for {
      v <- views
      tokens = v.split("\\|")
      revstring = "%s|%s".format(tokens(1), tokens(0))
    } yield revstring

    sortedViews.sortWith(_ < _)
  }

  /**
   * takes a wiki date string format of 8|2011_154 and turns it intoa  list of the views in order by date
   */
  def viewsIntoOrderedVector(views: List[String]): List[Int] = {

    val vector = for {
      v <- sortViewByDate(views)
      tokens = v.split("\\|")
    } yield tokens(1).toInt
    vector

  }


  def getTrendingScore(timedViews: ListBuffer[Statement]): Double = {
    // get the trend score for this link
    val pageviewDates = (for (stmt <- timedViews) yield stmt.objectString).toList

    val orderedPageviews = (wikiPageView.viewsIntoOrderedVector(pageviewDates))
    val trendingScore = grvmath.getTrendingScore(orderedPageviews)
    trendingScore
  }
}