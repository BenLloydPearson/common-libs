package com.gravity.interests.graphs.graphing

import org.openrdf.model._
import collection._
import com.gravity.ontology._
import impl.URIImpl
import mutable.{ListBuffer, HashMap, Buffer}
import nodes.TopicRelationshipTypes
import vocab.URIType
import org.neo4j.graphdb.{Direction, Node, Path}
import scala.collection.JavaConversions._
import com.gravity.utilities.grvmath
import com.gravity.utilities.cache.PermaCacher
import com.gravity.utilities.MySqlConnectionProvider
import java.sql.ResultSet


/*             )\._.,--....,'``.
 .b--.        /;   _.. \   _\  (`._ ,.
`=,-,-'~~~   `----(,_..'--(,_..'`-.;.'  */

trait Stuff {
  this : InterestGraph =>
  def aboveOrBelow(concept:ScoredConcept, ext:(ScoredConcept)=>Iterable[StrictOntologyNode])  = {
    val highers = mutable.ArrayBuffer[ScoredConcept]()
    for(interest <- ext(concept)) {
      conceptsByUri.get(interest.uri) match {
        case Some(broader) => {
          highers += broader
        }
        case None =>
      }
    }
    highers.toSeq
  }

  def aboveOrBelowTransitive(concept:ScoredConcept, ext:(ScoredConcept)=>Iterable[StrictOntologyNode]) = {
    val highers = mutable.ArrayBuffer[ScoredConcept]()
    aboveOrBelowTransitiveWk(concept, highers, ext)
    highers
  }

  def aboveOrBelowTransitiveWk(concept:ScoredConcept, buffer:mutable.ArrayBuffer[ScoredConcept], ext:(ScoredConcept)=>Iterable[StrictOntologyNode]) {
    for(interest <- ext(concept)) {
      conceptsByUri.get(interest.uri) match {
        case Some(broader) => {
          buffer += broader
          aboveOrBelowTransitiveWk(broader,buffer,ext)
        }
        case None =>
      }
    }
  }

}

class InterestGraph(conceptsPreFilter: Seq[ScoredConcept], filterBlackList : Boolean = true) extends Stuff {
  //val badInterests = List("History","Politics","Society","Social_Issues_%26_Advocacy")
  val badInterests = List("History")
  val concepts = if (filterBlackList) {
    conceptsPreFilter.filterNot(c=>badInterests.contains(c.interest.name))
  }
  else {
    conceptsPreFilter
  }

  val conceptsByUri = concepts.map(concept=>concept.interest.uri -> concept).toMap

  //All interests of level X
  def interestsOfLevel(level:Int) = concepts.filter(_.interest.level == level)

  //All topics that roll up to Interest X
  def topicsOfInterest(concept:ScoredConcept) = concept.interestMatches

  //All topics that roll up to a list of Interests
  def topicsOfInterests(concepts:Iterable[ScoredConcept]) = concepts.flatMap(concept=>topicsOfInterest(concept))

  //All topics that roll up to an interest and its children
  def topicsOfInterestTransitive(concept:ScoredConcept) =  topicsOfInterests(lowerInterestsTransitive(concept) += concept)

  //All topics that roll up to an interest GROUPED BY the Interest
  def topicsByInterestTransitive(concept:ScoredConcept) = topicsOfInterestTransitive(concept).groupBy(_.interestMatch)

  //Reverse version of the above, where you get a list of Topics where each one has a list of Interests it rolls up to
  def topicsUnderInterestTransitive(concept:ScoredConcept) = topicsOfInterestTransitive(concept).groupBy(_.topic)

  //Get the interests of a certain level reverse sorted by score
  def interestsOfLevelByScore(level:Int) = interestsOfLevel(level).sortBy(- _.score)

  //Get the top interest of a level if it exists
  def topInterestOfLevelByScore(level:Int) = interestsOfLevelByScore(level).headOption

  //Interests directly below an Interest
  def lowerInterests(concept:ScoredConcept) = aboveOrBelow(concept, _.interest.narrowerInterests)

  //Top interest directly below another interest
  def topInterestUnderInterest(concept:ScoredConcept) = lowerInterests(concept).sortBy(- _.score).headOption

  //Same thing, but with a buffer for capturing results in a recursive operation
  def captureTopInterestsUnderInterest(concept:ScoredConcept, buffer:Buffer[ScoredConcept]) {
    lowerInterests(concept).sortBy(- _.score).headOption match {
      case Some(foundit) => {
        buffer += foundit
        captureTopInterestsUnderInterest(foundit,buffer)
      }
      case None =>
    }
  }

  //For each level under the given interest, return the top interest of that level (under the top interest above it)
  def topInterestsUnderInterest(concept:ScoredConcept) = {
    val results = Buffer[ScoredConcept]()
    captureTopInterestsUnderInterest(concept,results)
    results
  }

  //The tree of interests below an Interest
  def lowerInterestsTransitive(concept:ScoredConcept) = aboveOrBelowTransitive(concept, _.interest.narrowerInterests)

  //Interests above an Interest
  def higherConcepts(concept:ScoredConcept) = aboveOrBelow(concept, _.interest.broaderInterests)

  def topicsByUri = concepts.flatMap(_.interestMatches).groupBy(_.topic.topic.uri)

  def topics = {
    val topicsToInterests = concepts.flatMap(_.interestMatches)
    val topicsByUri = topicsToInterests.groupBy(_.topic.topic.uri)
    val combinedTopics = topicsByUri.values.map{topicToInterest=>
      topicToInterest.head
    }
    combinedTopics
  }


  //graphalgo class gets passed in here
  //graphalgo.apply gets called, passing all necessary data

  def prettyFormat() = {
    println("Interests")
    for(scoredConcept <- concepts) {
      println("\t\t" + scoredConcept.interest.name + " " + scoredConcept.interest.level)
    }
    println("Topics")
    for(topic <- topics) {
      println("\t\t\t" + topic.topic.topic.name + " " + topic.topic.score)
    }

  }




}

object InterestGraph {

  case class topicWithPathCounts(topic1: String, totalPaths: Int, uniquePaths: Int, shortestDistance: Int, score: Double = 0d, nlpScore: Double = 0d)


  def findInterconnectivityOfTopics(topics: List[String], distance: Int = 4, followDbpediaOnlyRelationships: Boolean = true, debugMode: Boolean = false): topicClusterInterconnectivity = {
    var totalPaths = 0
    var totalShortestDistance = 0 //sum all the shortest path distances between nodes (probably useful only for averaging)
    var nodeConnections = 0 //if 2 nodes are connected through 30 paths that is still considered 1 nodeConnection
    var totalTraversedTopics = 0 //sum the total topics traversed across all of the available paths with nodes in this cluster
    var totalTraversedConcepts = 0 //sum the total concepts traversed across all of the available paths with nodes in this cluster
    val clusterNodes = topics.size

    val topicPathCount = HashMap[String, Int]() //allTraversalTopics
    val topicNodeConnectionCount = HashMap[String, Int]()

    val conceptPathCount = HashMap[String, Int]() //allTraversalConcepts
    val conceptNodeConnectionCount = HashMap[String, Int]() //uniqueTraversalConcepts

    val primaryTopicConnections = HashMap[String, Int]() //of just the nodes in our cluster how many other nodes do they connect to in the cluster

    topics.foreach(
      t1 => {
        topics.foreach(
          t2 => {
            if (t1 != t2)
              calculateShortestPath(t1, t2, distance, followDbpediaOnlyRelationships) match {
                case Some(sp) => {
                  val pathTopics = scala.collection.mutable.Set[String]()
                  val pathConcepts = scala.collection.mutable.Set[String]()
                  nodeConnections += 1
                  totalShortestDistance += sp.shortestDistance
                  totalPaths += sp.totalPaths

                  if (primaryTopicConnections.contains(t1)) {
                    //how many other nodes in the cluster does this node connect o
                    primaryTopicConnections(t1) = primaryTopicConnections(t1) + 1
                  }
                  else {
                    primaryTopicConnections(t1) = 1
                  }
                  if (debugMode) println("Paths Between")
                  if (debugMode) println("Node 1:" + t1)
                  if (debugMode) println("Node 2:" + t2)
                  if (debugMode) println("\n")
                  if (debugMode) prettyPrintShortestPath(sp)
                  sp.paths.foreach(
                    p => {

                      p.nodes().foreach(
                        n => {
                          //this is a concept
                          val node = OntologyNode(n)
                          if (node.uriType == URIType.WIKI_CATEGORY) {

                            totalTraversedConcepts = totalTraversedConcepts + 1 //increment our total concept traversal count
                            val conceptName = node.uri //get our name

                            pathConcepts.add(conceptName) //add this concept to our set specific for this path (for calculating how many paths each concept is in below)

                            if (conceptPathCount.contains(conceptName)) {
                              //add this concept to our all concept bucket
                              conceptPathCount(conceptName) = conceptPathCount(conceptName) + 1
                            }
                            else {
                              conceptPathCount(conceptName) = 1
                            }
                          }
                          //this is a topic
                          if (node.uriType == URIType.TOPIC) {

                            totalTraversedTopics = totalTraversedTopics + 1 // increment our total topic traversal count

                            val topicName = node.uri //get our name

                            pathTopics.add(topicName) //add this topic to our set specific for this path (for calculating how many paths each topic is in below)

                            if (topicPathCount.contains(topicName)) {
                              //add this topic to our all topic bucket
                              topicPathCount(topicName) = topicPathCount(topicName) + 1
                            }
                            else {
                              topicPathCount(topicName) = 1
                            }

                          }
                        }
                      )
                    }
                  )


                  pathTopics.foreach(
                    ptn => {

                      if (topicNodeConnectionCount.contains(ptn)) {
                        topicNodeConnectionCount(ptn) = topicNodeConnectionCount(ptn) + 1
                      }
                      else {
                        topicNodeConnectionCount(ptn) = 1
                      }
                    }
                  )


                  pathConcepts.foreach(
                    ptn =>
                      if (conceptNodeConnectionCount.contains(ptn)) {
                        conceptNodeConnectionCount(ptn) = conceptNodeConnectionCount(ptn) + 1
                      }
                      else {
                        conceptNodeConnectionCount(ptn) = 1
                      }
                  )


                }

                case None =>

              }

          }
        )

      }

    )

    val uniqueTraversedTopics = topicNodeConnectionCount.keySet.size
    val uniqueTraversedConcepts = conceptNodeConnectionCount.keySet.size


    val avgDistance = totalShortestDistance.toDouble / totalPaths.toDouble
    val avgNumberOfPaths = if (nodeConnections > 0) {
      totalPaths.toDouble / nodeConnections.toDouble
    } else {
      0d
    }

    val avgNumberOfTraversedTopics = totalTraversedTopics.toDouble / totalPaths.toDouble
    val avgTopicNodeConnections = if (nodeConnections > 0) {
      topicNodeConnectionCount.values.sum / nodeConnections
    } else {
      0d
    }

    val avgNumberOfTraversedConcepts = totalTraversedConcepts.toDouble / totalPaths.toDouble
    val avgConceptNodeConnections = if (nodeConnections > 0) {
      conceptNodeConnectionCount.values.sum / nodeConnections
    } else {
      0d
    }

    topicClusterInterconnectivity(clusterNodes, nodeConnections, totalShortestDistance, totalPaths, totalTraversedTopics, uniqueTraversedTopics, totalTraversedConcepts, uniqueTraversedConcepts, avgDistance, avgNumberOfPaths, avgNumberOfTraversedTopics, avgTopicNodeConnections, avgNumberOfTraversedConcepts, avgConceptNodeConnections, primaryTopicConnections, topicPathCount, topicNodeConnectionCount, conceptPathCount, conceptNodeConnectionCount)
  }

  def calculateShortestPath(startUri: String, endUri: String, maxDepth: Int, followDbpediaOnlyRelationships: Boolean = false,followOnlyConceptLinks : Boolean = false, followOnlyTopicLinks : Boolean = false): Option[shortestPath] = {

    OntologyGraph2.graph.shortestPathBetween(startUri, endUri, maxDepth, followDbpediaOnlyRelationships= followDbpediaOnlyRelationships, followOnlyConceptLinks = followOnlyConceptLinks, followOnlyTopicLinks = followOnlyTopicLinks) match {
      case Some(p) => {
        val paths = p.iterator().toList //convert our java object to a friendly scala list

        val totalPaths = paths.length
        var shortestDistance = 1000
        val traversalNodes = ListBuffer[Node]() //store all of our hop nodes

        if (totalPaths > 0) {
          //we have paths
          paths.foreach(
            path => {
              if (path.length() < shortestDistance) {
                shortestDistance = path.length()
              } //ensure we find the shortest path.  This should always come back the same # but doing some double checking here
              traversalNodes ++= path.nodes() //track all of the nodes we traverse through to get to our endUri
            }
          )
          val uniqueTraversalNodes = traversalNodes.toSet
          Some(new shortestPath(paths, totalPaths, shortestDistance, traversalNodes, uniqueTraversalNodes))
        }

        else {
          None
        }
      }
      case None =>
        None
    }
  }

  def calculateShortestPath2(startUri: String, endUri: String, maxDepth: Int, followDbpediaOnlyRelationships: Boolean = false,followOnlyConceptLinks : Boolean = false, followOnlyTopicLinks : Boolean = false): Option[shortestPath] = {

    OntologyGraph2.graph.shortestPathBetween(startUri, endUri, maxDepth, followDbpediaOnlyRelationships= followDbpediaOnlyRelationships, followOnlyConceptLinks = followOnlyConceptLinks, followOnlyTopicLinks = followOnlyTopicLinks) match {
      case Some(p) => {
        val paths = p.iterator().toList //convert our java object to a friendly scala list

        val totalPaths = paths.length
        var shortestDistance = 1000
        val traversalNodes = ListBuffer[Node]() //store all of our hop nodes

        if (totalPaths > 0) {
          //we have paths
          paths.foreach(
            path => {
              if (path.length() < shortestDistance) {
                shortestDistance = path.length()
              } //ensure we find the shortest path.  This should always come back the same # but doing some double checking here
              traversalNodes ++= path.nodes() //track all of the nodes we traverse through to get to our endUri
            }
          )
          val uniqueTraversalNodes = traversalNodes.toSet
          Some(new shortestPath(paths, totalPaths, shortestDistance, traversalNodes, uniqueTraversalNodes))
        }

        else {
          None
        }
      }
      case None =>
        None
    }
  }

  def calculateSingleShortestPath(startUri: String, endUri: String, maxDepth: Int, followDbpediaOnlyRelationships: Boolean = false,followOnlyConceptLinks : Boolean = false, followOnlyTopicLinks : Boolean = false): Option[shortestPath] = {

    val traversalNodes = ListBuffer[Node]() //store all of our hop nodes

    OntologyGraph2.graph.singlePathBetween(startUri, endUri, maxDepth, followDbpediaOnlyRelationships= followDbpediaOnlyRelationships, followOnlyConceptLinks = followOnlyConceptLinks, followOnlyTopicLinks = followOnlyTopicLinks) match {
      case Some(p) => {
          //we have a path

          val shortestDistance = p.length()

          traversalNodes ++= p.nodes() //track all of the nodes we traverse through to get to our endUri

          val uniqueTraversalNodes = traversalNodes.toSet
          Some(new shortestPath(p :: Nil, 1, shortestDistance, traversalNodes, uniqueTraversalNodes))
      }
      case None =>
        None
    }
  }

  def scoreInterconnectedConcepts(concepts: HashMap[String,Int]) : List[(String,Double)] = {

    val adjConceptScores = HashMap[String,Double]()
    val nonBlacklistedTopics = HashMap[String,Double]()
    val goodConcepts = HashMap[String,Double]()

    //kill any concepts in our blacklist
    concepts.foreach({c=> if (!conceptBlacklist(c._1)) nonBlacklistedTopics(c._1) = c._2 })

    //get a list of concepts that are within 2 hops from at least 1 other concept
    val prunedConcepts = pruneTopicsByPathDepth(nonBlacklistedTopics.keySet.toList,2,false)

    //filter down to our good concepts
    concepts.foreach(
      c=> {
        if (prunedConcepts.contains(c._1))
          goodConcepts(c._1) = c._2
      }
    )

    //val filteredConcepts = goodConcepts.filter(_._2 > -.5)

    goodConcepts.foreach(
      c=> {
        val totalInboundLinks = getInboundLinkCountForCategory(c._1)
        val adjTotalInboundLinks =  if (totalInboundLinks > 0) math.sqrt(totalInboundLinks) else 1 //if (totalInboundLinks > 10) math.log10(totalInboundLinks) else 1
        val adjConceptScore = c._2.toDouble //  / adjTotalInboundLinks
        adjConceptScores(c._1) = adjConceptScore
      }
    )


    val conceptZScores = HashMap[String,Double]()
    grvmath.zscoresBy(adjConceptScores)(_._2).zip(adjConceptScores).foreach {
      case (score, itm) => conceptZScores.update(itm._1,score)
    }

    //eject topics below zscore floor
    val filteredConceptScores = conceptZScores.filter(_._2 > -.5).toList.sortBy(-_._2).take(20)


    val conceptClusters = getShortestPathClusters(filteredConceptScores.map(_._1).toList,2,followDbpediaOnlyRelationships = false,followOnlyConceptLinks = true,dedupe=true)
    println("***shortest path concept clusters***")
    conceptClusters.foreach(
      c=> {
        println("****")
        c.foreach(
          t=> {
            println(t)
          }
        )
      }
    )
    println("**done shortest path clusters***")

    filteredConceptScores


  }

  def conceptBlacklist(concept: String): Boolean = {

    val blacklistRe = new scala.util.matching.Regex( """\d\d\d\ds?_|_of_|_in_|_by_|Living_people|_terminology_?|_manufacturers_?|_democracies_?|_equipment_?|_landforms_?|_words_?|_loanwords_?|(?i)Articles_|(?i)_Articles|(?i)stubs|(?i)_countries_?|(?i)_nations_?|_terms_?|Humans|Countries|Republics|_Republics_?|_films|_subsidiaries_?|Category:Day$|_brands_?|_organizations_?|_agencies_?""")
    blacklistRe.findFirstIn(concept) match {
      case Some(m) =>
        true
      case None =>
        false
    }
  }

  def getShortestPathClusters(topics: List[String], distance: Int = 3, followDbpediaOnlyRelationships: Boolean = true,followOnlyConceptLinks : Boolean = false, dedupe: Boolean = false): ListBuffer[ListBuffer[String]] = {
    val clusters = ListBuffer[ListBuffer[String]]()
    topics.foreach(
      t1 => {
        val t1URI = t1
        if (!conceptBlacklist(t1)) {
          //println(t1)
          topics.foreach(
            t2 => {
              if (!conceptBlacklist(t2)) {
                val t2URI = t2
                if (t1URI != t2URI)
                  calculateShortestPath(t1URI, t2URI, distance, followDbpediaOnlyRelationships = followDbpediaOnlyRelationships, followOnlyConceptLinks = followOnlyConceptLinks) match {
                    case Some(sp) => {  //t1 and t2 are connected, and should be part of the same cluster

                      var added = false
                      clusters.foreach(
                        c => {

                          //we have a cluster with t1 in it, lets add t2
                          if (c.contains(t1) && (!c.contains(t2))) {
                            c += t2
                            added = true
                          }

                          //we have a cluster with t2 in it, lets add t1
                          else if
                          (c.contains(t2) && (!c.contains(t1))) {
                            c += t1
                            added = true
                          }

                          //we already have a cluster with both t1 and t2 in it - do nothing
                          else if (c.contains(t1) && (c.contains(t2))) {
                            added = true
                          }
                        }
                      )

                      //we have no clusters with either t1 or t2, lets create a new one and add it
                      if (added == false) {
                        val nb = new ListBuffer[String]()
                        nb += t1
                        nb += t2
                        clusters += nb
                      }



                    }
                    case None =>

                  }

              }
            }
          )
        }
      }
    )


    if (dedupe) dedupeShortestPathClusters(clusters) else clusters
    //clusters
  }

  def dedupeShortestPathClusters(clusters: ListBuffer[ListBuffer[String]]): ListBuffer[ListBuffer[String]] = {
    var c1Index = 0

    clusters.foreach(
      c1 => {
        var c2Index = 0
        clusters.foreach(
          c2 => {
            if (c1Index != c2Index) {
              //make sure we aren't looking at the same cluster.  can't just compare c1 and c2 because we might have 2 different clusters with the same objects
              val intersect = c1.intersect(c2)

              if (intersect.size > 0) {

                intersect.foreach(
                  i => {

                    val cluster1IntersectNodes = getNodesWithinDistanceFromSingleNodeDirected(i,c1.toList,3,0,0,followDbpediaOnlyRelationships = true)
                    val cluster2IntersectNodes = getNodesWithinDistanceFromSingleNodeDirected(i,c2.toList,3,0,0,followDbpediaOnlyRelationships = true)

                    if (cluster1IntersectNodes.size > cluster2IntersectNodes.size) {
                      c2 -= i
                    }

                    else if (cluster1IntersectNodes.size < cluster2IntersectNodes.size) {
                      c1 -= i
                    }

                    else {
                      if (c1.size > c2.size) {
                        //c1 is the largest cluster
                        c1 -= i
                      }
                      else if (c1.size < c2.size) {
                        //c2 has the largest score
                        c2 -= i
                      }
                      else if (c1.size == c2.size) {
                        //pretty much the same, pull it out of c1
                        c1 -= i
                      }
                    }
                  }
                )
              }
            }
            c2Index = c2Index + 1
          }

        )
        c1Index = c1Index + 1
      }
    )

    clusters.foreach(
      c => {
        if (c.size == 0) {
          clusters.remove(clusters.indexOf(c))
        }
      }

    )
    clusters
  }

  def getNodesWithinDistanceFromSingleNodeDirected(node: String, nodes: List[String], distance: Int = 3, minTotalPaths: Int = 0, minUniquePaths: Int = 0, followDbpediaOnlyRelationships: Boolean = true,followOnlyTopicLinks: Boolean = false, followOnlyConceptLinks: Boolean = false): ListBuffer[topicWithPathCounts] = {

    val validTopics = ListBuffer[topicWithPathCounts]()

    val topicPathCount = new HashMap[String, Int]()
    val topicUniquePathCount = new HashMap[String, Int]()
    val topicShortestDistance = new HashMap[String, Int]()
    //var paths = HashMap[String,Path]

    //need a more scala magic way to get out of this nasty nested foreach
    nodes.foreach(
      t1 => {
        if (t1 != node)
          if (!conceptBlacklist(t1))
            calculateShortestPath(node,t1, distance, followDbpediaOnlyRelationships=followDbpediaOnlyRelationships,followOnlyTopicLinks=followOnlyTopicLinks,followOnlyConceptLinks=followOnlyConceptLinks) match {
              case Some(sp) => {
                if (topicPathCount.keySet.contains(t1)) {
                  topicPathCount(t1) = topicPathCount(t1) + sp.totalPaths
                } else {
                  topicPathCount(t1) = sp.totalPaths
                } //increment total paths
                if (topicUniquePathCount.keySet.contains(t1)) {
                  topicUniquePathCount(t1) = topicUniquePathCount(t1) + 1
                } else {
                  topicUniquePathCount(t1) = 1
                } //increment unique paths

                if (topicShortestDistance.keySet.contains(t1)) {
                  //we already have a shortest distance
                  if (topicShortestDistance(t1) > sp.shortestDistance) {
                    topicShortestDistance(t1) = sp.shortestDistance
                  } //if this one is smaller lets set it
                }
                else {
                  //we don't have a shortest distance
                  topicShortestDistance(t1) = sp.shortestDistance
                }

              }

              case None =>

            }
      }
    )



    nodes.foreach(
      t1 => {
        if (topicPathCount.keySet.contains(t1))
          if (topicUniquePathCount.keySet.contains(t1))
            if (topicPathCount(t1) >= minTotalPaths)
              if (topicUniquePathCount(t1) >= minUniquePaths) {
                val score = topicPathCount(t1).toDouble / topicShortestDistance(t1).toDouble
                validTopics += new topicWithPathCounts(t1,topicPathCount(t1), topicUniquePathCount(t1), topicShortestDistance(t1),score)
              }
      }
    )
    validTopics
  }






  def pruneTopicsByPathDepth(topics: List[String] ,depth : Int,useTopicLinks : Boolean = true) : List[String] = {
    val validTopicsByTopicLink = ListBuffer[String]()
    val validTopicsByConceptLink = ListBuffer[String]()

    topics.foreach(
      t1 => {
        topics.foreach(
          t2 => {

            if (t1 != t2) {

              if (useTopicLinks) {
                //by topic link - depth always hard coded to 1 for now
                this.findSinglePath(startUri = t1,endUri = t2,maxDepth = 1,followDbpediaOnlyRelationships = false, followOnlyTopicLinks = true, followOnlyConceptLinks = false)  match {
                  case Some(sp) => {
                    validTopicsByTopicLink += t1
                  }

                  case None =>
                }
              }

              //by concept connection
              this.findSinglePath(startUri = t1,endUri = t2,maxDepth = depth,followDbpediaOnlyRelationships = true, followOnlyTopicLinks = false, followOnlyConceptLinks = false)  match {
                case Some(sp) => {
                  validTopicsByConceptLink += t1
                }

                case None =>

              }

            }
          }
        )
      }
    )
    val validTopics = validTopicsByTopicLink ++ validTopicsByConceptLink
    validTopics.toList
  }


  def findSinglePath(startUri: String, endUri: String, maxDepth: Int = 3, followDbpediaOnlyRelationships: Boolean = true,followOnlyTopicLinks: Boolean = false,followOnlyConceptLinks: Boolean = false): Option[Path] = {

    OntologyGraph2.graph.singlePathBetween(startUri, endUri, maxDepth, followDbpediaOnlyRelationships=followDbpediaOnlyRelationships,followOnlyTopicLinks=followOnlyTopicLinks,followOnlyConceptLinks=followOnlyConceptLinks) match {
      case Some(p) => {
        if (p != null) {   //single path will return null
          Some(p)
        }
        else {
          None
        }
      }

      case None =>
        None
    }
  }



  def getInboundLinkCountForCategory(categoryURI : String, bypassCache : Boolean = false) : Int  = {
    //getting permacacher error
    //val inboundLinkCount = if (!bypassCache) {PermaCacher.getOrRegister("categoryLinkCount", getInboundLinkCountForCategoryFromDB(categoryURI), 600) }
    //else {
    //  getInboundLinkCountForCategoryFromDB(categoryURI)
    //}
    val inboundLinkCount = getInboundLinkCountForCategoryFromDB(categoryURI)
    inboundLinkCount
  }

  private def getInboundLinkCountForCategoryFromDB(categoryURI: String) : Int = {
    val wikiCategory = categoryURI.stripPrefix("http://dbpedia.org/resource/Category:")

    val dburi = "jdbc:mysql://grv-jimdb01:3306/wikipedia?autoReconnect=true"
    MySqlConnectionProvider.withConnection(dburi, "wikipedia", "pigeon", "F!ndTheR00st") { conn =>
      val statement = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)

      val rs = statement.executeQuery("select count(*) from categorylinks where cl_to = '" + wikiCategory + "'")

      while (rs.next) {
        return rs.getInt(1)
      }

    }
    0
  }

  def prettyPrintShortestPath(sp: shortestPath) {

    var outputString = ""

    sp.paths.foreach(
      path=> {
        var pathString = ""
        path.nodes.foreach(
          node =>  {
            //println(node.getPropertyKeys)
            if (node.hasProperty("Uri")) {
              pathString += node.getProperty("Uri") + "->\n"
            }
          }
        )
        outputString += pathString.stripSuffix("->") + "\n\n"
      }
    )
    outputString += "Path Data\n"
    outputString += "Total Paths: " + sp.totalPaths.toString + " Shortest Distance: " + sp.shortestDistance.toString + "\n"

    outputString += "Traversal Nodes\n"
    sp.traversalNodes.foreach(
      tn=>
        outputString += tn.getProperty("Uri") +  "\n"
    )

    outputString += "Unique Traversal Nodes\n"
    sp.uniqueTraversalNodes.foreach(
      tn=>
        outputString += tn.getProperty("Uri") + "\n"
    )

    println(outputString)

  }


  def printTopicClusterInterconnectivity(cc: topicClusterInterconnectivity) {

    var outputString = "\tclusterNodes\tnodeConnections\ttotalShortestDistance\ttotalPaths\ttotalTraversedTopics"
    outputString += "\tuniqueTraversedTopics\ttotalTraversedConcepts\tuniqueTraversedConcepts\tavgDistance"
    outputString += "\tavgNumberOfPaths\tavgNumberOfTraversedTopics\tavgTopicNodeConnections\tavgNumberOfTraversedConcepts"
    outputString += "\tavgConceptNodeConnections\n"

    outputString += "\t" + cc.clusterNodes + "\t" + cc.nodeConnections + "\t" + cc.totalShortestDistance + "\t" + cc.totalPaths + "\t" + cc.totalTraversedTopics + "\t"
    outputString += cc.uniqueTraversedTopics + "\t" + cc.totalTraversedConcepts + "\t" + cc.uniqueTraversedConcepts + "\t" + cc.avgDistance + "\t"
    outputString += cc.avgNumberOfPaths + "\t" + cc.avgNumberOfTraversedTopics + "\t" + cc.avgTopicNodeConnections + "\t" + cc.avgNumberOfTraversedConcepts + "\t"
    outputString += cc.avgConceptNodeConnections

    outputString += "\n\nTopic Path Count\nTopic\tCount"
    cc.topicPathCount.toList.sortBy(-_._2).foreach(
      t => outputString += "\n" + t._1 + "\t" + t._2
    )

    outputString += "\n\nTopic Node Connection Count\nTopic\tCount"
    cc.topicNodeConnectionCount.toList.sortBy(-_._2).foreach(
      t => outputString += "\n" + t._1 + "\t" + t._2
    )

    outputString += "\n\nConcept Path Count\nConcept\tCount"
    cc.conceptPathCount.toList.sortBy(-_._2).foreach(
      t => outputString += "\n" + t._1 + "\t" + t._2
    )

    outputString += "\n\nConcept Node Connection Count\nConcept\tCount"
    cc.conceptNodeConnectionCount.toList.sortBy(-_._2).foreach(
      t => outputString += "\n" + t._1 + "\t" + t._2
    )

    outputString += "\n\nPrimary Node Connections\nNode\tCount"
    cc.primaryTopicConnections.toList.sortBy(-_._2).foreach(
      t => outputString += "\n" + t._1 + "\t" + t._2
    )

    println(outputString)
  }

  def prettyPrintNeo4jPath(p: Path) {

    var outputString = ""

    var pathString = ""
    p.nodes.foreach(
      node =>  {
        //println(node.getPropertyKeys)
        if (node.hasProperty("Uri")) {
          pathString += node.getProperty("Uri") + "->\n"
        }
      }
    )
    outputString += pathString.stripSuffix("->") + "\n\n"

    println(outputString)

  }

  def getPathURIs(p: Path) : ListBuffer[String] = {

    val pathUris = ListBuffer[String]()
    p.nodes.foreach(
      node =>  {
        if (node.hasProperty("Uri")) {
          pathUris += node.getProperty("Uri").toString
        }
      }
    )

   pathUris

  }


  def getPathBroaderURIs(p: Path) : ListBuffer[String] = {

    val pathUris = ListBuffer[String]()
    p.nodes.foreach(
      node =>  {
        if ((node.concepts.contains (TopicRelationshipTypes.BROADER_CONCEPT,Direction.INCOMING)) || (node.hasRelationship(TopicRelationshipTypes.CONCEPT_OF_TOPIC,Direction.INCOMING)))
        //if (node.hasRelationship(TopicRelationshipTypes.BROADER_CONCEPT,Direction.INCOMING))
          if (node.hasProperty("Uri"))
           pathUris += node.getProperty("Uri").toString
      }
    )

    pathUris

  }


  /*pass in debug mode to see whats going on here.  we will often log certain paths twice, because the path above it splits which means the two topics
  that were collocated are connected through that concept with more then one path.

  first off the reason we have this guy is we want to be able to traverse both directions (up and down) when we are searching for connections between topics,
  but we only want broader ("up") traversals when we are logging paths.

  example - Polonium is connected to Lethal_dose
   http://dbpedia.org/resource/Lethal_dose->
   http://dbpedia.org/resource/Category:Toxicology->
   http://dbpedia.org/resource/Category:Carcinogens->
   http://dbpedia.org/resource/Polonium->

   http://dbpedia.org/resource/Lethal_dose->
   http://dbpedia.org/resource/Category:Toxicology->
   http://dbpedia.org/resource/Category:Element_toxicology->
   http://dbpedia.org/resource/Polonium->

   http://dbpedia.org/resource/Polonium->
   http://dbpedia.org/resource/Category:Carcinogens->
   http://dbpedia.org/resource/Category:Toxicology->
   http://dbpedia.org/resource/Lethal_dose->

   http://dbpedia.org/resource/Polonium->
   http://dbpedia.org/resource/Category:Element_toxicology->
   http://dbpedia.org/resource/Category:Toxicology->
   http://dbpedia.org/resource/Lethal_dose->

   we are going up and down here, and we can't just save these paths as annotated paths (path 2 - toxicology actually rolls down to element_toxicology)
   this guy makes sure after we build our connections our routes are going in the right direction.  We can't do it at path calculation time because path 2 is valuable.
   its not as clear in this example but picture a route where topic 1 is connected to topic 2 through 5 hops, topic 1 rolls up 3 hops, topic 2 rolls up 1 hop, and they meet at that hop.
   we need to know where we are rolling up and where we are traversing back down.

   In this above example Lethal_dose->Toxicology will get stored twice, one for each path.  The fact that it toxicology is traversed _through_ twice for a term that lethal dose is closely
   collocated with is valuable and that is why it is stored twice.  in the calling method the distance will still be stored as 3 to represent that the original path had 3 total hops away from the first topic

   Another example to show portal pages -
   http://dbpedia.org/resource/Apex_beat->
   http://dbpedia.org/resource/Category:Cardiology->
   http://dbpedia.org/resource/Portal:Contents/Categories/Health_and_fitness->
   http://dbpedia.org/resource/Category:Veterinary_medicine->
   http://dbpedia.org/resource/Category:Veterinary_professions->
   http://dbpedia.org/resource/Avian_veterinarian->
   */

  def getPathBroaderURIs3(p: Path, debug : Boolean = false) : ListBuffer[String] = {
    if (debug) println("*************************")
    if (debug) prettyPrintNeo4jPath(p)

    //get all the nodes in our path
    val path = p.nodes.toList

    val pathUris = ListBuffer[String]()
    //loop through them
    for (i <- 0 until path.size -1) {
      //get the uri of our node
      val uri = path(i).getProperty("Uri").toString
      //if its not a Portal: URI
      if (!uri.contains("Portal:")) {

        //get the broader concepts for this node.  BROADER_CONCEPT and CONCEPT_OF_TOPIC need to be used because this node could be a topic or a concept - we dont know
        val broaderConcepts = path(i).getRelatedNodes(TopicRelationshipTypes.BROADER_CONCEPT,Direction.OUTGOING) ++ path(i).getRelatedNodes(TopicRelationshipTypes.CONCEPT_OF_TOPIC,Direction.OUTGOING)

        //val filteredBroaderConcepts = broaderConcepts.filter(!_.getProperty("Uri").toString.contains("Portal:"))
        if (debug) println("URI:" + uri)
        if (debug) println("BROADER CONCEPTS")
        if (debug) println(broaderConcepts)

        //get the next URI in our path
        val broaderUri = path(i+1).getProperty("Uri").toString

        //if the next URI is a broader concept for this current URI, add it to our path
        if (broaderConcepts.contains(OntologyNode(path(i+1)))) {
          if (debug) println("CONTAINS URI: " + broaderUri)
          pathUris += broaderUri
        }

        //if its not, we are done, we've gone "UP" as much as we can and we are now at a point where we are turning around in path and going down
        else {
          //if the broaderURI is a Portal page, add it.  Portal pages are going to be valuable in autogeneration of a taxonomy
          if (broaderUri.contains("Portal:"))  {pathUris += broaderUri}
          return pathUris
        }


      }

      else {
        //I don't think we will ever get here, but this is here just in case
        if (debug) println("AT ELSE")
        pathUris += uri
        return pathUris
      }

    }

    pathUris
  }

  //old - but keeping it around for now until we make sure there are no bugs in the newer code
  def getPathBroaderURIs2(p: Path) : ListBuffer[String] = {
    println("*************************")
    prettyPrintNeo4jPath(p)
    val path = p.nodes.toList

    val pathUris = ListBuffer[String]()
    //pathUris += path(0).getProperty("Uri").toString
    for (i <- 0 until path.size -1) {
      val uri = path(i).getProperty("Uri").toString
      if (!uri.contains("Portal:")) {

        val broaderConcepts = path(i).getRelatedNodes(TopicRelationshipTypes.BROADER_CONCEPT,Direction.OUTGOING) ++ path(i).getRelatedNodes(TopicRelationshipTypes.CONCEPT_OF_TOPIC,Direction.OUTGOING)

        val filteredBroaderConcepts = broaderConcepts.filter(!_.getProperty("Uri").toString.contains("Portal:"))
        //println(filteredBroaderConcepts)
        //println("*****")
        if (filteredBroaderConcepts.contains(OntologyNode(path(i+1)))) {
          val broaderUri = path(i+1).getProperty("Uri").toString
          //println("CONTAINS URI: " + broaderUri)

          pathUris += broaderUri
        }


      }

      else {
        pathUris += uri
      }




    }

    pathUris

  }




}


case class shortestPath(paths: List[Path], totalPaths: Int, shortestDistance: Int, traversalNodes: ListBuffer[Node], uniqueTraversalNodes: Set[Node])


case class topicClusterInterconnectivity(clusterNodes: Int,
                                                nodeConnections: Int,
                                                totalShortestDistance: Int,
                                                totalPaths: Int,
                                                totalTraversedTopics: Int,
                                                uniqueTraversedTopics: Int,
                                                totalTraversedConcepts: Int,
                                                uniqueTraversedConcepts: Int,
                                                avgDistance: Double,
                                                avgNumberOfPaths: Double,
                                                avgNumberOfTraversedTopics: Double,
                                                avgTopicNodeConnections: Double,
                                                avgNumberOfTraversedConcepts: Double,
                                                avgConceptNodeConnections: Double,
                                                primaryTopicConnections: HashMap[String, Int],
                                                topicPathCount: HashMap[String, Int],
                                                topicNodeConnectionCount: HashMap[String, Int],
                                                conceptPathCount: HashMap[String, Int],
                                                conceptNodeConnectionCount: HashMap[String, Int])
