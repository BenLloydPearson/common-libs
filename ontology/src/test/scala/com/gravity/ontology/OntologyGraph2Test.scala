package com.gravity.ontology

import com.gravity.ontology.Implicits.dummyPhrase
import com.gravity.ontology.vocab.NS
import com.gravity.utilities.MurmurHash
import org.junit.Assert._
import org.junit.{Ignore, Test}

import scala.collection.JavaConversions._

/*             )\._.,--....,'``.
.b--.        /;   _.. \   _\  (`._ ,.
`=,-,-'~~~   `----(,_..'--(,_..'`-.;.'  */


@Ignore
class OntologyGraph2Test {

  val og = OntologyGraph2.graph

  @Ignore
  @Test def testCompareNodesFromDifferentOntologies() {

    val uris = Seq("http://dbpedia.org/resource/Category:London", "http://dbpedia.org/resource/Category:Capital_districts_and_territories")

    for (uri <- uris) {
      processUri(uri)
    }

    def processUri(uri: String) {


      val id = MurmurHash.hash64(uri) //1586708
      println(uri + " : " + id)

      val goldGraphName = "gold"
      val goldGraph = new OntologyGraph2(OntologyGraphMgr.getInstance().getGraph(goldGraphName))
      val goldNode = goldGraph.node(id).get

      val curGraph = new OntologyGraph2()
      val curNode = curGraph.node(id).get

      println("Gold Node")
      printNode(goldNode)

      println("Cur Node")
      printNode(curNode)

    }

    def printNode(node: OntologyNode) {
      //val frequency = GraphAnalysisService.frequencyForNode(OntologyNodeKey(node.uri).nodeId)

      node.node.getPropertyKeys.foreach(key => {
        val value = node.getProperty(key)
        println("\t" + key + ": " + value)
      })
    }

  }

  @Ignore
  @Test def testMultipleOntologies() {
    val g1 = "graph_concept"
    val og1 = new OntologyGraph2(OntologyGraphMgr.getInstance().getGraph(g1))

    val g2 = "graph_concept.backup5.latestBrokenOnt"
    val og2 = new OntologyGraph2(OntologyGraphMgr.getInstance().getGraph(g2))

    searchForNode("president", og1, g1)
    searchForNode("president", og2, g2)

    def searchForNode(search: String, g: OntologyGraph2, name: String) {
      println("Graph: " + name)
      g.search(search) match {
        case Some(node) => println("   " + node.uri)
        case _ => println("   none")
      }
    }


  }

  @Test def testSearchWithDisance() {
    og.searchWithDistance("Beagle") match {
      case Some(node) => println("Found it")
      case None => println("Nope")
    }
  }

  /*
   @Test def testShortestPath() {
     og.searchWithDistance("Surfing") match {
       case Some(startNode) => println("Found it")
         og.searchWithDistance("Snowboarding") match {
           case Some(endNode) =>
             val distance = startNode.node.getShortestPath(startNode.node.node,endNode.node.node)
             println(distance)

           case None =>
              println("No poodles")
         }



       case None => println("No Beagles")
     }


   }
  */

  @Test def testDiabetes() {
    println("diabetes")
    og.searchForTopicRichly("diabetes") foreach {
      node =>
        println(node.node.uri)
    }
  }


  @Ignore
  @Test def testClosestInterest() {

    og.searchForTopicRichly("Megan Fox") foreach {
      node =>
        node.node.interests().foreach {
          int =>
            println(int)
        }
        println(node.node.dbpediaClass.get.name)
    }
  }

  @Test def testSongTitle() {
    og.searchForTopicRichly("Beagle") match {
      case Some(node) => {

        println(node.node.dumpDataAsString)
      }
      case None => println("Nope")
    }
  }

  @Ignore
  @Test def testConceptIteration() {
    //    val topics = "Nuclear_power" :: "Maine_Coon" :: "Nuclear_fission" :: "Fukushima Nuclear Accident" :: "Los Angeles" :: "Los Angeles Dodgers" :: "Beagle" :: "Tsunami" :: "Hurricane Katrina" :: "Myspace" :: "Kobe Bryant" :: "Michael Jordan" :: Nil

    val topics = "Michael Jordan" :: Nil
    for (topic <- topics) {
      println("Concept popularities for topic " + topic)
      og.searchForTopicRichly(topic) match {
        case Some(node) => {
          //          val concepts = node.node.concepts.toSeq.sortBy(- _.node.popularity)
          //          concepts.foreach{concept=>
          //            println(concept.node.name + " : " + concept.node.popularity)
          //          }

          node.node.interests(3).foreach(println)
        }
        case None => println("No find!")
      }
    }
  }

  @Ignore
  @Test def test2() {
    testConceptIteration()
    testConceptIteration()
    testConceptIteration()
  }

  @Test def testSearch() {
    og.searchWithDistance("\\") match {
      case Some(node) => fail("Found a slash character named topic")
      case None =>
    }


    og.searchWithDistance("Murphy's law") match {
      case Some(node) => println("Found murphy")
      case None => fail("Didn't find a topic with apostrophes")
    }

    og.searchWithDistance("OPEC") match {
      case Some(node) =>
      case None => fail("Didn't find OPEC")
    }

    og.searchWithDistance("an") match {
      case Some(node) => fail("Found a stopword")
      case None =>
    }

    og.searchWithDistance("calling it quits") match {
      case Some(node) => println(node.toString)
      case None =>
    }
  }

  @Test def testTopicRetrieval() {
    og.node(NS.getTopicURI("Obama")) match {
      case Some(node) => {
        println(node)

      }
      case None => println("Nope")
    }

    og.topic(NS.getTopicURI("Obama")) match {
      case Some(node) => println(node.name)
      case None => println("Not again!")
    }

  }

  @Test def testNodeFromCache() {
    //    og.node(NS.getTopicURI("Obama")) match {
    //      case Some(node) => {
    //        println(node)
    //        OntologyGraph2.getCache(og.ontologyCreationDate).getItem(MurmurHash.hash64(node.uri)) match {
    //          case Some(nodeId) => {
    //            og.getNodeByNodeId(nodeId.self) match {
    //              case Some(nodeFromCachedId) => assertEquals("Nodes should be equal!", node, nodeFromCachedId)
    //              case None => fail("Failed to retrieve node by cached node ID")
    //            }
    //          }
    //          case None => fail("Failed to retrieve the node ID from cache!")
    //        }
    //      }
    //      case None => fail("Failed to retrieve node for Obama!")
    //    }
  }

  @Test def testOntologyCreationDate() {
    println(og.ontologyCreationDate)
  }

  //  @Test def testShortestPath() {
  //    val start = "http://dbpedia.org/resource/Futurama"
  //    val end = "http://dbpedia.org/resource/Category:Comedy_Central"
  //
  //    OntologyGraph2.graph.shortestPathBetween(start, end, 3) match {
  //      case Some(paths) => {
  //        for (path <- paths) {
  //          println("Path:")
  //          println("Length: " + (path.length()))
  //          val startNode = OntologyNode(path.startNode())
  //          val endNode = OntologyNode(path.endNode())
  //          println("Start: " + startNode.name)
  //          println("End: " + endNode.uri)
  //
  //          path.relationships().foreach{case rel: Relationship =>
  //          }
  //          path.iterator().foreach{case itm: PropertyContainer =>
  //            itm match {
  //              case node: Node => {
  //                val on = OntologyNode(node)
  //                println("Via: " + on.name)
  //              }
  //              case rel: Relationship => {
  //                println("rel: " + rel.getType.toString)
  //              }
  //            }
  //          }
  //        }
  //
  //      }
  //      case None => {
  //        println("Nope")
  //      }
  //    }
  //  }

}