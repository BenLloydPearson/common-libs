package com.gravity.interests.jobs.intelligence

import org.joda.time.DateTime
import StoredGraph._
import com.gravity.interests.graphs.graphing._
import com.gravity.interests.jobs.intelligence.algorithms.{StoredGraphCoalescingAlgos, StoredInterestSimilarityAlgos}
import com.gravity.ontology.OntologyGraphName
import com.gravity.utilities.{BaseScalaTest, Settings}
import org.scalatest.Tag

/*             )\._.,--....,'``.
 .b--.        /;   _.. \   _\  (`._ ,.
`=,-,-'~~~   `----(,_..'--(,_..'`-.;.'  */

class StoredGraphTest extends BaseScalaTest {
  val testGraph = StoredGraph(
    IndexedSeq(
      Node(1l, "Cats", "http://cats.com/cats", NodeType.Topic, count = 2),
      Node(2l, "Pets", "http://gravity.com/pets", NodeType.Interest, count = 2),
      Node(3l, "Lakers", "http://lakers.com/lakers", NodeType.Topic, count = 2),
      Node(4l, "Basketball", "http://gravity.com/basketball", NodeType.Interest, 2),
      Node(5l, "Sports", "http://gravity.com/sports", NodeType.Interest, 1),
      Node(7l, "Home", "http://gravity.com/home", NodeType.Interest, level = 1),
      Node(8l, "Fridges", "http://fridges.com", NodeType.Topic),
      Node(9l, "Appliances", "http://appliances.com", NodeType.Interest, level = 2),
      Node(10l, "Engineering", "http://engineering.com", NodeType.Interest, level = 1)
    ),
    IndexedSeq(
      Edge(1l, 2l, 3, 0.5, EdgeType.InterestOf),
      Edge(3l, 4l, 2, 0.5, EdgeType.InterestOf, count = 2),
      Edge(4l, 5l, 1, 1.0, EdgeType.BroaderThan),
      Edge(2l, 7l, 1, 1.0, EdgeType.BroaderThan, count = 1),
      Edge(8l, 9l, 1, 1.0, EdgeType.InterestOf),
      Edge(9l, 10l, 1, 1.0, EdgeType.BroaderThan),
      Edge(8l, 7l, 1, 1.0, EdgeType.InterestOf)
    )
  )

  val testGraph2 = StoredGraph(
    IndexedSeq(
      Node(1l, "Cats", "http://cats.com/cats", NodeType.Topic, score = 3.6),
      Node(2l, "Pets", "http://gravity.com/pets", NodeType.Interest, count = 2),
      Node(3l, "Lakers", "http://lakers.com/lakers", NodeType.Topic),
      Node(4l, "Basketball", "http://gravity.com/basketball", NodeType.Interest, 2),
      Node(5l, "Sports", "http://gravity.com/sports", NodeType.Interest, 1),
      Node(6l, "Dogs", "http://dogs.com/dogs", NodeType.Topic, count = 2)
    ),
    IndexedSeq(
      Edge(1l, 2l, 3, 0.5, EdgeType.InterestOf),
      Edge(3l, 4l, 2, 0.5, EdgeType.InterestOf, count = 2),
      Edge(4l, 5l, 1, 1.0, EdgeType.BroaderThan),
      Edge(6l, 2l, 3, 3.5, EdgeType.InterestOf, 3)
    )
  )

  test("testDocumentationMaking") {
    val storedGraph: StoredGraph = StoredGraph.make
      .addNode("http://cats.com/cats", "Cats", NodeType.Topic)
      .addNode("http://dogs.com/dogs", "Dogs", NodeType.Topic)
      .addNode("http://pets.com/pets", "Pets", NodeType.Interest)
      .relate("http://cats.com/cats", "http://pets.com/pets", EdgeType.InterestOf)
      .relate("http://dogs.com/dogs", "http://pets.com/pets", EdgeType.InterestOf)
      .build

    storedGraph.prettyPrint()

  }

  test("testDocumentationMultiplication") {

    val storedGraph: StoredGraph = StoredGraph.make
      .addNode("http://cats.com/cats", "Cats", NodeType.Topic)
      .addNode("http://dogs.com/dogs", "Dogs", NodeType.Topic)
      .addNode("http://pets.com/pets", "Pets", NodeType.Interest)
      .relate("http://cats.com/cats", "http://pets.com/pets", EdgeType.InterestOf)
      .relate("http://dogs.com/dogs", "http://pets.com/pets", EdgeType.InterestOf)
      .build

    storedGraph.prettyPrint()

    (storedGraph * 1000).prettyPrint()
  }

  test("testDocumentationMerging") {
    val graph1: StoredGraph = StoredGraph.make
      .addNode("http://cats.com/cats", "Cats", NodeType.Topic, score = 0.6)
      .addNode("http://dogs.com/dogs", "Dogs", NodeType.Topic)
      .addNode("http://pets.com/pets", "Pets", NodeType.Interest)
      .addNode("http://animals.com/animals", "Animals", NodeType.Interest)
      .relate("http://cats.com/cats", "http://pets.com/pets", EdgeType.InterestOf)
      .relate("http://dogs.com/dogs", "http://pets.com/pets", EdgeType.InterestOf)
      .relate("http://pets.com/pets", "http://animals.com/animals", EdgeType.BroaderThan)
      .build

    println("FIRST GRAPH:")
    graph1.prettyPrint()

    val graph2: StoredGraph = StoredGraph.make
      .addNode("http://cats.com/cats", "Cats", NodeType.Topic, score = 0.9)
      .addNode("http://birds.com/birds", "Birds", NodeType.Topic)
      .addNode("http://giraffes.com/giraffes", "Giraffes", NodeType.Topic)
      .addNode("http://animals.com/animals", "Animals", NodeType.Interest)
      .addNode("http://pets.com/pets", "Pets", NodeType.Interest)
      .relate("http://cats.com/cats", "http://pets.com/pets", EdgeType.InterestOf)
      .relate("http://birds.com/birds", "http://animals.com/animals", EdgeType.InterestOf)
      .relate("http://giraffes.com/giraffes", "http://animals.com/animals", EdgeType.InterestOf)
      .build

    println("SECOND GRAPH:")
    graph2.prettyPrint()

    val merged = graph1 + graph2

    println("THIRD GRAPH:")
    merged.prettyPrint()
  }

  test("testDistance") {
    val storedGraph: StoredGraph = StoredGraph.make
      .addNode("http://cats.com/cats", "Cats", NodeType.Topic)
      .addNode("http://dogs.com/dogs", "Dogs", NodeType.Topic)
      .addNode("http://pets.com/pets", "Pets", NodeType.Interest)
      .relate("http://cats.com/cats", "http://pets.com/pets", EdgeType.InterestOf)
      .relate("http://dogs.com/dogs", "http://pets.com/pets", EdgeType.InterestOf)
      .build


    val report = storedGraph.distanceReport(storedGraph.nodeByUri("http://cats.com/cats"))
    report.foreach {
      node =>
        println("Node: " + node.node.uri + " has distance " + node.distance + " from Cats")
    }
  }

  test("testSizeReduction") {
    val storedGraph: StoredGraph = StoredGraph.make
      .addNode("http://cats.com/cats", "Cats", NodeType.Topic, count = 2)
      .addNode("http://dogs.com/dogs", "Dogs", NodeType.Topic, count = 2)
      .addNode("http://pets.com/pets", "Pets", NodeType.Interest)
      .addNode("http://soda.com/soda", "Soda", NodeType.Topic)
      .addNode("http://food.com/food", "Food", NodeType.Interest)
      .relate("http://cats.com/cats", "http://pets.com/pets", EdgeType.InterestOf)
      .relate("http://dogs.com/dogs", "http://pets.com/pets", EdgeType.InterestOf)
      .relate("http://soda.com/soda", "http://food.com/food", EdgeType.InterestOf)
      .build

    println("INITIAL GRAPH:")
    storedGraph.prettyPrint()

    val finalGraph = storedGraph.subGraph(2)

    println("FINAL GRAPH:")
    finalGraph.prettyPrint()
  }

  test("testCentralNodeReduction") {
    val storedGraph: StoredGraph = StoredGraph.make
      .addNode("http://cats.com/cats", "Cats", NodeType.Topic, count = 2)
      .addNode("http://dogs.com/dogs", "Dogs", NodeType.Topic, count = 2)
      .addNode("http://pets.com/pets", "Pets", NodeType.Interest)
      .addNode("http://bacon.com/bacon", "Bacon", NodeType.Topic)
      .addNode("http://soda.com/soda", "Soda", NodeType.Topic)
      .addNode("http://food.com/food", "Food", NodeType.Interest)
      .relate("http://bacon.com/bacon", "http://food.com/food", EdgeType.InterestOf)
      .relate("http://cats.com/cats", "http://pets.com/pets", EdgeType.InterestOf)
      .relate("http://dogs.com/dogs", "http://pets.com/pets", EdgeType.InterestOf)
      .relate("http://soda.com/soda", "http://food.com/food", EdgeType.InterestOf)
      .build

    println("INITIAL GRAPH:")
    storedGraph.prettyPrint()

    val finalGraph = storedGraph.subGraph(2, storedGraph.nodeByUri("http://food.com/food").id)
    println("FINAL GRAPH:")

    finalGraph.prettyPrint()

  }

  test("testAddition") {
    val aggroGraph = testGraph + testGraph2

    assert(aggroGraph.nodeById(1).count == 3)
    assert(aggroGraph.nodeById(2).count == 4)
    assert(aggroGraph.edgeById(1, 2, EdgeType.InterestOf).count == 2)

    aggroGraph.prettyPrint()
  }

  test("test trim") {
    val trimmedGraph = testGraph.subGraph(2)

    assert(trimmedGraph.nodeByIdOption(1l).isDefined)
    assert(trimmedGraph.nodeByIdOption(8l).isEmpty) // Fridge should be gone because it is the lowest count
    assert(trimmedGraph.nodeByIdOption(9l).isEmpty) // Appliances should be gone because only Fridge rolls up to it
    assert(trimmedGraph.nodeByIdOption(10l).isEmpty) // Engineering should be gone because only Appliances rolls up to it
    trimmedGraph.prettyPrint()
  }


  test("testNodeAddition") {
    val cat = Node(1l, "Cats", "http://cats.com/cats", NodeType.Topic, 100, 1, 3.6)
    val cat2 = Node(1l, "Cats", "http://cats.com/cats", NodeType.Topic, 100, 3, 6.6)

    val aggroCat = cat + cat2
    assert(aggroCat.count == 4)
    aggroCat.score should be (5.1 +- 0.1)
  }

  test("testEdgeAddition") {
    val edge1 = Edge(1l, 2l, 3, 6.5, EdgeType.InterestOf, 2)
    val edge2 = Edge(1l, 2l, 6, 8.2, EdgeType.InterestOf, 2)

    val aggroEdge = edge1 + edge2
    assert(aggroEdge.distance == 4)
    aggroEdge.score should be (7.35 +- 0.1)
  }


  test("testMultiplication") {
    val threeGraph = testGraph * 3
    assert(threeGraph.nodeById(1l).count == 6)
    assert(threeGraph.nodeById(2l).count == 6)
    assert(threeGraph.nodeById(3l).count == 6)

    assert(threeGraph.edgeById(1l, 2l, EdgeType.InterestOf).count == 3)
    assert(threeGraph.edgeById(3l, 4l, EdgeType.InterestOf).count == 6)
    assert(threeGraph.edgeById(4l, 5l, EdgeType.BroaderThan).count == 3)
  }

  test("testInstantiation") {
    val bytes = StoredGraphConverter.toBytes(testGraph)

    val graphFromBytes = StoredGraphConverter.fromBytes(bytes)

    testGraph.nodes.foreach(n => {
      assert(n.id == graphFromBytes.nodesById(n.id).id)
    })

    testGraph.edges.foreach(e => {
      assert(e == graphFromBytes.edgeById(e.id))
    })

    println(graphFromBytes)

    val equality = if (graphFromBytes == testGraph) true else false
    println(equality)

    println(graphFromBytes.interests.mkString("Interests: ", ":", "."))
    println(graphFromBytes.topics.mkString("Topics: ", ":", "."))

    println(graphFromBytes.nodesVia(graphFromBytes.nodeById(1l), EdgeType.InterestOf).mkString("Interests of Cats: ", ":", "."))
  }

  def line(msg: String) {
    println(msg)
  }


  ignore("testInterestGraphTranslation") {
    implicit val ogName = new OntologyGraphName(Settings.ONTOLOGY_DEFAULT_GRAPH_NAME)

    val contents = ContentToGraph("http://mysite.com", new DateTime(), "I went to Google then Microsoft", Weight.High, ContentType.Article) :: Nil
    val grapher = new Grapher(contents)

    grapher.phrases foreach {
      phrase =>
      //        printf("\t%-10s\n", phrase.text)
    }
    grapher.scoredTopics.toList.sortBy(-_.score) foreach {
      topic =>
        line(("\t%-40s : %s : %s : %s").format(topic.topic.name, topic.score, topic.phrases.map(t => t.phrase.text + "|" + t.phrase.count).mkString("{", ":", "}"), topic.topic.uri))
    }
    grapher.scoredConcepts.toSeq.sortBy(-_.score) foreach {
      concept =>
        line(concept.makeDetail)
    }


    val ig = grapher.interestGraph(GrapherAlgo.LayeredAlgo)
    ig.prettyFormat()

    val storedGraph = StoredGraph(ig)

    storedGraph.prettyPrint()


  }

  test("testCoalesce") {
    val graph1 = StoredGraphExamples.petsAndSportsGraph
    val graph2 = StoredGraphExamples.adornmentsAndPetsGraph

    val result = StoredGraphCoalescingAlgos.Standard.coalesce(StoredGraphExampleUris.cats, Seq(graph1, graph2), 2).get
    assert(result.nodeByUriOption(StoredGraphExampleUris.cats).isDefined)
    result.prettyPrint()

  }

  test("testOpinionatedCosineSimilarity") {

    val graph1 = StoredGraphExamples.petsAndSportsGraph
    val graph2 = StoredGraphExamples.adornmentsAndPetsGraph
    val graph3 = StoredGraphExamples.appliancesGraph
    val graph4 = StoredGraphExamples.petsAndSportsGraphNeg


    val score1 = StoredInterestSimilarityAlgos.GraphCosineSimilarityByLevel.score(graph1, graph2)
    println(score1)

    val score2 = StoredInterestSimilarityAlgos.GraphCosineSimilarityByLevel.score(graph1, graph1)
    println(score2)

    val score3 = StoredInterestSimilarityAlgos.GraphCosineSimilarityByLevel.score(graph1, graph3)
    println(score3)

    val score4 = StoredInterestSimilarityAlgos.GraphCosineSimilarityByLevel.score(graph1, graph4)
    println(score4)
  }

  test("testCosineSimilarity") {
    val graph1 = StoredGraphExamples.petsAndSportsGraph
    val graph2 = StoredGraphExamples.adornmentsAndPetsGraph
    val graph3 = StoredGraphExamples.appliancesGraph
    val graph4 = StoredGraphExamples.petsAndSportsGraphNeg


    val score1 = StoredInterestSimilarityAlgos.GraphCosineSimilarity.score(graph1, graph2)
    println(score1)
   assert(score1.score > 0.3)

    val score2 = StoredInterestSimilarityAlgos.GraphCosineSimilarity.score(graph1, graph1)
    println(score2)
    score2.score should be (1.0 +- 0.00001)


    val score3 = StoredInterestSimilarityAlgos.GraphCosineSimilarity.score(graph1, graph3)
    println(score3)
    assert(score3.score == 0.0)

    val score4 = StoredInterestSimilarityAlgos.GraphCosineSimilarity.score(graph1, graph4)
    println(score4)
    assert(score4.score < 0)
  }

}