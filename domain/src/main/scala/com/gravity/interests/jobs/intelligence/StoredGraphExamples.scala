package com.gravity.interests.jobs.intelligence

import java.io.Serializable

object StoredGraphExampleUris {
  val fridges = "http://fridges.com"
  val home = "http://gravity.com/home"
  val appliances = "http://appliances.com"
  val engineering = "http://engineering.com"

  val jewels = "http://gravity.com/Jewels"
  val earrings = "http://jewelry.com/Earrings"
  val diamonds = "http://jewelry.com/Diamonds"
  val sapphires = "http://jewelry.com/Sapphires"
  val adornments = "http://gravity.com/Adornments"
  val cats = "http://cats.com/cats"
  val pets = "http://gravity.com/pets"

  val sports = "http://gravity.com/sports"
  val basketball = "http://gravity.com/basketball"
  val lakers = "http://lakers.com/lakers"
}

object StoredGraphExamples {

  import StoredGraphExampleUris._


  def appliancesGraph: StoredGraph = StoredGraph.make
    .addNode(home, "Home", NodeType.Interest, level = 1)
    .addNode(fridges, "Fridges", NodeType.Topic)
    .addNode(appliances, "Appliances", NodeType.Interest, level = 2)
    .addNode(engineering, "Engineering", NodeType.Interest, level = 1)
    .relate(fridges, appliances)
    .relate(appliances, engineering, EdgeType.BroaderThan)
    .relate(fridges, home)
    .build

  def catsAndAppliancesGraph: StoredGraph = StoredGraph.make
    .addNode(cats, "Cats", NodeType.Topic, count = 2, score = 0.5)
    .addNode(pets, "Pets", NodeType.Interest, level = 1, count = 2, score = 0.4)
    .addNode(lakers, "Lakers", NodeType.Topic, count = 2, score = 0.3)
    .addNode(basketball, "Basketball", NodeType.Interest, level = 2, score = 0.2)
    .addNode(sports, "Sports", NodeType.Interest, level = 1)
    .addNode(home, "Home", NodeType.Interest, level = 1)
    .addNode(fridges, "Fridges", NodeType.Topic)
    .addNode(appliances, "Appliances", NodeType.Interest, level = 2)
    .addNode(engineering, "Engineering", NodeType.Interest, level = 1)
    .relate(cats, pets)
    .relate(pets, home, EdgeType.BroaderThan, count = 2)
    .relate(lakers, basketball, count = 2)
    .relate(basketball, sports, EdgeType.BroaderThan)
    .relate(fridges, appliances)
    .relate(appliances, engineering, EdgeType.BroaderThan)
    .relate(fridges, home)
    .build


  def petsAndSportsGraphNeg: StoredGraph = StoredGraph.make
    .addNode(cats, "Cats", NodeType.Topic, count = -1)
    .addNode(pets, "Pets", NodeType.Interest, level = 2, count = -1)
    .relate(cats, pets)
    .addNode(lakers, "Lakers", NodeType.Topic, count = -1)
    .addNode(sports, "Sports", NodeType.Interest, level = 1, count = -1)
    .addNode(basketball, "Basketball", NodeType.Interest, level = 2, count = -1)
    .relate(lakers, basketball)
    .relate(basketball, sports, EdgeType.BroaderThan)
    .build

  def petsAndSportsGraph: StoredGraph = StoredGraph.make
    .addNode(cats, "Cats", NodeType.Topic)
    .addNode(pets, "Pets", NodeType.Interest, level = 2)
    .relate(cats, pets)
    .addNode(lakers, "Lakers", NodeType.Topic)
    .addNode(sports, "Sports", NodeType.Interest, level = 1)
    .addNode(basketball, "Basketball", NodeType.Interest, level = 2)
    .relate(lakers, basketball)
    .relate(basketball, sports, EdgeType.BroaderThan)
    .build

  def adornmentsAndPetsGraph: StoredGraph = StoredGraph.make
    .addNode(earrings, "Jewelry", NodeType.Topic)
    .addNode(diamonds, "Diamonds", NodeType.Topic)
    .addNode(sapphires, "Sapphires", NodeType.Topic)
    .addNode(jewels, "Jewels", NodeType.Interest, level = 2)
    .addNode(adornments, "Adornments", NodeType.Interest, level = 1)
    .addNode(cats, "Cats", NodeType.Topic)
    .addNode(pets, "Pets", NodeType.Interest, level = 2)
    .relate(earrings, jewels)
    .relate(diamonds, jewels)
    .relate(sapphires, jewels)
    .relate(jewels, adornments, EdgeType.BroaderThan)
    .relate(cats, pets)
    .build


  def catsAndAppliancesGraph_immutable: StoredGraph = StoredGraph.make
    .addNode(cats, "Cats", NodeType.Topic, count = 2, score = 0.5)
    .addNode(pets, "Pets", NodeType.Interest, level = 1, count = 2, score = 0.4)
    .addNode(lakers, "Lakers", NodeType.Topic, count = 2, score = 0.3)
    .addNode(basketball, "Basketball", NodeType.Interest, level = 2, score = 0.2)
    .addNode(sports, "Sports", NodeType.Interest, level = 1)
    .addNode(home, "Home", NodeType.Interest, level = 1)
    .addNode(fridges, "Fridges", NodeType.Topic)
    .addNode(appliances, "Appliances", NodeType.Interest, level = 2)
    .addNode(engineering, "Engineering", NodeType.Interest, level = 1)
    .relate(cats, pets)
    .relate(pets, home, EdgeType.BroaderThan, count = 2)
    .relate(lakers, basketball, count = 2)
    .relate(basketball, sports, EdgeType.BroaderThan)
    .relate(fridges, appliances)
    .relate(appliances, engineering, EdgeType.BroaderThan)
    .relate(fridges, home)
    .build


  def petsAndSportsGraph_immutable: StoredGraph = StoredGraph.make
    .addNode(cats, "Cats", NodeType.Topic)
    .addNode(pets, "Pets", NodeType.Interest, level = 2)
    .relate(cats, pets)
    .addNode(lakers, "Lakers", NodeType.Topic)
    .addNode(sports, "Sports", NodeType.Interest, level = 1)
    .addNode(basketball, "Basketball", NodeType.Interest, level = 2)
    .relate(lakers, basketball)
    .relate(basketball, sports, EdgeType.BroaderThan)
    .build

  def adornmentsAndPetsGraph_immutable: StoredGraph = StoredGraph.make
    .addNode(earrings, "Jewelry", NodeType.Topic)
    .addNode(diamonds, "Diamonds", NodeType.Topic)
    .addNode(sapphires, "Sapphires", NodeType.Topic)
    .addNode(jewels, "Jewels", NodeType.Interest, level = 2)
    .addNode(adornments, "Adornments", NodeType.Interest, level = 1)
    .addNode(cats, "Cats", NodeType.Topic)
    .addNode(pets, "Pets", NodeType.Interest, level = 2)
    .relate(earrings, jewels)
    .relate(diamonds, jewels)
    .relate(sapphires, jewels)
    .relate(jewels, adornments, EdgeType.BroaderThan)
    .relate(cats, pets)
    .build


  def graphsByDisplayDepths: Map[String, Serializable] = Map(
    StoredGraphDisplayDepth.NARROW.toString -> appliances,
    StoredGraphDisplayDepth.WIDE.toString -> catsAndAppliancesGraph
  )
}