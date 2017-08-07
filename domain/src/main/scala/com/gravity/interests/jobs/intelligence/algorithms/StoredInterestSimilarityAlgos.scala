package com.gravity.interests.jobs.intelligence.algorithms

import com.gravity.interests.jobs.intelligence.SchemaTypes._
import com.gravity.utilities.{grvcoll, grvmath, grvstrings}
import com.gravity.interests.jobs.intelligence.{NodeType, Node, StoredGraph}
import scala.collection.mutable

/*             )\._.,--....,'``.
.b--.        /;   _.. \   _\  (`._ ,.
`=,-,-'~~~   `----(,_..'--(,_..'`-.;.'  */

/**
 * Algorithms for determining how similar two StoredInterestGraphs are.  There is no absolute requirement that
 * the algos inherit from the abstract base, because some algos might require more data than others (say, normalized contextual
 * scores).
 */
case class ContentCandidate(siteGuid: String, title: String, body: String = "")

object StoredInterestSimilarityAlgos {

  object StandardSimilarity extends CountBasedSimilarity

  object LeftSidedCountBasedSimilarity extends LeftSidedCountBasedSimilarity

  object LeftSidedConceptBasedSimilarity extends LeftSidedConceptBasedSimilarity

  object LeftSidedCountBasedSimilarityWithConceptFilter extends LeftSidedCountBasedSimilarityWithConceptFilter

  object LeftSidedCountBasedSimilarityWithConceptFilterOptimized extends LeftSidedCountBasedSimilarityWithConceptFilterOptimized

  object LeftSidedScoreBasedSimilarityWithConceptFilterOptimized extends LeftSidedScoreBasedSimilarityWithConceptFilterOptimized

  object HeavyTopicSimilarity extends HeavierTopicBasedSimilarity

  object YahooSimilarity extends YahooBasedSimilarity

  object PercentileSimilarity extends PercentileSimilarity

  object PercentileSimilarityByTopics extends PercentileSimilarityByTopics

  /**
   * Use if the node's score is the best expression of the node's status
   */
  object GraphCosineScoreOnlySimilarity extends GraphCosineSimilarity(node => node.score)

  /**
   * Use if the node's score and level is the best expression of the node's status
   */
  object GraphCosineScoreSimilarity extends GraphCosineSimilarity(node => node.score * levelWeight(node.level))


  /**
   * Returns a value between 1 (completely the same) and -1 (completely dissimilar). Graphs with nothing in common will get 0
   */
  object GraphCosineSimilarity extends GraphCosineSimilarity(node => node.count * levelWeight(node.level))


  /**
   * Raw score based on count
   */
  object GraphCosineSimilarityByCount extends GraphCosineSimilarity(node => node.count)


  /**
   * This is an opinionated cosine similarity, where the lower the level, the less the node matters
   * This is useful because the lower the level, the more general the node (and therefore less indicative of similarity)
   */
  object GraphCosineSimilarityByLevel extends GraphCosineSimilarity(node => node.count * levelWeight(node.level))

  def levelWeight(level: Short): Short = {
    level match {
      case l if (l >= 100) => (l / 3).toShort // Topics
      case l if (l > 1) => (l * 3).toShort // Low & Mid level Concepts  (most valuable)
      case l => (l / 2).toShort // High level abstract Concepts
    }
  }
}

case class GraphSimilarityScore(score: Double)

abstract class StoredInterestSimilarityAlgo() {
  def score(thisGraph: StoredGraph, thatGraph: StoredGraph, counter: CounterFunc = CounterNoOp): GraphSimilarityScore
}

class PercentileSimilarity() {
  def score(thisGraph: StoredGraph, thatGraph: StoredGraph, counter: CounterFunc = CounterNoOp): GraphSimilarityScore = {
    val maxMatches = thisGraph.nodes.size max thatGraph.nodes.size
    var matches = 0
    for {node <- thisGraph.nodes} {
      if (thatGraph.nodeByIdOption(node.id).isDefined) matches = matches + 1
    }

    GraphSimilarityScore(matches.toDouble / maxMatches.toDouble)
  }

  def scoreVectors(vector1: mutable.HashMap[Long, Double], vector2: mutable.HashMap[Long, Double]): GraphSimilarityScore = {
    val maxMatches = vector1.size max vector2.size
    val intersectionKeys = vector1.keySet.intersect(vector2.keySet)

    GraphSimilarityScore(intersectionKeys.size.toDouble / maxMatches.toDouble)

  }
}

class PercentileSimilarityByTopics() {
  def score(thisGraph: StoredGraph, thatGraph: StoredGraph, counter: CounterFunc = CounterNoOp): GraphSimilarityScore = {
    val maxMatches = thisGraph.topics.size max thatGraph.topics.size
    var matches = 0
    for {topic <- thisGraph.topics} {
      //if(thatGraph.topics.exists(_.id == topic.id)) would also have worked
      if (thatGraph.nodeByIdOption(topic.id).isDefined) {
        matches = matches + 1
      }
    }

    GraphSimilarityScore(matches.toDouble / maxMatches.toDouble)
  }
}

/**
 * Will return 1 if the graphs are the same.  0 if they have no overlap.  -1 if they are opposite.
 * In practical terms, between 0 and 1, because nodes will not have negative scores.
 *
 * If we had a way to express disaffinity, then this would be a good measure because negative scores on a node will matter.
 * The weight function allows you to decide what the score of a node is
 */
class GraphCosineSimilarity(weightFunc: (Node) => Double) {
  def slowScore(thisGraph: StoredGraph, thatGraph: StoredGraph, counter: CounterFunc = CounterNoOp) : GraphSimilarityScore = {

    if (thisGraph.nodes.size == 0 || thatGraph.nodes.size == 0) {
      GraphSimilarityScore(0.0D)
    }
    else {
      val sortedNodeIds = grvcoll.distinctToSet(thisGraph.nodes, thatGraph.nodes)(_.id).toSeq.sorted

      /**
       * Build a vector of the nodes for a particular graph
        *
        * @param graph
       * @return
       */
      def vectorizeGraph(graph: StoredGraph) = {
        val vector = mutable.ArrayBuffer[Double]()
        for (nodeId <- sortedNodeIds) {
          graph.nodeByIdOption(nodeId) match {
            case Some(thisNode) => {
              vector += weightFunc(thisNode)
            }
            case None => vector += 0
          }
        }
        vector
      }

      val vectorA = vectorizeGraph(thisGraph)
      val vectorB = vectorizeGraph(thatGraph)
      GraphSimilarityScore(grvmath.cosineSimilarity(vectorA, vectorB))
    }
  }

  def vectorizeGraph(g: StoredGraph) = {

    val vector = new mutable.HashMap[Long, Double]()
    for {(nodeId, node) <- g.nodesById}{
      vector.put(nodeId, weightFunc(node))
    }
    vector
  }

  def vectoredGraphScoreOther_(vector1: mutable.HashMap[Long, Double], vector2: mutable.HashMap[Long, Double]): GraphSimilarityScore = {


    if (vector1.size == 0 || vector2.size == 0){
      return GraphSimilarityScore(0.0D)
    }



    def pad(maxLen: Int, v: Array[Long]) = {
      val buffer = new Array[Long](maxLen)
      Array.copy(v, 0, buffer, 0, v.size)
      buffer.toIndexedSeq
    }



//    val v1 = vector1.keys.toIndexedSeq
//    val v2 = vector2.keys.toIndexedSeq

    val v1Size = vector1.size
    val v2Size = vector2.size


    val (newV1, newV2) =
      if (v1Size < v2Size){
        (pad(v2Size, vector1.keys.toArray), vector2.keys.toIndexedSeq)
      }
      else if (v2Size < v1Size) {
        (vector1.keys.toIndexedSeq, pad(v1Size, vector2.keys.toArray))
      }
      else {
        (vector1.keys.toIndexedSeq, vector2.keys.toIndexedSeq)
      }


    val cosSim = grvmath.squaredEuclideanDistance(newV1, newV2)
    val cosSimSafe =
      cosSim match {
        case x if x.isNaN => 0.0
        case _ => cosSim
      }
    GraphSimilarityScore(cosSimSafe)

  }

  def vectoredGraphScoreOther2(vector1: mutable.HashMap[Long, Double], vector2: mutable.HashMap[Long, Double]) = {
    if (vector1.size == 0 || vector2.size == 0){
      GraphSimilarityScore(0.0D)
    }
    else{
      val ids1 = vector1.keySet
      val ids2 = vector2.keySet
      val intersect = ids1.intersect(ids2)


      if (intersect.size == 0){
        GraphSimilarityScore(0.0D)
      }
      else {

        val (m1, m2) =
          if (vector1.size > vector2.size){
            (vector1, vector2)
          }
          else{
            (vector2, vector1)
          }

        val vA = mutable.ArrayBuffer[Double]()
        val vB = mutable.ArrayBuffer[Double]()

        val m2Copy = new mutable.HashMap[Long, Double]() ++= m2  //this is mutable and will be changed as we go


        for {(n1, score1) <- m1} {
          vA += score1
          if (intersect.contains(n1)){
            vB += m2Copy(n1)
            m2Copy.remove(n1)
          }
          else {
            vB += 0
          }
        }

        //now m2Copy contains only ids that are not in m1
        for {(n2, score2) <- m2Copy}{
          vA += 0
          vB += score2
        }

//        val cosSim = grvmath.cosineSimilarity(vA, vB)
        val cosSim = grvmath.euclideanDistance(vA, vB)
        val cosSimSafe =
          cosSim match {
            case x if x.isNaN => 0.0
            case _ => cosSim
          }
        GraphSimilarityScore(cosSimSafe)

      }


    }

  }

  def vectoredGraphScore(vector1: mutable.HashMap[Long, Double], vector2: mutable.HashMap[Long, Double]) = {
    if (vector1.isEmpty || vector2.isEmpty){
      GraphSimilarityScore(0.0D)
    }
    else{
      val ids1 = vector1.keySet
      val ids2 = vector2.keySet
      val intersect = ids1.intersect(ids2)

      // a better .isEmpty that doesn't walk the full set
      if (!intersect.exists(_ => true)){
        GraphSimilarityScore(0.0D)
      }
      else {

        val (m1, m2) =
          if (vector1.size > vector2.size){
            (vector1, vector2)
          }
          else{
            (vector2, vector1)
          }

        val vA = mutable.ArrayBuffer[Double](vector1.size + vector2.size)
        val vB = mutable.ArrayBuffer[Double](vector1.size + vector2.size)

        for {(n1, score1) <- m1} {
          vA += score1
          vB += m2.getOrElse(n1, 0)
        }

        //any n2 contained in the intersection has already been added
        for {
          (n2, score2) <- m2
          if !intersect.contains(n2)
        }{
          vA += 0
          vB += score2
        }

        val cosSim = grvmath.cosineSimilarity(vA, vB)
        val cosSimSafe =
          cosSim match {
            case x if x.isNaN => 0.0
            case _ => cosSim
          }
        GraphSimilarityScore(cosSimSafe)

      }


    }

  }

  def score(thisGraph: StoredGraph, thatGraph: StoredGraph, counter: CounterFunc = CounterNoOp) : GraphSimilarityScore = {

    if (thisGraph.nodes.size == 0 || thatGraph.nodes.size == 0) {
      GraphSimilarityScore(0.0D)
    }
    else if (thatGraph.nodes.map(_.id).intersect(thisGraph.nodes.map(_.id)).size == 0){
      GraphSimilarityScore(0.0D)
    }
    else {
      //iterate through nodes in the larger graph. for each node, and score the node.
      //see if there exists a node for that id in the smaller graph, if so remove it from the copied mutable map and score it, otherwise zero
      //afterwards, add the leftovers in the smaller graph with score 0 for thisGraph
      val vectorA = mutable.ArrayBuffer[Double]()
      val vectorB = mutable.ArrayBuffer[Double]()

      //these size checks are not free! 4% of runtime. I'm not sure there's any net gain to be had here
//      val nodes1 = if(thisGraph.nodes.size > thatGraph.nodes.size) thisGraph.nodes else thatGraph.nodes
//      val nodes2 = grvcoll.toMap(if(thisGraph.nodes.size > thatGraph.nodes.size) thatGraph.nodes else thisGraph.nodes)(_.id)

      val nodes1 = thisGraph.nodes //this is immutable and will be the master loop
      val nodes2 = grvcoll.toMap(thatGraph.nodes)(_.id) //this is mutable and will be changed as we go

      nodes1.foreach{node1 => {
        val id = node1.id
        vectorA += weightFunc(node1)
        //find a way to short circuit when nodes2 is empty?
        nodes2.remove(id) match {
          case Some(node2) => vectorB += weightFunc(node2)
          case None => vectorB += 0
        }
      }}

      //now nodes2 contains only ids that are not in nodes1

      nodes2.values.foreach{node2 => {
        vectorA += 0
        vectorB += weightFunc(node2)
      }}

      val cosSim = grvmath.cosineSimilarity(vectorA, vectorB)
      val cosSimSafe =
        cosSim match {
          case x if x.isNaN => 0.0
          case _ => cosSim
        }
      GraphSimilarityScore(cosSimSafe)
    }
  }
}

class LeftSidedScoreSimilarity extends LeftSidedSimilarity(node => node.score, StoredInterestSimilarityAlgos.levelWeight) {}

class LeftSidedCountBasedSimilarity extends LeftSidedSimilarity(node => node.count, StoredInterestSimilarityAlgos.levelWeight) {}

class LeftSidedSimilarity(scoreFunc: (Node) => Double, levelWeightFunc: (Short) => Short) {
  def score(thisGraph: StoredGraph, thatGraph: StoredGraph, counter: CounterFunc = CounterNoOp): GraphSimilarityScore = {
    val intersectingConceptIds = thisGraph.interests.map(_.id).intersect(thatGraph.interests.map(_.id))
    val intersectingTopicIds = thisGraph.topics.map(_.id).intersect(thatGraph.topics.map(_.id))

    val aggregateTopicScore = (thisGraph.topics).filter {
      topic => intersectingTopicIds.contains(topic.id)
    }.foldLeft(0D)((sum, node) => sum + scoreFunc(node) + levelWeightFunc(node.level))

    val aggregateConceptScore = (thisGraph.interests).filter {
      concept => intersectingConceptIds.contains(concept.id)
    }.foldLeft(0D)((sum, node) => sum + scoreFunc(node) + levelWeightFunc(node.level))
    counter("Overlapping Topics", aggregateTopicScore.toLong)
    counter("Overlapping Concepts", aggregateConceptScore.toLong)

    GraphSimilarityScore(aggregateConceptScore + aggregateTopicScore)
  }

}

class LeftSidedCountBasedSimilarity_OLD() {
  def score(thisGraph: StoredGraph, thatGraph: StoredGraph, counter: CounterFunc = CounterNoOp): GraphSimilarityScore = {
    val intersectingConceptIds = thisGraph.interests.map(_.id).intersect(thatGraph.interests.map(_.id))
    val intersectingTopicIds = thisGraph.topics.map(_.id).intersect(thatGraph.topics.map(_.id))

    val aggregateTopicCounts = (thisGraph.topics).filter {
      topic => intersectingTopicIds.contains(topic.id)
    }.foldLeft(0l)(_ + _.count + 100)

    val aggregateConceptCounts = (thisGraph.interests).filter {
      concept => intersectingConceptIds.contains(concept.id)
    }.foldLeft(0l)(_ + _.count + 1)
    counter("Overlapping Topics", aggregateTopicCounts)
    counter("Overlapping Concepts", aggregateConceptCounts)

    GraphSimilarityScore(aggregateConceptCounts + aggregateTopicCounts)
  }

}

class LeftSidedScoreBasedSimilarityWithConceptFilterOptimized extends LeftSidedSimilarityWithConceptFilterOptimized(node => node.score, StoredInterestSimilarityAlgos.levelWeight) {}

class LeftSidedCountBasedSimilarityWithConceptFilterOptimized extends LeftSidedSimilarityWithConceptFilterOptimized(node => node.count, StoredInterestSimilarityAlgos.levelWeight) {}

class LeftSidedSimilarityWithConceptFilterOptimized(scoreFunc: (Node) => Double, levelWeightFunc: (Short) => Short) {

  def score(thisGraph: StoredGraph, thatGraph: StoredGraph, counter: CounterFunc = CounterNoOp): GraphSimilarityScore = {

    //unfortunately gotta filter these out here.  Everything hits business and management given our existing
    //annotated routes.  new concept based work should make this significantly smarter/not require filtering like this

    val interSectIdx = new gnu.trove.set.hash.TLongHashSet()

    var aggregateTopicCounts = 0.0
    var aggregateConceptCounts = 0.0
    thatGraph.nodes.foreach(node => {
      interSectIdx.add(node.id)
    })
    thisGraph.nodes.foreach {
      node =>
        if (interSectIdx.contains(node.id)) {
          node.nodeType match {
            case NodeType.Interest if (node.name != "Business" && node.name != "Management") => {
              val score = scoreFunc(node)
              aggregateConceptCounts += (scoreFunc(node) * levelWeightFunc(node.level))
            }
            case NodeType.Topic => {
              val score = scoreFunc(node)
              aggregateTopicCounts += (scoreFunc(node) * levelWeightFunc(node.level))
            }
            case _ => {}
          }
        }
    }

    GraphSimilarityScore(aggregateConceptCounts + aggregateTopicCounts)
  }

}

class LeftSidedCountBasedSimilarityWithConceptFilterOptimized_OLD() {

  def score(thisGraph: StoredGraph, thatGraph: StoredGraph, counter: CounterFunc = CounterNoOp): GraphSimilarityScore = {

    //unfortunately gotta filter these out here.  Everything hits business and management given our existing
    //annotated routes.  new concept based work should make this significantly smarter/not require filtering like this

    val interSectIdx = new gnu.trove.set.hash.TLongHashSet()

    var aggregateTopicCounts = 0.0
    var aggregateConceptCounts = 0.0
    thatGraph.nodes.foreach(node => interSectIdx.add(node.id))
    thisGraph.nodes.foreach {
      node =>
        if (interSectIdx.contains(node.id)) {
          node.nodeType match {
            case NodeType.Interest if (node.name != "Business" && node.name != "Management") => {
              aggregateConceptCounts += (node.count.toDouble * node.level.toDouble)
            }
            case NodeType.Topic => aggregateTopicCounts += (node.count + 100)
            case _ => {}
          }
        }
    }

    GraphSimilarityScore(aggregateConceptCounts + aggregateTopicCounts)
  }

}


class LeftSidedCountBasedSimilarityWithConceptFilter() {
  def score(thisGraph: StoredGraph, thatGraph: StoredGraph, counter: CounterFunc = CounterNoOp): GraphSimilarityScore = {

    //unfortunately gotta filter these out here.  Everything hits business and management given our existing
    //annotated routes.  new concept based work should make this significantly smarter/not require filtering like this
    val filteredThisGraph = thisGraph.interests.filter(i => i.name != "Business").filter(i => i.name != "Management")
    val filteredThatGraph = thatGraph.interests.filter(i => i.name != "Business").filter(i => i.name != "Management")

    val intersectingConceptIds = filteredThisGraph.map(_.id).intersect(filteredThatGraph.map(_.id))
    val intersectingTopicIds = thisGraph.topics.map(_.id).intersect(thatGraph.topics.map(_.id))

    val aggregateTopicCounts = (thisGraph.topics).filter {
      topic => intersectingTopicIds.contains(topic.id)
    }.foldLeft(0l)(_ + _.count + 100)

    val aggregateConceptCounts = (filteredThisGraph).filter {
      concept => intersectingConceptIds.contains(concept.id)
    }.foldLeft(0d)((a, b) => a + b.count.toDouble * b.level.toDouble)
    counter("Overlapping Topics", aggregateTopicCounts)
    counter("Overlapping Concepts", aggregateConceptCounts.toLong)

    GraphSimilarityScore(aggregateConceptCounts + aggregateTopicCounts)
  }

}

class LeftSidedConceptBasedSimilarity() {
  def score(thisGraph: StoredGraph, thatGraph: StoredGraph, counter: CounterFunc = CounterNoOp): GraphSimilarityScore = {

    //unfortunately gotta filter these out here.  Everything hits business and management given our existing
    //annotated routes.  new concept based work should make this significantly smarter/not require filtering like this
    val filteredThisGraph = thisGraph.interests.filter(i => i.name != "Business").filter(i => i.name != "Management").filter(i => i.level != 1)
    val filteredThatGraph = thatGraph.interests.filter(i => i.name != "Business").filter(i => i.name != "Management").filter(i => i.level != 1)

    val intersectingConceptIds = filteredThisGraph.map(_.id).intersect(filteredThatGraph.map(_.id))

    //val intersectingConceptIds = thisGraph.interests.map(_.id).intersect(thatGraph.interests.map(_.id))


    val aggregateConceptCounts = (filteredThisGraph).filter {
      concept => intersectingConceptIds.contains(concept.id)
    }.foldLeft(0d)((a, b) => a + b.score.toDouble)
    counter("Overlapping Concepts", aggregateConceptCounts.toLong)

    GraphSimilarityScore(aggregateConceptCounts)
  }

}

class CountBasedSimilarity() {
  def score(thisGraph: StoredGraph, thatGraph: StoredGraph, counter: CounterFunc = CounterNoOp): GraphSimilarityScore = {
    val intersectingConceptIds = thisGraph.interests.map(_.id).intersect(thatGraph.interests.map(_.id))
    val intersectingTopicIds = thisGraph.topics.map(_.id).intersect(thatGraph.topics.map(_.id))

    val aggregateTopicCounts = (thisGraph.topics ++ thatGraph.topics).filter {
      topic => intersectingTopicIds.contains(topic.id)
    }.foldLeft(0l)(_ + _.count)

    val aggregateConceptCounts = (thisGraph.interests ++ thatGraph.interests).filter {
      concept => intersectingConceptIds.contains(concept.id)
    }.foldLeft(0l)(_ + _.count)
    counter("Overlapping Topics", aggregateTopicCounts)
    counter("Overlapping Concepts", aggregateConceptCounts)

    GraphSimilarityScore(aggregateConceptCounts + aggregateTopicCounts)
  }

}

/**
 * idea here is to get rid of level 1 concepts and put more emphisis on topical overlaps
 */
class HeavierTopicBasedSimilarity() {

  val blackListedTopics: Set[String] = Set("Money", "Finance", "Factiva", "Google", "Facebook", "Internet")

  def score(thisGraph: StoredGraph, thatGraph: StoredGraph, counter: CounterFunc = CounterNoOp, contentCandidate: ContentCandidate, debugMode: Boolean = false): GraphSimilarityScore = {

    if (debugMode) {
      // thisGraph.prettyPrint()

      //     thatGraph.prettyPrint()
    }

    // tokenize the title of the article
    val titleTokens = grvstrings.tokenize(contentCandidate.title, " ").map {
      case s: String => s.toLowerCase
    }.toSet

    var intersectingTitleTokens = 0
    thatGraph.topics.map {
      topic =>
        if (titleTokens.contains(topic.name.toLowerCase)) {
          intersectingTitleTokens += 1
        }
    }

    //    val intersectingTitleTokens = articleTopics.toSet.intersect(titleTokens.toSet).size

    // filter out blacklisted topics, then sort by score desc and take the top n results
    val MAXTOPICS = 15
    val truncdArticleTopics = thatGraph.topics.filter(
      top => {
        try {
          if (blackListedTopics.contains(top.name)) false else true
        } catch {
          case e: Exception => false
        }

      }
    ).sortBy(-_.score).take(MAXTOPICS)

    var aggregateTopicCounts = 0
    truncdArticleTopics.foreach {
      topic =>
        thisGraph.nodesById.get(topic.id) match {
          case Some(thisTopic) => {
            aggregateTopicCounts += topic.count + thisTopic.count
          }
          case None =>
        }
    }

    var aggregateConceptCounts = 0
    thatGraph.interests.foreach {
      interest =>
        thisGraph.nodesById.get(interest.id) match {
          case Some(thisInterest) => {
            aggregateConceptCounts += interest.count + thisInterest.count
          }
          case None =>
        }
    }

    counter("Overlapping Topics", aggregateTopicCounts)
    counter("Overlapping Concepts", aggregateConceptCounts)

    val TOPIC_OVERLAP_BOOSTER = 1.7

    val TITLE_OVERLAP_BOOSTER = if (intersectingTitleTokens > 0) (intersectingTitleTokens * 1.05) else 1

    GraphSimilarityScore((aggregateTopicCounts * TOPIC_OVERLAP_BOOSTER * TITLE_OVERLAP_BOOSTER))
  }

}

/**
 * idea here is to get rid of level 1 concepts and put more emphisis on topical overlaps
 */
class YahooBasedSimilarity() {

  val blackListedTopics: Set[String] = Set("Money", "Finance", "Factiva", "Google", "Facebook", "Internet")

  def score(thisGraph: StoredGraph, thatGraph: StoredGraph, counter: CounterFunc = CounterNoOp, contentCandidate: ContentCandidate, debugMode: Boolean = false, titleOverlapBooster: Double = 1.7): GraphSimilarityScore = {
    /*
    if (debugMode) {
       thisGraph.prettyPrint()

       thatGraph.prettyPrint()
    }
    */

    // tokenize the title of the article
    val titleTokens = grvstrings.tokenize(contentCandidate.title, " ").map {
      case s: String => s.toLowerCase
    }.toSet

    var intersectingTitleTokens = 0
    thatGraph.topics.map {
      topic =>
        if (titleTokens.contains(topic.name.toLowerCase)) {
          intersectingTitleTokens += 1
        }
    }

    //    val intersectingTitleTokens = articleTopics.toSet.intersect(titleTokens.toSet).size

    // filter out blacklisted topics, then sort by score desc and take the top n results
    val MAXTOPICS = 15
    val truncdArticleTopics = thatGraph.topics.filter(
      top => {
        try {
          if (blackListedTopics.contains(top.name)) false else true
        } catch {
          case e: Exception => false
        }

      }
    ).sortBy(-_.score).take(MAXTOPICS)

    var aggregateTopicCounts = 0
    truncdArticleTopics.foreach {
      topic =>
        thisGraph.nodesById.get(topic.id) match {
          case Some(thisTopic) => {
            aggregateTopicCounts += topic.count + thisTopic.count
          }
          case None =>
        }
    }

    var aggregateConceptCounts = 0
    thatGraph.interests.foreach {
      interest =>
        thisGraph.nodesById.get(interest.id) match {
          case Some(thisInterest) => {
            aggregateConceptCounts += interest.count + thisInterest.count
          }
          case None =>
        }
    }


    if (debugMode) {

      println(contentCandidate.title + ":" + aggregateTopicCounts + ":" + aggregateConceptCounts)
      println("ARTICLE GRAPH")
      thatGraph.prettyPrint()
      println("***********")
    }
    counter("Overlapping Topics", aggregateTopicCounts)
    counter("Overlapping Concepts", aggregateConceptCounts)

    val TOPIC_OVERLAP_BOOSTER = 1.7

    val TITLE_OVERLAP_BOOSTER = if (intersectingTitleTokens > 0) (intersectingTitleTokens * titleOverlapBooster) else 1

    GraphSimilarityScore((aggregateTopicCounts * TOPIC_OVERLAP_BOOSTER * TITLE_OVERLAP_BOOSTER))
  }

}