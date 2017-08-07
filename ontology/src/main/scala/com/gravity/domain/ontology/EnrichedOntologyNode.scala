package com.gravity.domain.ontology

import java.util.StringTokenizer

/**
  * Created by apatel on 7/14/16.
  */

object EnrichedOntologyNode{

  def fromFileFormatOld(line: String) = {

    /*(id, nodeInfo.uri, nodeInfo.nodeType, nodeInfo.name,
        topicsTotalHop1, topicsTotalHop2, topicsTotalHop3
        topicsOnlyHop2, topicsOnlyHop3
        conceptsTotalHop1, conceptsTotalHop2, conceptsTotalHop3
        conceptsOnlyHop2, conceptsOnlyHop3,conceptsOnlyHop4
        pageRank, connectedComponentId, triangleCnt, inDeg, outDeg)

        //(-1389199041,-1389199041|||http://dbpedia.org/resource/Aircraft|||1|||Aircraft|||0|||0|||0|||0|||0|||0|||0|||0|||0|||0|||0|||0|||0|||0|||0.0|||-2105685942|||0|||0|||0)

    */
    val parts = new scala.collection.mutable.ArrayBuffer[String]()

    try{
      val stripped = line.slice(1, line.length-1)
      val tokenizer = new StringTokenizer(stripped, "|||")
      while (tokenizer.hasMoreElements()){
        parts += tokenizer.nextToken()
      }

      val id = parts(0).split(",")(1).toLong
      val uri = parts(1)
      val nodeType = parts(2).toInt
      val name = parts(3)

      val tth1 = parts(4).toInt
      val tth2 = parts(5).toInt
      val tth3 = parts(6).toInt

      val toh2 = parts(7).toInt
      val toh3 = parts(8).toInt

      val cth1 = parts(9).toInt
      val cth2 = parts(10).toInt
      val cth3 = parts(11).toInt

      val coh2 = parts(12).toInt
      val coh3 = parts(13).toInt

      val pgRnk = parts(14).toDouble
      val triCnt = parts(15).toInt
      val ccId = parts(16).toLong

      val inDeg = parts(17).toInt
      val outDeg = parts(18).toInt

      val nodeInfo = new NodeInfo(uri, name, nodeType)

      val rn = EnrichedOntologyNode(id, nodeInfo,
        new TraversalInfo(tth1, tth2, tth3, 0, toh2, toh3, 0),
        new TraversalInfo(cth1, cth2, cth3, 0, coh2, coh3, 0),
        pgRnk, triCnt, ccId, inDeg, outDeg)

      Some(rn)

    }
    catch{
      case ex: Exception =>
        println("can't convert line to ReachbilityNode wit parts " + parts.size + ": " + line)
        None
    }

  }


  def fromFileFormat(line: String) = {

    /*(id, nodeInfo.uri, nodeInfo.nodeType, nodeInfo.name,
        topicsTotalHop1, topicsTotalHop2, topicsTotalHop3,topicsTotalHop4
        topicsOnlyHop2, topicsOnlyHop3,topicsOnlyHop4
        conceptsTotalHop1, conceptsTotalHop2, conceptsTotalHop3, conceptsTotalHop4
        conceptsOnlyHop2, conceptsOnlyHop3,conceptsOnlyHop4
        pageRank, connectedComponentId, triangleCnt, inDeg, outDeg)

        //(-1389199041,-1389199041|||http://dbpedia.org/resource/Aircraft|||1|||Aircraft|||0|||0|||0|||0|||0|||0|||0|||0|||0|||0|||0|||0|||0|||0|||0.0|||-2105685942|||0|||0|||0)

    */
    val parts = new scala.collection.mutable.ArrayBuffer[String]()

    try{
      val stripped = line.slice(1, line.length-1)
      val tokenizer = new StringTokenizer(stripped, "|||")
      while (tokenizer.hasMoreElements()){
        parts += tokenizer.nextToken()
      }

      val id = parts(0).split(",")(1).toLong
      val uri = parts(1)
      val nodeType = parts(2).toInt
      val name = parts(3)

      val tth1 = parts(4).toInt
      val tth2 = parts(5).toInt
      val tth3 = parts(6).toInt
      val tth4 = parts(7).toInt

      val toh2 = parts(8).toInt
      val toh3 = parts(9).toInt
      val toh4 = parts(10).toInt

      val cth1 = parts(11).toInt
      val cth2 = parts(12).toInt
      val cth3 = parts(13).toInt
      val cth4 = parts(14).toInt

      val coh2 = parts(15).toInt
      val coh3 = parts(16).toInt
      val coh4 = parts(17).toInt

      val pgRnk = parts(18).toDouble
      val triCnt = parts(19).toInt
      val ccId = parts(20).toLong

      val inDeg = parts(21).toInt
      val outDeg = parts(22).toInt

      val nodeInfo = new NodeInfo(uri, name, nodeType)

      val rn = EnrichedOntologyNode(id, nodeInfo,
        new TraversalInfo(tth1, tth2, tth3, tth4, toh2, toh3, toh4),
        new TraversalInfo(cth1, cth2, cth3, cth4, coh2, coh3, coh4),
        pgRnk, triCnt, ccId, inDeg, outDeg)

      Some(rn)

    }
    catch{
      case ex: Exception =>
        println("can't convert line to ReachbilityNode wit parts " + parts.size + ": " + line)
        None
    }

  }

}

// sets store topics reachable with EXACTLY x hops, totals store sum of reachable topics at depth x
case class EnrichedOntologyNode(id: Long,
                                nodeInfo: NodeInfo,
                                topics: TraversalInfo = new TraversalInfo(),
                                concepts: TraversalInfo = new TraversalInfo(),
                                pageRank: Double = 0.0,
                                triangleCnt: Int = 0,
                                connectedComponentId: Long = 0,
                                inDegree: Int = 0,
                                outDegree: Int = 0
                               ) {

  override def toString() = {
    toFileFormat()
  }

  def toFileFormat() = {
    val delim = "|||"
    val data = Array(id, nodeInfo.uri, nodeInfo.nodeType, nodeInfo.name,
      topics.totalDepth1, topics.totalDepth2, topics.totalDepth3, topics.totalDepth4,
      topics.onlyDepth2, topics.onlyDepth3, topics.onlyDepth4,
      concepts.totalDepth1, concepts.totalDepth2, concepts.totalDepth3, concepts.totalDepth4,
      concepts.onlyDepth2, concepts.onlyDepth3, concepts.onlyDepth4,
      pageRank, connectedComponentId, triangleCnt, inDegree, outDegree)
    data.mkString(delim)
  }

  def toPrettyString() = {
    val base = NodeType.toString(nodeInfo.nodeType) +  " id: " + id + " uri: " + nodeInfo.uri + " name: " + nodeInfo.name

    val details =
      " Int/Out( " + inDegree + ", " + outDegree + ") " +
        " Topics( " + topics.totalDepth1 +
        ", " + topics.totalDepth2 +
        ", " + topics.totalDepth3 +
        ", " + topics.totalDepth4 +
        " onlyH2-H3-h4: " + topics.onlyDepth2 +
        ", " + topics.onlyDepth3 +
        ", " + topics.onlyDepth4 +
        ")  Misc(" +
        " pgRnk: " + pageRank +
        " cc: " + connectedComponentId +
        " triCnt: " + triangleCnt + ")" +
        "  Concepts( " + concepts.totalDepth1 +
        ", " + concepts.totalDepth2 +
        ", " + concepts.totalDepth3 +
        ", " + concepts.totalDepth4 +
        " onlyH2-H3-H4: " + concepts.onlyDepth2 +
        ", " + concepts.onlyDepth3 +
        ", " + concepts.onlyDepth4 +
        ")" +
        " details topics: (" +
        (if (topics.nodeIdsDepth1.size == 0) "-" else topics.nodeIdsDepth1.mkString(",")) + ")  T2: (" +
        (if (topics.nodeIdsDepth2.size == 0) "-" else topics.nodeIdsDepth2.mkString(",")) + ")  T3: (" +
        (if (topics.nodeIdsDepth3.size == 0) "-" else topics.nodeIdsDepth3.mkString(",")) + ")  T4: (" +
        (if (topics.nodeIdsDepth4.size == 0) "-" else topics.nodeIdsDepth4.mkString(",")) + ")  " +
        " details concepts: (" +
        (if (concepts.nodeIdsDepth1.size == 0) "-" else concepts.nodeIdsDepth1.mkString(",")) + ")  T2: (" +
        (if (concepts.nodeIdsDepth2.size == 0) "-" else concepts.nodeIdsDepth2.mkString(",")) + ")  T3: (" +
        (if (concepts.nodeIdsDepth3.size == 0) "-" else concepts.nodeIdsDepth3.mkString(",")) + ")  T4: (" +
        (if (concepts.nodeIdsDepth4.size == 0) "-" else concepts.nodeIdsDepth4.mkString(",")) + ")"


    if (topics.totalDepth1 > 0 || concepts.totalDepth1 > 0){
      base + details
    }
    else {
      ""
    }

    //    base + details

  }

  def copy(id: Long = this.id,
           nodeInfo: NodeInfo = this.nodeInfo,
           topics: TraversalInfo = this.topics,
           concepts: TraversalInfo = this.concepts,
           pageRank: Double = this.pageRank,
           triangleCount: Int = this.triangleCnt,
           connectedComponentId: Long = this.connectedComponentId,
           inDegree: Int = this.inDegree,
           outDegree: Int = this.outDegree

          ) = {
    new EnrichedOntologyNode(
      id, nodeInfo,
      topics, concepts,
      pageRank, triangleCount, connectedComponentId,
      inDegree, outDegree)
  }
}

