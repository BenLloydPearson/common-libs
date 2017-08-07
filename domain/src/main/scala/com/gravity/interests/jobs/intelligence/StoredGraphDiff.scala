package com.gravity.interests.jobs.intelligence

/**
 * Created by apatel on 1/21/14.
 */

case class StoredGraphDiffResult(commonNodes: Set[Node], leftNodes: Set[Node], rightNodes: Set[Node])

object StoredGraphDiff {

  def compare(sgLeft: StoredGraph, sgRight: StoredGraph): StoredGraphDiffResult = {
    val left = sgLeft.nodes.map(_.id).toSet
    val right = sgRight.nodes.map(_.id).toSet

    val leftOnly = left -- right
    val rightOnly = right -- left
    val common = left.intersect(right)

    val commonNodes = common.map(sgLeft.nodeById)
    val leftNodes = leftOnly.map(sgLeft.nodesById)
    val rightNodes = rightOnly.map(sgRight.nodesById)

    StoredGraphDiffResult(commonNodes, leftNodes, rightNodes)
  }

}
