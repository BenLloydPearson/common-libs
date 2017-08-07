package com.gravity.interests.jobs.intelligence

/**
 * Created with IntelliJ IDEA.
 * User: apatel
 * Date: 11/29/12
 * Time: 2:50 PM
 * To change this template use File | Settings | File Templates.
 */

object GraphQuality {

  def isGoodTfIdfGraph(sg: StoredGraph,
                       requiredMidLevelConcepts: Int = 8,
                       requiredAvgScore: Double = 0.3D,
                       considerTopXScores: Int = 10 ) : Double = {

    val numLevel2Concepts = sg.nodesByLevel.getOrElse(2, Set.empty).size
    val numLevel3Concepts = sg.nodesByLevel.getOrElse(3, Set.empty).size
    val numMidLevelConcepts = numLevel2Concepts + numLevel3Concepts

    val topScores = sg.nodes.sortBy(n=> -n.score).take(considerTopXScores)
    val avgScore = (0.0 /: topScores){_ + _.score} / topScores.size

    computeScoreBasedOnScoreAndMidLevelConcepts(numMidLevelConcepts, requiredMidLevelConcepts, avgScore, requiredAvgScore)
  }

  // Negative score => bad graph, zero or positive score => good graph
  def computeScoreBasedOnScoreAndMidLevelConcepts(midConcepts: Int, requiredMidLevelConcepts: Int,  avgScore: Double, requiredScore: Double) : Double = {
    val deltaNumConcepts = (midConcepts - requiredMidLevelConcepts) / requiredMidLevelConcepts.toDouble
    val deltaScore = (avgScore - requiredScore) / requiredScore

    val normalizer = requiredMidLevelConcepts / requiredScore
    val normalizedDeltaScore = deltaScore * normalizer
    normalizedDeltaScore + deltaNumConcepts

    if ((deltaNumConcepts < 0 && normalizedDeltaScore >= 0) || (deltaNumConcepts >= 0 && normalizedDeltaScore < 0)) math.min(deltaNumConcepts, normalizedDeltaScore)
    else deltaNumConcepts + normalizedDeltaScore
  }
}
