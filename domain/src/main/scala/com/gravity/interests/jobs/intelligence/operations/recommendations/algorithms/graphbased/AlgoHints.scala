package com.gravity.interests.jobs.intelligence.operations.recommendations.algorithms.graphbased

import com.gravity.service
import com.gravity.service.grvroles
import com.gravity.valueclasses.ValueClassesForDomain.MaxRecoResults

/**
 * Going to deprecate maxCandidateSize, maxDaysOld, and maxResults in favor of AlgoSettings.
 * @param maxCandidateSize
 * @param maxResults
 * @param currentUrl
 * @param cacheCandidatesForMinutes
 * @param keyValues
 */
@SerialVersionUID(1259390244753777643l)
case class AlgoHints(maxCandidateSize: Int = 15000,
                     maxResults: MaxRecoResults = MaxRecoResults(60),
                     currentUrl: Option[String] = None,
                     cacheCandidatesForMinutes: Int = 12,
                     keyValues: Map[String, String] = Map())


object AlgoHints {
  val candidateSize: Int = if(service.grvroles.isInOneOfRoles(grvroles.RECOGENERATION, grvroles.RECOGENERATION2)) 12000 else 4000

  val DefaultHints: AlgoHints = AlgoHints(candidateSize)
  val GraphOverlapHints: AlgoHints = AlgoHints(candidateSize, cacheCandidatesForMinutes = 15)
}
