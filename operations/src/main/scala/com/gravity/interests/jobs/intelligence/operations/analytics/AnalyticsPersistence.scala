package com.gravity.interests.jobs.intelligence.operations.analytics

import com.gravity.utilities.components.FailureResult
import scalaz.{Failure, Success, Validation}
import com.gravity.interests.graphs.graphing.GrapherOntology
import com.gravity.ontology.OntologyGraph2
import com.gravity.utilities.analytics.TimeSliceResolutions
import com.gravity.interests.jobs.intelligence.operations.SiteTopicService
import com.gravity.interests.jobs.intelligence.{SiteKey, InterestCounts}

trait AnalyticsPersistence {
  def getTopicNames(uris: Traversable[String], useOntologyName: Boolean): Map[String, String] = {
    if (uris.isEmpty) {
      Map.empty
    }
    else if (useOntologyName) {
      GrapherOntology.instance.getTopicNames(uris = uris)
    }
    else {
      uris.map(uri => (uri, OntologyGraph2.getTopicNameWithoutOntology(uri))).toMap
    }
  }

  def fillTopicNames(interests: Traversable[InterestCounts], useOntologyName: Boolean) {
    val withoutName = interests.flatMap((interest: InterestCounts) => {
      if (interest.name.isEmpty) Some(interest) else None
    })

    if (!withoutName.isEmpty) {
      val names = getTopicNames(withoutName.map(_.uri), useOntologyName)
      for (interest <- withoutName) {
        interest.name = names.getOrElse(interest.uri, OntologyGraph2.getTopicNameWithoutOntology(interest.uri))
      }
    }
  }

  /**
   * Returns metadata about all the sites.  Should be cached by the implementing persistence mechanism because the sites will not grow very large and should seldom change.
   */


}
