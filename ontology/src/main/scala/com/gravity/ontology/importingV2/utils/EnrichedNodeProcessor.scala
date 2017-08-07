package com.gravity.ontology.importingV2.utils

import com.gravity.domain.ontology.EnrichedOntologyNode

/**
  * Created by apatel on 7/19/16.
  */
trait EnrichedNodeProcessor{
  def processNode(enrichedOntNode: EnrichedOntologyNode)
  def processComplete()
}
