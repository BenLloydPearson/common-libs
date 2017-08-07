package com.gravity.ontology.importingV2

import com.gravity.domain.ontology.EdgeType
import com.gravity.ontology.nodes.TopicRelationshipTypes

/**
  * Created by apatel on 7/19/16.
  */
object RelationshiptTypeConverter{
  def getRelationshipType(edgeType: Int) = {

    edgeType match {
      case EdgeType.SubjectOf => Some(TopicRelationshipTypes.CONCEPT_OF_TOPIC)
      case EdgeType.BroaderThan => Some(TopicRelationshipTypes.BROADER_CONCEPT)
      case EdgeType.SameAs => Some(TopicRelationshipTypes.REDIRECTS_TO)
      case _ => None
    }
  }

}

