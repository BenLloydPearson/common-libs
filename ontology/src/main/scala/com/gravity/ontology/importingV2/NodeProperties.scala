package com.gravity.ontology.importingV2

/**
  * Created by apatel on 7/19/16.
  */
object NodeProperties{
  val NAME_PROPERTY = "Name"
  val URI_PROPERTY = "Uri"

  val InEdges = "GRAV_InEdges"
  val OutEdges = "GRAV_OutEdges"

  val TopicPrefix = "GRAV_TopicClosenessScore_Level"
  val TopicL1 = TopicPrefix + "1"
  val TopicL2 = TopicPrefix + "2"
  val TopicL3 = TopicPrefix + "3"
  val TopicL4 = TopicPrefix + "4"

  //  val TopicL2Exact = TopicL2 + "_Exact"
  //  val TopicL3Exact = TopicL3 + "_Exact"
  //  val TopicL4Exact = TopicL4 + "_Exact"

  val TopicL2Sum = TopicL2 + "_Sum"
  val TopicL3Sum = TopicL3 + "_Sum"
  val TopicL4Sum = TopicL4 + "_Sum"


  val ConceptPrefix = "GRAV_ConceptParentScore_Level"
  val ConceptL1 = ConceptPrefix + "1"
  val ConceptL2 = ConceptPrefix + "2"
  val ConceptL3 = ConceptPrefix + "3"
  val ConceptL4 = ConceptPrefix + "4"

  val ConceptL2Sum = ConceptL2 + "_Sum"
  val ConceptL3Sum = ConceptL3 + "_Sum"
  val ConceptL4Sum = ConceptL4 + "_Sum"

  val PageRank = "GRV_PageRank"
  val TriangleCount = "GRV_TriangleCount"
  val ConnectedComponenetId = "GRV_CC_ID"



}
