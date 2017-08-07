package com.gravity.ontology.importingV2

/**
  * Created by apatel on 7/19/16.
  */
class OntologyImporterFileInfo(useTestFiles: Boolean){
  val neo4JGraphDir = "/opt/interests/data2/graph.2015.04/"

  val basePath = "/opt/interests/export/"

  val nodeProdFile = basePath + "vertex.txt"
  val edgeProdFile = basePath + "edge.txt"

  val nodeTestFile = basePath + "vertex.test.txt"
  val edgeTestFile = basePath + "edge.test.txt"


  val nodeFile = if (useTestFiles) nodeTestFile else nodeProdFile
  val edgeFile = if (useTestFiles) edgeTestFile else edgeProdFile
}
