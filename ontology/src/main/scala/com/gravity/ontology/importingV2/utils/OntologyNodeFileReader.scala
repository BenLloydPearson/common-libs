package com.gravity.ontology.importingV2.utils

import com.gravity.domain.ontology.EnrichedOntologyNode
import com.gravity.ontology.importingV2.OntologyImporterFileInfo

/**
  * Created by apatel on 7/19/16.
  */
class OntologyNodeFileReader(fileInfo: OntologyImporterFileInfo){
  val printFreq = 10000

  def processNodes(nodeProcessor: EnrichedNodeProcessor) {
    println("")

    // read file
    var printCnt = 0
    //    val fileName = "/opt/interests/export/old/vertex.txt"
    val fileName = fileInfo.nodeFile

    for {line <- scala.io.Source.fromFile(fileName).getLines
         reachabilityNode <- EnrichedOntologyNode.fromFileFormat(line)}{

      printCnt +=1

      nodeProcessor.processNode(reachabilityNode)

      if (printCnt % printFreq == 0){
        print(".")
      }

    }

    println("")
    nodeProcessor.processComplete()
    println("completed processing file: " + fileName)
  }

}

