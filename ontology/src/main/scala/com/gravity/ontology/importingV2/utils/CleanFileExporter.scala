package com.gravity.ontology.importingV2.utils

import com.gravity.ontology.importingV2.OntologyImporterFileInfo
import java.io.{BufferedWriter, File, FileWriter}

import com.gravity.domain.ontology.EnrichedOntologyNode

/**
  * Created by apatel on 7/19/16.
  */
class CleanFileExporter(fileInfo: OntologyImporterFileInfo) extends EnrichedNodeProcessor {
  val outPath = fileInfo.basePath + "clean.node.txt"
  val file = new File(outPath)
  val bw = new BufferedWriter(new FileWriter(file))

  override def processNode(enrichedOntNode: EnrichedOntologyNode): Unit ={
    // write to csv file
    val ary = Array(enrichedOntNode.id, enrichedOntNode.nodeInfo.uri, enrichedOntNode.nodeInfo.nodeType, enrichedOntNode.nodeInfo.name,
      enrichedOntNode.topics.totalDepth1, enrichedOntNode.topics.totalDepth2,enrichedOntNode.topics.totalDepth3,enrichedOntNode.topics.totalDepth4,
      enrichedOntNode.topics.onlyDepth2,enrichedOntNode.topics.onlyDepth3,enrichedOntNode.topics.onlyDepth4,
      enrichedOntNode.concepts.totalDepth1,enrichedOntNode.concepts.totalDepth2,enrichedOntNode.concepts.totalDepth3,enrichedOntNode.concepts.totalDepth4,
      enrichedOntNode.concepts.onlyDepth2,enrichedOntNode.concepts.onlyDepth3,enrichedOntNode.concepts.onlyDepth4,
      enrichedOntNode.pageRank, enrichedOntNode.triangleCnt, enrichedOntNode.connectedComponentId,
      enrichedOntNode.inDegree, enrichedOntNode.outDegree
    )

    val text = ary.mkString("|||") + "\n"

    bw.write(text)
  }

  override def processComplete(): Unit ={
    bw.close()
    println("output created at: " + outPath)

  }
}
