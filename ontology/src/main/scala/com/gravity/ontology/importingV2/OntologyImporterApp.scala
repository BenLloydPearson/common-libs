package com.gravity.ontology.importingV2

import com.gravity.domain.ontology.GraphFileInfo
import com.gravity.utilities.grvtime

/**
  * Created by apatel on 7/19/16.
  */
object OntologyImporterApp extends App {

  val timedOpResult = grvtime.timedOperation({
    val fileInfo = new GraphFileInfo(useTestFiles = false)
    val importer = new OntologyImporter(fileInfo, tsNode = 1473379574523L, tsEdge = 1473378890061L)

    importer.doImport()
    importer.showCounters()
  })
  println("Ontology Import time [min]: " + timedOpResult.duration/1000/60)


}
