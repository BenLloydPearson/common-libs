package com.gravity.ontology.importingV2.utils

import com.gravity.ontology.importingV2.OntologyImporterFileInfo

/**
  * Created by apatel on 7/19/16.
  */
object OntologyCleanFileExporterApp extends App{
  val fileInfo = new OntologyImporterFileInfo(useTestFiles = false)
  val nodeFileExporter = new CleanFileExporter(fileInfo)

  val reader = new OntologyNodeFileReader(fileInfo)
  reader.processNodes(nodeFileExporter)

}
