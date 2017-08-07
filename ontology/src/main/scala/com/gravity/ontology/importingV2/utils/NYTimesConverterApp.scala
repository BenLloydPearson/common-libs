package com.gravity.ontology.importingV2.utils

/**
  * Created by apatel on 7/25/16.
  */
object NYTimesConverterApp extends App {
  val basepath = "/Users/akash/Documents/ontology/nytimes/"

  val inExt = ".rdf"
  val outExt = ".nt"

  val files = Seq("people", "organizations", "descriptors", "locations")

  val converter = new NYTimesRDFtoNTconverter()



  for (file <- files){
    val inPath = basepath + file + inExt
    val outPath = basepath + file + outExt

    converter.convert(inPath, outPath, (file == "people"))

    println("generated: " + outPath)
  }

}
