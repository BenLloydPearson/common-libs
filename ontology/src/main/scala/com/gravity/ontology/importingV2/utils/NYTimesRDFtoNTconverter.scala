package com.gravity.ontology.importingV2.utils

import java.io.{File, FileReader, FileWriter}

//import org.openrdf.model.vocabulary.RDFS
import com.hp.hpl.jena.vocabulary.RDFS
import com.hp.hpl.jena.rdf.model.ModelFactory

///**
//  * Created by apatel on 7/25/16.
//  */
class NYTimesRDFtoNTconverter {
//
  def convert(inPath: String, outPath: String, isPeople: Boolean) = {

//    throw new NotImplementedError("Need proper Jar: com.hp.hpl.jena")
    val f = new File(inPath)
    val fr = new FileReader(f)

    val of = new File(outPath)
    val fw = new FileWriter(of)

    var dbpediaRefCnt = 0
    var lblCnt = 0

    val model = ModelFactory.createDefaultModel()

    model.read(fr, RDFS.getURI())
    val iter = model.listStatements()

    while (iter.hasNext) {
      val st = iter.nextStatement()

      //        println(st)

      val subj = st.getSubject
      val pred = st.getPredicate
      val obj = st.getObject.toString

      //    println("s: " + subj)
      //    println("p: " + pred)
      //    println("o: " + obj)
      //    println("")

      if (pred.getLocalName.contains("prefLabel")) {

        val lbl = obj.replace("@en", "")
        val objFormatted = {

          val lblFormatted = {
            if (isPeople) {
              val parts = lbl.split(',')
              if (parts.size == 2) {
                val newLbl = parts(1).trim + " " + parts(0)
                newLbl
              }
              else{
                lbl
              }
            }
            else {
              lbl
            }
          }

          "\"" + lblFormatted + "\"" + "@en"
        }



        val tripLine = "<" + subj + "> <" + pred + "> " + objFormatted + " ."
        println(tripLine)
        lblCnt +=1
        fw.write(tripLine + "\n")
      }


      if (obj.contains("http://dbpedia.org/resource/")) {
        val tripLine = "<" + subj + "> <" + pred + "> <" + obj + "> ."
        println(tripLine)
        dbpediaRefCnt += 1
        fw.write(tripLine + "\n")
      }

    }

    fw.close()

    println("total lbls: " + lblCnt)
    println("dbpediaRef Obj: " + dbpediaRefCnt)

  }

}
