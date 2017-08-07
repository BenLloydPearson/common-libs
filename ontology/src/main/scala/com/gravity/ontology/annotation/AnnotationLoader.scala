package com.gravity.ontology.annotation

import scala.collection.JavaConversions._
import scalaz._
import Scalaz._
import com.gravity.ontology.VirtuosoOntology2
import org.apache.commons.io.FileUtils
import java.io.File
import org.joda.time.DateTime

object AnnotationLoader {
  val ANNOTATIONS_RDF_DIR = "/mnt/habitat_backups/writable/annotation"
  val GRAPH_DB_NAME = "http://insights.gravity.com/annotations"

  private val recursive = true
  def getAnnotationLogs = FileUtils.listFiles(new File(ANNOTATIONS_RDF_DIR), Array("nt"), recursive)

  def loadIntoVirtuoso(startDate: Option[DateTime] = None) {
    val rdfToLoad = getAnnotationLogs filter { file =>
      val fileLastModified = new DateTime(file.lastModified())
      startDate some (_ isBefore fileLastModified) none (true)
    } map (_.getAbsoluteFile)

    VirtuosoOntology2 withNativeConnection { stmt =>
      rdfToLoad foreach { rdfPath =>
        val load = """DB.DBA.TTLP_MT_LOCAL_FILE('%s', '', '%s')""".format(rdfPath, GRAPH_DB_NAME)
        stmt.execute(load)
      }
    }
  }

  def main(args: Array[String]) {
    loadIntoVirtuoso()
  }
}