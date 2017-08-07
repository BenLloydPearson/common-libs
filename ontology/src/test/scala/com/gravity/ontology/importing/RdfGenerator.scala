package com.gravity.ontology.importing

import scala.io.Source
import org.openrdf.rio.{RDFFormat, RDFWriter, Rio}
import java.io.FileOutputStream

import org.openrdf.model.impl.{StatementImpl, ValueFactoryImpl}
import org.openrdf.model.{Resource, Statement, URI, Value}
import com.gravity.ontology.vocab.NS
import org.openrdf.model.vocabulary.RDFS
import com.gravity.ontology.VirtuosoOntology2
import com.gravity.utilities.Settings

/**
 * User: chris
 * Date: 12/10/10
 * Time: 7:48 PM
 */


object RdfGenerator {
  val vf = new ValueFactoryImpl()

  def gimmeWriter(file: String): RDFWriter = {
    return Rio.createWriter(RDFFormat.NTRIPLES, new FileOutputStream(Settings.tmpDir.toString + "/" + file))
  }

  def eachline(resource: String, s: String => Unit) {
    lines(resource).foreach(s(_))
  }

  def lines(resource: String): Iterator[String] = {
    return Source.fromInputStream(getClass().getResourceAsStream(resource)).getLines
  }

  def eachlineSplit(resource: String, delim: String, s: Array[String] => Unit) {
    eachline(resource, line => {
      s(line.split(delim))
    })
  }

  def rdfwriter(file: String, writer: RDFWriter => Unit) {
    val wrt = gimmeWriter(file)
    wrt.startRDF
    writer(wrt)
    wrt.endRDF
  }

  def statement(subject: Resource, predicate: URI, obj: Value): Statement = {
    return new StatementImpl(subject, predicate, obj)
  }

  def bnode = vf.createBNode()

  def literal(itm: String) = vf.createLiteral(itm)

  def convertByUniqueLine(file: String, rdfFile: String, lineHandler: String => List[Statement]) = {
    rdfwriter(rdfFile, writer => {
      lines(file).toSet.foreach((line: String) => lineHandler(line).foreach(writer.handleStatement(_)))
    })
  }

  def convertBySplit(file: String, rdfFile: String, delim: String, lineHandler: Array[String] => List[Statement]) {
    rdfwriter(rdfFile, writer => {
      lines(file).foreach((line: String) => lineHandler(line.split(delim)).foreach(writer.handleStatement(_)))
    })
  }

  def convertByLine(file: String, rdfFile: String, lineHandler: String => Statement) = {
    rdfwriter(rdfFile, writer => {
      eachline(file, line => {
        writer.handleStatement(lineHandler(line))
      })
    })
  }

  def topicFromLabel(name: String): URI = {
    return NS.getTopicURI(NS.labelToUriStem(name))
  }

  def ontoUri(typeName: String): URI = NS.getOntologyUri(typeName)

  def generateDyingScene = {
    val genreSet = scala.collection.mutable.Set.empty[String]
    val delim = "\\|\\|\\|\\|\\|"

    convertBySplit("dyingscene_punklabels.csv", "dyingscene_labels.n3", delim, line => {
      val label = line(0)
      val genre = line(1)
      genreSet += genre
      statement(topicFromLabel(label), RDFS.LABEL, literal(label)) ::
              statement(topicFromLabel(label), ontoUri("genre"), ontoUri(genre)) ::
              Nil
    })

    convertBySplit("dyingschene_bands.csv", "dyingscene_bands.n3", delim, line => {
      val band = line(0)
      val genre = line(1)
      genreSet += genre
      statement(topicFromLabel(band), RDFS.LABEL, literal(band)) :: Nil
    })

    genreSet.foreach(println(_))
  }

  def generateBlacklist = convertByUniqueLine("game_blacklist.csv", "blacklist.n3", line => statement(bnode, NS.BLACKLIST_LOWERCASE, literal(line)) :: Nil)


  def main(args: Array[String]) = {
//    VirtuosoOntology2.alternateLabels((uri,label)=>{
//    })
    generateBlacklist
    generateDyingScene
  }
}