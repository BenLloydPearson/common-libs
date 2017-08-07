package com.gravity.interests.jobs.intelligence.algorithms.graphing

import scala.io.Source
import scala.collection._
import scala.Some
import scala.collection.JavaConversions._
import scala.Predef._

import scala.io.Source
import scala.collection.mutable
import sun.reflect.generics.reflectiveObjects.NotImplementedException

/**
 * Created with IntelliJ IDEA.
 * User: apatel
 * Date: 1/15/13
 * Time: 6:51 PM
 * To change this template use File | Settings | File Templates.
 */

// Entity Hierarchy
//  AdTerm
//    ParentAdTerm
//    ChildAdTerm
//
//  AdConcept
//    HighAdConcept
//    LowAdConcept
//
//  An AdTerm (parent or child) typically points to a High AdConcept and this in-turn points to a set of Low AdConcepts


case class AdTermEntry(webAdTerm: String, uri: String, id: Int, parentId: Option[Int])

object AdTermTaxonomy {


  val adTermMap = Source.fromInputStream(getClass.getResourceAsStream("openx_adword_taxonomy.txt")).getLines().map(adTermEntryFromLine).toList.flatMap(n => n).map(e => (e.id, e)).toMap

  val parentAdTermMap = adTermMap.values.filter(e => e.parentId.isEmpty).map(e => (e.webAdTerm, e)).toMap
  val childAdTermMap = adTermMap.values.filter(e => !e.parentId.isEmpty).map(e => (e.webAdTerm, e)).toMap

  val higherToLowerConceptMap = Source.fromInputStream(getClass.getResourceAsStream("openx_higher_concept_to_lower_concept_mapping.txt")).getLines().map(conceptMapEntryFromLine).flatMap(n => n).toList.toMap
  val lowerToHigherConceptMap = Source.fromInputStream(getClass.getResourceAsStream("openx_lower_concept_to_higher_concept_mapping.txt")).getLines().map(conceptMapEntryFromLine).flatMap(n => n).toList.toMap

  val adTermIdToAdTerm = adTermMap.values.map(e => (e.id, e.webAdTerm)).toMap

  val parentTermToChildTermMap = {
    val map = new mutable.HashMap[String, mutable.HashSet[String]]()
    adTermMap.values.foreach(e => {
      val id = e.id
      try {
        e.parentId match {
          case Some(parentId) =>
            val term = adTermIdToAdTerm(id)
            val parentTerm = adTermIdToAdTerm(parentId)

            val childSet = map.getOrElse(parentTerm, new mutable.HashSet[String]())
            childSet.add(term)
            map(parentTerm) = childSet
          case None =>
        }
      }
      catch {
        case ex: Exception =>
          println("Error while processing id: " + id + " parent id: " + e.parentId)
      }
    })
    map
  }


  private def adTermEntryFromLine(line: String): Option[AdTermEntry] = {
    val SPLIT = "\t"
    val parts = line.split(SPLIT)

    try {
      if (parts.size == 5) {
        val id = parts(0).toInt
        val term = parts(1)
        val parentId = if (parts(2) == "none") None else Some(parts(2).toInt)
        val uri = parts(3)
        val method = parts(4)

        //        if (id == 22) {
        //          println("**22**: " + id + SPLIT + term + SPLIT + parentId + SPLIT + uri + SPLIT + method)
        //
        //        }

        Some(new AdTermEntry(term, uri, id, parentId))
      }
      else {
        //println("cannot process: "  + line + " from output file  (wrong number of columns)")
        None
      }
    }
    catch {
      case _: Exception =>
        //println("cannot process: "  + line + " from output file (parse error)")
        None
    }

  }

  private def conceptMapEntryFromLine(line: String): Option[(String, collection.Set[String])] = {
    val SPLIT = "\t"
    val parts = line.split(SPLIT)
    try {
      if (parts.size == 2) {
        val concept = parts(0)
        val relatedConcepts = parts(1).split(",").filter(c => c.size > 0).toSet
        Some((concept, relatedConcepts))
      }
      else {
        None
      }
    }
    catch {
      case _: Exception =>
        None
    }
  }

  def getAdTerm(adTermId: Int): Option[AdTerm] = {

    adTermMap.get(adTermId) match {
      case Some(entry) =>
        val adTerm = if (entry.parentId.isDefined) new ChildAdTerm(entry) else new ParentAdTerm(entry)
        Some(adTerm)

      //        val dummy =  new {} with AdTerm {
      //          def adTermEntry = entry
      //        }
      //        Some(dummy)

      case None => None
    }
  }


  def getParentAdTerm(adTermId: Int): Option[ParentAdTerm] = {
    getAdTerm(adTermId) match {
      case Some(parentAdTerm: ParentAdTerm) =>
        Some(parentAdTerm)
      case _ =>
        None
    }
  }

  def getChildAdTerm(adTermId: Int): Option[ChildAdTerm] = {
    //getTypedAdTerm[ChildAdTerm](adTermId)

    getAdTerm(adTermId) match {
      case Some(childAdTerm: ChildAdTerm) =>
        Some(childAdTerm)
      case _ =>
        None
    }
  }

  //  private def getTypedAdTerm[TT](adTermId: Int) : Option[TT] = {
  //    getAdTerm(adTermId) match {
  //      case Some(adTerm) =>
  //
  //        if (adTerm.isInstanceOf[TT]) {
  //          Some(adTerm.asInstanceOf[TT])
  ////          adTerm match {
  ////            case t:T => Some(t)
  ////            case _ => None
  ////          }
  //          //Some((adTerm:T))
  //        }
  //        else {
  //          None
  //        }
  //      case _ =>
  //        None
  //    }
  //  }

  def getChildAdTerm(adTerm: String): Option[ChildAdTerm] = {
    childAdTermMap.get(adTerm) match {
      case Some(adTermEntry) => Some(new ChildAdTerm(adTermEntry))
      case None => None
    }
  }

  def getParentAdTerm(adTerm: String): Option[ParentAdTerm] = {
    parentAdTermMap.get(adTerm) match {
      case Some(adTermEntry) => Some(new ParentAdTerm(adTermEntry))
      case None => None
    }

  }

  def getRelatedConcepts(adTermId: Int, includeSubTerms: Boolean = false, includeLowConcepts: Boolean = true): Seq[String] = {


    val adTerms = new mutable.HashSet[AdTerm]()
    val concepts = new mutable.HashSet[AdConcept]()

    // handle parent
    for (parentAdTerm <- AdTermTaxonomy.getParentAdTerm(adTermId)) {
      adTerms += parentAdTerm

      for (childAdTerms <- parentAdTerm.getChildren if includeSubTerms) {
        adTerms ++= childAdTerms
      }
    }

    // handle child
    for (childAdTerm <- AdTermTaxonomy.getChildAdTerm(adTermId)) {
      adTerms += childAdTerm
    }

    // get related high and low concepts
    adTerms.foreach(adTerm => {
      val highConcept = adTerm.getHighAdConcept
      concepts.add(highConcept)
      if (includeLowConcepts) {
        for (lowConcepts <- highConcept.getLowerConcepts) {
          concepts ++= lowConcepts
        }
      }
    })

    concepts.filter(c => c.uri != "none").map(c => c.uri).toSeq
  }
}

trait AdTerm {
  def adTermEntry: AdTermEntry

  lazy val adTerm = adTermEntry.webAdTerm
  lazy val relatedUri = adTermEntry.uri

  def getHighAdConcept: HighAdConcept = {
    new HighAdConcept(relatedUri)
  }

  override def toString: String = {
    "AdTerm (" + adTerm + ", id: " + adTermEntry.id + ", parentId: " + adTermEntry.parentId + ", uri: " + relatedUri + ")"
  }
}

class ParentAdTerm(entry: AdTermEntry) extends AdTerm {

  def adTermEntry = entry

  def getChildren: Option[Seq[ChildAdTerm]] = {
    val childTerms = AdTermTaxonomy.parentTermToChildTermMap(adTerm)
    if (childTerms.size > 0) {
      Some(childTerms.map(childTerm => AdTermTaxonomy.getChildAdTerm(childTerm)).flatMap(a => a).toSeq)
    }
    else {
      None
    }
  }
}

class ChildAdTerm(entry: AdTermEntry) extends AdTerm {

  def adTermEntry = entry

  def getParent: Option[ParentAdTerm] = {
    for (parentId <- adTermEntry.parentId;
         entry <- AdTermTaxonomy.getParentAdTerm(parentId)) {
      return Some(entry)
    }
    None
  }
}


trait AdConcept {
  def uri: String

  override def toString: String = {
    "AdConcept(" + uri + ")"
  }
}

class HighAdConcept(conceptUri: String) extends AdConcept {
  def uri = conceptUri

  def getLowerConcepts: Option[Seq[LowAdConcept]] = {
    for (lowerConcepts <- AdTermTaxonomy.higherToLowerConceptMap.get(uri)) {
      return Some(lowerConcepts.map(lowerConceptUri => new LowAdConcept(lowerConceptUri)).toSeq)
    }
    None
  }
}

class LowAdConcept(conceptUri: String) extends AdConcept {
  def uri = conceptUri

  def getHigherConcepts: Option[Seq[HighAdConcept]] = {

    for (higherConcepts <- AdTermTaxonomy.lowerToHigherConceptMap.get(uri)) {
      return Some(higherConcepts.map(higherConceptUri => new HighAdConcept(higherConceptUri)).toSeq)
    }
    None
  }
}
