package com.gravity.utilities.geo

import com.gravity.utilities.grvfunc._

import scala.collection._

/*
 *    __   _         __         
 *   / /  (_)__  ___/ /__  ____
 *  / _ \/ / _ \/ _  / _ \/ _  /
 * /_//_/_/_//_/\_,_/\___/\_, / 
 *                       /___/
 *
 *  These classes allow us to model include/exclusion rules around Geo Targeting.
 *
 */

trait GeoRestriction {

  val ge: GeoEntry

  def isEligibleFor(res: Seq[GeoRestriction]): GeoRestrictionResult

  override def toString: String
}

object GeoRestriction {
  def apply(id: String): Option[GeoRestriction] = id match {
    case v if v.startsWith("-") => GeoDatabase.findById(v.stripPrefix("-")).map(GeoExclusion)
    case v => GeoDatabase.findById(v).map(GeoInclusion)
  }
}

case class GeoRestrictionResult(isEligible: Boolean, input: GeoRestriction, matched: Option[GeoRestriction]) {

  import GeoRestrictionResult._

  def matchType: MatchType = matched match {
    case None => NoMatch
    case Some(m) if m.ge == input.ge => Exact
    case Some(m) if m.ge.isWithin(input.ge) => Child
    case Some(m) if input.ge.isWithin(m.ge) => Parent
    case _ => Other
  }

  def reason: String = {
    "eligible".ifThen(!isEligible)(s => "in-" + s) + s" because '$input' had $matchType match on '${matched.fold("empty")(_.toString)}' restriction"
  }

  override def toString: String = reason
}

object GeoRestrictionResult {

  trait MatchType {
    val name: String

    override def toString: String = name
  }

  object Exact extends MatchType { val name = "exact" }
  object NoMatch extends MatchType { val name = "no" }
  object Parent extends MatchType { val name = "parent" }
  object Child extends MatchType { val name = "child" }
  object Other extends MatchType { val name = "other" }

}

case class GeoInclusion(ge: GeoEntry) extends GeoRestriction  {

  def isEligibleFor(restrictions: Seq[GeoRestriction]): GeoRestrictionResult = {

    if (restrictions.isEmpty) return GeoRestrictionResult(true, this, None)

    val inclusions = restrictions.collect { case v: GeoInclusion => v }
    val exclusions = restrictions.collect { case v: GeoExclusion => v }

    val matched = {
      restrictions.collectFirst{ case i:GeoInclusion if i.ge.isWithin(ge) => i }.map(child => GeoRestrictionResult(isEligible = true, this, Some(child)))
    } orElse {
      ge.zoomOut.foldLeft(None: Option[GeoRestrictionResult])((acc, cur) => acc orElse {
        inclusions.find(_.ge == cur).map(i => GeoRestrictionResult(isEligible = true, this, Some(i))) orElse
        exclusions.find(_.ge == cur).map(i => GeoRestrictionResult(isEligible = false, this, Some(i)))
      })
    }

    matched.getOrElse(GeoRestrictionResult(isEligible = inclusions.isEmpty, this, None))
  }

  override def toString: String = ge.id

}

case class GeoExclusion(ge: GeoEntry) extends GeoRestriction {

  def isEligibleFor(restrictions: Seq[GeoRestriction]): GeoRestrictionResult = {

    if (restrictions.isEmpty) return GeoRestrictionResult(isEligible = true, this, None)

    val inclusions = restrictions.collect { case v: GeoInclusion => v }
    val exclusions = restrictions.collect { case v: GeoExclusion => v }

    val matched = ge.zoomOut.foldLeft(None: Option[GeoRestrictionResult])((acc, cur) => acc orElse {
      inclusions.find(!_.ge.isWithin(cur)).map(i => GeoRestrictionResult(isEligible = true, this, Some(i)))
    })

    matched.getOrElse(GeoRestrictionResult(isEligible = inclusions.isEmpty, this, None))
  }

  override def toString: String = "-" + ge.id

}
