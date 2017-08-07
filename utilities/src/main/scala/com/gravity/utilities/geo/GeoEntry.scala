package com.gravity.utilities.geo

import java.nio.charset.Charset

import com.csvreader.CsvReader
import com.gravity.utilities.Predicates._
import com.gravity.utilities._
import com.gravity.utilities.grvfunc._
import play.api.data.validation.ValidationError
import play.api.libs.functional.syntax._
import play.api.libs.json.Reads._
import play.api.libs.json.Writes._
import play.api.libs.json._
import com.gravity.utilities.grvjson._

import scala.annotation.tailrec
import scala.collection._


/*
 *    __   _         __         
 *   / /  (_)__  ___/ /__  ____
 *  / _ \/ / _ \/ _  / _ \/ _  /
 * /_//_/_/_//_/\_,_/\___/\_, / 
 *                       /___/
 */
@SerialVersionUID(0l)
object GeoEntryType extends grvenum.GrvEnum[Int] {

  case class Type(i: Int, n: String) extends ValueTypeBase(i, n)

  override def mkValue(id: Int, name: String): GeoEntryType.Type = Type(id, name)

  override def defaultValue: GeoEntryType.Type = Unknown

  val Unknown: Type = Value(10, "Unknown")
  val Continent: Type = Value(20, "Continent")
  val Country: Type = Value(30, "Country")
  val DMA: Type = Value(40, "DMA")
  val Region: Type = Value(50, "Region")
  val Subdivision: Type = Value(60, "Subdivision")
  val City: Type = Value(70, "City")
  val Postal: Type = Value(80, "Postal")

  val JsonSerializer: EnumNameSerializer[GeoEntryType.type] = new EnumNameSerializer(this)
  implicit val jsonFormat: Format[Type] = makeJsonFormat[Type]

}


sealed trait GeoEntry {
  def id: String
  def code: String
  def name: String
  def `type`: GeoEntryType.Type
  def parent: Option[GeoEntry]

  def parentId: Option[String] = parent.map(_.id)

  def canonicalName: String = zoomIn.ifThen(_.size > 1)(_.filter(_.`type` != GeoEntryType.Continent)).map(_.name).mkString(", ")

  def isWithin(other: GeoEntry): Boolean = zoomOut.contains(other)

  override def toString: String = `type`.name + ": " + name + " [" + id + "]"

  def zoomOut: Seq[GeoEntry] = {
    def go(entry: GeoEntry): Seq[GeoEntry] = entry.parent match {
      case Some(parent) => entry +: go(parent)
      case None => Seq(entry)
    }

    go(this)
  }

  def zoomIn: Seq[GeoEntry] = {
    @tailrec
    def go(entry: GeoEntry, cur: Seq[GeoEntry] = Seq.empty): Seq[GeoEntry] = entry.parent match {
      case Some(parent) => go(parent, entry +: cur)
      case None => entry +: cur
    }

    go(this)
  }
}


case class GeoContinent(id: String, code: String, name: String, parent: Option[GeoEntry] = None) extends GeoEntry {
  override val `type`: GeoEntryType.Type = GeoEntryType.Continent
}

case class GeoCountry(id: String, code: String, name: String, parent: Option[GeoEntry] = None) extends GeoEntry {
  override val `type`: GeoEntryType.Type = GeoEntryType.Country
}

case class GeoRegion(id: String, code: String, name: String, parent: Option[GeoEntry] = None) extends GeoEntry {
  override val `type`: GeoEntryType.Type = GeoEntryType.Region
}

case class GeoDMA(id: String, code: String, name: String, parent: Option[GeoEntry] = None) extends GeoEntry {
  override val `type`: GeoEntryType.Type = GeoEntryType.DMA
}


object GeoEntry {
 import com.gravity.logging.Logging._

  // order by GeoEntryType order then entry's name
  implicit def GeoEntryOrdering(implicit ord: Ordering[(Int, String)]): Ordering[GeoEntry] = new Ordering[GeoEntry] {
    override def compare(x: GeoEntry, y: GeoEntry): Int = {
      ord.compare((x.`type`.id, x.name), (y.`type`.id, y.name))
    }
  }

  def get(id: String): Option[GeoEntry] = GeoDatabase.findById(id)

  implicit val jsonRead: Reads[GeoEntry] = (
    (JsPath \ "type").read[String] and
      (JsPath \ "id").read[String] and
      (JsPath \ "name").readNullable[String] and
      (JsPath \ "parentId").readNullable[String]
    )((tpe, id, name, parentId) => {
    GeoEntryType.get(tpe).fold(throw new JsResultException(Seq(JsPath \ "type" -> Seq(ValidationError(s"Geo entry type not recognized: $tpe"))))) {
      case GeoEntryType.Continent | GeoEntryType.Country | GeoEntryType.DMA | GeoEntryType.Region => GeoDatabase.findById(id)
      case GeoEntryType.Subdivision => None // not yet ingesting
      case GeoEntryType.City => None // not yet ingesting
      case GeoEntryType.Postal => None // not yet ingesting
    }.getOrElse(throw new JsResultException(Seq(JsPath \ "id" -> Seq(ValidationError(s"Geo entry not found: $id, type: $tpe")))))
  })

  implicit val jsonWrite: Writes[GeoEntry] = (
    (__ \ "type").write[String] and
      (__ \ "id").write[String] and
      (__ \ "name").write[String] and
      (__ \ "code").write[String] and
      (__ \ "canonicalName").write[String] and
      (__ \ "parentId").writeNullable[String]
    )((geo: GeoEntry) => (geo.`type`.name, geo.id, geo.name, geo.code, geo.canonicalName, geo.parentId))




}
