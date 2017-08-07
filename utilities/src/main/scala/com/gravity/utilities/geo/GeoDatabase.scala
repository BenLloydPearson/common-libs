package com.gravity.utilities.geo

import com.gravity.utilities._

import scala.collection._
import scala.util._

/*
 *    __   _         __         
 *   / /  (_)__  ___/ /__  ____
 *  / _ \/ / _ \/ _  / _ \/ _  /
 * /_//_/_/_//_/\_,_/\___/\_, / 
 *                       /___/
 */
trait GeoDatabase {

  def findByIpAddress(ipAddress: String): Option[GeoLocation]

  def findById(id: String): Option[GeoEntry]

  def findByParentId(parentId: Option[String], recursive: Boolean = false): Set[GeoEntry]

  // Convenience methods to hide the ID format from clients
  def findByCountryCode(cc: String): Option[GeoEntry] = findById(cc)
  def findByStateCode(state: String): Option[GeoEntry] = findById("US-" + state.toUpperCase)
  def findByDMACode(dma: Int): Option[GeoEntry] = findById("US-DMA-" + dma)

  def query(idQuery: Seq[String] = Seq.empty, typeQuery: Seq[String] = Seq.empty, codeQuery: Seq[String] = Seq.empty, nameQuery: Seq[String] = Seq.empty, canonicalNameQuery: Seq[String] = Seq.empty, parentIdQuery: Seq[String] = Seq.empty): Set[GeoEntry]

	def close(): Unit = {}

}



object GeoDatabase extends GeoDatabase {
 import com.gravity.logging.Logging._
  import com.gravity.utilities.Counters._

	val counterCategory: String = "Geo IP Location"

	private lazy val implementation: GeoDatabase = Settings2.getPropertyOrDefault("geo.database.provider", "netacuity").toLowerCase match {
	  case "maxmind" => MaxMindGeoDatabase
		case "netacuity" => NetAcuityGeoDatabase
		case other => throw new RuntimeException(s"Invalid geo.database.provider: $other")
  }

  override def findByIpAddress(ipAddress: String): Option[GeoLocation] = {
    val result = Try(GeoLocation.customGeoLocationLookup.get(ipAddress) orElse implementation.findByIpAddress(ipAddress)) match {
      case Success(r) => r
      case Failure(ex) => warn(ex, s"Geo Lookup failed for ip: $ipAddress"); None
    }

    result match {
      case Some(us) if us.isUS => countPerSecond(counterCategory, "Identified US Location")
      case Some(_) => countPerSecond(counterCategory, "Identified Non-US Location")
      case None => countPerSecond(counterCategory, "Unidentified IP Address")
    }

    result
  }

  override def findById(id: String): Option[GeoEntry] = implementation.findById(id)

  override def findByParentId(parentId: Option[String], recursive: Boolean): Set[GeoEntry] = implementation.findByParentId(parentId, recursive)

  override def query(idQuery: Seq[String], typeQuery: Seq[String], codeQuery: Seq[String], nameQuery: Seq[String], canonicalNameQuery: Seq[String], parentIdQuery: Seq[String]): Set[GeoEntry] = implementation.query(idQuery, typeQuery, codeQuery, nameQuery, canonicalNameQuery, parentIdQuery)

}