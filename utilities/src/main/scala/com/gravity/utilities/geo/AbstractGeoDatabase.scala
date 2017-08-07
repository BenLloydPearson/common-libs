package com.gravity.utilities.geo

import com.gravity.utilities.geo.AbstractGeoDatabase.RawGeoEntry
import com.gravity.utilities.geo.GeoEntry._
import com.gravity.utilities.grvfunc._
import com.gravity.utilities.Predicates._

import scala.collection._
import scalaz._
import Scalaz._

/*
 *    __   _         __         
 *   / /  (_)__  ___/ /__  ____
 *  / _ \/ / _ \/ _  / _ \/ _  /
 * /_//_/_/_//_/\_,_/\___/\_, / 
 *                       /___/
 */


/**
  * Partially implemented GeoDatabase that includes logic to build the hierarchical representation of GeoEntry
  * data from a flat, raw database file.
  *
  * Two methods need to be implemented:
  *
  *   loadDatabase(): Returns a stream of the raw data used to build the database
  *
  *   findByIpAddress(): Resolve a GeoLocation from an ipAddress.
  *                      Implementation should be ultra-performant as this is called per-request!
  */
trait AbstractGeoDatabase extends GeoDatabase {
  import com.gravity.logging.Logging._

  protected def loadDatabase(): Stream[RawGeoEntry]

  def findByIpAddress(ipAddress: String): Option[GeoLocation]

  protected lazy val continentIds = Map(
    "Asia" -> "Asia",
    "Africa" -> "Africa",
    "Europe" -> "Europe",
    "North America" -> "NorthAmerica",
    "South America" -> "SouthAmerica",
    "Oceania" -> "AustraliaAndOceana",
    "Antarctica" -> "Antarctica"
  )

  protected lazy val countryCodeMap = Map(
    "UK" -> "GB" // Map United Kingdom to Great Britain
  )

  /**
    *  ID -> GeoEntry where id is like "US" or "US-CA" or "US-DMA-903", etc
    *  GeoEntry's are created in hierarchical form such that the 'parent' points to the encompassing
    *  GeoEntry, ie:
    *
    *  NorthAmerica (parent = None)
    *  US (parent = NorthAmerica)
    *  US-CA (parent = US)
    *  Los Angeles (parent = US-CA)
    *  San Diego (parent = US-CA)
    *  US-DMA-903 (parent = US-CA)
    */
  protected lazy val geoEntries: Map[String, GeoEntry] = {

    val start = System.currentTimeMillis()

    val continents = new mutable.HashMap[String, GeoContinent]
    val countries = new mutable.HashMap[String, GeoCountry]
    val regions = new mutable.HashMap[String, GeoRegion]
    val dmas = new mutable.HashMap[String, GeoDMA]

    val entries = loadDatabase()

    entries.foreach(g => {
      val cc = countryCodeMap.getOrElse(g.countryCode, g.countryCode)

      val continent = GeoContinent(continentIds(g.continentName), g.continentCode, g.continentName.toLowerCase())
      val country = GeoCountry(cc, cc, g.countryName.toLowerCase(), Some(continent))

      if (g.continentName.nonEmpty)
        continents += continentIds(g.continentName) -> continent

      if (g.countryName.nonEmpty && cc.nonEmpty)
        countries += cc -> country

      (g.regionName |@| g.regionCode) {
        (regionName, regionCode) =>
          regions += country.id + "-" + regionCode -> GeoRegion(country.id + "-" + regionCode, regionCode, regionName.toLowerCase, Some(country))
      }

      (g.dmaName |@| g.dmaCode) {
        (dmaName, dmaCode) =>
          dmas += "US-DMA-" + dmaCode -> GeoDMA("US-DMA-" + dmaCode, dmaCode, dmaName.toLowerCase, Some(country))
      }
    })


    val allIds = (countries.values.map(_.id) ++ regions.values.map(_.id) ++ dmas.values.map(_.id)).toVector

    if (allIds.toSet.size != allIds.size) throw new IllegalStateException("Duplicate IDs found!: " + allIds.toSet.toVector.diff(allIds).mkString(","))

    info("Loaded " + allIds.size + " geo entries in " + (System.currentTimeMillis() - start) + "ms")

    (continents ++ countries ++ regions ++ dmas).map{ case (k, v) => k.toLowerCase() -> v }.toMap
  }

  // Geo Data keyed by parentId
  private val parentIdIndex = geoEntries.values.groupBy(_.parentId).mapValues(_.toSet)


  // Method that allows us to query out Geo data by various fields
  // Each field can be queried for multiple values that will be matched using 'OR'
  // Queries across multiple fields will be matched using 'AND'
  // for example:
  //    query(idQuery = Seq("A","B"), typeQuery = Seq("T1","T2"))
  // would be equivalent to this SQL clause:
  //    WHERE (toLowerCase(id) LIKE '%a%' OR toLowerCase(id) LIKE '%b%') AND
  //          (toLowerCase(type) LIKE '%t1%' OR toLowerCase(type) LIKE '%t2%')
  def query(idQuery: Seq[String] = Seq.empty, typeQuery: Seq[String] = Seq.empty, codeQuery: Seq[String] = Seq.empty, nameQuery: Seq[String] = Seq.empty, canonicalNameQuery: Seq[String] = Seq.empty, parentIdQuery: Seq[String] = Seq.empty): Set[GeoEntry] = {

    def filter(query: Seq[String], f: GeoEntry => String): GeoEntry => Boolean = {
      entry => {
        // loop each "query" and if any of them match our value then accept the entry
        val queries = query.filterNot(v => v == "\"\"" || v.isEmpty)
        queries.foldLeft(queries.isEmpty /* empty defaults to pass */)((acc, cur) => {
          if (cur.startsWith("\"") && cur.endsWith("\"")) {
            // exact match
            acc || f(entry).toLowerCase.replaceAll("[^0-9a-z]", "") == cur.stripPrefix("\"").stripSuffix("\"").toLowerCase.replaceAll("[^0-9a-z]", "")
          } else {
            // substring match
            acc || f(entry).toLowerCase.replaceAll("[^0-9a-z]", "").contains(cur.toLowerCase.replaceAll("[^0-9a-z]", ""))
          }
        })
      }
    }

    val allFilters = filter(idQuery, _.id) and
                     filter(typeQuery, _.`type`.name) and
                     filter(codeQuery, _.code) and
                     filter(nameQuery, _.name) and
                     filter(canonicalNameQuery, _.canonicalName) and
                     filter(parentIdQuery, _.parentId.getOrElse(""))

    geoEntries.values.filter(allFilters).toSet
  }

  def findById(id: String): Option[GeoEntry] = geoEntries.get(id.toLowerCase)

  // find all GeoEntry's that are children of a parentId with an optionally recursive search
  def findByParentId(parentId: Option[String], recursive: Boolean = false): Set[GeoEntry] = {
    parentIdIndex.getOrElse(parentId, Set.empty[GeoEntry]).ifThen(recursive)(entries => entries ++ entries.flatMap(e => findByParentId(e.parentId)))
  }

  override def finalize() {
    super.finalize()
    try {
      close()
    } catch {
      case e: java.lang.Exception => e.printStackTrace()
    }
  }
}

object AbstractGeoDatabase {

  /**
    * Used strictly for parsing.. we use this to later build the relationships
    * and convert to a 'GeoEntry'
    */
  protected[geo] case class RawGeoEntry private (
    continentCode: String,
    continentName: String,
    countryCode: String,
    countryName: String,
    regionCode: Option[String] = None,
    regionName: Option[String] = None,
    dmaCode: Option[String] = None,
    dmaName: Option[String] = None
  )

  protected[geo] object RawGeoEntry {
    def create(
      continentCode: String,
      continentName: String,
      countryCode: String,
      countryName: String,
      regionCode: Option[String] = None,
      regionName: Option[String] = None,
      dmaCode: Option[String] = None,
      dmaName: Option[String] = None
    ): RawGeoEntry = {
      RawGeoEntry(
        continentCode,
        continentName,
        countryCode,
        countryName,
        regionCode,
        regionName,
        dmaCode,
        dmaName
      )
    }
  }

}
