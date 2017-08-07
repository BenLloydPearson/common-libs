package com.gravity.utilities.geo

import java.nio.charset.Charset

import com.csvreader.CsvReader
import com.gravity.utilities.geo.AbstractGeoDatabase.RawGeoEntry
import com.gravity.utilities.geo.GeoEntry._
import com.maxmind.geoip.LookupService

/*
 *    __   _         __         
 *   / /  (_)__  ___/ /__  ____
 *  / _ \/ / _ \/ _  / _ \/ _  /
 * /_//_/_/_//_/\_,_/\___/\_, / 
 *                       /___/
 */
object MaxMindGeoDatabase extends AbstractGeoDatabase {
  import com.gravity.logging.Logging._
  import AbstractGeoDatabase._

  private val geoIpDatabase = "/opt/interests/geo/maxmind_current.dat"

  private lazy val geoDb: LookupService = try {
    new LookupService(geoIpDatabase, LookupService.GEOIP_MEMORY_CACHE)
  } catch {
    case e: Exception => {
      warn(e, "MaxMind GeoIP database is missing! File should be at '" + geoIpDatabase + "'")
      null
    }
  }

  override def findByIpAddress(ipAddress: String): Option[GeoLocation] = {
    try {
      Option(geoDb.getLocation(ipAddress)).map(location =>
        GeoLocation(
          location.countryCode,
          location.countryName,
          location.latitude,
          location.longitude,
          Option(location.region),
          Option(location.city),
          Option(location.postalCode),
          Option(location.dma_code),
          Option(location.area_code),
          Option(location.metro_code)
        )
      )
    } catch {
      case ex: Exception => None
    }
  }

  override def loadDatabase(): Stream[RawGeoEntry] = {
    // geoname_id,locale_code,continent_code,continent_name,country_iso_code,country_name,subdivision_1_iso_code,subdivision_1_name,subdivision_2_iso_code,subdivision_2_name,city_name,metro_code,time_zone
    val csv = new CsvReader(getClass.getResourceAsStream("/com/gravity/utilities/GeoIP2-City-Locations-en.csv"), Charset.forName("UTF-8"))
    csv.setTextQualifier('"')
    csv.setTrimWhitespace(true)
    csv.readHeaders()

    val countryAndRegions = Stream.continually(csv.readRecord()).takeWhile(_ == true).map(_ => {
      val continentName = csv.get("continent_name")
      val continentCode = csv.get("continent_code")
      val countryName = csv.get("country_name")
      val countryCode = csv.get("country_iso_code")
      val regionCode = Option(csv.get("subdivision_1_iso_code")).filter(_.nonEmpty)
      val regionName = Option(csv.get("subdivision_1_name")).filter(_.nonEmpty)

      RawGeoEntry.create(continentCode, continentName, countryCode, countryName, regionCode, regionName)
    })

    //City,Criteria ID,State,DMA Region,DMA Region Code
    val dmaCsv = new CsvReader(getClass.getResourceAsStream("/com/gravity/utilities/AdWords API Cities-DMA Regions 2015-04-03.csv"), Charset.forName("UTF-8"))
    dmaCsv.setTextQualifier('"')
    dmaCsv.setTrimWhitespace(true)
    dmaCsv.readHeaders()

    val dmas = Stream.continually(dmaCsv.readRecord()).takeWhile(_ == true).map(_ => {
      val dmaName = dmaCsv.get("DMA Region")
      val dmaCode = dmaCsv.get("DMA Region Code")
      RawGeoEntry.create("NA", "North America", "US", "United States", dmaCode = Some(dmaCode), dmaName = Some(dmaName))
    })

    countryAndRegions ++ dmas
  }
}
