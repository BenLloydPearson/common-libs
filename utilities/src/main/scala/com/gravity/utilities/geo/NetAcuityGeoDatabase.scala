package com.gravity.utilities.geo

import java.io.{InputStream, File}
import java.nio.charset.Charset

import com.gravity.utilities.grvfunc._
import com.csvreader.CsvReader
import net.digitalenvoy.embedded.EmbeddedAccessor
import com.gravity.utilities.grvstrings._

import scala.collection._

/*
 *    __   _         __         
 *   / /  (_)__  ___/ /__  ____
 *  / _ \/ / _ \/ _  / _ \/ _  /
 * /_//_/_/_//_/\_,_/\___/\_, / 
 *                       /___/
 */
object NetAcuityGeoDatabase extends AbstractGeoDatabase {

	import AbstractGeoDatabase._

	private lazy val featureCode = 4
	private lazy val geoIpDatabase = "/opt/interests/geo/netacuity_db"
	private lazy val geoIpDatabaseRaw = "/com/gravity/utilities/GeoIP2-City-Locations-en.csv"

	private lazy val continentLookup   = loadDecodeFile("/com/gravity/utilities/country_codes.csv", "continent-code", "continent-name").toMap
	private lazy val countryLookup     = loadDecodeFile("/com/gravity/utilities/country_codes.csv", "ISO-2", "Country-name").toMap
	private lazy val regionLookup      = loadDecodeFile("/com/gravity/utilities/region_codes.csv", "REGION-CODE", "REGION-DESC").toMap

	protected lazy val continentCodeMap = Map(
    "AS" -> "Asia",
    "AF" -> "Africa",
    "EU" -> "Europe",
    "NA" -> "North America",
    "SA" -> "South America",
    "AU" -> "Oceania",
    "AN" -> "Antarctica"
	)

	private lazy val db = EmbeddedAccessor.getEmbeddedAccessor(featureCode, new File(geoIpDatabase), true)

	override protected def loadDatabase(): Stream[RawGeoEntry] = {
		val csv = new CsvReader(geoIpDatabase + "/na_04_01.db", ';', Charset.forName("UTF-8"))
	  csv.setTextQualifier('"')
		csv.setDelimiter(';')
		csv.setHeaders("edge-country,edge-region,edge-city,edge-conn-speed,edge-metro-code,edge-latitude,edge-longitude,edge-postal-code,edge-country-code,edge-region-code,edge-city-code,edge-continent-code,edge-two-letter-country,edge-internal-code,edge-area-codes,edge-country-conf,edge-region-conf,edge-city-conf,edge-postal-conf,edge-gmt-offset,edge-in-dst".split(","))

	  val countryAndRegions = Stream.continually(csv.readRecord()).takeWhile(_ == true)
		  .filter(_ => csv.getColumnCount == 22)
		  .filter(_ => csv.get("edge-two-letter-country").length == 2 && csv.get("edge-two-letter-country") != "**")
		  .map(_ => {
			  val countryCode = csv.get("edge-two-letter-country").toUpperCase

			  val countryName = countryLookup(countryCode)
			  val continentCode = continentLookup(csv.get("edge-continent-code")).toUpperCase
			  val continentName = continentCodeMap(continentCode)
			  val regionCode = Option(csv.get("edge-region")).map(_.toUpperCase).filter(_.nonEmpty)
			  val regionName = regionLookup.get(csv.get("edge-region-code"))

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
	    RawGeoEntry.create("NA", "North America", "US", "united states", dmaCode = Some(dmaCode), dmaName = Some(dmaName))
	  })

	  countryAndRegions ++ dmas
	}

	override def findByIpAddress(ipAddress: String): Option[GeoLocation] = {
		val result = db.query(ipAddress)
		Option(result.get("edge-country")) match {
			case Some("***") | None => None
			case Some(_) => Some(GeoLocation(
				result.get("edge-two-letter-country").toUpperCase.ifThen(_ == "UK")(_ => "GB"), // if UK then map => GB
				result.get("edge-country"),
				Option(result.get("edge-latitude")).flatMap(_.tryToFloat).getOrElse(0f),
				Option(result.get("edge-longitude")).flatMap(_.tryToFloat).getOrElse(0f),
				Option(result.get("edge-region")).map(_.toUpperCase()),
				Option(result.get("edge-city")),
				Option(result.get("edge-postal-code")),
				Option(result.get("edge-metro-code")).flatMap(_.tryToInt),
				Option(result.get("edge-area-codes")).flatMap(_.tryToInt),
				Option(result.get("edge-metro-code")).flatMap(_.tryToInt)
			))
		}
	}

	protected def loadDecodeFile(resource: String, idField: String, nameField: String): Map[String, String] = {
		val csv = new CsvReader(getClass.getResourceAsStream(resource), Charset.forName("UTF-8"))
	  csv.setTextQualifier('"')
	  csv.setTrimWhitespace(true)
	  csv.readHeaders()

		try {
			val map = new mutable.HashMap[String, String]()
			Stream.continually(csv.readRecord()).takeWhile(_ == true).map(_ => {
				val id = csv.get(idField)
				val name = csv.get(nameField)
				id.toUpperCase -> name
			}).foreach(v => map += v)
			map
		} finally {
			csv.close()
		}
	}
}
