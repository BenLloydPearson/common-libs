package com.gravity.utilities.geo

import java.net.URLDecoder

import com.gravity.utilities.grvstrings._
import com.gravity.valueclasses.ValueClassesForUtilities._
import play.api.libs.json.{Format, Json}

import scala.collection._


@SerialVersionUID(5552390866671414020l)
case class GeoLocation(countryCode: String, countryName: String, latitude: Float, longitude: Float, region: Option[String] = None, city: Option[String] = None, postalCode: Option[String] = None, dmaCode: Option[Int] = None, areaCode: Option[Int] = None, metroCode: Option[Int] = None) {

  def distance(other: GeoLocation): Double = {
    GeoLocation.distanceBetween(this, other)
  }

  def geoRestrictions: Seq[GeoRestriction] = {

    val countryGeoEntry = GeoDatabase.findById(countryCode)

    Seq(
      // dma
      dmaCode.flatMap(dma => GeoDatabase.findById(countryCode + "-DMA-" + dma)).filter(_.`type` == GeoEntryType.DMA),
      // region
      region.flatMap(r => GeoDatabase.findById(countryCode + "-" + r)).filter(_.`type` == GeoEntryType.Region),
      // country
      countryGeoEntry.filter(_.`type` == GeoEntryType.Country),
      // continent
      countryGeoEntry.flatMap(c => c.parentId.flatMap(p => GeoDatabase.findById(p))).filter(_.`type` == GeoEntryType.Continent)
    ).flatten.map(r => GeoInclusion(r))
  }

  def isEmpty: Boolean = false

  override def toString: String = {
    import GeoLocation.delim

    val sb = new StringBuilder

    def appendOpt(opt: Option[Any]): StringBuilder = opt match {
      case Some(thing) => sb.append(thing)
      case None => sb
    }

    sb.append(countryCode).append(delim)
    sb.append(countryName).append(delim)
    appendOpt(areaCode).append(delim)
    appendOpt(city).append(delim)
    appendOpt(dmaCode).append(delim)
    sb.append(latitude).append(delim)
    sb.append(longitude).append(delim)
    appendOpt(metroCode).append(delim)
    appendOpt(postalCode).append(delim)
    appendOpt(region).toString()
  }

  def cc: CountryCode = CountryCode(countryCode)

  //todo: Implement lookup of 3 char country code
  def cc3: CountryCode3 = CountryCode3(countryCode + "x")

  lazy val isUS: Boolean = countryCode == "US"

  def isNonUS: Boolean = !isUS
}

object GeoLocation {
   val diameterOfEarth = 12756.4D
   val radConvert = 0.017453292500000002D
   val doubleTwo = 2.0D

  def fromString(input: String, urlEncoded: Boolean = false): GeoLocation = {
    val geoDesc = urlEncoded match {case false => input; case true => URLDecoder.decode(input, "UTF-8");}
    val geoFields = geoDesc.split(delim).toSeq
    def tryGetGeoField(index: Int): Option[String] = {
      if (index >= geoFields.length)
        None
      else
        Some(geoFields(index))
    }
    GeoLocation(
      tryGetGeoField(0).getOrElse(""), // countryCode
      tryGetGeoField(1).getOrElse(""), // countryName
      tryGetGeoField(5).getOrElse("-1").toFloat, // latitude
      tryGetGeoField(6).getOrElse("-1").toFloat, // longitude
      tryGetGeoField(9), // region
      tryGetGeoField(3), // city
      tryGetGeoField(8), // postalCode
      tryGetGeoField(4) match {case Some(x) if x.isNumeric => Some(x.toInt); case _ => None;}, // dmaCode
      tryGetGeoField(2) match {case Some(x) if x.isNumeric => Some(x.toInt); case _ => None;}, // areaCode
      tryGetGeoField(7) match {case Some(x) if x.isNumeric => Some(x.toInt); case _ => None;} // metroCode
    )
  }

   def distanceBetween(loc1: GeoLocation, loc2: GeoLocation): Double = {
     val lat1 = loc1.latitude
     val lon1 = loc1.longitude
     val lat2 = loc2.latitude
     val lon2 = loc2.longitude

     val convertedLat1 = (lat1.toDouble * radConvert).toFloat
     val convertedLat2 = (lat2.toDouble * radConvert).toFloat

     val latDelta = (convertedLat2 - convertedLat1).toDouble
     val lonDelta = (lon2 - lon1).toDouble * radConvert

     val magicCalc = math.pow(math.sin(latDelta / doubleTwo), doubleTwo) + math.cos(lat1.toDouble) * math.cos(lat2.toDouble) * math.pow(math.sin(lonDelta / doubleTwo), doubleTwo)
     val distance = diameterOfEarth * math.atan2(math.sqrt(magicCalc), math.sqrt(1.0D - magicCalc))

     distance
   }

   val empty: GeoLocation {val toString: String; val geoRestrictions: scala.Seq[GeoRestriction]; val isEmpty: Boolean} = new GeoLocation(emptyString, emptyString, 0f, 0f) {
     override val toString: String = "!!!!!!!!!"
     override val isEmpty: Boolean = true
     override lazy val geoRestrictions: scala.Seq[GeoRestriction] = Seq.empty
   }

   val delim = '!'

   val defaultIntlIPAddress = "intl" // this is intentionally an invalid ip address because it is to be used internally only!

   val usCountryCode: CountryCode = CountryCode("US")

   lazy val defaultUsGeoLocation: GeoLocation {val geoRestrictions: scala.Seq[GeoInclusion]} = new GeoLocation("US", "US-Only", 0f, 0f) {
     override lazy val geoRestrictions: scala.Seq[GeoInclusion] = Seq(GeoInclusion(GeoDatabase.findById("US").getOrElse(throw new RuntimeException("US entry not found"))))
   }

   lazy val defaultIntlGeoLocation: GeoLocation {val geoRestrictions: scala.Seq[GeoExclusion]} = new GeoLocation("-US", "International-Only", 0f, 0f) {
     override lazy val geoRestrictions: scala.Seq[GeoExclusion] = Seq(GeoExclusion(GeoDatabase.findById("US").getOrElse(throw new RuntimeException("US entry not found"))))
   }

   lazy val usGeoId: Int = defaultUsGeoLocation.countryCode.getMurmurHash.toInt
   lazy val intlGeoId: Int = "DJ".getMurmurHash.toInt

   private[utilities] val customGeoLocationLookup = Seq(
     defaultUsGeoLocation,
     defaultIntlGeoLocation
   ).map(e => e.countryCode -> e).toMap

   implicit val jsonFormat: Format[GeoLocation] = Json.format[GeoLocation]

  lazy val commonwealthGeoLocation: GeoLocation {val geoRestrictions: scala.Seq[GeoInclusion]} = new GeoLocation("COMMONWEALTH", "COMMONWEALTH-GB-IE-AU-NZ-CA", 0f, 0f) {
    override lazy val geoRestrictions: scala.Seq[GeoInclusion] = Seq(
      GeoInclusion(GeoDatabase.findById("GB").getOrElse(throw new RuntimeException("GB entry not found"))),
      GeoInclusion(GeoDatabase.findById("IE").getOrElse(throw new RuntimeException("IE entry not found"))),
      GeoInclusion(GeoDatabase.findById("AU").getOrElse(throw new RuntimeException("AU entry not found"))),
      GeoInclusion(GeoDatabase.findById("NZ").getOrElse(throw new RuntimeException("NZ entry not found"))),
      GeoInclusion(GeoDatabase.findById("CA").getOrElse(throw new RuntimeException("CA entry not found")))
    )
  }

  lazy val frGeoLocation: GeoLocation {val geoRestrictions: scala.Seq[GeoInclusion]} = new GeoLocation("FR", "FR-Only", 0f, 0f) {
    override lazy val geoRestrictions: scala.Seq[GeoInclusion] = Seq(GeoInclusion(GeoDatabase.findById("FR").getOrElse(throw new RuntimeException("FR entry not found"))))
  }

  lazy val inGeoLocation: GeoLocation {val geoRestrictions: scala.Seq[GeoInclusion]} = new GeoLocation("IN", "IN-Only", 0f, 0f) {
    override lazy val geoRestrictions: scala.Seq[GeoInclusion] = Seq(GeoInclusion(GeoDatabase.findById("IN").getOrElse(throw new RuntimeException("IN entry not found"))))
  }

  lazy val jpGeoLocation: GeoLocation {val geoRestrictions: scala.Seq[GeoInclusion]} = new GeoLocation("JP", "JP-Only", 0f, 0f) {
    override lazy val geoRestrictions: scala.Seq[GeoInclusion] = Seq(GeoInclusion(GeoDatabase.findById("JP").getOrElse(throw new RuntimeException("JP entry not found"))))
  }

  lazy val cnGeoLocation: GeoLocation {val geoRestrictions: scala.Seq[GeoInclusion]} = new GeoLocation("CN", "CN-Only", 0f, 0f) {
    override lazy val geoRestrictions: scala.Seq[GeoInclusion] = Seq(GeoInclusion(GeoDatabase.findById("CN").getOrElse(throw new RuntimeException("CN entry not found"))))
  }

  lazy val deGeoLocation: GeoLocation {val geoRestrictions: scala.Seq[GeoInclusion]} = new GeoLocation("DE", "DE-Only", 0f, 0f) {
    override lazy val geoRestrictions: scala.Seq[GeoInclusion] = Seq(GeoInclusion(GeoDatabase.findById("DE").getOrElse(throw new RuntimeException("DE entry not found"))))
  }

  lazy val myGeoLocation: GeoLocation {val geoRestrictions: scala.Seq[GeoInclusion]} = new GeoLocation("MY", "MY-Only", 0f, 0f) {
    override lazy val geoRestrictions: scala.Seq[GeoInclusion] = Seq(GeoInclusion(GeoDatabase.findById("MY").getOrElse(throw new RuntimeException("MY entry not found"))))
  }

  lazy val phGeoLocation: GeoLocation {val geoRestrictions: scala.Seq[GeoInclusion]} = new GeoLocation("PH", "PH-Only", 0f, 0f) {
    override lazy val geoRestrictions: scala.Seq[GeoInclusion] = Seq(GeoInclusion(GeoDatabase.findById("PH").getOrElse(throw new RuntimeException("PH entry not found"))))
  }

  lazy val mxGeoLocation: GeoLocation {val geoRestrictions: scala.Seq[GeoInclusion]} = new GeoLocation("MX", "MX-Only", 0f, 0f) {
    override lazy val geoRestrictions: scala.Seq[GeoInclusion] = Seq(GeoInclusion(GeoDatabase.findById("MX").getOrElse(throw new RuntimeException("MX entry not found"))))
  }

  lazy val brGeoLocation: GeoLocation {val geoRestrictions: scala.Seq[GeoInclusion]} = new GeoLocation("BR", "MX-Only", 0f, 0f) {
    override lazy val geoRestrictions: scala.Seq[GeoInclusion] = Seq(GeoInclusion(GeoDatabase.findById("BR").getOrElse(throw new RuntimeException("BR entry not found"))))
  }

  lazy val idGeoLocation: GeoLocation {val geoRestrictions: scala.Seq[GeoInclusion]} = new GeoLocation("ID", "ID-Only", 0f, 0f) {
    override lazy val geoRestrictions: scala.Seq[GeoInclusion] = Seq(GeoInclusion(GeoDatabase.findById("ID").getOrElse(throw new RuntimeException("ID entry not found"))))
  }

  lazy val itGeoLocation: GeoLocation {val geoRestrictions: scala.Seq[GeoInclusion]} = new GeoLocation("IT", "IT-Only", 0f, 0f) {
    override lazy val geoRestrictions: scala.Seq[GeoInclusion] = Seq(GeoInclusion(GeoDatabase.findById("IT").getOrElse(throw new RuntimeException("IT entry not found"))))
  }

  lazy val zaGeoLocation: GeoLocation {val geoRestrictions: scala.Seq[GeoInclusion]} = new GeoLocation("ZA", "ZA-Only", 0f, 0f) {
    override lazy val geoRestrictions: scala.Seq[GeoInclusion] = Seq(GeoInclusion(GeoDatabase.findById("ZA").getOrElse(throw new RuntimeException("ZA entry not found"))))
  }

  lazy val twGeoLocation: GeoLocation {val geoRestrictions: scala.Seq[GeoInclusion]} = new GeoLocation("TW", "TW-Only", 0f, 0f) {
    override lazy val geoRestrictions: scala.Seq[GeoInclusion] = Seq(GeoInclusion(GeoDatabase.findById("TW").getOrElse(throw new RuntimeException("TW entry not found"))))
  }
}
