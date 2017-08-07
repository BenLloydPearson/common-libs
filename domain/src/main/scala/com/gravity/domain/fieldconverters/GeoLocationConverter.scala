package com.gravity.domain.fieldconverters

import com.gravity.domain.FieldConverters
import com.gravity.utilities.eventlogging.{FieldValueRegistry, FieldRegistry}
import com.gravity.utilities.geo.GeoLocation
import com.gravity.utilities.grvfields.FieldConverter

/**
  * Created by jengelman14 on 1/26/17.
  */
trait GeoLocationConverter {
  this: FieldConverters.type =>

  implicit object GeoLocationConverter extends FieldConverter[GeoLocation] {
    val fields = new FieldRegistry[GeoLocation]("GeoLocation")
      .registerStringField("countryCode", 0, "")
      .registerStringField("countryName", 1, "")
      .registerFloatField("latitude", 2, -1.0.toFloat)
      .registerFloatField("longitude", 3, -1.0.toFloat)
      .registerStringOptionField("region", 4, None)
      .registerStringOptionField("city", 5, None)
      .registerStringOptionField("postalCode", 6, None)
      .registerIntOptionField("dmaCode", 7, None)
      .registerIntOptionField("areaCode", 8, None)
      .registerIntOptionField("metroCode", 9, None)

    def fromValueRegistry(reg: FieldValueRegistry) = new GeoLocation(
      reg.getValue[String](0),
      reg.getValue[String](1),
      reg.getValue[Float](2),
      reg.getValue[Float](3),
      reg.getValue[Option[String]](4),
      reg.getValue[Option[String]](5),
      reg.getValue[Option[String]](6),
      reg.getValue[Option[Int]](7),
      reg.getValue[Option[Int]](8),
      reg.getValue[Option[Int]](9)
    )

    def toValueRegistry(o: GeoLocation) = {
      import o._
      new FieldValueRegistry(fields)
        .registerFieldValue(0, countryCode)
        .registerFieldValue(1, countryName)
        .registerFieldValue(2, latitude)
        .registerFieldValue(3, longitude)
        .registerFieldValue(4, region)
        .registerFieldValue(5, city)
        .registerFieldValue(6, postalCode)
        .registerFieldValue(7, dmaCode)
        .registerFieldValue(8, areaCode)
        .registerFieldValue(9, metroCode)
    }
  }
}
