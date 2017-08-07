package com.gravity.domain.fieldconverters

import com.gravity.domain.FieldConverters
import com.gravity.interests.jobs.intelligence.operations.AlgoSettingsData
import com.gravity.utilities.eventlogging.{FieldRegistry, FieldValueRegistry}
import com.gravity.utilities.grvfields.FieldConverter
import com.gravity.utilities.grvstrings._

trait AlgoSettingsDataConverter {
  this: FieldConverters.type =>

  implicit object AlgoSettingsDataConverter extends FieldConverter[AlgoSettingsData] {
    val fields: FieldRegistry[AlgoSettingsData] = {
      new FieldRegistry[AlgoSettingsData]("AlgoSettings")
        .registerUnencodedStringField("settingName", 0, emptyString, required = true)
        .registerIntField("settingType", 1, 0, required = true)
        .registerUnencodedStringField("settingData", 2, emptyString, required = true)
    }

    def fromValueRegistry(vals: FieldValueRegistry): AlgoSettingsData = new AlgoSettingsData(vals)

    def toValueRegistry(o: AlgoSettingsData): FieldValueRegistry = {
      import o._
      new FieldValueRegistry(fields)
        .registerFieldValue(0, settingName)
        .registerFieldValue(1, settingType)
        .registerFieldValue(2, settingData)
    }
  }
}