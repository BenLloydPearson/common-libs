package com.gravity.domain.fieldconverters

import com.gravity.domain.FieldConverters
import com.gravity.interests.jobs.intelligence.ArtGrvMapMetaVal
import com.gravity.utilities.eventlogging.{FieldRegistry, FieldValueRegistry}
import com.gravity.utilities.grvfields.FieldConverter

trait ArtGrvMapMetaValConverter {
  this: FieldConverters.type =>

  implicit object ArtGrvMapMetaValConverter extends FieldConverter[ArtGrvMapMetaVal] {
    override def toValueRegistry(o: ArtGrvMapMetaVal): FieldValueRegistry = new FieldValueRegistry(fields)
      .registerFieldValue(0, o.isPrivate).registerFieldValue(1, o.xsdTypeName).registerFieldValue(2, o.grvMapVal)

    override def fromValueRegistry(reg: FieldValueRegistry): ArtGrvMapMetaVal = ArtGrvMapMetaVal(reg.getValue[Boolean](0), reg.getValue[String](1), reg.getValue(2))

    override val fields: FieldRegistry[ArtGrvMapMetaVal] = new FieldRegistry[ArtGrvMapMetaVal]("ArtGrvMapMetaVal")
      .registerBooleanField("isPrivate", 0)
      .registerStringField("xsdTypeName", 1)
      .registerStringField("grvMapVal", 2)
  }
}