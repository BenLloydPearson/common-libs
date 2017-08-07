package com.gravity.domain.fieldconverters

import com.gravity.domain.FieldConverters
import com.gravity.interests.jobs.intelligence.ArtGrvMap._
import com.gravity.interests.jobs.intelligence.ArtGrvMapMetaVal
import com.gravity.utilities.eventlogging.{FieldRegistry, FieldValueRegistry}
import com.gravity.utilities.grvfields.FieldConverter

trait OneScopeMapConverter {
  this: FieldConverters.type =>

  implicit object OneScopeMapConverter extends FieldConverter[OneScopeMap] {
    override def toValueRegistry(o: OneScopeMap): FieldValueRegistry = new FieldValueRegistry(fields).registerFieldValue(0, o.keys.toSeq).registerFieldValue(1, o.values.toSeq)

    override def fromValueRegistry(reg: FieldValueRegistry): OneScopeMap = {
      val keys = reg.getValue[Seq[String]](0)
      val values = reg.getValue[Seq[ArtGrvMapMetaVal]](1)
      if (keys.length != values.length) {
        throw new Exception("Could not read one scope map because keys and values did not have the same number of elements")
      }
      else {
        keys.zip(values).toMap
      }
    }

    override val fields: FieldRegistry[OneScopeMap] = new FieldRegistry[OneScopeMap]("OneScopeMap")
      .registerStringSeqField("k", 0)
      .registerSeqField[ArtGrvMapMetaVal]("v", 1, Seq.empty[ArtGrvMapMetaVal])
  }
}