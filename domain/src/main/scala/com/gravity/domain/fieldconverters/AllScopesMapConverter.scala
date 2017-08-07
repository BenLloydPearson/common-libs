package com.gravity.domain.fieldconverters

import com.gravity.domain.FieldConverters
import com.gravity.interests.jobs.intelligence.ArtGrvMap._
import com.gravity.utilities.eventlogging.{FieldRegistry, FieldValueRegistry}
import com.gravity.utilities.grvfields.FieldConverter

trait AllScopesMapConverter {
  this: FieldConverters.type =>

  implicit object AllScopesMapConverter extends FieldConverter[AllScopesMap] {
    override def toValueRegistry(o: AllScopesMap): FieldValueRegistry = new FieldValueRegistry(fields).registerFieldValue(0, o.keys.toSeq).registerFieldValue(1, o.values.toSeq)

    override def fromValueRegistry(reg: FieldValueRegistry): AllScopesMap = {
      val keys = reg.getValue[Seq[OneScopeKey]](0)
      val values = reg.getValue[Seq[OneScopeMap]](1)
      if (keys.length != values.length) {
        throw new Exception("Could not read all scopes map because keys and values did not have the same number of elements")
      }
      else {
        keys.zip(values).toMap
      }
    }

    override val fields: FieldRegistry[AllScopesMap] = new FieldRegistry[AllScopesMap]("AllScopesMap")
      .registerSeqField[OneScopeKey]("k", 0, Seq.empty[OneScopeKey])
      .registerSeqField[OneScopeMap]("v", 1, Seq.empty[OneScopeMap])
  }
}