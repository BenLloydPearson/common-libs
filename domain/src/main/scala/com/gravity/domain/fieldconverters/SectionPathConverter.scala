package com.gravity.domain.fieldconverters

import com.gravity.domain.FieldConverters
import com.gravity.interests.jobs.intelligence.SectionPath
import com.gravity.utilities.eventlogging.{FieldRegistry, FieldValueRegistry}
import com.gravity.utilities.grvfields.FieldConverter

trait SectionPathConverter {
  this: FieldConverters.type =>

  implicit object SectionPathConverter extends FieldConverter[SectionPath] {
    override def toValueRegistry(o: SectionPath): FieldValueRegistry = new FieldValueRegistry(fields).registerFieldValue(0, o.paths)

    override def fromValueRegistry(reg: FieldValueRegistry): SectionPath = SectionPath(reg.getValue[Seq[String]](0))

    override val fields: FieldRegistry[SectionPath] = new FieldRegistry[SectionPath]("SectionPath").registerStringSeqField("paths", 0)
  }
}