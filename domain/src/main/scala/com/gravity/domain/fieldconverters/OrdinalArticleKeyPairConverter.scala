package com.gravity.domain.fieldconverters

import com.gravity.domain.FieldConverters
import com.gravity.interests.jobs.intelligence.operations.OrdinalArticleKeyPair
import com.gravity.utilities.eventlogging.{FieldRegistry, FieldValueRegistry}
import com.gravity.utilities.grvfields.FieldConverter

trait OrdinalArticleKeyPairConverter {
  this: FieldConverters.type =>

  implicit object OrdinalArticleKeyPairConverter extends FieldConverter[OrdinalArticleKeyPair] {
    val fields: FieldRegistry[OrdinalArticleKeyPair] = new FieldRegistry[OrdinalArticleKeyPair]("OrdinalArticleKeyPair")
    fields.registerIntField("ordinal", 0, -1)
    fields.registerLongField("ak", 1, -1l)

    def fromValueRegistry(reg: FieldValueRegistry): OrdinalArticleKeyPair = new OrdinalArticleKeyPair(reg)

    def toValueRegistry(o: OrdinalArticleKeyPair): FieldValueRegistry = {
      import o._
      new FieldValueRegistry(fields)
        .registerFieldValue(0, ordinal)
        .registerFieldValue(1, ak.articleId)

    }
  }
}