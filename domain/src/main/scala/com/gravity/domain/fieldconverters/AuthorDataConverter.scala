package com.gravity.domain.fieldconverters

import com.gravity.domain.FieldConverters
import com.gravity.domain.articles.{Author, AuthorData}
import com.gravity.utilities.eventlogging.{FieldRegistry, FieldValueRegistry}
import com.gravity.utilities.grvfields.FieldConverter

trait AuthorDataConverter {
  this: FieldConverters.type =>

  implicit object AuthorDataConverter extends FieldConverter[AuthorData] {
    override def toValueRegistry(o: AuthorData): FieldValueRegistry = new FieldValueRegistry(fields).registerFieldValue(0, o.authors)

    override def fromValueRegistry(reg: FieldValueRegistry): AuthorData = AuthorData(reg.getValue[Seq[Author]](0))

    override val fields: FieldRegistry[AuthorData] = new FieldRegistry[AuthorData]("AuthorData").registerSeqField[Author]("authors", 0, Seq.empty[Author])
  }
}