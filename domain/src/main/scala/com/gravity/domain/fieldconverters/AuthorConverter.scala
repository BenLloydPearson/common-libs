package com.gravity.domain.fieldconverters

import com.gravity.domain.FieldConverters
import com.gravity.domain.articles.Author
import com.gravity.utilities.eventlogging.{FieldRegistry, FieldValueRegistry}
import com.gravity.utilities.grvfields.FieldConverter

trait AuthorConverter {
  this: FieldConverters.type =>

  implicit object AuthorConverter extends FieldConverter[Author] {
    override def toValueRegistry(o: Author): FieldValueRegistry = new FieldValueRegistry(fields)
      .registerFieldValue(0, o.name)
      .registerFieldValue(1, o.urlOption)
      .registerFieldValue(2, o.titleOption)
      .registerFieldValue(3, o.imageOption)

    override def fromValueRegistry(reg: FieldValueRegistry): Author =
      Author(reg.getValue[String](0), reg.getValue[Option[String]](1), reg.getValue[Option[String]](2), reg.getValue[Option[String]](3))

    override val fields: FieldRegistry[Author] = new FieldRegistry[Author]("Author")
      .registerStringField("name", 0)
      .registerStringOptionField("urlOption", 1)
      .registerStringOptionField("titleOption", 2)
      .registerStringOptionField("imageOption", 3)
  }
}