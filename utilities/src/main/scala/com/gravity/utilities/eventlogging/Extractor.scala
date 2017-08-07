package com.gravity.utilities.eventlogging

import cascading.tuple.Fields
import com.gravity.utilities.eventlogging.Extractor.FieldExtractorFx
import com.gravity.utilities.grvfields.FieldConverter

/**
 * Created by cstelzmuller on 11/6/15.
 */

object Extractor {
  type FieldExtractorFx = FieldRegistry[_] => SerializationField[_]
}

abstract class NameExtractor[T <: FieldConverter[_]](override val converter:T)(names: String*) extends Extractor(converter)({
  val extractors = names.map(name=>{(fr: FieldRegistry[_]) => fr.sortedFields.find(_.name == name).getOrElse(throw new RuntimeException("Incorrectly specified field in extractor: " + name))})
  extractors
}:_*)

abstract class Extractor[T <: FieldConverter[_]](val converter:T)(extractors: FieldExtractorFx*) {
  def subFields: Seq[SerializationField[_]] = extractors.map(fx=>fx(converter.fields))
  def subFieldNames: Seq[String] = subFields.map(_.name)

  def renameFields(namePrefix: String) = new Fields(subFields.map(field => s"$namePrefix${field.name}"):_*)
  val selectFields = new Fields(subFieldNames:_*)

  def fields(namePrefix : String): Fields = {
    converter.cascadingFields.select(selectFields).rename(selectFields, renameFields(namePrefix))
  }

  def locations(fieldPath: String = ""): Vector[String] = {
    subFields.map(field => s"$fieldPath${field.name}").toVector
  }
}
