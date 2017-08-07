package com.gravity.utilities.web

import com.gravity.utilities.components.FailureResult
import com.gravity.utilities.swagger.adapter.DefaultValueWriter
import play.api.libs.json.{Format, Json}

import scala.collection.Set
import scalaz.Validation
import scalaz.syntax.validation._

/**
 * @param property Property name being sorted on.
 */
case class SortSpec(property: String, asc: Boolean = true) {
  def desc: Boolean = !asc

  override def toString: String = if(asc) property else s"-$property"
}

object SortSpec {
  /** Accepts prefix of "-" to denote descending. */
  def fromString(sortSpec: String): Validation[FailureResult, SortSpec] = {
    val asc = !(sortSpec startsWith "-")
    val property = if(asc) sortSpec else sortSpec.substring(1)

    if(property.isEmpty)
      FailureResult("Invalid sort spec; no property given.").failure
    else
      SortSpec(property, asc).success
  }

  implicit val jsonFormat: Format[SortSpec] = Json.format[SortSpec]
  implicit val defaultValueWriter: DefaultValueWriter[SortSpec] with Object {def serialize(t: SortSpec): String} = new DefaultValueWriter[SortSpec] {
    override def serialize(t: SortSpec): String = t.toString
  }
  implicit val setDefaultValueWriter: DefaultValueWriter[Set[SortSpec]] = DefaultValueWriter.setDefaultValueWriter[SortSpec]()
  implicit val listDefaultValueWriter: DefaultValueWriter[List[SortSpec]] = DefaultValueWriter.listDefaultValueWriter[SortSpec]()
}