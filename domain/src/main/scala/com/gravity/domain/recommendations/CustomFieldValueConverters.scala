package com.gravity.domain.recommendations

import com.gravity.utilities.eventlogging.FieldValueRegistry
import org.joda.time.DateTime
import org.joda.time.format.{DateTimeFormat, DateTimeFormatter}

trait CustomFieldValueConverter[T] {
  def getValue(index: Int, reg: FieldValueRegistry): T
  def registerFieldValue(index: Int, reg: FieldValueRegistry, value: T): FieldValueRegistry
}

object RdsDateCustomFieldValueConverter extends CustomFieldValueConverter[DateTime] {
  val dateFormat: DateTimeFormatter = DateTimeFormat.forPattern("yyyy-MM-dd")

  def registerFieldValue(index: Int, reg: FieldValueRegistry, value: DateTime): FieldValueRegistry = reg.registerFieldValue(index, value.toString(dateFormat))

  def getValue(index: Int, reg: FieldValueRegistry): DateTime = dateFormat.parseDateTime(reg.getValue[String](index))
}
