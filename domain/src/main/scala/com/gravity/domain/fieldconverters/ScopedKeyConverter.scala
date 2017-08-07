package com.gravity.domain.fieldconverters

import com.gravity.domain.FieldConverters
import com.gravity.interests.jobs.intelligence.hbase.ScopedKey
import com.gravity.utilities.eventlogging.{FieldRegistry, FieldValueRegistry}
import com.gravity.utilities.grvfields.FieldConverter

import scalaz.{Failure, Success}

trait ScopedKeyConverter {
  this: FieldConverters.type =>

  implicit object ScopedKeyConverter extends FieldConverter[ScopedKey] {
    override def toValueRegistry(o: ScopedKey): FieldValueRegistry = new FieldValueRegistry(fields).registerFieldValue(0, o.keyString)

    override def fromValueRegistry(reg: FieldValueRegistry): ScopedKey = ScopedKey.validateKeyString(reg.getValue[String](0)) match {
      case Success(key) => key
      case Failure(fails) => throw new Exception(fails.toString)
    }

    override val fields: FieldRegistry[ScopedKey] = new FieldRegistry[ScopedKey]("ScopedKey", 0).registerStringField("keyString", 0)
  }
}