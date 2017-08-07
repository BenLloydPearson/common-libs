package com.gravity.domain.fieldconverters

import com.gravity.domain.FieldConverters
import com.gravity.interests.jobs.intelligence.hbase.{ScopedFromToKey, ScopedKey}
import com.gravity.utilities.eventlogging.{FieldRegistry, FieldValueRegistry}
import com.gravity.utilities.grvfields.FieldConverter
import scalaz.{Failure, Success}

/**
 * Created by agrealish14 on 10/12/16.
 */
trait ScopedMetricsFromToKeyConverter {

  this: FieldConverters.type =>

  implicit object ScopedMetricsFromToKeyConverter extends FieldConverter[ScopedFromToKey] {

    val fields: FieldRegistry[ScopedFromToKey] = new FieldRegistry[ScopedFromToKey]("ScopedFromToKey", version = 1)
      .registerStringField("from", 0)
      .registerStringField("to", 1)

    def toValueRegistry(o: ScopedFromToKey): FieldValueRegistry = {
      new FieldValueRegistry(fields, version = 1)
        .registerFieldValue(0, o.from.keyString)
        .registerFieldValue(1, o.to.keyString)
    }

    def fromValueRegistry(vals: FieldValueRegistry): ScopedFromToKey = {

      ScopedFromToKey(
        ScopedKey.validateKeyString(vals.getValue[String](0)) match {
          case Success(key) => key
          case Failure(fails) => throw new Exception(fails.toString)
        },
        ScopedKey.validateKeyString(vals.getValue[String](1)) match {
          case Success(key) => key
          case Failure(fails) => throw new Exception(fails.toString)
        }

      )
    }
  }

}
