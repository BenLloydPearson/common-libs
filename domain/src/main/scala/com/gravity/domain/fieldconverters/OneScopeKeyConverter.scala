package com.gravity.domain.fieldconverters

import com.gravity.domain.FieldConverters
import com.gravity.interests.jobs.intelligence.ArtGrvMap._
import com.gravity.interests.jobs.intelligence.hbase.ScopedKey
import com.gravity.utilities.eventlogging.{FieldRegistry, FieldValueRegistry}
import com.gravity.utilities.grvfields.FieldConverter

import scalaz.{Failure, Success}

trait OneScopeKeyConverter {
  this: FieldConverters.type =>

  implicit object OneScopeKeyConverter extends FieldConverter[OneScopeKey] {
    override def toValueRegistry(o: OneScopeKey): FieldValueRegistry = new FieldValueRegistry(fields).registerFieldValue(0, o._1.map(_.keyString)).registerFieldValue(1, o._2)

    override def fromValueRegistry(reg: FieldValueRegistry): OneScopeKey = {
      val keyOpt = reg.getValue[Option[String]](0).map { str => {
        ScopedKey.validateKeyString(str) match {
          case Success(key) => key
          case Failure(fails) => throw new Exception(fails.toString)
        }
      }
      }
      Tuple2(keyOpt, reg.getValue[String](1))
    }

    override val fields: FieldRegistry[OneScopeKey] = new FieldRegistry[OneScopeKey]("OneScopeKey").registerStringOptionField("keyOption", 0).registerStringField("Two", 1)
  }
}