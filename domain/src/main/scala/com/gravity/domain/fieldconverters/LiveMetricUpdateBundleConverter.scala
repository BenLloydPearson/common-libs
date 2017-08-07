package com.gravity.domain.fieldconverters

import com.gravity.domain.FieldConverters
import com.gravity.interests.jobs.intelligence.hbase.{ScopedFromToKey, ScopedKey}
import com.gravity.interests.jobs.intelligence.operations.metric.{LiveMetricUpdateBundle, LiveMetrics}
import com.gravity.utilities.eventlogging.{FieldRegistry, FieldValueRegistry}
import com.gravity.utilities.grvfields.FieldConverter
import scalaz.{Failure, Success}

/**
 * Created by agrealish14 on 10/12/16.
 */
trait LiveMetricUpdateBundleConverter {
  this: FieldConverters.type =>

  implicit object LiveMetricUpdateBundleConverter extends FieldConverter[LiveMetricUpdateBundle] {

    val fields: FieldRegistry[LiveMetricUpdateBundle] = new FieldRegistry[LiveMetricUpdateBundle]("LiveMetricUpdateBundle", version = 1)
      .registerStringField("from", 0)
      .registerStringField("to", 1)
      .registerSeqField[LiveMetrics]("updates", 2, Seq.empty[LiveMetrics])

    def toValueRegistry(o: LiveMetricUpdateBundle): FieldValueRegistry = {
      new FieldValueRegistry(fields, version = 1)
        .registerFieldValue(0, o.key.from.keyString)
        .registerFieldValue(1, o.key.to.keyString)
        .registerFieldValue(2, o.metrics)
    }

    def fromValueRegistry(vals: FieldValueRegistry): LiveMetricUpdateBundle = {
      LiveMetricUpdateBundle(
        ScopedFromToKey(
          ScopedKey.validateKeyString(vals.getValue[String](0)) match {
            case Success(key) => key
            case Failure(fails) => throw new Exception(fails.toString)
          },
          ScopedKey.validateKeyString(vals.getValue[String](1)) match {
            case Success(key) => key
            case Failure(fails) => throw new Exception(fails.toString)
          }
        ),
        vals.getValue[Seq[LiveMetrics]](2)
      )
    }
  }
}
