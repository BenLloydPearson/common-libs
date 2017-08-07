package com.gravity.interests.jobs.intelligence.operations

import com.gravity.utilities.grvtime
import org.joda.time.DateTime

/**
 * Created by cstelzmuller on 8/5/15.
 *
 * Uses all manner of field types to ensure conversion to bytes, delimited string, and avro schema all work
 * as expected.
 */
case class SampleEvent( seString: String,
                          seInt: Int,
                          seLong: Long,
                          seFloat: Float,
                          seDouble: Double,
                          seBoolean: Boolean,
                          seListString: Seq[String],
                          seListInt: Seq[Int],
                          seListLong: Seq[Long],
                          seListFloat: Seq[Float],
                          seListDouble: Seq[Double],
                          seListBoolean: Seq[Boolean],
                          seDateTime: DateTime,
                          seListDateTime: Seq[DateTime],
                          seObject: SampleNestedEvent,
                          seListObject: Seq[SampleNestedEvent],
                          seArrayByte: Array[Byte],
                          seOptionString: Option[String],
                          seOptionStringTwo: Option[String],
                          seUnencodedString: String,
                          seListUnencodedString: Seq[String],
                          seListArrayByte: Seq[Array[Byte]]
                          ) extends DiscardableEvent {}

object SampleEvent {
  val empty: SampleEvent = SampleEvent("", -1, -1L, -1.0F, -1.0, false,
    List.empty[String], List.empty[Int], List.empty[Long],List.empty[Float], List.empty[Double], List.empty[Boolean],
    grvtime.epochDateTime, List.empty[DateTime],
    SampleNestedEvent.empty, List.empty[SampleNestedEvent],
    Array.empty[Byte],
    Option(null),Option(null),
    "", List.empty[String], List.empty[Array[Byte]]
   )
}
