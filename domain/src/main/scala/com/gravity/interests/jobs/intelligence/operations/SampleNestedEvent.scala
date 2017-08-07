package com.gravity.interests.jobs.intelligence.operations

import com.gravity.utilities.grvtime
import org.joda.time.DateTime

/**
 * Created by cstelzmuller on 8/7/15.
 */
case class SampleNestedEvent ( sneString: String,
                          sneInt: Int,
                          sneLong: Long,
                          sneFloat: Float,
                          sneDouble: Double,
                          sneBoolean: Boolean,
                          sneListString: Seq[String],
                          sneListInt: Seq[Int],
                          sneListLong: Seq[Long],
                          sneListFloat: Seq[Float],
                          sneListDouble: Seq[Double],
                          sneListBoolean: Seq[Boolean],
                          sneDateTime: DateTime,
                          sneListDateTime: Seq[DateTime],
                          sneNestEvent: ArticleRecoData
                          ) {}

object SampleNestedEvent {
  val empty: SampleNestedEvent = new SampleNestedEvent("", -1, -1L, -1.0F, -1.0, false, List.empty[String], List.empty[Int], List.empty[Long],
    List.empty[Float], List.empty[Double], List.empty[Boolean], grvtime.epochDateTime,
    List.empty[DateTime], ArticleRecoData.empty)
}
