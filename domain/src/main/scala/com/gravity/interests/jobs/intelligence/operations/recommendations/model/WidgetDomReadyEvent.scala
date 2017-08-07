package com.gravity.interests.jobs.intelligence.operations.recommendations.model

import com.gravity.utilities.eventlogging.FieldValueRegistry
import org.joda.time.DateTime

/**
 * @param clientTime Actual time of DOM ready event generated on client side.
 * @param hashHex           Impression hash.
 */
case class WidgetDomReadyEvent(dt: DateTime,
                               pageViewIdHash: String,
                               siteGuid: String,
                               userGuid: String,
                               sitePlacementId: Long,
                               userAgent: String,
                               remoteIp: String,
                               clientTime: DateTime,
                               gravityHost: String,
                               hashHex: String) {

  def this(vals: FieldValueRegistry) = this(
    vals.getValue[DateTime](0),
    vals.getValue[String](1),
    vals.getValue[String](2),
    vals.getValue[String](3),
    vals.getValue[Long](4),
    vals.getValue[String](5),
    vals.getValue[String](6),
    vals.getValue[DateTime](7),
    vals.getValue[String](8),
    vals.getValue[String](9)
  )

}

object WidgetDomReadyEvent {
  val logCategory = "widgetDomReadyEvent"
}