package com.gravity.interests.jobs.intelligence.operations.recommendations.model

import com.gravity.interests.jobs.intelligence.operations.EventExtraFields
import org.joda.time.DateTime


/*             )\._.,--....,'``.
 .b--.        /;   _.. \   _\  (`._ ,.
`=,-,-'~~~   `----(,_..'--(,_..'`-.;.'  */

case class ImpressionFailedEventDetails(
                           failureType: String, // unfilled for syndication otherwise error
                           userGuid: String,
                           currentUrl: String,
                           geoLocationId: Int,
                           geoLocationDesc: String,
                           isMobile: Boolean,
                           pageViewIdWidgetLoaderWindowUrl: String,
                           pageViewIdTime: DateTime,
                           pageViewIdRand: Long,
                           userAgent: String,
                           clientTime: Long,
                           ipAddress: String,
                           gravityHost: String,
                           isMaintenance: Boolean,
                           affiliateId: String,
                           isOptedOut: Boolean,
                           more: EventExtraFields
                            )

case class ImpressionFailedEvent(date: DateTime,
                                 id: Int,
                                 siteGuid: String,
                                 message: String,
                                 exception: String = "",
                                 failedImpression: ImpressionFailedEventDetails) {

  def getSiteGuid: String = siteGuid

  def getBucketId : Int = {
    failedImpression.more.recoBucket
  }

  def getSitePlacementId : Long = {
    failedImpression.more.sitePlacementId
  }

  def ipAddress : String = {
    failedImpression.ipAddress
  }
}

object ImpressionFailedEventDetails {

  val ERROR = "ERROR"
  val SYNDICATION_UNFILLED = "SYNDICATION_UNFILLED"
}