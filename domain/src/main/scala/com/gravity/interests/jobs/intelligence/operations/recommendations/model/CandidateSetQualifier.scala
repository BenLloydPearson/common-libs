package com.gravity.interests.jobs.intelligence.operations.recommendations.model

import play.api.libs.json.{Json, Format}

/**
 * Describes the scoped in which canddiate sets can exist.
 * @param sitePlacementId The site placement id against which they were defined
 * @param bucketId The bucket in which they were defined
 * @param slotIndex The slot index
 * @param deviceType The device type
 * @param geoLocation
 */
case class CandidateSetQualifier(
                                  sitePlacementId:Option[Int],
                                  bucketId:Option[Int],
                                  slotIndex:Option[Int],
                                  deviceType:Option[Int],
                                  geoLocation:Option[Int]) {

  def applyToCandidateSetQualifier(deviceTypeOverride:Option[Int], geoLocationOverride:Option[Int]) = {

    CandidateSetQualifier(sitePlacementId, bucketId, slotIndex, deviceTypeOverride, geoLocationOverride)
  }
}

object CandidateSetQualifier {
  implicit val jsonFormat: Format[CandidateSetQualifier] = Json.format[CandidateSetQualifier]
}
