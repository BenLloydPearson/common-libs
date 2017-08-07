package com.gravity.interests.jobs.intelligence.reporting

import play.api.libs.json.Json

case class PlacementWithSlotAboutToRunOutOfContent(
  sitePlacementId: Long,
  publisherGuid: String,
  recoBucket: Long,
  slotIndex: Int
)

object PlacementWithSlotAboutToRunOutOfContent {

  implicit val jsonFormat = Json.format[PlacementWithSlotAboutToRunOutOfContent]

}

case class PlacementContentDroughtSlot(index: Int)

object PlacementContentDroughtSlot {

  implicit val jsonFormat = Json.format[PlacementContentDroughtSlot]

}

case class PlacementContentDroughtBucket(id: Long, slots: Seq[PlacementContentDroughtSlot])

object PlacementContentDroughtBucket {

  implicit val jsonFormat = Json.format[PlacementContentDroughtBucket]

}

case class PlacementContentDroughtPlacement(id: Long, buckets: Seq[PlacementContentDroughtBucket])

object PlacementContentDroughtPlacement {

  implicit val jsonFormat = Json.format[PlacementContentDroughtPlacement]

}

case class PlacementContentDroughtSite(siteGuid: String, placements: Seq[PlacementContentDroughtPlacement])

object PlacementContentDroughtSite {

  implicit val jsonFormat = Json.format[PlacementContentDroughtSite]

}