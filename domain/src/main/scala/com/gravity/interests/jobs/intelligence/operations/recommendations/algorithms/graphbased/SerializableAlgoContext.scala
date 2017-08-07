package com.gravity.interests.jobs.intelligence.operations.recommendations.algorithms.graphbased

import com.gravity.domain.recommendations.SlotDef
import com.gravity.interests.jobs.intelligence.{Device, SectionPath}
import com.gravity.utilities.geo.GeoLocation

@SerialVersionUID(-1319758803124354948l)
case class SerializableAlgoContext(
                                    siteGuid: String
                                    ,userGuid: String
                                    ,hints: AlgoHints
                                    ,deviceType: Device.Type
                                    ,currentUrl: Option[String]
                                    ,placementId: Option[Int]
                                    ,bucketId: Option[Int]
                                    ,geoLocation: Option[GeoLocation]
                                    ,requestedSectionPath:Option[SectionPath]
                                    ,slotIndex: Int
                                    ,slot: Option[SlotDef]
                                    ,recommenderId: Option[Long]
                                  )