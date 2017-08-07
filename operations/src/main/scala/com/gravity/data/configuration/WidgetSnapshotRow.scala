package com.gravity.data.configuration

import com.gravity.utilities.grvcoll._
import com.gravity.valueclasses.ValueClassesForDomain.HostName
import com.gravity.valueclasses.ValueClassesForUtilities.{DashboardUserId, Html, Url}
import org.joda.time.DateTime
import play.api.libs.json.Json

/*
              ___...---''
  ___...---'\'___
''       _.-''  _`'.______\\.
        /_.)  )..-  __..--'\\
       (    __..--''
        '-''\@


 Ⓐ Ⓐ Ⓐ Ⓐ Ⓐ Ⓐ Ⓐ Ⓐ Ⓐ Ⓐ Ⓐ
*/

/**
  * @param widgetUrl As from /w2.
  * @param widgetBody As from /w2.
  */
case class WidgetSnapshotRow(id: Long, sitePlacementId: Long, widgetUrl: Url, widgetBody: Html, gravityHost: HostName,
                             dateTaken: DateTime, dashboardUserId: Option[DashboardUserId])

object WidgetSnapshotRow
extends ((Long, Long, Url, Html, HostName, DateTime, Option[DashboardUserId]) => WidgetSnapshotRow) {
  def forInsert(sitePlacementId: Long, widgetUrl: Url, widgetBody: Html, gravityHost: HostName,
                dashboardUserId: Option[DashboardUserId]): WidgetSnapshotRow =
    WidgetSnapshotRow(-1L, sitePlacementId, widgetUrl, widgetBody, gravityHost, new DateTime, dashboardUserId)

  implicit val jsonFormat = Json.format[WidgetSnapshotRow]

  implicit class WidgetSnapshotRows(snapshots: Traversable[WidgetSnapshotRow]) {
    def withSitePlacements(implicit configDb: ConfigurationQuerySupport): Traversable[WidgetSnapshotWithSitePlacement] = {
      val spIds = snapshots.map(_.sitePlacementId)
      val spsById = configDb.getSitePlacements(spIds.toList).mapBy(_.id)
      snapshots.map(s => WidgetSnapshotWithSitePlacement(s, spsById(s.sitePlacementId)))
    }
  }
}

case class WidgetSnapshotWithSitePlacement(snapshot: WidgetSnapshotRow, sp: SitePlacementRow)

object WidgetSnapshotWithSitePlacement {
  private implicit val spWrites = SitePlacementRowCompanion.jsonWrites
  implicit val jsonWrites = Json.writes[WidgetSnapshotWithSitePlacement]
}