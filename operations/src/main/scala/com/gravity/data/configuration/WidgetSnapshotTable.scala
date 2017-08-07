package com.gravity.data.configuration

import com.gravity.data.MappedTypes
import com.gravity.valueclasses.ValueClassesForDomain.{HostName, SitePlacementId}
import com.gravity.valueclasses.ValueClassesForUtilities.{DashboardUserId, Html, Url}
import org.joda.time.DateTime

/*
              ___...---''
  ___...---'\'___
''       _.-''  _`'.______\\.
        /_.)  )..-  __..--'\\
       (    __..--''
        '-''\@


 Ⓐ Ⓐ Ⓐ Ⓐ Ⓐ Ⓐ Ⓐ Ⓐ Ⓐ Ⓐ Ⓐ
*/

trait WidgetSnapshotTable extends MappedTypes {
  this: ConfigurationDatabase =>

  import driver.simple._

  class WidgetSnapshotTable(tag: Tag) extends Table[WidgetSnapshotRow](tag, "WidgetSnapshot") {
    def id = column[Long]("id", O.PrimaryKey, O.NotNull, O.AutoInc)

    /** Would make this a [[SitePlacementId]] but the foreign key typing confounds that. */
    def sitePlacementId = column[Long]("sitePlacementId", O.NotNull)

    def widgetUrl = column[Url]("widgetUrl", O.NotNull, O.DBType("TEXT"))
    def widgetBody = column[Html]("widgetBody", O.NotNull, O.DBType("TEXT"))
    def gravityHost = column[HostName]("gravityHost", O.NotNull)
    def dateTaken = column[DateTime]("dateTaken", O.NotNull)
    def dashboardUserId = column[Option[DashboardUserId]]("dashboardUserId")

    def sp = foreignKey(tableName + "_sitePlacementId_fk", sitePlacementId, SitePlacementTable)(_.id)

    override def * = (id, sitePlacementId, widgetUrl, widgetBody, gravityHost, dateTaken, dashboardUserId) <>
      (WidgetSnapshotRow.tupled, WidgetSnapshotRow.unapply)
  }

  val widgetSnapshotQuery = scala.slick.lifted.TableQuery[WidgetSnapshotTable]

  val widgetSnapshotQueryForInsert = widgetSnapshotQuery.map(s => (s.sitePlacementId, s.widgetUrl, s.widgetBody,
    s.gravityHost, s.dateTaken, s.dashboardUserId))
  def widgetSnapshotForInsert(s: WidgetSnapshotRow) = (s.sitePlacementId, s.widgetUrl, s.widgetBody, s.gravityHost,
    s.dateTaken, s.dashboardUserId)
}

trait WidgetSnapshotQuerySupport extends MappedTypes {
  this: ConfigurationQuerySupport =>

  import configDb.driver.simple._

  def lastNWidgetSnapshots(n: Int = 25): List[WidgetSnapshotRow] = readOnlyDatabase withSession {
    implicit s: Session => widgetSnapshotQuery.sortBy(_.dateTaken.desc).take(n).list
  }

  /** @return The widget snapshot model with the newly generated widget snapshot ID. */
  def insertWidgetSnapshot(snapshot: WidgetSnapshotRow): WidgetSnapshotRow = database withSession { implicit s: Session =>
    (
      configDb.widgetSnapshotQueryForInsert
        returning widgetSnapshotQuery.map(_.id)
        into ((_, id) => snapshot.copy(id = id))
    ) += configDb.widgetSnapshotForInsert(snapshot)
  }
}