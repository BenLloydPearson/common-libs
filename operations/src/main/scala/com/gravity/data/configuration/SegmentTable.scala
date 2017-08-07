package com.gravity.data.configuration

import com.gravity.interests.interfaces.userfeedback.{UserFeedbackPresentation, UserFeedbackVariation}
import org.joda.time.{DateTime, DateTimeZone}
import play.api.libs.json.JsValue

import scala.slick.ast.ColumnOption

/**
 * Created by IntelliJ IDEA.
 * Author: Robbie Coleman
 * Date: 8/8/14
 * Time: 3:27 PM
 *            _ 
 *          /  \
 *         / ..|\
 *        (_\  |_)
 *        /  \@'
 *       /     \
 *   _  /  \   |
 *  \\/  \  | _\
 *   \   /_ || \\_
 *    \____)|_) \_)
 *
 */
trait SegmentTable {
  this: ConfigurationDatabase =>

  import driver.simple._

  class SegmentTable(tag: Tag) extends Table[SegmentRow](tag, "Segment2") {
    implicit val dateTimeMapper = MappedColumnType.base[DateTime, java.sql.Timestamp](
      dateTime => new java.sql.Timestamp(dateTime.withZone(DateTimeZone.UTC).getMillis),
      timestamp => new DateTime(timestamp.getTime, DateTimeZone.UTC)
    )

    def id = column[Long]("id", O.PrimaryKey, O.AutoInc)

    def bucketId = column[Int]("bucketId")

    // the id for display
    def isControl = column[Boolean]("isControl")

    def sitePlacementId = column[Long]("sitePlacementId")

    def isLive = column[Boolean]("isLive")

    def displayName = column[Option[String]]("displayName")

    def minUserInclusive = column[Int]("minUserInclusive")

    def maxUserExclusive = column[Int]("maxUserExclusive")

    def widgetConfId = column[Option[Long]]("widgetId")

    def insertedTime = column[DateTime]("insertedTime")

    def insertedByUserId = column[Long]("insertedByUserId")

    def updatedTime = column[DateTime]("updatedTime")

    def updatedByUserId = column[Long]("updatedByUserId")

    def sitePlacementConfigVersion = column[Long]("sitePlacementConfigVersion")

    def fallbacksLastAttemptTime = column[Option[DateTime]]("fallbacksLastAttemptTime")

    def fallbacksLastSuccessTime = column[Option[DateTime]]("fallbacksLastSuccessTime")

    def fallbacksLastErrorMessage = column[Option[String]]("fallbacksLastErrorMessage")

    def index = column[Int]("index")

    def criteria = column[Option[JsValue]]("criteria")

    /**
      * Maintained as a String and not a UserFeedbackVariation.Type to allow downstream systems to pass through new
      * user feedback variations without needing the most up-to-date code.
      */
    def userFeedbackVariation = column[String]("userFeedbackVariation",
      ColumnOption.Default(UserFeedbackVariation.none.name))

    /**
      * Maintained as a String and not a UserFeedbackPresentation.Type to allow downstream systems to pass through new
      * user feedback presentations without needing the most up-to-date code.
      */
    def userFeedbackPresentation = column[String]("userFeedbackPresentation",
      ColumnOption.Default(UserFeedbackPresentation.none.name))

    def * = (id, bucketId, isControl, sitePlacementId, isLive, minUserInclusive, maxUserExclusive, widgetConfId,
      insertedTime, insertedByUserId, updatedTime, updatedByUserId, sitePlacementConfigVersion,
      fallbacksLastAttemptTime, fallbacksLastSuccessTime, fallbacksLastErrorMessage, displayName, index, criteria,
      userFeedbackVariation, userFeedbackPresentation) <> (SegmentRow.tupled, SegmentRow.unapply)

    def forUpdateFallbackDetails = (fallbacksLastAttemptTime, fallbacksLastSuccessTime, fallbacksLastErrorMessage) <> ({
        tup : (Option[DateTime], Option[DateTime], Option[String]) => FallbackDetails(tup._1, tup._2, tup._3)
      }, {
      (sfd: FallbackDetails) => Some((sfd.lastAttemptTime, sfd.lastSuccessTime, sfd.errorMessage))
    })

    def sitePlacement = foreignKey(tableName + "_sitePlacement_fk", sitePlacementId, SitePlacementTable)(_.id)
    def widgetConf = foreignKey(tableName + "_widgetId_fk", widgetConfId, widgetConfQuery)(_.id)
  }

  val SegmentTable = scala.slick.lifted.TableQuery[SegmentTable]
}
