package com.gravity.interests.jobs.intelligence.operations.analytics

/**
 * Created by IntelliJ IDEA.
 * Author: Aaron Hiniker
 * Date: 1/1/13
 * Time: 3:01 PM
 */

import java.sql.ResultSet

import com.gravity.utilities.cache.PermaCacher
import com.gravity.utilities.{MySqlConnectionProvider, RoostConnectionSettings, Settings2}
import com.gravity.valueclasses.ValueClassesForDomain.SiteGuid
import com.gravity.valueclasses.ValueClassesForUtilities.DashboardUserId
import org.joda.time.{Hours, Minutes}
import com.gravity.utilities.grvcoll._
import play.api.libs.json._

object DashboardUserService {
  case class User(id: DashboardUserId, email: String, firstName: Option[String] = None, lastName: Option[String] = None) {
    override def toString: String = Seq(lastName, firstName).flatten.mkString(", ")

    // format User into RFC compliant email, ie: "First Name <email@example.com>" or possibly just "email@example.com"
    def toEmailAddress = Seq(firstName, lastName).flatten.mkString(" ") match {
      case name if name.nonEmpty => name + s" <$email>"
      case _ => email
    }
  }

  object User {
    implicit val jsonFormat: Format[User] = Json.format[User]

    implicit val jsonWriteMap: Writes[Map[DashboardUserId, User]] = Writes[Map[DashboardUserId, User]](
      m => Json.toJson(m.mapKeys(_.raw.toString).toMap)
    )
  }

  object GravityJobsUser extends User(DashboardUserId(-1L), "No Email", Some("GravityJobs"), None)
  object GravityInterestServiceUser extends User(DashboardUserId(Settings2.INTEREST_SERVICE_USER_ID), "No Email", Some("GravityInterestService"), None)
  object GravityBulkImportUser extends User(DashboardUserId(-206L), "No Email", Some("Bulk Import"), None)

  def getUser(id: Long): Option[User] = {
    PermaCacher.getOrRegister(getClass.getSimpleName + ".getUser(" + id + ")", {

      // User id's used by interest service
      id match {
        case GravityJobsUser.id.raw =>
          Some(GravityJobsUser)

        case GravityInterestServiceUser.id.raw =>
          Some(GravityInterestServiceUser)

        case GravityBulkImportUser.id.raw =>
          Some(GravityBulkImportUser)

        case _ =>
          MySqlConnectionProvider.withConnection(RoostConnectionSettings) {
            conn =>
              val statement = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)
              val rs = statement.executeQuery("SELECT id, email, first_name, last_name FROM roost1.users WHERE id = " + id)

              if (rs.next) {
                Some(User(DashboardUserId(rs.getLong("id")), rs.getString("email"), Option(rs.getString("first_name")),
                  Option(rs.getString("last_name"))))
              } else {
                None
              }
          }
      }
    }, Hours.hours(6).toStandardSeconds.getSeconds)
  }

  def getUsersById(ids: Traversable[DashboardUserId]): Map[DashboardUserId, User] = {
    // Normally we would want to fetch these in bulk, but the single method uses PermaCacher so NBD
    ids.flatMap(id => getUser(id.raw).map((id, _))).toMap
  }

  def getManagerForSite(siteGuid: SiteGuid): Option[User] = {
    PermaCacher.getOrRegister(getClass.getSimpleName + ".getManagerForSite(" + siteGuid + ")", {
      MySqlConnectionProvider.withConnection(RoostConnectionSettings) {
        conn =>
          val statement = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)
          val rs = statement.executeQuery(s"SELECT manager_id FROM roost1.sites WHERE site_guid = '${siteGuid.raw}'")

          if (rs.next) {
            getUser(rs.getLong("manager_id"))
          } else {
            None
          }
      }
    }, Minutes.minutes(30).toStandardSeconds.getSeconds)
  }
}
