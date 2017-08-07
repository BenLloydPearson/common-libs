package com.gravity.domain

import com.gravity.utilities.components.FailureResult
import com.gravity.valueclasses.ValueClassesForDomain.SiteGuid
import play.api.libs.json.{Format, Json}

import scalaz.ValidationNel
import scalaz.syntax.validation._

/**
 * Created by tdecamp on 11/30/15.
 * {{insert neat ascii diagram here}}
 */
case class DashboardUser(
  id: Long, // this is the Roost DB id
  firstName: String,
  lastName: String,
  email: String,
  disabled: Boolean,
  isHidden: Boolean,
  accountLocked: Boolean,
  assignedSites: Option[List[SiteGuid]] = None,
  roles: Option[List[String]] = None,
  pluginIds: Option[List[Int]] = None)

object DashboardUser {

  implicit val jsonFormat: Format[DashboardUser] = Json.format[DashboardUser]

  val example: DashboardUser = DashboardUser(9000l, "John", "Smith", "jsmith@gravity.com", false, false, false,
    assignedSites = Some(
      List(SiteGuid("e9fce3f7001d9d32fe584d8b6b309439"), SiteGuid("fca4fa8af1286d8a77f26033fdeed202"))),
    roles = Some(List("role1", "role2")),
    pluginIds = Some(List(456, 789)))

  val exampleLite: DashboardUser = DashboardUser(9000l, "John", "Smith", "jsmith@gravity.com", false, false, false)

}

case class UpdatedDashboardUser(oldUser: DashboardUser, newUser: DashboardUser) {

  def isValid: ValidationNel[FailureResult, Boolean] = {
    if (oldUser.id == newUser.id) true.successNel
    else FailureResult(s"oldUser.id ${oldUser.id} and newUser.id ${newUser.id} do not match").failureNel
  }

}

object UpdatedDashboardUser {

  implicit val jsonFormat: Format[UpdatedDashboardUser] = Json.format[UpdatedDashboardUser]

  val example: UpdatedDashboardUser = UpdatedDashboardUser(DashboardUser.example, DashboardUser.example.copy(email = "JOHNSMITH@gravity.com"))

}