package com.gravity.service

import com.gravity.utilities.Settings

case class RoleMeta(role: String) {
  /** These purposely do not use caching! */
  def servers: Seq[String] = ServerRoleManager.serversInRole(role)
  def isValid: Boolean = servers.nonEmpty
}
object ThisRoleMeta extends RoleMeta(grvroles.firstRole)

case class SingleServerRoleMeta(server: String, roleMeta: RoleMeta) {
  /** These purposely do not use caching! */
  def thisServerIndex: Int = roleMeta.servers.indexOf(server)
  def isValid: Boolean = roleMeta.isValid && thisServerIndex > -1
}
object ThisServerRoleMeta extends SingleServerRoleMeta(Settings.CANONICAL_HOST_NAME, ThisRoleMeta)
