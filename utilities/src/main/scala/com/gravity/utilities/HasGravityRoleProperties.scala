package com.gravity.utilities

trait HasGravityRoleProperties {
  val properties : GravityRoleProperties = Settings.getProperties
}