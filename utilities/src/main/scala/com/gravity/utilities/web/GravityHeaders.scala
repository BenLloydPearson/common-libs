package com.gravity.utilities.web

import com.gravity.utilities.Settings
import com.gravity.utilities.grvstrings._
import org.scalatra.ScalatraServlet

trait GravityHeaders {
  this: ScalatraServlet =>

  before() {
    val isDebugRequest = params.getBoolean("debug") getOrElse false
    val isHostRespondingRole = GrvResponseHeaders.rolesToRespondWithHostHeader.contains(Settings.APPLICATION_ROLE)
    if (isDebugRequest || isHostRespondingRole) {
      response.setHeader("Grv-Host", Settings.CANONICAL_HOST_NAME)
      response.setHeader("Grv-Role", Settings.APPLICATION_ROLE)
    }
  }

}