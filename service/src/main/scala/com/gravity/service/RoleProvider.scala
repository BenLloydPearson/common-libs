package com.gravity.service

import com.gravity.utilities.ScalaMagic
import scala.collection.mutable

object RoleUpdateType extends Enumeration {
  type Type = Value
  val ADD: RoleUpdateType.Value = Value(0)
  val REMOVE: RoleUpdateType.Value = Value(1)
  val UPDATE: RoleUpdateType.Value = Value(2)

}

case class RoleUpdateEvent(data: RoleData, updateType: RoleUpdateType.Type)

trait RoleProvider {
 import com.gravity.logging.Logging._
  val typeMapper: remoteoperations.TypeMapper

  protected val callbacks = mutable.Buffer[(RoleUpdateEvent) => Unit]()

  def getInstancesByRoleName(roleName: String): Iterable[RoleData]

  def addCallback(callback: (RoleUpdateEvent) => Unit) {
    callbacks += callback
  }

  protected def doCallbacks(event: RoleUpdateEvent) {
    try {
      trace("Calling " + callbacks.size + " callbacks for registry update")
      callbacks.foreach {
        _ (event)
      }
    }
    catch {
      case (e: Exception) => {
        info("got exception " + ScalaMagic.formatException(e) + " doing callbacks during build registry")
      }
    }
  }
}
