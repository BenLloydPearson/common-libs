package com.gravity.service.remoteoperations

import com.gravity.service.{RoleUpdateEvent, RoleProvider}
import com.gravity.utilities.GrvConcurrentMap

/*
*     __         __
*  /"  "\     /"  "\
* (  (\  )___(  /)  )
*  \               /
*  /               \
* /    () ___ ()    \  erik 1/9/14
* |      (   )      |
*  \      \_/      /
*    \...__!__.../
*/

class RemoteEndpoints(roleProvider : RoleProvider) {
 import com.gravity.logging.Logging._
  private val byRole = new GrvConcurrentMap[String, RoleEndpoints]()

  roleProvider.addCallback(registryChanged)

  def registryChanged(event: RoleUpdateEvent) {
    trace("got event {0}", event)
    byRole.get(event.data.roleName).foreach(_.update(event))
  }

  def getEndpointsFor(roleName: String) : RoleEndpoints = {
    byRole.getOrElseUpdate(roleName, new RoleEndpoints(roleName, roleProvider))
  }

  def getModdedActiveEndpointFor(roleName:String, modId: Long, reservedServersOpt: Option[Int] ) : Option[RemoteEndpoint] = {
    val res = getEndpointsFor(roleName).getModdedActiveEndpoint(modId, reservedServersOpt)
    res
  }

  def belongsHere(message : MessageWrapper, routeId: Long) : Boolean = {
    message.sentToRole.isEmpty || {
      val endpoints = getEndpointsFor(message.sentToRole)
      endpoints.getModdedActiveEndpoint(routeId, None) match {
        case Some(routedDestination) => routedDestination.hostAddress == message.sentToHost && routedDestination.port == message.sentToPort
        case None => false
      }
    }
    true
  }

  def disconnect(endpoint: RemoteEndpoint) {
    getEndpointsFor(endpoint.roleName).disconnect(endpoint)
  }

  def disconnectAll() {
    byRole.values.foreach(_.disconnectAll())
  }
}

