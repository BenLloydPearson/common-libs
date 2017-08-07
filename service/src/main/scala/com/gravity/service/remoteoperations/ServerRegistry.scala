package com.gravity.service.remoteoperations

import com.gravity.utilities.{GrvConcurrentMap, Settings, Settings2}

object ServerRegistry {
 import com.gravity.logging.Logging._
  private val serverMap = new GrvConcurrentMap[Int, RemoteOperationsServer]
  private val componentMap = new GrvConcurrentMap[String, ServerComponent[_]]()
  val hostName: String = Settings.CANONICAL_HOST_NAME

  def isProductionServer: Boolean = {
    Settings2.getBooleanOrDefault("com.gravity.settings.force.production", Settings.isProductionServer)
  }

  def isAwsProductionServer: Boolean = {
    Settings.isAws && Settings.isAwsProduction
  }

  // Sends the message to the component, if component registered. Logs warning if component not found.
  def sendToComponent(message: Any, componentName: String): Unit = {
    val componentFound = sendToComponentIfExists(message, componentName)

    if (!componentFound)
      warn("No component '" + componentName + "' found to send message to!")
  }

  // Sends the message to the component, if component registered. Returns true if component found.
  def sendToComponentIfExists(message: Any, componentName: String): Boolean = {
    componentMap.get(componentName) match {
      case Some(component) =>
        component.balancer ! message
        true

      case None =>
        false
    }
  }

  def getComponents: Seq[ServerComponent[_]] = {
    componentMap.values.toSeq
  }

  def stopAll() {
    serverMap.values.foreach(server => server.stop())
    serverMap.clear()
  }

  def registerComponent(component : ServerComponent[_]) {
    componentMap.update(component.name, component)
  }

  def registerServer(server : RemoteOperationsServer) {
    serverMap.update(server.tcpPort, server)
  }

  def unregisterServer(server : RemoteOperationsServer) {
    serverMap.remove(server.tcpPort)
  }

  def isPortUsed(port : Int): Boolean = {
    serverMap.contains(port)
  }


}











