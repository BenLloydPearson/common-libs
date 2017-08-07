package com.gravity.service

import java.net.{InetAddress, NetworkInterface}

import com.gravity.service.remoteoperations.TestMapper
import com.gravity.utilities.network.SocketSettings
import com.gravity.utilities.{GrvConcurrentMap, Settings, Settings2}
import org.joda.time.DateTime

import scala.collection._

object RoleDataTesting extends App {
//  NetworkInterface.getNetworkInterfaces.foreach(nic => {
//    if(!nic.isLoopback && nic.isUp) {
//      println(nic)
//      nic.getInetAddresses.foreach(address => {
//        if(address)
//        println(address)
//      }
//      )
//    }
//  })

  println(InetAddress.getLocalHost.getHostAddress)


}

object RoleData {
  def apply(serverRole: ServerRole, serverIndex: Int) : RoleData = {
    val (port, messages) = serverRole.getRemoteOperationsServerOpt match {
      case Some(server) =>
        val messageTypes = {
          for {
            component <- server.getComponents
          } yield component.messageTypeNames }.flatten.toList

        (server.tcpPort, messageTypes.toSet.toList)
      case None => (0, List.empty[String])
    }

    val roleName = if(serverRole.getSettingsRoleName.isEmpty) serverRole.getRoleName else serverRole.getSettingsRoleName

    RoleData(Settings.CANONICAL_HOST_NAME, getIp, roleName, new DateTime, new DateTime, Settings.APPLICATION_BUILD_NUMBER, port, messages, serverIndex)
  }

  def getIp : String = {
    val hostName = Settings.CANONICAL_HOST_NAME
    if(hostName.toLowerCase.startsWith("ip")) {
      //aws gets host names like ip-xxx-xxx-xxx-xxx
      hostName.split('-').tail.mkString(".")
    }
    else {
      import JavaConversions._
      NetworkInterface.getNetworkInterfaces.foreach(nic => {
        if(!nic.isLoopback && nic.isUp) {
          nic.getInetAddresses.foreach(address => {
            val addrStr = address.toString.replace("/","")
            if(!addrStr.contains(":"))
              return addrStr
          }
          )
        }
      })
      InetAddress.getLocalHost.getHostAddress //fallback in case we don't find a local ipv4 address. but dns is shady so we prefer to look at the actual nics.
    }
  }

  lazy val defaultSocketSettings =
    Settings2.getBooleanOrDefault("service.roledata.defaultSocketSettings", default = false)
}

case class RoleData(serverName: String = Settings.CANONICAL_HOST_NAME,
                    serverAddress: String = RoleData.getIp,
                    roleName: String,
                    startTime: DateTime = new DateTime(),
                    var lastHeardFrom: DateTime = new DateTime(),
                    buildNumber: Int = Settings.APPLICATION_BUILD_NUMBER,
                    remoteOperationsPort: Int = 0,
                    remoteOperationsMessageTypes : Seq[String] = Seq.empty,
                    serverIndex: Int
                    ) {

  def update(): Unit = {
    lastHeardFrom = new DateTime()
  }

  //@Erik this means “when talking to roleName from here, use which settings?"
  def socketSettings : SocketSettings = {
    roleName match {
      case grvroles.URL_VALIDATION if !RoleData.defaultSocketSettings => SocketSettings.defaultWithTenSockets
      case grvroles.RECO_STORAGE if !RoleData.defaultSocketSettings => SocketSettings.defaultWith128Sockets
      case grvroles.USERS_ROLE if !RoleData.defaultSocketSettings => SocketSettings.defaultWith20Sockets
      case _ => SocketSettings.default
    }
  }

}

case class RoleDataJson(server: String, roles: Seq[String], buildNumber: Int)

object RoleDataJson {
  def apply(rd: RoleData): RoleDataJson = RoleDataJson(rd.serverName, Seq(rd.roleName), rd.buildNumber)
}

case class RoleDataGroupedByRole(role: String, serverToBuild: Map[String, Int], buildToPercentage: Map[String, Double])

object RoleDataGroupedByRole {

  def apply(rds: Iterable[RoleData]): Iterable[RoleDataGroupedByRole] = {
    val groupedBeforePercentages = (for {
      rd <- rds
      role = rd.roleName
      unwrapped = new RoleDataGroupedByRole(role, Map(rd.serverName -> rd.buildNumber), Map.empty)
    } yield unwrapped).groupBy(_.role).mapValues {
      _.reduce[RoleDataGroupedByRole] {
        case (lft, rgt) => lft.copy(serverToBuild = lft.serverToBuild ++ rgt.serverToBuild)
      }
    }.values

    groupedBeforePercentages.map(
      grouped => grouped.copy(
        buildToPercentage = grouped.serverToBuild.groupBy(_._2.toString).mapValues(_.size.toDouble / grouped.serverToBuild.size)
      )
    )
  }
}

object testparse extends App {
  val ip = "IP-10-116-180-221".split('-').tail.mkString(".")
  println(ip)
}

object TestRoleProvider extends RoleProvider {
  val typeMapper: TestMapper.type = TestMapper
  val registry: GrvConcurrentMap[String, RoleData] =  new GrvConcurrentMap[String, RoleData]
  val instancesByRoleName: GrvConcurrentMap[String, scala.IndexedSeq[RoleData]] = new GrvConcurrentMap[String, IndexedSeq[RoleData]]

  def addData(role : RoleData) {
    registry.update(role.serverName, role)
    doCallbacks(RoleUpdateEvent(role, RoleUpdateType.ADD))
  }

  def removeData(role : RoleData) {
    registry.remove(role.serverName)
    doCallbacks(RoleUpdateEvent(role, RoleUpdateType.REMOVE))
  }

  def rebuild() {
    //manually managed, not much to do here
  }

  def getInstancesByRoleName(roleName: String): scala.IndexedSeq[RoleData] = {
    instancesByRoleName.getOrElseUpdate(roleName, registry.map{case (serverName: String, roleData: RoleData) => roleData }.filter(_.roleName == roleName).toIndexedSeq)
  }
}
