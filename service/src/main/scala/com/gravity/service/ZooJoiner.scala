package com.gravity.service

import java.util.concurrent.TimeUnit

import akka.actor.Cancellable
import com.gravity.service.FieldConverters.{RoleDataConverter, ServerIndexConverter}
import com.gravity.utilities.{Settings, Settings2}
import com.gravity.utilities.components.FailureResult
import org.apache.curator.framework.CuratorFramework
import org.apache.curator.framework.recipes.nodes.PersistentEphemeralNode

import scala.concurrent.duration._
import scalaz.syntax.validation._
import scalaz.{Failure, Success, ValidationNel}
/*
*     __         __
*  /"  "\     /"  "\
* (  (\  )___(  /)  )
*  \               /
*  /               \
* /    () ___ ()    \  erik 10/9/15
* |      (   )      |
*  \      \_/      /
*    \...__!__.../
*/


object AwsEphemeralZooJoiner extends EphemeralZooJoiner {
  override val frameworkClient: CuratorFramework = ZooCommon.awsClient
}

object DevelopmentEphemeralZooJoiner extends EphemeralZooJoiner {
  override val frameworkClient: CuratorFramework = ZooCommon.devClient
}

object EphemeralZooJoiner {
  val basePath : String = "/interest_service_by_role"
  val baseOrderingPath : String = "/interest_service_ordering"
  val updateIntervalMinutes = 5
  def instance: EphemeralZooJoiner = if(ZooCommon.isAwsServer) AwsEphemeralZooJoiner else DevelopmentEphemeralZooJoiner
}

trait EphemeralZooJoiner {
 import com.gravity.logging.Logging._
  import EphemeralZooJoiner._
  import ZooCommon.system
  import system.dispatcher

  val frameworkClient : CuratorFramework

  private var nodeOption : Option[PersistentEphemeralNode] = None

  private var roleOption : Option[ServerRole] = None

  private var updateCancellable : Option[Cancellable] = None

  private var roleDataOption : Option[RoleData] = None

  private def getNodePath(roleData : RoleData) : String = {
    roleOption match {
      case Some(role) =>
        val roleName = {
          if(role.getSettingsRoleName.isEmpty) role.getRoleName else role.getSettingsRoleName
        }
        basePath + "/" + roleName + "/" + roleData.serverName
      case None => throw new Exception("Tried to get service node path without role being set")
    }
  }

  def updateNode(): Unit = {
    roleDataOption.foreach(roleData => {
      roleData.update()
      //println("updating role data to " + roleData)
      val roleBytes = RoleDataConverter.toBytes(roleData)
      nodeOption.foreach(node => node.setData(roleBytes))
    })
  }

  def getServerIndex: ValidationNel[FailureResult,Int]  = {
    roleDataOption match {
      case Some(data) =>
        data.serverIndex.successNel
      case None =>
        FailureResult("Could not get this server's index").failureNel
    }
  }

  def getServerIndex(serverName: String, roleName: String) : ValidationNel[FailureResult,Int] = {
    //get lock on this role's ordering
    val lock = ZooCommon.buildLock(ZooCommon.getEnvironmentClient, "/ServerOrderingLocks/" + roleName)
    try {
      lock.acquire()
      //look up this server name
      if(Settings2.isDevelopmentRole) return (-1).successNel

      ServerOrderingWatcherByRole.instance.getRoleWatcher(roleName) match {
        case Success(roleWatcher) =>

          roleWatcher.getServerIndex(serverName) match {
            case Some(index) =>
              //if found, done
              info("Server " + serverName + " index " + index.serverIndex + " found in zookeeper, reusing.")
              index.serverIndex.successNel
            case None =>
              //if not, get number of servers from aws (throw exception if not aws)
              if (ZooCommon.isAwsServer) {
                val indexNodePath = EphemeralZooJoiner.baseOrderingPath + "/" + roleName + "/" + serverName

                AwsQuerier.getRoleServerCount(roleName) match {
                  case Success(nominalServerCount) =>
                    //now we need the list of servers that are up right now

                    val currentlyUpServers: Iterable[RoleData] = ZooWatcherByRole.instance.getInstancesByRoleName(roleName)
                    val allKnownIndexes: Iterable[ServerIndex] = roleWatcher.getAll

                    val upServerMap: Map[String, Iterable[RoleData]] = currentlyUpServers.map(upServer => {
                      upServer.serverName -> currentlyUpServers
                    }).toMap

                    val upKnownIndexes: Iterable[ServerIndex] = allKnownIndexes.filter(server => {
                      upServerMap.contains(server.serverName)
                    })

                    val requiredIndexes: Set[Int] = (for (i <- 0 until nominalServerCount) yield i).toSet

                    val knownIndexes: Set[Int] = upKnownIndexes.map(index => {
                      index.serverIndex
                    }).toSet

                    val available = requiredIndexes.diff(knownIndexes)

                    if (available.isEmpty) {
                      val newIndex = knownIndexes.max + 1 // add new out of bounds because we don't trust the nominal server count. that needs to be fixed.
                      info("No server indexes were available, so taking out of bounds index " + newIndex + ". This shouldn't happen.")
                      ZooCommon.createOrUpdateNode(path = indexNodePath, bytes = ServerIndexConverter.toBytes(ServerIndex(serverName, newIndex)))
                      newIndex.successNel
                    }
                    else {
                      val index = available.min // replace existing
                      allKnownIndexes.find(_.serverIndex == index) match {
                        case Some(downServerIndex) =>
                          val oldServerNodePath = EphemeralZooJoiner.baseOrderingPath + "/" + roleName + "/" + downServerIndex.serverName
                          ZooCommon.deleteNodeIfExists(path = oldServerNodePath)
                          info("Taking index " + index + ", which was previously held by " + downServerIndex.serverName)
                        case None =>
                          info("Taking index " + index + ", which we thought was previously held by another server, but now we can't find it. This shouldn't happen.")
                      }
                      ZooCommon.createOrUpdateNode(path = indexNodePath, bytes = ServerIndexConverter.toBytes(ServerIndex(serverName, index)))
                      index.successNel
                    }
                  case Failure(fails) =>
                    fails.failure //Improvement: include a failureresult saying why it was being queried
                }
              }
              else {
                FailureResult("Tried to get a server index for a server that does not already have an ordering entry, and is not AWS. I don't know what to tell you.").failureNel
              }

          }
        case Failure(fails) =>
          FailureResult("Could not get server ordering watcher for role " + roleName + ": " + fails.toString()).failureNel
      }
    }
    finally {
      lock.release()
    }

  }

  def start(serverRole: ServerRole): Unit = {
    //Improvement: consolidate the name/rolename logic
    val serverIndex = getServerIndex(Settings.CANONICAL_HOST_NAME, if(serverRole.getSettingsRoleName.isEmpty) serverRole.getRoleName else serverRole.getSettingsRoleName) match {
      case Success(si) => si
      case Failure(fails) =>
        warn("Failed to get server index: " + fails.toString())
        -1
    }

    val roleData = RoleData(serverRole, serverIndex)

    roleOption = Some(serverRole)
    roleDataOption = Some(roleData)
    val roleBytes = RoleDataConverter.toBytes(roleData)

    val node = new PersistentEphemeralNode(frameworkClient, PersistentEphemeralNode.Mode.EPHEMERAL, getNodePath(roleData), roleBytes )
    node.start()
    if(!node.waitForInitialCreate(60, TimeUnit.SECONDS)) {
      throw new Exception("Service discovery node not created within 60 seconds")
    }
    nodeOption = Some(node)
    updateCancellable = Some(system.scheduler.schedule(updateIntervalMinutes minutes, updateIntervalMinutes minutes)(updateNode()))
    if(roleData.remoteOperationsPort != 0)
      info("Role " + roleData.roleName + " listening with new TCP Server on port " + roleData.remoteOperationsPort + " for  " + roleData.remoteOperationsMessageTypes.mkString(" & "))
  }


  def stop(): Unit = {
    info("Disconnecting service discovery zookeeper node.")
    updateCancellable.foreach(_.cancel())
    nodeOption.foreach(_.close())
  }
}

