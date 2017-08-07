package com.gravity.service

import java.util

import com.gravity.service.FieldConverters.ServerIndexConverter
import com.gravity.utilities._
import com.gravity.utilities.components.FailureResult
import org.apache.curator.framework.CuratorFramework
import org.apache.curator.framework.recipes.cache.{ChildData, PathChildrenCache}

import scala.Predef._
import scala.collection.JavaConversions._
import scala.collection._
import scalaz.syntax.validation._
import scalaz.{Failure, Success, ValidationNel}

/*
*     __         __
*  /"  "\     /"  "\
* (  (\  )___(  /)  )
*  \               /
*  /               \
* /    () ___ ()    \  erik 8/15/16
* |      (   )      |
*  \      \_/      /
*    \...__!__.../
*/

case class ServerIndex(serverName: String, serverIndex: Int)

class ServerOrderingWatcher(client: CuratorFramework, roleName: String) {
 import com.gravity.logging.Logging._
  private val registry = new GrvConcurrentMap[String, ServerIndex]()

  private lazy val cache = buildCache()

  private def buildCache() = {
    val cache = new PathChildrenCache(client, EphemeralZooJoiner.baseOrderingPath + "/" + roleName, true)
    cache.start(PathChildrenCache.StartMode.BUILD_INITIAL_CACHE)
    cache
  }

  def getServerIndex(serverName: String): Option[ServerIndex] = registry.get(serverName)

  def getAll: scala.Iterable[ServerIndex] = registry.values

  private def buildData = {
    val data = cache.getCurrentData

    info("ZooWatcher sees " + data.size + " servers in role " + roleName + " ordering")

    data.map(item => {
      ServerIndexConverter.getInstanceFromBytes(item.getData) match {
        case Success(serverIndex: ServerIndex) => {
          info("server:  " + serverIndex.serverName + " index: " + serverIndex.serverIndex)
          registry.put(serverIndex.serverName, serverIndex)
        }
        case Failure(fails) =>
          warn("Failed deserializing role index: " + fails.toString)
      }
    })

    data
  }

  buildData
}

trait ServerOrderingWatcherByRole {
 import com.gravity.logging.Logging._
  val frameworkClient: CuratorFramework

  private def roleNameFromPath(path: String): String = {
    grvstrings.tokenize(path, "/").last
  }

  def getRoleNames: Iterable[String] = {
    roles.toSeq
  }

  private val roles = mutable.Set[String]()

  private val rolesCaches = new GrvConcurrentMap[String, ServerOrderingWatcher]()

  private lazy val rolesCache = buildCache(frameworkClient)

  def getRoleWatcher(roleName: String) : ValidationNel[FailureResult, ServerOrderingWatcher] = {
    try {
        rolesCaches.getOrElseUpdate(roleName, {
          val watcher = new ServerOrderingWatcher(frameworkClient, roleName)
          watcher
        }).successNel
    }
    catch {
      case e:Exception =>
        FailureResult("Exception querying for server ordering in role " + roleName).failureNel

    }
  }

  private def buildCache(client: CuratorFramework) = {
    val cache = new PathChildrenCache(client, EphemeralZooJoiner.baseOrderingPath, true)
    cache.start(PathChildrenCache.StartMode.BUILD_INITIAL_CACHE)
    cache
  }

  private lazy val currentData = buildData

  private def buildData = {
    val data = rolesCache.getCurrentData

    info("ZooWatcher sees " + data.size + " roles for ordering")

    data.map(item => {
      roles += roleNameFromPath(item.getPath)
    })

    data
  }

  def getCurrentData: util.List[ChildData] = {
    currentData
  }

}

object AwsZooOrderingWatcherByRole extends ServerOrderingWatcherByRole {
  override val frameworkClient: CuratorFramework = ZooCommon.awsClient
  getCurrentData
}

object DevelopmentZooOrderingWatcherByRole extends ServerOrderingWatcherByRole {
  override val frameworkClient: CuratorFramework = ZooCommon.devClient
  getCurrentData
}

object ServerOrderingWatcherByRole {
  private var useAws = ZooCommon.isAwsServer

  def setAws(): Unit = {
    useAws = true
  }

  def instance: ServerOrderingWatcherByRole = if (useAws) AwsZooOrderingWatcherByRole else DevelopmentZooOrderingWatcherByRole
}
