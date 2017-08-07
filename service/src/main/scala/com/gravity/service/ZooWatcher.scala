package com.gravity.service


import java.util

import com.gravity.service.FieldConverters.RoleDataConverter
import com.gravity.service.remoteoperations.{ProductionMapper, RemoteOperationsServer, TypeMapper}
import com.gravity.utilities._
import org.apache.curator.framework.CuratorFramework
import org.apache.curator.framework.recipes.cache.{ChildData, PathChildrenCache, PathChildrenCacheEvent, PathChildrenCacheListener}

import scala.Predef._
import scala.collection.JavaConversions._
import scala.collection._
import scalaz.{Failure, Success}

class RoleWatcher(client: CuratorFramework, roleName: String, typeMapperToUse : TypeMapper) extends PathChildrenCacheListener with RoleProvider {
  import com.gravity.logging.Logging._

  private val registry = new GrvConcurrentMap[String, RoleData]()

  def getInstances: Iterable[RoleData] = registry.values

  override val typeMapper: TypeMapper = typeMapperToUse

  override def getInstancesByRoleName(roleName: String): scala.Iterable[RoleData] = getInstances

  private lazy val cache = buildCache()

  private def buildCache() = {
    val cache = new PathChildrenCache(client, EphemeralZooJoiner.basePath + "/" + roleName, true)
    cache.start(PathChildrenCache.StartMode.BUILD_INITIAL_CACHE)
    cache
  }

  override def childEvent(client: CuratorFramework, pathChildrenCacheEvent: PathChildrenCacheEvent): Unit = {
    this.synchronized {
      import PathChildrenCacheEvent.Type._

      pathChildrenCacheEvent.getType match {
        case CHILD_ADDED =>
          RoleDataConverter.getInstanceFromBytes(pathChildrenCacheEvent.getData.getData) match {
            case Success(newData) =>
              info("Zookeeper node " + newData.serverName + " (" + newData.roleName + ") added.")
              registry.update(newData.serverName, newData)
              doCallbacks(RoleUpdateEvent(newData, RoleUpdateType.ADD))
            case Failure(fails) =>
              warn("Watcher for " + roleName + " got an add event, but could not deserialize the data: " + fails)
          }
        case CHILD_REMOVED =>
          RoleDataConverter.getInstanceFromBytes(pathChildrenCacheEvent.getData.getData) match {
            case Success(newData) =>
              info("Zookeeper node " + newData.serverName + " (" + newData.roleName + ") removed.")
              registry.remove(newData.serverName)
              doCallbacks(RoleUpdateEvent(newData, RoleUpdateType.REMOVE))
            case Failure(fails) =>
              warn("Watcher for " + roleName + " got a remove event, but could not deserialize the data: " + fails)
          }
        case CHILD_UPDATED =>
           RoleDataConverter.getInstanceFromBytes(pathChildrenCacheEvent.getData.getData) match {
             case Success(newData) =>
               trace("Zookeeper node " + pathChildrenCacheEvent.getData.getPath + " (" + newData.roleName + ") updated")
               registry.update(newData.serverName, newData)
               doCallbacks(RoleUpdateEvent(newData, RoleUpdateType.UPDATE))
             case Failure(fails) =>
               warn("Watcher for " + roleName + " got an update event, but could not deserialize the data: " + fails)
           }
        case CONNECTION_RECONNECTED => //trace("Curator's ZooKeeper Client Reconnected")
        case CONNECTION_SUSPENDED => //trace("Curator's ZooKeeper Client Connection Suspended")
        case CONNECTION_LOST => //trace("Curator's ZooKeeper Client Connection Lost")
        case _ => info("Ignored ZooKeeper Curator Path Child Event " + pathChildrenCacheEvent + " occurred. This is new so being verbose about it for now.")
      }
    }
  }

  private def buildData = {
    val data = cache.getCurrentData

    info("ZooWatcher sees " + data.size + " servers in role " + roleName)

    data.map(item => {
      RoleDataConverter.getInstanceFromBytes(item.getData) match {
        case Success(roleData) =>
          registry.put(roleData.serverName, roleData)
        case Failure(fails) =>
          warn("Failed deserializing role data: " + fails.toString)
      }
    })

    cache.getListenable.addListener(this)
    data
  }

  buildData
}

trait ZooWatcherByRole extends RoleProvider with PathChildrenCacheListener {
  import com.gravity.logging.Logging._

  val typeMapper: TypeMapper
  val frameworkClient: CuratorFramework

  def registryChanged(event: RoleUpdateEvent): Unit = {
    doCallbacks(event)
  }

  private def roleNameFromPath(path: String): String = {
    grvstrings.tokenize(path, "/").last
  }

  def getRoleNames: Iterable[String] = {
    roles.toSeq
  }

  private val roles = mutable.Set[String]()

  private val rolesCaches = new GrvConcurrentMap[String, RoleWatcher]()

  private lazy val rolesCache = buildCache(frameworkClient)

  def getAll: Iterable[RoleData] = {
    val all = for (role <- roles.toSeq) yield {
      getInstancesByRoleName(role)
    }

    all.flatten
  }

  def getInstancesByRoleName(roleName: String): Iterable[RoleData] = {
    if (roles.contains(roleName)) {
      rolesCaches.getOrElseUpdate(roleName, {
        val watcher = new RoleWatcher(frameworkClient, roleName, typeMapper)
        watcher.addCallback(registryChanged)
        watcher
      }).getInstances

    }
    else {
      warn("Service discovery queried for non-existent role " + roleName)
      Iterable.empty
    }
  }

  private def buildCache(client: CuratorFramework) = {
    val cache = new PathChildrenCache(client, EphemeralZooJoiner.basePath, true)
    cache.start(PathChildrenCache.StartMode.BUILD_INITIAL_CACHE)
    cache
  }

  override def childEvent(client: CuratorFramework, pathChildrenCacheEvent: PathChildrenCacheEvent): Unit = this.synchronized {
    import PathChildrenCacheEvent.Type._

    pathChildrenCacheEvent.getType match {
      case CHILD_ADDED =>
        val path = roleNameFromPath(pathChildrenCacheEvent.getData.getPath)
        info("Service discovery added role " + path)
        roles += path
      case CHILD_REMOVED =>
        val path = roleNameFromPath(pathChildrenCacheEvent.getData.getPath)
        info("Service discovery removed role " + path)
        roles -= path
      case CHILD_UPDATED =>
      case CONNECTION_RECONNECTED => info("Curator's ZooKeeper Client Reconnected")
      case CONNECTION_SUSPENDED => warn("Curator's ZooKeeper Client Connection Suspended")
      case CONNECTION_LOST => warn("Curator's ZooKeeper Client Connection Lost")
      case _ => info("Ignored ZooKeeper Curator Path Child Event " + pathChildrenCacheEvent + " occurred. This is new so being verbose about it for now.")
    }
  }

  private lazy val currentData = buildData

  private def buildData = {
    val data = rolesCache.getCurrentData

    info("ZooWatcher sees " + data.size + " roles")

    data.map(item => {
      roles += roleNameFromPath(item.getPath)
    })

    rolesCache.getListenable.addListener(this)
    data
  }

  def getCurrentData: util.List[ChildData] = {
    currentData
  }

  //-1 means no servers in role
  def getModForRole(roleName: String, key: Long): Long = {
    val instances = getInstancesByRoleName(roleName)
    val size = instances.size
    if (size == 0) -1
    else math.abs(key % size) + 1
  }

}

object AwsZooWatcherByRole extends ZooWatcherByRole {
  override val typeMapper: TypeMapper = ProductionMapper
  override val frameworkClient: CuratorFramework = ZooCommon.awsClient
  getCurrentData
}

object DevelopmentZooWatcherByRole extends ZooWatcherByRole {
  override val typeMapper: TypeMapper = ProductionMapper
  override val frameworkClient: CuratorFramework = ZooCommon.devClient
  getCurrentData
}

object ZooWatcherByRole {
 import com.gravity.logging.Logging._
  private var useAws = ZooCommon.isAwsServer

  def setAws(): Unit = {
    if(!useAws) {
      info("Setting current ZooKeeper environment to AWS")
      useAws = true
    }
  }

  def instance: ZooWatcherByRole = if (useAws) AwsZooWatcherByRole else DevelopmentZooWatcherByRole
}

object TestRole extends ServerRole {
  override def start(): Unit = {}

  override def getRemoteOperationsServerOpt: Option[RemoteOperationsServer] = None
}

//object ZooApp extends App {
//
//  ProductionZooWatcherByRole.addCallback(reloadRegistry)
//  println(ProductionZooWatcherByRole.getInstancesByRoleName("INTEREST_INTELLIGENCE_OPERATIONS").mkString("\n"))
//
//  scala.io.StdIn.readLine("press enter to exit")
//
//  def reloadRegistry(event: RoleUpdateEvent) {
//    println(event)
//  }
//}

/**
 * Dumps the zookeeper tree
 */
object ZooScan extends App {
 import com.gravity.logging.Logging._
  //import org.apache.zookeeper.ZooKeeper

 // implicit val zooKeeper = ZooCommon.prodClient.getZookeeperClient.getZooKeeper

//  def traverse(path: String)(callback: String => Unit)(implicit zooKeeper: ZooKeeper): Unit = {
//    zooKeeper.getChildren(path, false).sorted.foreach(child => {
//      val currentPath = new File(path, child).toString
//      callback(currentPath)
//      val stat = zooKeeper.exists(currentPath, false)
//      //      val ctime = new DateTime(stat.getCtime)
//      //      val mtime = new DateTime(stat.getMtime)
//
//      //println(ctime)
//      //println(mtime)
//      val lock = ZooCommon.buildLock(ZooCommon.prodClient, currentPath)
//      val participants = lock.getParticipantNodes
//      //      if(participants.size > 0) {
//      //        println(lock.getParticipantNodes.mkString("participants:", ",", ""))
//      //      }
//      //      else {
//      val ageMs = System.currentTimeMillis() - stat.getCtime
//      val ageDays = new org.joda.time.Duration(ageMs).getStandardDays
//      if (ageDays > 1) {
//        println("deleting " + currentPath)
//        ZooCommon.deleteNodeIfExists(ZooCommon.prodClient, currentPath)
//      }
//      else {
//        traverse(currentPath)(callback)
//      }
//      //}
//
//    })
//  }
//
//  traverse("/logStage/processingLocks/RecoFailure")(path => {
//    println(path)
//  })

  //ZooCommon.cleanLockFolder(ZooCommon.prodClient, "/logStage/outputLocks2")
}



