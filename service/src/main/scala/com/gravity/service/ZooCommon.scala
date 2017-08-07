package com.gravity.service

import java.io.File

import akka.actor.ActorSystem
import com.gravity.interests.jobs.hbase.HBaseConfProvider
import com.gravity.service.remoteoperations.ServerRegistry
import com.gravity.utilities.components.FailureResult
import com.gravity.utilities.grvakka.Configuration._
import com.gravity.utilities.{HasGravityRoleProperties, ScalaMagic, Settings}
import org.apache.curator.framework.api.UnhandledErrorListener
import org.apache.curator.framework.recipes.locks.InterProcessMutex
import org.apache.curator.framework.{CuratorFramework, CuratorFrameworkFactory}
import org.apache.curator.retry.ExponentialBackoffRetry
import org.apache.zookeeper.KeeperException.NoNodeException

import scalaz.Validation
import scalaz.syntax.validation._

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

/**
 * This trait allows for migration of the ZooCommon object to be testable one method at a time.
 *
 * To enable testing of a ZooCommon method:
 * 1. add the method on this trait.
 * 2. the method to be tested (which calls this method) should be updated to accept this interface as a parameter
 *    instead of statically referring to the implementation.
 * 3. the parameter can be in a separate parameter list with a default to ZooCommon. It can be marked as implicit so
 *    that callers (other than the method to be tested) won't need to be updated.
 *
 * An example of this can be seen on SponsoredPoolSponseeOperations.addSponseeToPool(...)
 */
trait ZooCommonInterface {
  def lock[T](frameworkClient: CuratorFramework, path: String)(work: => T): T
}

object ZooCommon extends HasGravityRoleProperties with UnhandledErrorListener with ZooCommonInterface {
  import com.gravity.logging.Logging._

  val maxDataSize = 1048576
  private var useAws: Boolean = ServerRegistry.isAwsProductionServer

  def isAwsServer : Boolean = useAws

  def setAws(): Unit = {
    useAws = true
  }

  val awsZoos: String = HBaseConfProvider.getConf.defaultConf.get("zookeeper.servers.aws", properties.getProperty("zookeeper.servers.aws"))
  val devZoos: String = HBaseConfProvider.getConf.defaultConf.get("zookeeper.servers.dev", properties.getProperty("zookeeper.servers.dev"))

  val instanceName: String = Settings.CANONICAL_HOST_NAME

  val system: ActorSystem = ActorSystem("Zoo", defaultConf)

  //info("Using " + zoos + " as zookeeper servers for curator")
  private val retryPolicy = new ExponentialBackoffRetry(60000, 29) //29 is their hard-coded max

  def buildClient(zoos: String): CuratorFramework = {
    val client = CuratorFrameworkFactory.builder().retryPolicy(retryPolicy).sessionTimeoutMs(240000).connectionTimeoutMs(120000).connectString(zoos).build()
    client.getUnhandledErrorListenable.addListener(this)
    client.start()
    client
  }

  lazy val awsClient: CuratorFramework = buildClient(awsZoos)
  private lazy val devClientInstance = buildClient(devZoos)
  private var localClientOption : Option[CuratorFramework] = None

  def devClient: CuratorFramework = localClientOption.getOrElse(devClientInstance)

  def useLocalForDev(withPort: Int): Unit = {
    localClientOption = Some(buildClient("localhost:" + withPort))
  }

  def getEnvironmentClient: CuratorFramework = if(isAwsServer) awsClient else devClient

  def unhandledError(s: String, throwable: Throwable) {
    warn("Unhandled curator exception " + s + ": " + ScalaMagic.formatException(throwable))
  }

  def buildLock(frameworkClient: CuratorFramework, path: String): InterProcessMutex = {
    new InterProcessMutex(frameworkClient, path)
  }

  /** Inter-process lock for some work. */
  def lock[T](frameworkClient: CuratorFramework, path: String)(work: => T): T = {
    val theLock = buildLock(frameworkClient ,path)
    theLock.acquire()
    try work finally theLock.release()
  }

  def deleteNodeIfExists(frameworkClient: CuratorFramework = getEnvironmentClient, path: String) : Boolean = {
    try {
      frameworkClient.delete().forPath(path)
      true
    }
    catch {
      case nn: NoNodeException =>
        warn("Tried to delete non-existent node " + path)
        false
      case e: Exception =>
        warn("Exception deleting node: " + ScalaMagic.formatException(e))
        false
    }
  }

  def getNodeIfExists(frameworkClient: CuratorFramework, path: String) : Option[Array[Byte]] = {
    try {
      val ret = frameworkClient.getData.forPath(path)
      Option(ret)
    }
    catch {
      case nn: NoNodeException => None //could do an exists check, but that would be two calls and introduce a race, so might as well do this
      case e: Exception => warn(e, "Exception getting zookeeper node " + path)
        None
    }
  }

  def createNode(frameworkClient: CuratorFramework = getEnvironmentClient, path: String, bytes: Array[Byte]): Validation[FailureResult, String] = {
    if (bytes.length > maxDataSize) {
      FailureResult("Data for node " + path + " is " + bytes.length + " bytes, which exceeds the zookeeper maximum of " + maxDataSize).failure
    }
    else {
      try {
        val stat = frameworkClient.checkExists().forPath(path)
        if (stat == null) {
          frameworkClient.create().creatingParentsIfNeeded().forPath(path, bytes).success
        }
        else {
          FailureResult("Node " + path + " already exists").failure
        }
      }
      catch {
        case e: Exception => FailureResult("Exception creating node " + path, e).failure
      }
    }
  }

  def createOrUpdateNode(frameworkClient: CuratorFramework = getEnvironmentClient, path: String, bytes: Array[Byte]): Validation[FailureResult, String] = {

    if (bytes.length > maxDataSize) {
      FailureResult("Data for node " + path + " is " + bytes.length + " bytes, which exceeds the zookeeper maximum of " + maxDataSize).failure
    }
    else {
      try {
        val stat = frameworkClient.checkExists().forPath(path)
        if (stat == null) {
          frameworkClient.create().creatingParentsIfNeeded().forPath(path, bytes).success
        }
        else {
          val result = frameworkClient.setData().forPath(path, bytes)
          result.getCversion.toString.success
        }
      }
      catch {
        case e: Exception => FailureResult("Exception creating node " + path, e).failure
      }
    }
  }


  def setTimeStamp(frameworkClient: CuratorFramework, path: String, millis: Long) : Validation[FailureResult, String] = {
    createOrUpdateNode(frameworkClient, path, longToBytes(millis))
  }

  def getTimeStamp(frameworkClient: CuratorFramework, path: String): Option[Long] = {
    getNodeIfExists(frameworkClient, path).map(bytesToLong)
  }

  def getChildPaths(frameworkClient: CuratorFramework, parentPath: String): List[String] = {
    try {
      import scala.collection.JavaConversions._
      val pathToUse = {
        if(parentPath.endsWith("/"))
          parentPath.substring(0, parentPath.length - 1)
        else
          parentPath
      }
      val pathToPrepend = {
        if(parentPath.endsWith("/"))
          parentPath
        else
          parentPath + "/"
      }

      frameworkClient.getChildren.forPath(pathToUse).toList.map(pathToPrepend + _)
    }
    catch {
      case e: Exception =>
        warn("Exception getting child folders of " + parentPath + ": " + ScalaMagic.formatException(e))
        List.empty
    }
  }

  def cleanLockFolder(frameworkClient: CuratorFramework, path: String): Unit = {
    try {
      val pathToUse = {
        if(path.endsWith("/"))
          path.substring(0, path.length - 1)
        else
          path
      }
      info("Cleaning lock folder " + pathToUse)
      var deletedLocks = 0
      import scala.collection.JavaConversions._
      val zooKeeper = frameworkClient.getZookeeperClient.getZooKeeper
      val lockPath = "/GrvZooCommon/cleaning" + pathToUse
      val lock = buildLock(frameworkClient, lockPath)

      try {
        lock.acquire()
        info("Got cleaning lock " + lockPath)
        zooKeeper.getChildren(pathToUse, false).sorted.foreach(child => {
          val currentPath = new File(pathToUse, child).toString

          val stat = zooKeeper.exists(currentPath, false)

          val ageMs = System.currentTimeMillis() - stat.getCtime
          val ageDays = new org.joda.time.Duration(ageMs).getStandardDays
          if (ageDays > 1) {
            info("Deleting lock node " + currentPath + " because it is " + ageDays + " days old.")
            ZooCommon.deleteNodeIfExists(frameworkClient, currentPath)
            deletedLocks += 1
          }
          else {
            trace("Not deleting lock node " + currentPath + " because it is only " + ageDays + " days old.")
          }
        })
      }
      finally {
        lock.release()
      }
      info("Done cleaning lock folder " + pathToUse + ", deleted " + deletedLocks + " lock nodes.")
    }
    catch {
      case e: Exception =>
        warn("Exception cleaning lock folder " + path + ": " + ScalaMagic.formatException(e))
    }
  }

  private def longToBytes(v: Long) : Array[Byte] = {
    val bytes = new Array[Byte](8)
    bytes(0) = (v >>> 56).toByte
    bytes(1) = (v >>> 48).toByte
    bytes(2) = (v >>> 40).toByte
    bytes(3) = (v >>> 32).toByte
    bytes(4) = (v >>> 24).toByte
    bytes(5) = (v >>> 16).toByte
    bytes(6) = (v >>> 8).toByte
    bytes(7) = (v >>> 0).toByte
    bytes
  }

  private def bytesToLong(bytes: Array[Byte]) : Long = {
    (bytes(0).toLong << 56) +
      ((bytes(1) & 255).toLong << 48) +
      ((bytes(2) & 255).toLong << 40) +
      ((bytes(3) & 255).toLong << 32) +
      ((bytes(4) & 255).toLong << 24) +
      ((bytes(5) & 255) << 16) +
      ((bytes(6) & 255) << 8) +
      ((bytes(7) & 255) << 0)
  }

}
