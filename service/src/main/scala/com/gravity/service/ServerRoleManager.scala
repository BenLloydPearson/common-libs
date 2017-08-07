package com.gravity.service

import java.lang.management.ManagementFactory
import java.nio.charset.Charset
import javax.management.ObjectName

import com.gravity
import com.gravity.grvlogging
import com.gravity.interests.jobs.hbase.HBaseConfProvider
import com.gravity.logging.Logging._
import com.gravity.service.remoteoperations.ServerRegistry
import com.gravity.utilities._
import com.gravity.utilities.actor.CancellableWorkerPool
import com.gravity.utilities.cache.{EhCacher, PermaCacher}

object ServerRoleManager {
 import com.gravity.logging.Logging._
  var roleOpt: Option[ServerRole] = None
  var started = false
  val transitionLock: Object = new Object
  val ephZooJoiner: EphemeralZooJoiner = EphemeralZooJoiner.instance

  def isCacheWarm: Boolean = {
    roleOpt match {
      case Some(role) => role.getIsWarm
      case None => false
    }
  }

  /** This purposely does not use caching! */
  def serversInRole(role: String): Seq[String] = {
    val roleData = ZooWatcherByRole.instance.getInstancesByRoleName(role)
    roleData.map(_.serverName).toSeq.sorted
  }

  def start(roleName: String) {
    transitionLock.synchronized {
      if (started) {
        throw new RuntimeException("Attempted to start RoleManager more than once")
      }
      try {
        info("Fired up with default encoding: " + Charset.defaultCharset().name())
        info("Starting Role Now")
        if(!grvroles.isDevelopmentRole && CachingForRole.shouldUseMemCached) {
          info("Forcing grvmemcached to start up: ")
          try {
            val client = grvmemcached.getClient
            client
          } catch {
            case ex: Exception =>
              warn(ex, "Exception whilst initting memcached")
          }
        }
        else {
          info("NOT Forcing grvmemcached to start up")
        }

        if(ServerRegistry.isAwsProductionServer) {
          HBaseConfProvider.setAws()
        }

        if (Settings2.getBooleanOrDefault("service.root.tracelogger.operations", default = false)) {
          gravity.grvlogging.updateLoggerToTrace("com.gravity.interests.jobs.intelligence.operations")
        }
        else if (Settings2.getBooleanOrDefault("service.root.tracelogger", default = false)) {
          gravity.grvlogging.updateLoggerToTrace("com.gravity")
        }

        Settings2.getProperty("service.root.tracelogger.for.packages").filter(_.nonEmpty).foreach(packageString => {
          packageString.split(',').foreach(gravity.grvlogging.updateLoggerToTrace)
        })

        roleOpt = {
          val roleClassName = Settings.getProperty("application.role.type." + roleName, Option(System.getenv("COM_GRAVITY_SETTINGS_ROLE")).getOrElse(""))
          try {
            if (roleClassName.isEmpty) {
              throw new IllegalArgumentException("No RoleManager defined for role name " + roleName)
            }

            val roleInstance = Class.forName(roleClassName).getConstructors()(0).newInstance().asInstanceOf[ServerRole]
            roleInstance.setSettingsRoleName(roleName)
            Some(roleInstance)
          }
          catch {
            case e: Exception => {
              critical("Error starting role " + roleName + " because of exception: " + ScalaMagic.formatException(e))
            }
              EmailUtility.send("development@gravity.com", "alerts@gravity.com", "Failed to start application role: `" + roleName + "` on: " + Settings.CANONICAL_HOST_NAME, ScalaMagic.formatException(e))
              None
          }
        }

        roleOpt.foreach{ role => {
          started = true

          info("Starting role: " + role.getRoleName)
          role.start()
        }}

      }
      catch {
        case e:Exception =>
          val msg = "Could not start: " + ScalaMagic.formatException(e)
          critical(msg)
          EmailUtility.send("development@gravity.com", "alerts@gravity.com", "Failed to start application role on: " + Settings.CANONICAL_HOST_NAME, msg)
      }


      startMonitoring()
      if(roleOpt.nonEmpty) ephZooJoiner.start(roleOpt.get)

    }
  }

  def getRoleName: String = {
    roleOpt match {
      case Some(role) => role.getRoleName
      case None => "No role set"
    }
  }

  def startMonitoring() {
    try {
      ManagementFactory.getPlatformMBeanServer.registerMBean(ServerStatistics, new ObjectName("com.gravity.interests.graphs.service.counters:type=ServerStatisticsMXBean"))
    }
    catch {
      case e:Exception => critical("Exception registering Mbean: " + ScalaMagic.formatException(e))
    }
  }

  def stopMonitoring() {
    try {
      ManagementFactory.getPlatformMBeanServer.unregisterMBean(new ObjectName("com.gravity.interests.graphs.service.counters:type=ServerStatisticsMXBean"))
    }
    catch {
      case e:Exception => critical("Exception removing mbean: " + ScalaMagic.formatException(e))
    }
  }

  def shutdown() {
    transitionLock.synchronized {
      if(started && !grvroles.isDevelopmentRole) {
        ephZooJoiner.stop()
        grvtime.stop()
        roleOpt.foreach{ role => {
          try {
            info("Shutting down role " + role.getRoleName)
            role.stop()
            info("Successfully shut down role " + role.getRoleName)
          }
          catch {
            case e:Exception => critical("Error shutting down role " + role.getRoleName + ": " + ScalaMagic.formatException(e))
          }
        }}
        CancellableWorkerPool.shutdownAll()
        started = false
        stopMonitoring()
      }
      else {
        critical("Tried to stop stopped role manager")
      }

      if(!grvroles.isDevelopmentRole) {
        shutdownZookeeperCurator()
        shutdownAkka()
        shutdownMemcached()
        shutdownCaches()
      }
    }
  }

  def shutdownZookeeperCurator() {

    try {
      info("Shutting down zookeeper curator")
      ZooCommon.getEnvironmentClient.close()
      info("Done: Shutting down zookeeper curator")
    }
    catch {
      case ex: Exception =>
        warn("Exception whilst shutting down memcached : " + ScalaMagic.formatException(ex))
    }
  }

  /**
   * Shut down memcached because it holds open non-daemon threads
   */
  def shutdownMemcached() {
    try {
      info("Shutting down memcached connections")
      grvmemcached.shutdown()
      info("Done: Shutting down memcached connections")
    }
    catch {
      case ex: Exception =>
        warn("Exception whilst shutting down memcached : " + ScalaMagic.formatException(ex))
    }
  }

  def shutdownAkka() {
    try {
      info("Shutting down akka servers")
      ServerRegistry.stopAll()
      info("Done: Shutting down akka servers")
    }catch {
      case e:Exception => critical("Exception while shutting down Akka : " + ScalaMagic.formatException(e))
    }
  }

  def shutdownCaches() {
    try {
      info("Shutting down EhCache and Permacacher")
      EhCacher.shutdown()
      PermaCacher.shutdown()
      info("Done: Shutting down EhCache and Permacacher")
    }
    catch {
      case e:Exception => critical("Exception shutting down caches : " + ScalaMagic.formatException(e))
    }
  }

  def getIsRunning: Boolean = started
}
