package com.gravity.service.remoteoperations

import java.util.concurrent.CountDownLatch

import akka.actor.{Actor, ActorRef, Props}
import akka.routing.RandomGroup
import com.gravity.service._
import com.gravity.utilities.GrvConcurrentMap
import com.gravity.utilities.actor.CancellableWorkerPool
import com.gravity.utilities.components.FailureResult
import com.gravity.utilities.network.SocketSettings
import org.joda.time.DateTime

import scala.actors.threadpool.locks.ReentrantReadWriteLock
import scala.collection._
import scala.concurrent.forkjoin.ThreadLocalRandom
import scalaz.syntax.validation._
import scalaz.{Failure, Validation, ValidationNel}

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

case class EndpointCreate(roleData: RoleData, endpointMap: GrvConcurrentMap[String, RemoteEndpoint], doneLatch: CountDownLatch, now: DateTime)

class EndpointCreatorActor(roleEndpoints: RoleEndpoints) extends Actor {
  import com.gravity.logging.Logging._

  override def receive: Receive = {
    case EndpointCreate(roleData, endpointMap, doneLatch, now) =>
      //info("creating endpoint on thread " + Thread.currentThread().getName())
      if (roleData.lastHeardFrom.getMillis > now.minusMinutes(EphemeralZooJoiner.updateIntervalMinutes * 2).getMillis) {
        val endpoint = new TCPRemoteEndpoint(roleEndpoints, roleData.serverAddress, roleData.serverName, roleData.remoteOperationsPort, roleData.socketSettings, roleData.serverIndex)
        endpointMap.update(endpoint.key, endpoint)
      }
      else {
        info("Ignoring server " + roleData.serverName + " in role " + roleEndpoints.roleName + " because its last heard from is " + roleData.lastHeardFrom)
      }
      doneLatch.countDown()
  }
}

class RoleEndpoints(val roleName: String, roleProvider: RoleProvider) {
  import com.gravity.service.remoteoperations.RemoteOperationsClient.system
  import com.gravity.utilities.Counters._
  import com.gravity.logging.Logging._

  val counterCategory = "Remote Ops Client"

  val creators: ActorRef = {
    val actors = for(i <- 0 until 5) yield {
      val actor = system.actorOf(Props(new EndpointCreatorActor(this)))
      actor
    }
    system.actorOf(RandomGroup(actors.map(_.path.toString)).props())
  }

  private final val activeLock = new ReentrantReadWriteLock()
  private final val writeLock = activeLock.writeLock()
  private final val readLock = activeLock.readLock()

  private final val all: concurrent.Map[String, RemoteEndpoint] = buildEndpoints()
  final protected[remoteoperations] lazy val asyncRequestPool : CancellableWorkerPool = CancellableWorkerPool("RemoteOpsRole-" + roleName, all.size * SocketSettings.default.socketPoolSize)
  private final var sortedActive: immutable.IndexedSeq[RemoteEndpoint] = buildSortedActiveEndpoints()
  private final var activeSize: Int = sortedActive.size

  private def noServerFailure() = {
    countPerSecond(counterCategory, "No servers in role: " + roleName)
    warn("[ROC] There were no active servers in the " + roleName + " role for this server to communicate with!")
    FailureResult("There were no active services available in the " + roleName + " role.").failure
  }

  private def noServerRequestFailure[T]() = {
    countPerSecond(counterCategory, "No servers in role: " + roleName)
    warn("[ROC] There were no active servers in the " + roleName + " role for this server to communicate with!")
    FailureResult("There were no active services available in the " + roleName + " role to request from").failureNel[T]
  }

  def send(msg: MessageWrapper) : Validation[FailureResult, MessageWrapper] = {
    val endpointOpt = msg.sentToModOpt match {
      case Some(modId) => getModdedActiveEndpoint(modId, msg.reservedServersOpt)
      case None => getRandomActiveEndpoint
    }

    endpointOpt match {
      case Some(endpoint) =>
        endpoint.sendOneWay(msg)
      case None =>
        noServerFailure()
    }
  }

  def request[T](msg: MessageWrapper, timeOut: scala.concurrent.duration.Duration)(implicit m: Manifest[T]): ValidationNel[FailureResult, T] = {
    val endpointOpt = msg.sentToModOpt match {
      case Some(modId) => getModdedActiveEndpoint(modId, msg.reservedServersOpt)
      case None => getRandomActiveEndpoint
    }

    endpointOpt match {
      case Some(endpoint) =>
        endpoint.sendToForReply[T](msg, timeOut)
      case None =>
        noServerRequestFailure[T]()
    }
  }


  def resend(msg: MessageWrapper): Validation[FailureResult, MessageWrapper] = {
    Failure(FailureResult("not done"))
  }

  def disconnectAll() {
    writeLock.lock()
    try {
      all.values.foreach(_.disconnect())
      resetActiveEndpoints(false)
    }
    finally {
      writeLock.unlock()
    }
  }

  def disconnect(endpoint: RemoteEndpoint) {
    writeLock.lock()
    try {
      endpoint.disconnect()
      resetActiveEndpoints(false)
    }
    finally {
      writeLock.unlock()
    }
  }

  protected[remoteoperations] def resetActiveEndpoints(lock: Boolean) {
    if(lock) {
      writeLock.lock()
      try {
        sortedActive = buildSortedActiveEndpoints()
        activeSize = sortedActive.size
      }
      finally {
        writeLock.unlock()
      }
    }
    else {
      sortedActive = buildSortedActiveEndpoints()
      activeSize = sortedActive.size
    }
  }

  private def buildSortedActiveEndpoints() = {
    readLock.lock()
    try {
      val currentAll = all.values
      if(currentAll == null || currentAll.isEmpty) {
        immutable.IndexedSeq.empty[RemoteEndpoint]
      }
      else {
        currentAll.toList.filter(_.isConnected).sortWith((a, b) => {
          //we want the clients to have things in the same order so routing is consistent!
          if(a.index == b.index) {
            if (a.hostAddress == b.hostAddress) {
              a.port < b.port
            }
            else {
              a.hostAddress < b.hostAddress
            }
          }
          else {
            a.index < b.index
          }
        }).toIndexedSeq
      }
    }
    finally {
      readLock.unlock()
    }
  }

  private def buildEndpoints() = {
    //info("Building endpoints for " + roleName)
    val roleDaters = roleProvider.getInstancesByRoleName(roleName)

    val roleEndpoints = new GrvConcurrentMap[String, RemoteEndpoint]()
    val now = new DateTime()
    val createLatch = new CountDownLatch(roleDaters.size)
    roleDaters.foreach(roleData => {
      creators ! EndpointCreate(roleData, roleEndpoints, createLatch, now)
    })
    createLatch.await()
    //info("Connecting to servers in role " + roleName)
    val connectLatch = new CountDownLatch(roleEndpoints.size)
    roleEndpoints.values.foreach(_.asyncConnect(connectLatch))
    connectLatch.await()
    roleEndpoints
  }

  protected[remoteoperations] def update(event: RoleUpdateEvent) {
    val roleData = event.data
    val port = roleData.remoteOperationsPort
    val key = RemoteEndpoint.getKey(roleData.serverName, roleData.remoteOperationsPort, roleName)

    event.updateType match {
      case RoleUpdateType.ADD =>
        val existingOption = try {
          readLock.lock()
          all.get(key)
        }
        finally {
          readLock.unlock()
        }

        existingOption match {
          case Some(existing) =>
            if (existing.isConnected)
              warn("Got an add for remote endpoint " + key + " that is already connected.")
            else {
              info("Got an add for remote endpoint " + key + " that was previously connected. Reconnecting.")
              if(existing.isConnected || existing.connect())
                resetActiveEndpoints(true)
              else
                warn("Failed to reconnect to " + key)
            }
          case None =>
            info("Adding remote endpoint " + key)
            val newEndPoint = new TCPRemoteEndpoint(this, roleData.serverAddress, roleData.serverName, port, roleData.socketSettings, roleData.serverIndex)
            if(newEndPoint.isConnected || newEndPoint.connect()) {
              writeLock.lock()
              try {
                all.update(key, newEndPoint)
                resetActiveEndpoints(false)
              }
              finally {
                writeLock.unlock()
              }
            }
            else {
              warn("Failed to connect to new endpoint " + key)
            }
        }
      case RoleUpdateType.REMOVE =>
        writeLock.lock()
        try {
          all.get(key) match {
            case Some(existing) =>
              info("Removing remote endpoint " + key)
              if (existing.isConnected) existing.disconnect()
              all.remove(key)
              val newActive = sortedActive.filterNot(endpoint => endpoint.key == key)
              sortedActive = newActive
              activeSize = sortedActive.size
            case None => warn("Got remove for nonexistent remote endpoint " + key)
          }
          resetActiveEndpoints(false)
        }
        finally {
          writeLock.unlock()
        }
      case RoleUpdateType.UPDATE =>
        //for this one, we only want a write lock if we're adding a non-existent server, or reconnecting an existing one.
        //the majority of these are going to updates for already connected servers, for which we only need a read lock.
        //right now we assume that updates will not cause a disconnection. this very well could change in the future, especially if we move away from akka remoting

        var change = false //if we need to update the collections

        val existingOption = try {
          readLock.lock()
          all.get(key)
        }
        finally {
          readLock.unlock()
        }

        existingOption match {
          case Some(existing) =>
            change = !existing.isConnected //but we only need to change if it is not connected
          case None =>
            change = true
        }

        if (change) {
          existingOption match {
            case Some(existing) => //just reconnect
              info("Got update for " + key + " that was previously connected. Reconnecting.")
              if(existing.connect())
                resetActiveEndpoints(true) //either way, the active list changed
              else
                warn("Failed to reconnect to " + key)
            case None =>
              info("Got update for " + key + " that is new. Connecting.")
              val newEndPoint = new TCPRemoteEndpoint(this, roleData.serverAddress, roleData.serverName, port, roleData.socketSettings, roleData.serverIndex)
              if(newEndPoint.isConnected || newEndPoint.connect()) {
                writeLock.lock()
                try {
                  all.update(key, newEndPoint)
                  resetActiveEndpoints(false) //either way, the active list changed
                }
                finally {
                  writeLock.unlock()
                }
              }
              else {
                warn("Failed to connect to " + key)
              }
          }
        }
    }
  }

  def getRandomActiveEndpoint: Option[RemoteEndpoint] = {
    var result: Option[RemoteEndpoint] = None
    while (activeSize > 0 && result.isEmpty) {
      val candidate = getRandomMaybeActiveEndpoint
      if (candidate.isConnected) result = Some(candidate)
      else resetActiveEndpoints(true)
    }
    result
  }

  def getAllActiveEndpoints: immutable.IndexedSeq[RemoteEndpoint] = {
    readLock.lock()
    val (connectedActive, needToReset) = try {
      val connectedActive = sortedActive.filter(_.isConnected)
      (connectedActive, connectedActive.size != sortedActive.size)
    }
    finally {
      readLock.unlock()
    }
    if(needToReset) resetActiveEndpoints(true)
    connectedActive
  }

  private def getRandomMaybeActiveEndpoint: RemoteEndpoint = {
    readLock.lock()
    try {
      sortedActive(ThreadLocalRandom.current().nextInt(activeSize))
    }
    finally {
      readLock.unlock()
    }
  }

  def getModdedActiveEndpoint(modId: Long, reservedServersOpt: Option[Int]): Option[RemoteEndpoint] = {
    var result: Option[RemoteEndpoint] = None
    while (activeSize > 0 + reservedServersOpt.getOrElse(0) && result.isEmpty) {
      val candidate = getModdedMaybeActiveEndpoint(modId, reservedServersOpt)
      if (candidate.isConnected) result = Some(candidate)
      else resetActiveEndpoints(true)
    }
    result
  }

  private def getModdedMaybeActiveEndpoint(modId: Long, reservedServersOpt: Option[Int]): RemoteEndpoint = {
    readLock.lock()
    try {
      sortedActive(mod(modId, activeSize, reservedServersOpt.getOrElse(0)))
    }
    finally {
      readLock.unlock()
    }
  }

  private def mod(id: Long, actorSize: Int, reservedServers: Int = 0): Int = {
    scala.math.abs((id %  (actorSize-reservedServers)).toInt) + reservedServers
  }

}