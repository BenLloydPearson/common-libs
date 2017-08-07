package com.gravity.utilities.network

import java.nio.channels.UnresolvedAddressException
import java.util.concurrent.{Semaphore, TimeUnit}

import com.gravity.utilities.components.FailureResult

import scala.actors.threadpool.locks.ReentrantReadWriteLock
import scalaz.syntax.validation._
import scalaz.{Failure, Success, Validation}


/*
*     __         __
*  /"  "\     /"  "\
* (  (\  )___(  /)  )
*  \               /
*  /               \
* /    () ___ ()    \  erik 7/1/15
* |      (   )      |
*  \      \_/      /
*    \...__!__.../
*/

object SocketPool {
  def getPool(client: TCPClient, hostAddress:String, hostName : String, port: Int, settings: SocketSettings): SocketPool = {
    if(settings.socketPoolSize == 1)
      new OneSocketPool(client, hostAddress, hostName, port, settings)
    else
      new SocketQueue(client, hostAddress, hostName, port, settings)
  }
}

abstract class SocketPool(client: TCPClient, hostAddress: String, hostName: String, port: Int, settings: SocketSettings) {
  import com.gravity.logging.Logging._
  import com.gravity.utilities.Counters._

  val remoteName: String = hostName + ":" + port
  val socketsCounter: AverageCounter = getOrMakeAverageCounter("TCP Client", "Sockets to " + remoteName)
  val activeSocketsCounter: AverageCounter = getOrMakeAverageCounter("TCP Client", "Active Sockets to " + remoteName)

  def isConnected: Boolean
  def connect() : Boolean
  def getSocket : Validation[FailureResult, ManagedSocket]
  def releaseSocket(socket: ManagedSocket)
  def disconnect() : Boolean

  def buildSocket(): Validation[FailureResult, ManagedSocket] = {
    try {
      val socket = new ManagedSocket(this, hostAddress, hostName, port, settings)
      socket.success
    }
    catch {
      case ce: java.net.ConnectException =>
        val message = SocketErrorMessage("Could not connect to " + remoteName + ": " + ce.getMessage, remoteName)
        warn(message)
        FailureResult(message.message, ce).failure
      case t: java.util.concurrent.TimeoutException =>
        val message = SocketErrorMessage("Timed out connecting to " + remoteName, remoteName)
        warn(message)
        FailureResult(message.message, t).failure
      case u: UnresolvedAddressException =>
        val message = SocketErrorMessage("Could not resolve " + remoteName, remoteName)
        warn(message)
        FailureResult(message.message, u).failure
      case e: Exception =>
        val message = SocketErrorMessage("Exception connecting to " + remoteName, remoteName, Some(e))
        warn(e, message.message)
        FailureResult(message.message, e).failure
    }
  }

  def notifyDisconnect(socket: ManagedSocket)
}

class SocketQueue(client: TCPClient, hostAddress: String, hostName: String, port: Int, settings: SocketSettings) extends SocketPool(client, hostAddress, hostName, port, settings) {
  import com.gravity.logging.Logging._
  private val phore = new Semaphore(settings.socketPoolSize)
  private val stateLock = new ReentrantReadWriteLock()
  private val availableSockets = new scala.collection.mutable.Queue[ManagedSocket]()
  private val allSockets = scala.collection.mutable.Set.empty[ManagedSocket]
  private var disconnecting = false
  private var connected = false
  private val useAgeOut = settings.maxSocketAgeMs > 0

  def notifyDisconnect(socket: ManagedSocket): Unit = {
    var disconnected = false
    if(!disconnecting) {
      val lock = stateLock.writeLock()
      lock.lock()
      try {
        allSockets.remove(socket)
        socketsCounter.decrement
        val newAvailableList = availableSockets.toList.filter(_ != socket)
        availableSockets.clear()
        newAvailableList.foreach(availableSockets.enqueue(_))
        if(allSockets.isEmpty) {
          info("Socket pool to " + remoteName  + " is now disconnected")
          disconnected = true
          connected = false
        }
      }
      finally {
        lock.unlock()
      }
      if(disconnected) client.notifyDisconnect()
    }
  }

  def isConnected: Boolean = {
    var ret = false
    val lock = stateLock.readLock()
    lock.lock()
    ret = connected
    lock.unlock()
    ret
  }

  def disconnect(): Boolean = {
    val lock = stateLock.writeLock()
    lock.lock()
    try {
      disconnecting = true
      if (allSockets.nonEmpty) {
        val successes = for {
          socket <- allSockets
        } yield {
          socket.close()
        }
        allSockets.clear()
        availableSockets.clear()
        socketsCounter.set(0)
        activeSocketsCounter.set(0)
        connected = false
        info("Disconnected from " + remoteName)
        !successes.contains(false)
      }
      else {
        warn(SocketErrorMessage("Tried to disconnect unconnected client for " + remoteName, remoteName))
        false
      }
    }
    finally {
      lock.unlock()
    }
  }

  def connect(): Boolean = {
    val lock = stateLock.writeLock()
    lock.lock()
    try {
      if(connected) {
        info("Tried to connect to already connected socket " + remoteName)
        true
      }
      else {
        buildSocket() match {
          case Success(socket) =>
            allSockets.add(socket)
            availableSockets.enqueue(socket)
            socketsCounter.set(1)
            connected = true
            info("Socket pool connected to " + remoteName)
            true
          case Failure(fails) => //build socket logs these nicely already
            trace("Failed to connect to " + remoteName + " :" + fails)
            false
        }
      }
    }
    finally {
      lock.unlock()
    }
  }

  def getSocket: Validation[FailureResult, ManagedSocket] = {
    if (connected) {
      if (phore.tryAcquire(settings.connectTimeoutMs, TimeUnit.MILLISECONDS)) {
        val lock = stateLock.writeLock()
        if (lock.tryLock(settings.connectTimeoutMs, scala.actors.threadpool.TimeUnit.MILLISECONDS)) {
          try {
            if (connected) { //but we might have disconnected during the wait!
              var retOpt: Option[ManagedSocket] = None
              while (retOpt.isEmpty && availableSockets.nonEmpty) {
                val candidate = availableSockets.dequeue()
                if (candidate.isConnected) retOpt = Some(candidate)
              }
              if (retOpt.isEmpty) {
                buildSocket() match {
                  case Success(socket) =>
                    allSockets.add(socket)
                    activeSocketsCounter.increment
                    socketsCounter.increment
                    socket.success
                  case Failure(fails) =>
                    phore.release() //if we're not returning anything release will never be called
                    fails.failure
                }
              }
              else {
                activeSocketsCounter.increment
                if (retOpt.isEmpty) phore.release()
                retOpt.get.success
              }
            }
            else {
              phore.release()
              FailureResult("Socket Pool is not connected to " + remoteName).failure
            }
          }
          finally {
            lock.unlock()
          }
        }
        else {
          //we couldn't get inside the state lock so another connect is likely being slow too
          phore.release()
          FailureResult("Timed out waiting for lock on socket pool to " + remoteName).failure
        }
      }
      else {
        FailureResult("Timed out waiting for socket from pool to " + remoteName).failure
      }
    }
    else {
      FailureResult("Socket Pool is not connected to " + remoteName).failure
    }
  }

  def releaseSocket(socket: ManagedSocket): Unit = {
    phore.release()
    activeSocketsCounter.decrement
    val lock = stateLock.writeLock()
    lock.lock()
    val reconnect: Boolean = try {
      if (socket.isConnected) {
        if (useAgeOut && socket.age > settings.maxSocketAgeMs) {
          trace("Closing connection to " + socket.remoteName + " because this one has aged out.")
          allSockets.remove(socket)
          socket.close()
          false
        }
        else {
          availableSockets.enqueue(socket)
          false
        }
      }
      else if (socket.shouldReplaceIfClosed) {
        allSockets.remove(socket)
        true
      }
      else {
        socketsCounter.decrement
        false
      }
    }
    finally {
      lock.unlock()
    }

    if (reconnect) {
      val newSocketValidation = buildSocket()
      newSocketValidation match {
        case Success(newSocket) =>
          lock.lock()
          try {
            allSockets.add(newSocket)
            availableSockets.enqueue(newSocket)
          }
          finally {
            lock.unlock()
          }
        case Failure(_) =>
          socketsCounter.decrement
      }
    }
  }
}

class OneSocketPool(client: TCPClient, hostAddress: String, hostName: String, port: Int, settings: SocketSettings) extends SocketPool(client, hostAddress, hostName, port, settings) {
  import com.gravity.logging.Logging._

  private val phore = new Semaphore(1)
  private var socketValidation : Validation[FailureResult, ManagedSocket] = FailureResult("Not connected").failure
  private val stateLock = new ReentrantReadWriteLock()

  def notifyDisconnect(socket: ManagedSocket): Unit = {
    if(isConnected) {
      disconnect()
      client.notifyDisconnect()
    }
  }

  def isConnected: Boolean = {
    val lock = stateLock.readLock()
    lock.lock()

    try {
      socketValidation.isSuccess
    }
    finally {
      lock.unlock()
    }
  }

  def disconnect(): Boolean = {
    val lock = stateLock.writeLock()
    lock.lock()
    try {
      socketValidation match {
        case Failure(_) =>
          warn(SocketErrorMessage("Tried to disconnect unconnected client for " + remoteName, remoteName))
          false
        case Success(socket) =>
          val success = socket.close()
          socketValidation = FailureResult("Disconnected").failure
          success
      }
    }
    finally {
      lock.unlock()
    }
  }

  def connect(): Boolean = {
    val lock = stateLock.writeLock()
    lock.lock()
    try {
      socketValidation match {
        case Success(existingSocket) =>
          warn(SocketErrorMessage("Tried to connect to already connected socket " + existingSocket.remoteName, remoteName))
          false
        case Failure(_) =>
          socketValidation = buildSocket()
          if(socketValidation.isSuccess) socketsCounter.set(1)
          socketValidation.isSuccess
      }
    }
    finally {
      lock.unlock()
    }
  }

  def getSocket: Validation[FailureResult, ManagedSocket] = {
    val lock = stateLock.readLock()
    lock.lock()

    try {
      if(socketValidation.isSuccess) {
        phore.acquire()
        activeSocketsCounter.set(1)
        socketValidation
      }
      else {
        FailureResult("Not connected").failure
      }

    }
    finally {
      lock.unlock()
    }
  }

  def releaseSocket(socket: ManagedSocket): Unit = {
    phore.release()
    activeSocketsCounter.set(0)
  }

}