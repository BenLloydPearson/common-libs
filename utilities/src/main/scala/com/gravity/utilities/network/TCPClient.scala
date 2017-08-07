package com.gravity.utilities.network

import com.gravity.utilities.components.FailureResult

import scalaz.{Failure, Success, Validation}


/*
*     __         __
*  /"  "\     /"  "\
* (  (\  )___(  /)  )
*  \               /
*  /               \
* /    () ___ ()    \  erik 6/16/15
* |      (   )      |
*  \      \_/      /
*    \...__!__.../
*/


trait TCPDisconnectHandler {
  def handleDisconnect(client: TCPClient)
}

class TCPClient(val hostAddress : String, val hostName : String, val port: Int, settings: SocketSettings = SocketSettings.default, disconnectHandlerOption: Option[TCPDisconnectHandler] = None) {
  import com.gravity.logging.Logging._
  import com.gravity.utilities.Counters._

  val remoteName: String = hostName + ":" + port
  val sendsCounter: PerSecondCounter = getOrMakePerSecondCounter("TCP Client", "Sends to " + remoteName)
  val requestsCounter: PerSecondCounter = getOrMakePerSecondCounter("TCP Client", "Requests to " + remoteName)
  val socketPool : SocketPool = SocketPool.getPool(this, hostAddress, hostName, port, settings)

  def notifyDisconnect(): Unit = {
    disconnectHandlerOption.foreach(_.handleDisconnect(this))
  }

  def isConnected : Boolean = {
    socketPool.isConnected
  }

  //If success, returns the  number of bytes written. Which should always be the payload length, but there really isn't much to say about successfully writing
  def send(commandId: Int, payload: Array[Byte]) : Validation[FailureResult, Int] = {
    val envelope = TCPCommon.createEnvelopeFor(commandId, replyExpected = false, payload, 0)
    val socketValidation = socketPool.getSocket
    try {
      socketValidation match {
        case Success(socket) =>
          sendsCounter.increment
          socket.send(envelope)
        case Failure(fails) =>
          val message = "Error getting socket for send to " + remoteName + ": " + fails.toString
          if(!message.contains("Socket Pool is not connected")) warn(SocketErrorMessage(message, remoteName))
          Failure(FailureResult(message))
      }
    }
    finally {
      socketValidation.foreach(socketPool.releaseSocket)
    }
  }

  def request(requestId: Int, payload: Array[Byte], timeoutMs: Int = settings.requestTimeoutMs) : Validation[FailureResult, Array[Byte]]= {
    val envelope = TCPCommon.createEnvelopeFor(requestId, replyExpected = true, payload, timeoutMs)
    val socketValidation = socketPool.getSocket
    try {
      socketValidation match {
        case Success(socket) =>
          try {
            requestsCounter.increment
            socket.request(envelope, timeoutMs)
          }
          catch {
            case e: Exception =>
              val message = "Exception requesting from " + remoteName
              warn(SocketErrorMessage(message, remoteName, Some(e)))
              Failure(FailureResult(message, Some(e)))
          }
        case Failure(fails) =>
          val message = "Error getting socket for request from " + remoteName + ": " + fails.toString
          if(!message.contains("Socket Pool is not connected") && !message.contains("Timed out connecting")) warn(message)
          Failure(FailureResult(message))
      }
    }
    finally {
      socketValidation.foreach(socketPool.releaseSocket)
    }
  }

  def connect(): Boolean = {
    socketPool.connect()
  }

  def disconnect(): Boolean = {
    socketPool.disconnect()
  }
}
