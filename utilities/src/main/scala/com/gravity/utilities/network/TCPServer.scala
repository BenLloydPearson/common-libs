package com.gravity.utilities.network

import java.io.IOException
import java.net.StandardSocketOptions._
import java.net._
import java.nio.channels.{AsynchronousServerSocketChannel, AsynchronousSocketChannel, CompletionHandler}

import com.gravity.utilities.components.FailureResult
import com.gravity.utilities.resourcepool.{ByteBufferPool, ByteBufferPools}
import com.gravity.utilities.ScalaMagic

import scalaz.{Failure, Validation}

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

trait TCPHandler {
  def handleCommand(commandId: Int, commandBytes: Array[Byte])
  def handleRequest(requestId: Int, requestBytes: Array[Byte], timeoutMs: Int) : Validation[FailureResult,Array[Byte]]
}


class TCPServer(val listenPort: Int, val handler: TCPHandler, val settings: SocketSettings = SocketSettings.serverDefault) {
 import com.gravity.logging.Logging._
  val stateLock: Object = new Object
  var serverSocketOption : Option[AsynchronousServerSocketChannel] = None
  val acceptHandler: TCPAcceptCompletionHandler = new TCPAcceptCompletionHandler(this)
  val responseByteBuffers: ByteBufferPool = ByteBufferPools.get(settings.sendBufferSize, 1024, 256)

  protected[network] def accept(): Unit = {
    serverSocketOption.foreach {
      serverSocket =>
        try {
          serverSocket.accept(listenPort, acceptHandler)
        }
        catch {
          case e: Exception =>
            warn("Exception during accept on port " + listenPort + ". Stopping server. " + ScalaMagic.formatException(e))
            stop() //better than having a listen socket that isn't responding
            throw e //seriously don't really know to do at this point
        }
    }
  }

  def isRunning: Boolean = serverSocketOption.isDefined

  def start(): Boolean = {
    stateLock.synchronized {
      serverSocketOption match {
        case Some(_) =>
          warn("Tried to start already running TCP server on port " + listenPort)
          false
        case None =>
          try {
            val serverSocket = AsynchronousServerSocketChannel.open().bind(new InetSocketAddress(listenPort)) //Improvement: likely use a specific channel group, configure that!
            serverSocketOption = Some(serverSocket)
            accept()
            info("TCP Server listening on port " + listenPort)
            true
          }
          catch {
            case io:BindException =>
              warn(io, "Bind exception while starting TCP server on port " + listenPort + ":" + io.getMessage + " Is another server already running on that port?")
              false
            case e:Exception =>
              warn(e, "Unknown exception while starting TCP server on port " + listenPort + ".")
              false
          }
      }
    }
  }

  def stop(): Boolean = {
    stateLock.synchronized {
      serverSocketOption match {
        case None =>
          warn("Tried to stop stopped TCP server on port " + listenPort)
          false
        case Some(serverSocket) =>
          try {
            serverSocket.close()
            info("TCP Server on port " + listenPort + " stopped")
            true
          }
          catch {
            case io:IOException =>
              warn(io, "IO exception while stopping TCP server on port " + listenPort + ".")
              false
            case e:Exception =>
              warn(e, "Unknown exception while stopping TCP server on port " + listenPort + ".")
              false
          }
          finally {
            serverSocketOption = None
          }
      }
    }
  }
}


class TCPAcceptCompletionHandler(val server: TCPServer) extends CompletionHandler[AsynchronousSocketChannel, Int]  {
  import server._
  import com.gravity.logging.Logging._

  override def completed(clientChannel: AsynchronousSocketChannel, attachment: Int): Unit = {

    val stateOption =
      try {
        clientChannel.setOption[java.lang.Boolean](TCP_NODELAY, true)
          .setOption[java.lang.Boolean](SO_KEEPALIVE, true)
          .setOption[java.lang.Boolean](SO_REUSEADDR, true)
          .setOption[java.lang.Integer](SO_SNDBUF, settings.sendBufferSize)
          .setOption[java.lang.Integer](SO_RCVBUF, settings.receiveBufferSize)
        Some(new ReadState(clientChannel, server.settings))
      }
      catch {
        case e: Exception =>
          warn(e, "Error creating read state for new connection.")
          None
      }

    stateOption.foreach(state => {
      try {
        trace("Connect from " + state.remoteAddress)
        state.read(new TCPServerReadHandler(server))
      }
      catch {
        case e: Exception =>
          warn(SocketErrorMessage("Exception doing initial read from " + state.remoteAddress +" . Closing socket because state is unknown.", state.remoteName, Some(e)))
          stateOption.foreach(_.close())
      }
    })

    server.accept()
  }

  override def failed(exc: Throwable, attachment: Int): Unit = {
    exc match {
      case _: java.nio.channels.ClosedChannelException =>
      case _ =>
        warn(exc, "Exception accepting connection")
        server.accept()
    }
  }
}

class TCPServerReadHandler(server: TCPServer) extends CompletionHandler[Integer, ReadState] {
 import com.gravity.logging.Logging._
  import com.gravity.utilities.Counters._

  def counterCategory : String = "TCP Server on " + server.listenPort

  override def completed(bytesRead: Integer, state: ReadState): Unit = {
    if (bytesRead == -1) {
      trace("Socket from " + state.remoteAddress + " closed")
      state.close()
    }
    else {
      state.synchronized {
        //println("read " + bytesRead)
        state.postRead(bytesRead)

        while (state.messageLength > 0 && state.messagePosition >= state.messageLength + 4) {
          //println("complete message")
          val envelopeOption = try {
            val envelope = TCPCommon.readEnvelope(state.messageStreamOpt.get.underlyingArray, 4)

            Some(envelope)
          }
          catch {
            case e: Exception =>
              warn("Exception reading envelope from " + state.remoteAddress, state.remoteAddress, Some(e))
              None
          }
          finally {
            state.reset()
          }

          //Improvement: time out response handling. clients won't wait forever! and if this is taking a long time it will cause more requests to time out
          envelopeOption foreach { envelope =>
            if (envelope.replyExpected) {
              countPerSecond(counterCategory, "Requests")
              countPerSecond(counterCategory, "Requests from " + state.remoteHost)

              val responseValidation =
                try {
                  server.handler.handleRequest(envelope.commandId, envelope.payload, envelope.requestTimeoutMs)
                }
                catch {
                  case e: Exception =>
                    warn("Exception processing message", state.remoteAddress, Some(e))
                    Failure(FailureResult("Exception in message handler", e))
                }

              val responseBytes = TCPCommon.createResponseEnvelopeFor(responseValidation)

              val bufferItem = server.responseByteBuffers.getItem()

              try {
                TCPCommon.bufferedSend(responseBytes, bufferItem.thing, state.socket)
              }
              catch {
                case e: Exception =>
                  warn(e, "Exception writing to " + state.remoteAddress + ". Closing socket because state is unknown.")
                  state.close()
              }
              finally {
                bufferItem.release()
              }
            }
            else {
              countPerSecond(counterCategory, "Commands")
              countPerSecond(counterCategory, "Commands from " + state.remoteHost)
              server.handler.handleCommand(envelope.commandId, envelope.payload)
            }
          }
        }
        try {
          state.read(this)
        }
        catch {
          case e: Exception =>
            warn(SocketErrorMessage("Exception reading or responding to request from " + state.remoteAddress + ". Closing socket because state is unknown.", state.remoteName, Some(e)))
            state.close()
        }
      }
    }
  }

  override def failed(exc: Throwable, state: ReadState): Unit = {
    warn(SocketErrorMessage("Exception reading from " + state.remoteAddress, state.remoteName, Some(exc)))
  }
}
