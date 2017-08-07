package com.gravity.utilities.network

import java.net.InetSocketAddress
import java.net.StandardSocketOptions._
import java.nio.channels.{AsynchronousSocketChannel, CompletionHandler}
import java.util.concurrent.{CountDownLatch, ExecutionException, Future, TimeUnit}

import com.gravity.logging.Logstashable
import com.gravity.utilities.ScalaMagic
import com.gravity.utilities.components.FailureResult
import com.gravity.utilities.resourcepool.ByteBufferPools

import scalaz.{Failure, Success, Validation}

/*
*     __         __
*  /"  "\     /"  "\
* (  (\  )___(  /)  )
*  \               /
*  /               \
* /    () ___ ()    \  erik 6/25/15
* |      (   )      |
*  \      \_/      /
*    \...__!__.../
*/

class ManagedSocket(parent: SocketPool, hostAddress: String, hostName: String, port: Int, settings: SocketSettings) {
  import com.gravity.logging.Logging._
  import com.gravity.utilities.Counters._

  private val sendByteBuffers = ByteBufferPools.get(settings.sendBufferSize, 1024, 128)

  private val createdAt : Long = System.currentTimeMillis()

  private val socket = AsynchronousSocketChannel.open()
    .setOption[java.lang.Boolean](TCP_NODELAY, true)
    .setOption[java.lang.Boolean](SO_KEEPALIVE, true)
    .setOption[java.lang.Boolean](SO_REUSEADDR, true)
    .setOption[java.lang.Integer](SO_SNDBUF, settings.sendBufferSize)
    .setOption[java.lang.Integer](SO_RCVBUF, settings.receiveBufferSize)

  private var connected = true
  private val requestLock = new Object //we only support one request at a time pending per socket
  private var replyLockOpt : Option[CountDownLatch] = None //CDLs are one-shot, each request has to create a new one
  private var requestErrorCount = 0 //set to true if something went wrong with getting the response (not whether or not the response was an error)
  private var replaceIfClosed = false

  protected[network] def shouldReplaceIfClosed: Boolean = replaceIfClosed

  protected[network] val remoteName: String = hostName + ":" + port
  private var replyValidationOption : Option[Validation[FailureResult, Array[Byte]]] = None

  private val readHandler = new ManagedSocketReadCompleteHandler(this)

  def isConnected: Boolean = connected

  def age : Long = System.currentTimeMillis() - createdAt

  val timeoutsCounter: PerSecondCounter = getOrMakePerSecondCounter("TCP Client", "Timeouts for " + remoteName)
  val currentRequestErrorsCounter: AverageCounter = getOrMakeAverageCounter("TCP Client", "Queued Errors for " + remoteName)

  protected[network] def postResponse(response: Validation[FailureResult, Array[Byte]]) {
    requestLock.synchronized {
      if (requestErrorCount > 0) {
        //then this is likely a late response to a request, and we don't want to post it because it will be the wrong answer
        trace("Got a response for a request that already errored from " + remoteName)
        currentRequestErrorsCounter.decrement
        requestErrorCount -= 1
      }
      else {
        replyValidationOption = Some(response)
        replyLockOpt.foreach(_.countDown())
      }
    }
  }

  try {
    val connectionFuture: Future[Void] = socket.connect(new InetSocketAddress(hostAddress, port))
    connectionFuture.get(settings.connectTimeoutMs, TimeUnit.MILLISECONDS)
  }
  catch {
    case ee:java.util.concurrent.ExecutionException =>
      socket.close()
      throw ee.getCause
    case e:Throwable =>
      socket.close()
      throw e
  }

  private val state = new ReadState(socket, settings)

  state.read(readHandler)

  trace("Socket connected to " + socket.getRemoteAddress + " (" + remoteName + ") from " + socket.getLocalAddress)



  def close(): Boolean = {
    connected = false

    try {
      state.close() //this closes the socket and releases the read to the pool
      true
    }
    catch {
      case e:Exception =>
        val logMsg = SocketErrorMessage("Exception closing socket to " + remoteName, remoteName, Some(e))
        warn(logMsg)
        false
    }
  }

  def notifyDisconnect(): Unit = {
    if(connected) {
      connected = false
      info("Socket to " + remoteName + " closed")
      parent.notifyDisconnect(this)
    }
  }

  private def doWrite(envelope: Array[Byte])  = {
    val bufferItem = sendByteBuffers.getItem()
    try {
      val sendBuffer = bufferItem.thing
      TCPCommon.bufferedSend(envelope, sendBuffer, socket)
    }
    finally {
      bufferItem.release()
    }
  }

  def request(envelope: Array[Byte], requestTimeoutMs : Int) : Validation[FailureResult, Array[Byte]] = {

    try {
      var replyLock : CountDownLatch = null
      requestLock.synchronized {
        if (replyLockOpt.isDefined) throw new Exception("Request CDL existed before making request. Something went horribly wrong.")
        replyValidationOption = None
        replyLock = new CountDownLatch(1)
        replyLockOpt = Some(replyLock)
      }

      doWrite(envelope)

      if (replyLock.await(requestTimeoutMs, TimeUnit.MILLISECONDS)) {
        requestLock.synchronized {
          replyValidationOption match {
            case Some(replyValidation) =>
              replyValidationOption = None
              replyValidation
            case None =>
              Failure(FailureResult("No reply found"))
          }
        }
      }
      else {
        requestLock.synchronized {
          currentRequestErrorsCounter.increment
          requestErrorCount += 1
          timeoutsCounter.increment
          replaceIfClosed = true
        }
        Failure(FailureResult("Timed out requesting from " + remoteName + " on thread " + Thread.currentThread().getName))
      }
    }
    catch {
      case e: Exception =>
        requestLock.synchronized {
          val message = "Exception requesting from " + remoteName
          val exceptionDesc = describeCommunicationException(e)
          val logMsg = SocketErrorMessage(message + ": " + exceptionDesc._1, remoteName)
          warn(logMsg)
          replaceIfClosed = exceptionDesc._2
          currentRequestErrorsCounter.increment
          requestErrorCount += 1
          close()
          Failure(FailureResult(message, Some(e)))
        }
    }
    finally {
      if (replyLockOpt.isDefined) replyLockOpt = None
    }

  }

  def send(envelope: Array[Byte]) : Validation[FailureResult, Int] = {
    try {
      val written = doWrite(envelope)
      Success(written)
    }
    catch {
      case e: Exception =>
        val message = "Exception sending to " + remoteName
        val exceptionDesc = describeCommunicationException(e)
        val logMsg = SocketErrorMessage(message + ": " + exceptionDesc._1, remoteName)
        warn(logMsg)
        replaceIfClosed = exceptionDesc._2
        if(replaceIfClosed)
          close()
        else
          notifyDisconnect()
        Failure(FailureResult(message, e))
    }
  }

  private def describeCommunicationException(e: Exception) : (String, Boolean) = {
    /*
        write can throw
        WritePendingException - If the channel does not allow more than one write to be outstanding and a previous write has not completed
        NotYetConnectedException - If this channel is not yet connected
        by current design neither of these is possible, so I'll want the whole stack
        getting the future can throw
        CancellationException - if the computation was cancelled
        ExecutionException - if the computation threw an exception
        InterruptedException - if the current thread was interrupted while waiting
        so far all I have observed is an executionexception that contains java.io.IOException: Broken pipe. I'm sure there will be more, but since
        it's IO execution I'm guessing the IOException message will be all we ever need from these. Time will tell....
        todo: figure out which of these are actually recoverable by reconnecting.
     */
    e match {
      case ee: ExecutionException =>
        ee.getCause match {
          case jio: java.io.IOException =>
            val message = jio.getMessage
            message match {
              case "Connection reset by peer" => (message, false)
              case "Broken pipe" => (message, false)
              case _ => (message + ". Reconnecting.", true)
            }
          case _ => (ScalaMagic.formatException(ee.getCause), true)
        }
      case _ => (ScalaMagic.formatException(e), true)
    }
  }
}


class ManagedSocketReadCompleteHandler(socket: ManagedSocket) extends CompletionHandler[Integer, ReadState]{
  import com.gravity.logging.Logging._
  import socket._

  override def completed(bytesRead: Integer, state: ReadState): Unit = {
    if (bytesRead == -1) {
      notifyDisconnect()
    }
    else {
      state.synchronized {
        state.postRead(bytesRead)

        //todo deal with multiple here because it's not the same...
        if (state.messageLength > 0 && state.messagePosition >= state.messageLength + 4) {
          val envelopeOption = try {
            val envelope = TCPCommon.readResponseEnvelope(state.messageStreamOpt.get.underlyingArray, 4)
            Some(envelope)
          }
          catch {
            case e: Exception =>
              val msg = SocketErrorMessage("Exception reading envelope from " + state.remoteName, state.remoteName, Some(e))
              warn(msg)
              None
          }
          finally {
            state.reset()
          }

          envelopeOption foreach (envelope =>
            socket.postResponse(envelope.responseValidation))
        }
        state.read(this)
      }
    }
  }

  override def failed(exc: Throwable, state: ReadState): Unit = {
    exc match {
      case _: java.nio.channels.AsynchronousCloseException => notifyDisconnect()
      case _ =>
    }

    socket.postResponse(Failure(FailureResult("Exception reading from " + remoteName,exc)))
  }
}

case class SocketErrorMessage(message: String, remoteHost: String, exceptionOpt: Option[Throwable] = None) extends Logstashable {
  override def getKVs: Seq[(String, String)] = {
    import com.gravity.logging.Logstashable._
    Seq(
      Message -> message, RemoteHost -> remoteHost
    )
  }

  override def exceptionOption: Option[Throwable] = exceptionOpt
}