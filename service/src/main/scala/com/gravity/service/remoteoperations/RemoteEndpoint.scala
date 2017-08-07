package com.gravity.service.remoteoperations

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, ObjectInputStream}
import java.nio.channels.{ClosedChannelException, NotYetConnectedException}
import java.util.concurrent.CountDownLatch

import akka.actor._
import akka.routing.RandomGroup
import com.gravity.logging.Logstashable
import com.gravity.utilities._
import com.gravity.utilities.actor.CancellableWorkerPool
import com.gravity.utilities.components.FailureResult
import com.gravity.utilities.grvakka.MessageQueueAppendFailedException
import com.gravity.utilities.grvfields.FieldConverter
import com.gravity.utilities.network.{SocketSettings, TCPClient, TCPDisconnectHandler}

import scala.concurrent.duration._
import scalaz.Scalaz._
import scalaz._

/*
*     __         __
*  /"  "\     /"  "\
* (  (\  )___(  /)  )
*  \               /
*  /               \
* /    () ___ ()    \  erik 1/8/14
* |      (   )      |
*  \      \_/      /
*    \...__!__.../
*/

object RemoteEndpoint {
  import com.gravity.logging.Logging._

  def getKey(host: String, port: Int, roleName: String): String = {
    host + ":" + port + " (" + roleName + ")"
  }

  protected[remoteoperations] def serialize(msg:MessageWrapper): Array[Byte] = {
    val bos = new ByteArrayOutputStream(64)
    try {
      msg.writeTo(bos)
      bos.close()
      bos.toByteArray
    }
    catch {
      case e:Exception =>
        warn(e, "Exception serializing message for transport: " + msg.toString)
        throw e
    }
  }

  protected[remoteoperations] def deserialize[T](bytes: Array[Byte], remoteName: String)(implicit m: Manifest[T]) : ValidationNel[FailureResult, T] = {
    //the result should be a byte array that is either 1 followed by a length prefixed objectoutputstream encoding of the object, or 0 and a length prefixed UTF8 encoding of an error message,
    //along with the NEL nonsense.

    val bis = new ByteArrayInputStream(bytes)
    val serializationId = bis.read
    serializationId match {
      case 0 => //error
        val messageLength = VariableLengthEncoding.readIntFromStream(bis)
        val messageBytes = new Array[Byte](messageLength)
        bis.read(messageBytes)
        val message = new String(messageBytes, MessageWrapper.charSetName)
        FailureResult("Error from " + remoteName + ": " + message).failureNel
      case 1 => //java serialized response
        val objLength = VariableLengthEncoding.readIntFromStream(bis)
        val objBytes = new Array[Byte](objLength)
        bis.read(objBytes)
        val in = new ObjectInputStream(new ByteArrayInputStream(objBytes))
        val obj = in.readObject
        in.close()
        if (obj.isInstanceOf[List[_]]) {
          try {
            Failure(obj.asInstanceOf[List[FailureResult]].toNel.get).asInstanceOf[T].successNel
          }
          catch {
            case c: ClassCastException =>
              warn("Tried to cast during deserialize of a list of FailureResults and got exception " + ScalaMagic.formatException(c))
              val boxedClass = {
                val c = m.runtimeClass
                if (c.isPrimitive) toBoxed(c) else c
              }
              require(boxedClass != null)
              boxedClass.cast(obj).asInstanceOf[T].successNel
          }
        }
        else {
          val boxedClass = {
            val c = m.runtimeClass
            if (c.isPrimitive) toBoxed(c) else c
          }
          require(boxedClass != null)
          boxedClass.cast(obj).asInstanceOf[T].successNel
        }
      case 2 => //field serialized response
        val serializationCategory = MessageWrapper.readStringFrom(bis)
        RemoteOperationsHelper.getReplyConverter(serializationCategory) match {
          case Some(converter) =>
            val objectLength = VariableLengthEncoding.readIntFromStream(bis)
            val objectBytes = new Array[Byte](objectLength)
            bis.read(objectBytes)
            converter.asInstanceOf[FieldConverter[T]].getInstanceFromBytes(objectBytes)
          case None =>
            FailureResult("No field serializer found for reply category " + serializationCategory + ". Please ensure that RemoteOperationsHelper.registerReplyConverter is called for this category.").failureNel
        }
    }
  }

  //taken from Future.scala
  private val toBoxed = Map[Class[_], Class[_]](
    classOf[Boolean] -> classOf[java.lang.Boolean],
    classOf[Byte]    -> classOf[java.lang.Byte],
    classOf[Char]    -> classOf[java.lang.Character],
    classOf[Short]   -> classOf[java.lang.Short],
    classOf[Int]     -> classOf[java.lang.Integer],
    classOf[Long]    -> classOf[java.lang.Long],
    classOf[Float]   -> classOf[java.lang.Float],
    classOf[Double]  -> classOf[java.lang.Double],
    classOf[Unit]    -> classOf[scala.runtime.BoxedUnit]
  )
}

abstract class RemoteEndpoint(val hostAddress: String, val hostName: String, val port: Int, val roleName: String, val index: Int) {
  import com.gravity.logging.Logging._
  import Counters._

  val key: String = RemoteEndpoint.getKey(hostName, port, roleName)
  protected val maxSends = 5
  private val counterGroup = "Remote Ops Client for " + roleName
  val sendCounter: PerSecondCounter = getOrMakePerSecondCounter(counterGroup, "Sends -  " + hostName + ":" + port)
  val requestCounter: PerSecondCounter = getOrMakePerSecondCounter(counterGroup, "Requests -  " + hostName + ":" + port)
  val requestTimeoutCounter: PerSecondCounter = getOrMakePerSecondCounter(counterGroup, "Request Timeouts -  " + hostName + ":" + port)
  val requestErrorCounter: PerSecondCounter = getOrMakePerSecondCounter(counterGroup, "Request Errors -  " + hostName + ":" + port)
  val latencyCounter: AverageCounter = getOrMakeAverageCounter(counterGroup, "Request Latency -  " + hostName + ":" + port) //maybe move back to moving avg but the new avg might be good enough
  val sender : ActorRef
  val requester : ActorRef

  def isConnected : Boolean

  protected[remoteoperations] def connect() : Boolean
  protected[remoteoperations] def asyncConnect(cdl: CountDownLatch)
  protected[remoteoperations] def disconnect() : Boolean
  protected[remoteoperations] def doSend(msg: MessageWrapper) : Validation[FailureResult, MessageWrapper]
  protected[remoteoperations] def doRequestOnThreadPool(msg: MessageWrapper, timeout: scala.concurrent.duration.Duration) : ValidationNel[FailureResult, Array[Byte]]
  protected[remoteoperations] def doRequest(msg: MessageWrapper, timeout: scala.concurrent.duration.Duration) : ValidationNel[FailureResult, Array[Byte]]
  protected[remoteoperations] def doRequestAsync(msg: MessageWrapper, timeout: scala.concurrent.duration.Duration, startMillis: Long)// : Validation[FailureResult, Array[Byte]]

  protected def updateMessageTrackingInfo(msg: MessageWrapper) {
    msg.sentToHost = hostAddress
    msg.sentToPort = port
    msg.sendCount += 1
  }

  protected[remoteoperations] def sendOneWay(msg: MessageWrapper): Validation[FailureResult, MessageWrapper] = {
    try {
      if(msg.sendCount > maxSends) {
        RemoteOperationsClient.haltedRedirectsCounter.increment
        val failMessage = "A message destined for " + key + " has exceeded the maximum redirect count! Something is horribly wrong!"
        critical(failMessage)
        Failure(FailureResult(failMessage))
      }
      else {
        try {
          sender ! msg
        }
        catch {
          case m: MessageQueueAppendFailedException =>
            val desc = "Timed out on local queue for " + key + ". Its mailbox is full!"
            val logMsg = RemotingErrorMessage(desc, hostName, roleName)
            warn(logMsg)
            Failure(FailureResult(desc, m))
        }
        Success(msg)
      }
    }
    catch {
      case e: Exception =>
        val desc = "Error while sending message with ! to " + key
        val logMsg = RemotingErrorMessage(desc, hostName, roleName, Some(e))
        warn(logMsg)
        Failure(FailureResult(desc, e))
    }
  }

  protected[remoteoperations] def sendToForReplyAsync[T](msg: MessageWrapper, timeout: Duration)(implicit m: Manifest[T]) { //: ValidationNel[FailureResult, T] = {
    //instead of returning something, use the reply stuff on the message wrapper
    try {
      if (msg.sendCount > maxSends) {
        RemoteOperationsClient.haltedRedirectsCounter.increment
        val failMessage = "A message destined for " + key + " has exceeded the maximum redirect count! Something is horribly wrong!"
        val logMsg = RemotingErrorMessage(failMessage, hostName, roleName)
        critical(logMsg)
        FailureResult(failMessage).failureNel
      }
      else {
        msg.replyExpected = true
        updateMessageTrackingInfo(msg)
        val startStamp = System.currentTimeMillis()
        doRequestAsync(msg, timeout, startStamp)
      }
    }
    catch {
      case i:java.lang.InterruptedException =>
        val desc = "Interrupted while requesting from " + key + ". Likely a thread pool enforced timeout from the caller."
        val logMsg = RemotingErrorMessage(desc, hostName, roleName)
        warn(logMsg)
        FailureResult(desc, i).failureNel
      case e: Exception =>
        val desc = "Error while sending message with ? to " + key
        val logMsg = RemotingErrorMessage(desc, hostName, roleName, Some(e))
        warn(logMsg)
        FailureResult(desc, e).failureNel
    }
  }

  protected[remoteoperations] def sendToForReply[T](msg: MessageWrapper, timeout: Duration)(implicit m: Manifest[T]): ValidationNel[FailureResult, T] = {
    try {
      if (msg.sendCount > maxSends) {
        RemoteOperationsClient.haltedRedirectsCounter.increment
        val failMessage = "A message destined for " + key + " has exceeded the maximum redirect count! Something is horribly wrong!"
        critical(failMessage)
        FailureResult(failMessage).failureNel
      }
      else {
        msg.replyExpected = true
        updateMessageTrackingInfo(msg)
        val startStamp = System.currentTimeMillis()
        doRequestOnThreadPool(msg, timeout) match {
          case Success(resultBytes) =>
            try {
              latencyCounter.set(System.currentTimeMillis() - startStamp)
              RemoteEndpoint.deserialize[T](resultBytes, msg.sentToHost)
            }
            catch {
              case cce: ClassCastException => FailureResult("Result for request to " + key + " returned wrong type: " + cce.getMessage).failureNel
              case e: Exception => FailureResult("Exception requesting response from " + key, Some(e)).failureNel
            }
          case Failure(fails) =>
            fails.failure
        }
      }
    }
    catch {
      case i:java.lang.InterruptedException =>
        val desc = "Interrupted while requesting from " + key + ". Likely a thread pool enforced timeout from the caller."
        val logMsg = RemotingErrorMessage(desc, hostName, roleName)
        warn(logMsg)
        FailureResult(desc, i).failureNel
      case e: Exception =>
        val desc = "Error while sending message with ? to " + key
        val logMsg = RemotingErrorMessage(desc, hostName, roleName, Some(e))
        warn(logMsg)
        FailureResult(desc, e).failureNel
    }
  }



}

object TCPRemoteEndpoint {
  import RemoteOperationsClient.{pinnedDispatcherName, system}
  //Improvement: these should be culled after a period of inactivity, in order to properly support AWS
  private val actors = new GrvConcurrentMap[String, ActorRef]()
  //private val pools = new GrvConcurrentMap[String, CancellableWorkerPool]()
  private val tcpClients = new GrvConcurrentMap[String, TCPClient]()

  def getActor(name: String, instance: TCPRemoteEndpoint) : ActorRef = {
    actors.getOrElseUpdate(name, system.actorOf(Props(EndpointWorkerActor(instance)).withDispatcher(pinnedDispatcherName), name))
  }

  def getPool(name: String, size: Int): CancellableWorkerPool = {
    CancellableWorkerPool(name, size)
  }

  def getTcpClient(forEndpoint: TCPRemoteEndpoint): TCPClient = {
    import forEndpoint._
    tcpClients.getOrElseUpdate(hostName + ":" + port, new TCPClient(hostAddress, hostName, port, socketSettings, Some(forEndpoint)))
  }

}
class TCPRemoteEndpoint(parent: RoleEndpoints, hostAddress: String, hostName: String, port: Int, val socketSettings: SocketSettings, index: Int) extends RemoteEndpoint(hostAddress, hostName, port, parent.roleName, index) with TCPDisconnectHandler {
  import RemoteOperationsClient.{pinnedDispatcherName, system}
  import com.gravity.logging.Logging._

  val client: TCPClient = TCPRemoteEndpoint.getTcpClient(this)
  val sender: ActorRef = TCPRemoteEndpoint.getActor(hostName + ":" + port + ":sender", this)
  val requester: ActorRef = {
    val actors = for(i <- 0 until socketSettings.socketPoolSize) yield {
      val actor = TCPRemoteEndpoint.getActor( hostName + ":" + port + ":requestor" + i, this)
     // MeteredMailboxExtension.setCounter(actor, mailboxCounter)
      actor
    }
    system.actorOf(RandomGroup(actors.map(_.path.toString)).withDispatcher(pinnedDispatcherName).props())
  }
  private val pool: CancellableWorkerPool = TCPRemoteEndpoint.getPool("RemoteOpsEndpoint" + hostName + ":" + port, socketSettings.socketPoolSize)

  override protected[remoteoperations] def connect(): Boolean = client.connect()

  override protected[remoteoperations] def disconnect(): Boolean = client.disconnect()

  override protected[remoteoperations] def asyncConnect(cdl: CountDownLatch): Unit = {
    sender ! cdl
  }

  override protected[remoteoperations] def doSend(msg: MessageWrapper): Validation[FailureResult, MessageWrapper] = {
    if(isConnected) {
      try {
        updateMessageTrackingInfo(msg)
        val result = client.send(0, RemoteEndpoint.serialize(msg))
        sendCounter.increment
        result match {
          case Success(_) => Success(msg)
          case Failure(fails) => fails.failure
        }
      }
      catch {
        case nyce: NotYetConnectedException =>
          disconnect()
          val desc = "Channel to " + key + " was not yet connected. Pulling from list."
          val logMsg = RemotingErrorMessage(desc, hostName, roleName)
          warn(logMsg)
          Failure(FailureResult(desc, nyce))
        case cce: ClosedChannelException =>
          disconnect()
          val desc = "Channel to " + key + " was closed. Pulling from list."
          val logMsg = RemotingErrorMessage(desc, hostName, roleName)
          warn(logMsg)
          Failure(FailureResult(desc, cce))
        case e: Exception =>
          val desc = "Error while sending message with ! to " + key
          val logMsg = RemotingErrorMessage(desc, hostName, roleName, Some(e))
          warn(logMsg)
          Failure(FailureResult(desc, e))
      }
    }
    else { //not connected
      /*
      this will only happen if the endpoint became disconnected between the message being submitted and getting here
      resubmitting the message to the role for sending means it'll end at a node that is now connected. this could
      possibly happen in a loop, but if it's because all of the nodes have become disconnected a no services warning
      will happen
      */
      parent.send(msg)
    }

  }

  override protected[remoteoperations] def doRequestOnThreadPool(msg: MessageWrapper, timeout: scala.concurrent.duration.Duration): ValidationNel[FailureResult, Array[Byte]] = {
    pool.apply(timeout.toMillis.toInt, cancelIfTimedOut = false, additionalFailureInfo = "Request to " + key + " cancelled due to thread pool execution timeout.") {
      doRequest(msg, timeout)
    }
  }

  override protected[remoteoperations] def doRequest(msg: MessageWrapper, timeout: scala.concurrent.duration.Duration): ValidationNel[FailureResult, Array[Byte]] = {
    requestCounter.increment
    val response = client.request(0, RemoteEndpoint.serialize(msg), timeout.toMillis.toInt)
    response match {
      case Failure(fails) if fails.message.startsWith("Timed out requesting from") => requestTimeoutCounter.increment
      case Failure(_) => requestErrorCounter.increment
      case _ => //success
    }
    response.toValidationNel
  }

  override protected[remoteoperations] def doRequestAsync(msg: MessageWrapper, timeout: scala.concurrent.duration.Duration, startMillis: Long): Unit = {
    requester ! AsyncRequest(msg, timeout, startMillis)
  }

  override def isConnected: Boolean = client.isConnected

  override def handleDisconnect(client: TCPClient): Unit = {
    info("Disconnect from " + client.remoteName + ". Removing until zookeeper update.")
    parent.resetActiveEndpoints(true)
  }
}

case class AsyncRequest(message: MessageWrapper, timeout:scala.concurrent.duration.Duration, startMillis: Long )

case class EndpointWorkerActor(endpoint: RemoteEndpoint) extends Actor {
  def receive: PartialFunction[Any, Unit] = {
    case cdl:CountDownLatch =>
      endpoint.connect()
      cdl.countDown()
    case m:MessageWrapper =>
      endpoint.doSend(m)
    case AsyncRequest(message, timeout, startMillis) =>
      val result = endpoint.doRequest(message, timeout)
      result match {
        case Success(resultBytes) =>
          try {
            endpoint.latencyCounter.set(System.currentTimeMillis() - startMillis)
            message.postResponse(resultBytes)
          }
          catch {
            case cce: ClassCastException =>
              message.postResponseFailure(FailureResult("Result for request to " + endpoint.key + " returned wrong type: " + cce.getMessage))
            case e: Exception =>
              message.postResponseFailure(FailureResult("Exception requesting response from " + endpoint.key, Some(e)))
          }
        case Failure(fails) =>
          message.postResponseFailure(fails.head)
      }

  }
}

case class RemotingErrorMessage(message: String, remoteHost: String, remoteRole: String, exceptionOpt: Option[Exception] = None) extends Logstashable {

  override def getKVs: Seq[(String, String)] = {
    import com.gravity.logging.Logstashable._
    Seq(
      Message -> message, RemoteHost -> remoteHost, RemoteRole -> remoteRole
    )
  }

  override def exceptionOption: Option[Throwable] = exceptionOpt

}