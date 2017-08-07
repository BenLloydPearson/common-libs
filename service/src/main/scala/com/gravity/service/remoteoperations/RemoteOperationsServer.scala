package com.gravity.service.remoteoperations

import java.io.ByteArrayInputStream
import java.util.concurrent.TimeUnit
import akka.actor.{ActorRef, Actor, ActorSystem, Props}
import com.gravity.utilities.components.FailureResult
import com.gravity.utilities.grvfields.FieldConverter
import com.gravity.utilities.network.{TCPServer, TCPHandler}
import com.typesafe.config.{Config, ConfigFactory}
import com.gravity.utilities._
import com.gravity.utilities.grvakka.MessageQueueAppendFailedException
import scalaz.{Validation, Failure, Success}

object RemoteOperationsServer {
 import com.gravity.logging.Logging._
  def getSystemName(listenPort : Int): String = "RemoteOperationsServer-" + listenPort

  def deserialize(bytes: Array[Byte]): Validation[FailureResult, MessageWrapper] = {
    val in = new ByteArrayInputStream(bytes)
    try {
      val wrapper = MessageWrapper.readFrom(in)
      in.close()
      wrapper
    }
    catch {
      case e: java.io.InvalidClassException =>
        warn("Invalid class while deserializing object in RemoteOperationsServerActor: " + e.getMessage)
        throw e
      case e: Exception =>
        warn("Exception deserializing object in RemoteOperationsServerActor: " + ScalaMagic.formatException(e))
        throw e
    }
  }
}

case class ComponentWrapper(server: RemoteOperationsServer, component: ServerComponent[_ <: ComponentActor], distributionActor: ActorRef) {
  component.setServer(server)
}

class RemoteOperationsServer(val tcpPort: Int = 0, val components : Seq[ServerComponent[_ <: ComponentActor]], backlogSize : Int = 4096) {
  import com.gravity.logging.Logging._
  import Counters._
  val tcpServer: TCPServer = new TCPServer(tcpPort, new RemoteOperationsServerHandler(this))
  if(tcpPort > 0) tcpServer.start()

  val dispatcherName: String = RemoteOperationsServer.getSystemName(tcpPort) + "-dispatcher"

  val distributionDispatcherName: String = RemoteOperationsServer.getSystemName(tcpPort) + "-messageDistributor"

  val conf: Config = ConfigFactory.load(ConfigFactory.parseString(
    dispatcherName + """
    {
      type = Dispatcher
      mailbox-type = "com.gravity.utilities.grvakka.BoundedMeteredFILOMailboxType"
      mailbox-capacity = 10000
      mailbox-push-timeout-time = "0ms"
    }
                     """ +
      distributionDispatcherName +  """
    {
      type = PinnedDispatcher
      mailbox-type = "com.gravity.utilities.grvakka.BoundedMeteredFILOMailboxType"
      mailbox-capacity = 10000
      mailbox-push-timeout-time = "0ms"
      executor = thread-pool-executor
    }
    akka {
      daemonic = on
      loglevel = "INFO"
      stdout-loglevel = "INFO"
      log-config-on-start = off
      log-dead-letters-during-shutdown = off
      debug {
        lifecycle = off
        receive = off
        autoreceive = off
      }
    }                                   """
  ))

  implicit val system: ActorSystem = ActorSystem(RemoteOperationsServer.getSystemName(tcpPort), conf)
  val host: String = ServerRegistry.hostName
  def getComponents: Seq[ServerComponent[_ <: ComponentActor]] = { components }
  def getComponentWrappers: Seq[ComponentWrapper] = { componentWrappers }

  var started = false
  val counterGroupName: String = "Remote Operations Server : " + tcpPort

  val componentWrappers: Seq[ComponentWrapper] = components.map(component => {
    val actor = system.actorOf(Props(new RemoteOperationsComponentDistributionActor(component, this)).withDispatcher(distributionDispatcherName), "remoteOperationsComponentDistribution-" + component.name.replace(" ", ""))
    ComponentWrapper(this, component, actor)
  })

  private val allFieldConverters : Map[String, FieldConverter[_]]= //toset is in there to uniquify before creating a map
    {for(component <- components) yield component.messageConverterMap.values}.flatten.toSet.map{converter:FieldConverter[_] => converter.getCategoryName -> converter}.toMap

  def getPayloadObjectFrom(messageWrapper: MessageWrapper): Any = {
    messageWrapper.getPayloadObject(allFieldConverters.get(messageWrapper.fieldCategory))
  }

  val messagesReceivedCounter: PerSecondCounter = getOrMakePerSecondCounter(counterGroupName, "Messages Received")
  val messagesErroredCounter: PerSecondCounter = getOrMakePerSecondCounter(counterGroupName, "Messages Errored")
  val messageProcessesCounter: PerSecondCounter = getOrMakePerSecondCounter(counterGroupName, "Message Processes")

  def onPreHandleMessage(message : MessageWrapper) {}
  def onPostHandleMessage(message : MessageWrapper) {}

  buildCounters()

  def buildCounters() {
    components.foreach(component => {
      val counter = getOrMakeMaxCounter(counterGroupName, "Mailbox Size : " + component.name, shouldLog = true)
      component.setMailboxCounter(counter)
    })
  }

  def start() {
    if(!ServerRegistry.isPortUsed(tcpPort)) {
      ServerRegistry.registerServer(this)
      info("Starting Remote Operations Service on : " + host + ": " + tcpPort)
      started = true
    }
    else
    {
      critical("Attempt to start remote operations server on already used port " + tcpPort)
      //throw an exception i would imagine
    }
  }
  
  def stop() {
    started = false
    components.foreach(component => component.stop())
    ServerRegistry.unregisterServer(this)
    system.shutdown()
    if(tcpPort > 0) tcpServer.stop()
  }
}

class RemoteOperationsComponentDistributionActor(component: ServerComponent[_], parent: RemoteOperationsServer) extends Actor {
 import com.gravity.logging.Logging._
  def receive: PartialFunction[Any, Unit] = {
    case w:MessageWrapper =>
      try {
        component.handleMessage(w)
      }
      catch {
        case full:MessageQueueAppendFailedException =>
          warn("Component " + component.name + " could not enqueue message to mailbox: " + full.getMessage)
          parent.messagesErroredCounter.increment
        case e:Exception =>
          warn("Exception processing message: " + ScalaMagic.formatException(e))
          parent.messagesErroredCounter.increment
      }
  }
}

class RemoteOperationsServerHandler(parent : RemoteOperationsServer) extends TCPHandler {
 import com.gravity.logging.Logging._
  import com.gravity.utilities.Counters._
  def counterCategory = parent.counterGroupName
  private def handleMessage(m: MessageWrapper) {
      try {
        m.processingStartMs = System.currentTimeMillis()
        parent.messagesReceivedCounter.increment
        countPerSecond(counterCategory, "Messages received with payload: " + m.payloadTypeName)
        countPerSecond(counterCategory, "Messages received from: " + m.sentFrom)
        parent.onPreHandleMessage(m)
        var handled = false
        parent.getComponentWrappers.foreach(componentWrapper => {
          if (componentWrapper.component.handlesType(m.payloadTypeName)) {
            m.componentHandlingCount.incrementAndGet()
            handled = true
            componentWrapper.distributionActor ! m
          }
        })
        if (!handled && m.replyExpected)
          m.errorReply("Server at " + m.sentToHost + " in role " + m.sentToRole + " does not have a component for " + m.payloadTypeName)

      }
      catch {
        case e: Exception =>
          warn("Exception processing message: " + ScalaMagic.formatException(e))
          parent.messagesErroredCounter.increment

      }
    }

  override def handleCommand(commandId: Int, commandBytes: Array[Byte]) {
    try {
      RemoteOperationsServer.deserialize(commandBytes) match {
        case Success(messageWrapper) => handleMessage(messageWrapper)
        case Failure(fail) =>
          warn("Could not read message wrapper from byte array: " + fail)
          countPerSecond(counterCategory, "Error processing byte array message")
      }

    }
    catch {
      case e: Exception =>
        warn("Exception processing message received as byte array: " + ScalaMagic.formatException(e))
        countPerSecond(counterCategory, "Error processing byte array message")
    }
  }

  override def handleRequest(requestId: Int, requestBytes: Array[Byte], timeoutMs: Int): Validation[FailureResult, Array[Byte]] = {
    try {
      RemoteOperationsServer.deserialize(requestBytes) match {
        case Success(messageWrapper) =>
          handleMessage(messageWrapper)
          val success = messageWrapper.replyCdl.await(timeoutMs, TimeUnit.MILLISECONDS)
          if(success) {
            messageWrapper.replyOption match {
              case Some(reply) => Success(reply)
              case None => Failure(FailureResult("No response provided"))
            }
          }
          else {
           Failure(FailureResult("Timed out waiting for message processing on server."))
          }
        case Failure(fail) =>
          val message = "Could not read message wrapper from byte array: " + fail
          warn(message)
          countPerSecond(counterCategory, "Error processing byte array message")
          Failure(FailureResult(message))
      }
    }
    catch {
      case e: Exception =>
        warn("Exception processing message received as byte array: " + ScalaMagic.formatException(e))
        countPerSecond(counterCategory, "Error processing byte array message")
        Failure(FailureResult("Exception processing request", e))
    }
  }
}

