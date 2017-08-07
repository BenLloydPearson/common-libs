package com.gravity.service.remoteoperations

import akka.actor.{ActorRef, Actor, ActorSystem, Props}
import com.gravity.utilities.grvfields.FieldConverter
import com.gravity.utilities.Crementable
import akka.routing.RandomGroup
import com.typesafe.config.{Config, ConfigFactory}
import com.gravity.utilities.grvakka.MeteredMailboxExtension

import scala.collection.immutable.IndexedSeq

trait ComponentActor extends Actor {
  import com.gravity.utilities.Counters._

  private var componentOption : Option[ServerComponent[_ <: ComponentActor]] = None

  def setComponent(component: ServerComponent[_  <: ComponentActor]): Unit = {
    componentOption = Some(component)
  }

  def getPayloadObjectFrom(messageWrapper: MessageWrapper): Any = componentOption.get.getPayloadObjectFrom(messageWrapper)
  def handleMessage(messageWrapper: MessageWrapper)
  def handlePayload(payload: Any) { } //override to work with sendToComponent
  def receive: PartialFunction[Any, Unit] = {
    case w: MessageWrapper =>
      handleMessage(w)
      if(w.componentHandlingCount.decrementAndGet() == 0) {
        componentOption.foreach(component => {
          component.getServer.foreach(server => {
            val endMs = System.currentTimeMillis()
            val processingMs = endMs - w.processingStartMs
            server.messageProcessesCounter.increment
            setAverageCount(server.counterGroupName, "Messages processing ms for payload: " + w.payloadTypeName,  processingMs)
            setAverageCount(server.counterGroupName, "Messages processing ms", processingMs)
            server.onPostHandleMessage(w)
          })
        })
      }
    case p:Any => handlePayload(p)
  }
}

class ServerComponent[T <: ComponentActor](componentName: String, val messageTypes : Seq[Class[_]] = Seq.empty[Class[_]], numActors : Int = 12, numThreads : Int = 4, rate : Int = 1, mailboxCapacity : Int = 50000, messageConverters : Seq[FieldConverter[_]] = Seq.empty[FieldConverter[_]], pushTimeOutMs: Int = 0 )(implicit m: scala.reflect.Manifest[T]) {
 import com.gravity.logging.Logging._
  val dispatcherName: String = componentName + "-dispatcher"
  val perActorCapacity: Int = mailboxCapacity / numActors
  val messageTypeNames : Set[String] = messageTypes.map(messageType => messageType.getCanonicalName).toSet
  protected[remoteoperations] val messageConverterMap : Map[String, FieldConverter[_]] = {for(messageConverter <- messageConverters) yield messageConverter.getCategoryName -> messageConverter}.toMap

  private var serverOption : Option[RemoteOperationsServer] = None

  def setServer(server: RemoteOperationsServer): Unit = {
    serverOption = Some(server)
  }

  def getServer = serverOption

  //lazy, to allow for override
  lazy val conf: Config = ConfigFactory.load(ConfigFactory.parseString(
    dispatcherName + """
    {
      type = Dispatcher
      mailbox-type = "com.gravity.utilities.grvakka.BoundedMeteredFILOMailboxType"
      mailbox-capacity = """ + perActorCapacity + """
      mailbox-push-timeout-time = """ + pushTimeOutMs + """
      executor = "fork-join-executor"
      throughput = """ + rate + """
      fork-join-executor {
        parallelism-min = """ + numThreads + """
        parallelism-factor = 64
        parallelism-max = """ + numThreads + """
      }
    }
    akka {
      daemonic = on
      log-dead-letters-during-shutdown = off
    }
                                               """))

  //private val typesHandled = new GrvConcurrentMap[String, Boolean]()
  implicit val system: ActorSystem = ActorSystem("Component-" + componentName.replace(" ", "-"), conf)
  val actors: IndexedSeq[ActorRef] = buildActorPool(numActors)

  def name: String = { componentName }

  private def buildActorPool(numActors : Int) = {
    def makeActor() = {
      val actor = m.runtimeClass.newInstance().asInstanceOf[ComponentActor]
      actor.setComponent(this)
      actor
    }

    val actors = for(i <- 0 until numActors) yield {
      system.actorOf(Props(makeActor()).withDispatcher(dispatcherName), "componentActor" + i)
    }

    //println("built actor pool with actors: " + actors.mkString(", "))

    actors
  }

  def setMailboxCounter(counter: Crementable) {
    actors.foreach(actor => {
      MeteredMailboxExtension.setCounter(actor, counter)
    })
  }

  val balancer: ActorRef = system.actorOf(RandomGroup(actors.map(_.path.toString)).withDispatcher(dispatcherName).props(), "componentBalancer")
  //val balancer = system.actorOf(Props.empty.withRouter(RandomRouter(actors)).withDispatcher(dispatcherName), "componentBalancer")
  val blah = "blah"
  ServerRegistry.registerComponent(this)

  start()

  def handleMessage(message : MessageWrapper) {
    balancer ! message
  }

  def handlesType(messageType : String): Boolean = messageTypeNames.isEmpty || messageTypeNames.contains(messageType)

  def fieldConverterFor(fieldCategory: String) : Option[FieldConverter[_]] = messageConverterMap.get(fieldCategory)

  def start() {} //override to perform custom start up action

  final def stop() {
    customStop()
    system.shutdown()
  }

  def customStop() { //override to perform any custom shutdown action
  }

  def getPayloadObjectFrom(messageWrapper: MessageWrapper): Any = {
    messageWrapper.getPayloadObject(messageConverterMap.get(messageWrapper.fieldCategory))
  }
}











