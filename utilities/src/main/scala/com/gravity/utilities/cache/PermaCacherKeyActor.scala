package com.gravity.utilities.cache

import akka.actor._
import com.gravity.utilities._
import com.typesafe.config.{Config, ConfigFactory}
import scala.concurrent.duration._
import scala.util.Random


/**
 * Created by alsq on 6/30/14.
 */
object PermaCacherKeySystem {
 import com.gravity.logging.Logging._

  val keyConcurrentActors = 3
  def dispatcherName(resourceType: String): String = if(resourceType.isEmpty) "default" else resourceType

  def getSystemConf(resourceType: String): Config = ConfigFactory.load(ConfigFactory.parseString(
    dispatcherName(resourceType) + """
    {
      type = Dispatcher
      mailbox-type = "com.gravity.utilities.grvakka.UnboundedFILOMailboxType"
      executor = "thread-pool-executor"
      thread-pool-executor {
        core-pool-size-min = """ + keyConcurrentActors + """
        core-pool-size-max = """ + keyConcurrentActors + """
      }
      throughput = """ + 1 + """
    }
    akka {
      daemonic = on
      logger-startup-timeout = 30s
      stdout-loglevel = "OFF"
      loglevel = "OFF"
    }
                                                            """))

  private val keySystems = new GrvConcurrentMap[String, ActorSystem]
  val keySystemBaseName = "PermaCacherKeySystem"
  def getKeySystem(resourceType: String): ActorSystem = keySystems.getOrElseUpdate(dispatcherName(resourceType), ActorSystem(keySystemBaseName, getSystemConf(resourceType)))

  def createKeyActor(key: String,  ticks : List[Tick], resourceType: String): ActorRef = {
    val keySystem = getKeySystem(resourceType)
    // safe in a factory object, no leaking closures
    val props = Props(new PermaCacherKeyAgent(ticks)).withDispatcher(dispatcherName(resourceType))

    // REM: Actor names are URIs
    val actorName = try {
      // try being informative
      new java.net.URI(key + "_" +System.currentTimeMillis)
    } catch {
      // most likely an issue with key syntax (not an URI)
      case ex: Exception => new java.net.URI("keyIsBadURI_" + System.currentTimeMillis())
    }
    val childOfKeySystemForKey = try {
      keySystem.actorOf(props, actorName.toASCIIString)
    } catch {
      case ex: InvalidActorNameException =>
        // usually, trouble is from non-unique names, so make sure this time it's different
        val simpleName = new java.net.URI("NU_" + System.currentTimeMillis + "_NU_" + Random.nextInt(Integer.MAX_VALUE))
        trace("trouble creating actor with name {0}, trying instead {1}",actorName.toASCIIString,simpleName.toASCIIString)
        keySystem.actorOf(props, simpleName.toASCIIString)
    }
    childOfKeySystemForKey

  }

  type Tick = (Any, Long)
  val impossibleTick : Tick = ("",-1)

}


abstract class TickingKeyActor(val ticks : List[PermaCacherKeySystem.Tick], val actorSystem : ActorSystem = PermaCacherKeySystem.getKeySystem("")) extends Actor {

  import actorSystem.dispatcher
  import PermaCacherKeySystem.Tick
  import PermaCacherKeySystem.impossibleTick

  def scheduleTick(tick : Tick): Cancellable = actorSystem.scheduler.scheduleOnce(tick._2.seconds, self, tick._1)

  override def preStart() {
    ticks.foreach( scheduleTick(_) )
  }

  // override postRestart so we don't call preStart and schedule a new message
  override def postRestart(reason: Throwable): Unit = {}

  // this way, the mailbox will never be overwhelmed by hang factories
  protected def rescheduleOnce(msg : Any) : Boolean = {
      // send another periodic tick after the specified delay
      val tick = (ticks.find(x => x._1 == msg)) match {
        case Some(tick) => tick
        case None => impossibleTick
      }
      if (impossibleTick!=tick) {
        scheduleTick(tick)
        true
      } else false
  }
}

protected[cache] object PermaCacherKeyAgent {
  import Counters._
  private [cache] val actorNameLogHead = "PermaCacherKeyAgent: "
  private [cache] val strangeness = actorNameLogHead + " received strangeness"
  private [cache] val reloadingMessageIntro = actorNameLogHead + " reloading key: {0}"
  private [cache] val reloadedMessageIntro = actorNameLogHead + " reloaded key: {0}"
  private [cache] val rescheduledMessageIntro = actorNameLogHead + " successfully rescheduled factory op for key: {0}"
  private [cache] val notRescheduledMessageIntro = actorNameLogHead + " failure rescheduling factory op for key: {0} actor: {1}"
  private [cache] val unableMessageIntro = actorNameLogHead + " unable to reload key: {0}"
  private [cache] val reloadingOptionIntro = actorNameLogHead + " reloading with Option for key: {0}"
  private [cache] val reloadedOptionIntro = actorNameLogHead + " reloaded with Option for key: {0}"
  private [cache] val reloadNoneIntro = actorNameLogHead + " reloading with None!" +
    " The previous Some(value) is being overwritten with `None` for key: {0}"
  private [cache] val unableOptionIntro = actorNameLogHead + " with Option, unable to reload key: {0}"
  private [cache] val unableConditionalRetrieve = actorNameLogHead + " with Conditional, unable to examine key: {0} " +
    "(attempting recovery with None)"
  private [cache] val skipCachingMessage = actorNameLogHead+"unable to reload key: {0} because Factory opined " +
    "to disapprove"
  private [cache] val replaceAt0 = "{0}".length
  private [cache] val unableOptionIntroShort = unableOptionIntro.dropRight(replaceAt0)
  private [cache] val unableMessageIntroShort = unableMessageIntro.dropRight(replaceAt0)
  private [cache] val reloadCounter = getOrMakeAverageCounter("PermaCacher", "Reloads")
  private [cache] val reloadNoneCounter = getOrMakeAverageCounter("PermaCacher", "Reload None")
  private [cache] val reloadRejectCounter = getOrMakeAverageCounter("PermaCacher", "Reload Rejections")

  private [cache] val nowRunningForKey = actorNameLogHead + "timed eviction check at UTC {0} for key {1}"
  private [cache] val neverAccessedMessage = actorNameLogHead + "found now-expired never-accessed key {0}"
  private [cache] val evictionMessage = actorNameLogHead + "timed eviction of key {0} at UTC {1}"
  private [cache] val evictionTroubleMessage = actorNameLogHead + "trouble during timed eviction check at UTC {0}"
  private [cache] val evictionInconceivableMessage = actorNameLogHead +
    "Inconceivable! Evictor running for key {0}, but no key entry exists for timestamp in cacheLastAccess"

  private [cache] val evictionCounter = getOrMakeAverageCounter("PermaCacher", "Timed Evictions")
  private [cache] val unusedCounter = getOrMakeAverageCounter("PermaCacher", "Never-used Evictions")
  private [cache] val unusedYetCounter = getOrMakeAverageCounter("PermaCacher", "Never-used Yet")
  private [cache] val runCounter = getOrMakeAverageCounter("PermaCacher", "Elemental Eviction Checks")

  private [cache] val msInTwoHours = 1000*60*60*2
  private [cache] val msTimeToLiveDefault = msInTwoHours
  private [cache] val scheduledEvictionRunIntervalSec = 60+msTimeToLiveDefault/1000

}

sealed abstract class PceTimeToLiveKey()
protected[cache] case class PceTimeToLiveKeyCheckOp
  (key : String, ttlVal: Long = PermaCacherKeyAgent.msTimeToLiveDefault) extends PceTimeToLiveKey()


protected[cache] class PermaCacherKeyAgent(override val ticks : List[PermaCacherKeySystem.Tick]) extends TickingKeyActor(ticks) {
 import com.gravity.logging.Logging._

  import PermaCacherKeyAgent._
  import PermaCacher.listeners
  import PermaCacher.metaOrDefault
  import PermaCacher.cache

  private [this] val firstFound = new GrvConcurrentMap[String, Long]()
  private [this] var isShutdown : Boolean = false

  def isRunning : Boolean = !isShutdown

  val handler: PCAgentHandler = new PCAgentHandler(
    reloadingMessageIntro,
    reloadedMessageIntro,
    unableMessageIntro,
    unableMessageIntroShort,
    reloadCounter,
    reloadRejectCounter,
    reloadingOptionIntro,
    reloadNoneIntro,
    reloadNoneCounter,
    reloadedOptionIntro,
    unableOptionIntro,
    unableOptionIntroShort,
    runCounter,
    unusedCounter,
    evictionCounter,
    nowRunningForKey,
    neverAccessedMessage,
    evictionMessage,
    evictionTroubleMessage,
    evictionInconceivableMessage)

  def receive: PartialFunction[Any, Unit] = {
    case msg@ReloadOp(key, reloadOperation, reloadInSeconds) =>
      try {
        handler.processReloadOp(key,reloadInSeconds,reloadOperation)
      }
      finally {
        val isRescheduled = rescheduleOnce(msg)
        if (isRescheduled) trace(rescheduledMessageIntro,key) else error(notRescheduledMessageIntro,key,self.toString())
      }

    case msg@ReloadOpConditional(key, reloadOperation, reloadInSeconds) =>
      try {
        handler.processReloadOpConditional(key,reloadInSeconds,reloadOperation)
      }
      finally {
        val isRescheduled = rescheduleOnce(msg)
        if (isRescheduled) trace(rescheduledMessageIntro,key) else error(notRescheduledMessageIntro,key,self.toString())
      }

    case msg@ReloadOptionally(key, reloadOperation, reloadInSeconds) =>
      try {
        handler.processReloadOptionally(key,reloadInSeconds,reloadOperation)
      } finally {
        val isRescheduled = rescheduleOnce(msg)
        if (isRescheduled) trace(rescheduledMessageIntro,key) else error(notRescheduledMessageIntro,key,self.toString())
      }

    case msg@PceTimeToLiveKeyCheckOp(key, ttlVal) =>
      try {
        handler.processPceTimeToLiveKeyCheckOp(key, firstFound, ttlVal)
      }
      finally {
        val isRescheduled = rescheduleOnce(msg)
        if (isRescheduled) trace(rescheduledMessageIntro,key) else error(notRescheduledMessageIntro,key,self.toString())
      }

    case "shutdown" =>
      context.stop(self)
      isShutdown = true

    case _ => warn(strangeness)

  }

  override def postStop: Unit = {
    trace("PCActor shut down: "+self.toString())
  }
}

