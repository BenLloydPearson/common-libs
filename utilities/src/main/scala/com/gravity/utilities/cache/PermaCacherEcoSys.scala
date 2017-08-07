package com.gravity.utilities.cache


import java.util.concurrent.atomic.AtomicLong

import akka.actor._
import akka.routing.RoundRobinGroup
import com.gravity.utilities.Counters.CounterT
import com.gravity.utilities.cache.PermaCacher._
import com.gravity.utilities.grvakka.MeteredMailboxExtension
import com.gravity.utilities.{GrvConcurrentMap, ScalaMagic, grvtime}
import com.typesafe.config.{Config, ConfigFactory}
import org.joda.time.format.{DateTimeFormat, DateTimeFormatter}

import scala.collection.immutable.IndexedSeq
import scala.collection.{immutable, mutable}
import scala.concurrent.duration._

/**
 * Created by alsq on 1/14/14.
 */

// ##############################################################################
//
//                    TTL and eviction refactoring
//
// Since PermaCacher is not (almost) read-only any more, but is now a fully fledged
// read-write-evict cache, a snake has entered eden and must be dealt with.  Near-
// immutability and its intrinsic thread-safety has gone the way of innocence.
//
// There is an embedded state machine built from the state of the following Maps:
// cache, cacheLastAccess, cacheKeyMutex, reloadOps
//
// ------------------------------------------------------------------------------
// Their internal collective state must remain consistent at all times.  Here are
// all possible collectively-internally-consistent states.
// ------------------------------------------------------------------------------
//
// LEGEND:
// EMPTY  globally empty, i.e. isEmpty==true
// K-val  there is some value for key K
// K-nil  there is no value for key K (i.e. contains(K) == false)
// K-not  last access time is "never yet" (well-known constant)
// K-inf  last access time is "infinite TTL" (well-known constant)
// K-ts   last access time is an actual timestamp
// K-here lock object for key K exists
// K-fact factory function for key K exists
//
//
// ** global start state
// (EMPTY)      cache EMPTY, cacheLastAccess EMPTY, cacheKeyMutex EMPTY,  reloadOps EMPTY
//
// ** initial state for evictable key
// (KEY-INIT-E) cache K-val, cacheLastAccess K-not, cacheKeyMutex K-here, reloadOps K-fact
//
// ** generic state for persistent key
// (KEY-P)      cache K-val, cacheLastAccess K-inf, cacheKeyMutex K-here, reloadOps K-fact
//
// ** generic state for evictable key
// (KEY-E)      cache K-val, cacheLastAccess K-ts,  cacheKeyMutex K-here, reloadOps K-fact
//
// ** removed (i.e. after eviction)
// (REMOVE)     cache K-nil, cacheLastAccess K-nil, cacheKeyMutex K-nil,  reloadOps K-nil
//
// ------------------------------------------------------------------------------
// here are all verbs to move from one state to another
// ------------------------------------------------------------------------------
// (public) restart
// (public) get(key) <== NOT readonly! may change last-access timestamp
// (public) getOrRegister [with or without Option, irrelevant here] in short getOrR
// (asynchronous) refresh [i.e. refresh from persistent storage] in short refresh
// (asynchronous) evict
//
// ------------------------------------------------------------------------------
// here are all legal transitions
// = is the start, and < or > is the end of a transition. A transition with arrows on
// both ends indicates that no change in state occurred.
// ------------------------------------------------------------------------------
//    EMPTY    KEY-INIT-E   KEY-P      KEY-E     REMOVED
//      =          =          =          =          =
//      =          =          =          =          =
//      =          =          =          =          =
//      V          V          V          V          V
//
//                                ## get family ##
//
// 1|<--get->|
// 2    =--getOrR-->
// 3    =--getOrR------------->
// 4               =--getOrR------------->
// 5               =--get---------------->
// 6                      |<--get->|
// 7                      |<getOrR>|
// 8                                 |<--get->|
// 9                                 |<getOrR>|
// A                                            |<--get->|
// B                          <-----------getOrR----=
// C               <----------------------getOrR----=
//
//                                   ## refresh ##
//
// D           |<rfrsh->|
// E                      |<rfrsh->|
// F                                 |<rfrsh->|
//
//                                   ## restart ##
//
// G    <----------=--restart--=---------=----------=       (4 transitions)
// H|<restart>|
//
//                                    ## evict ##
//
// I               =---evict---=---------=---------->       (3 transitions)
// J    <----------=---evict---=---------=                  (3 transitions)
// K|<-evict>|
// L                                            |<-evict>|
//
// This table lays out 28 transitions
//
// ##############################################################################


// ################################################################################
//
//                  S T A T E    M A C H I N E
//
// ################################################################################

sealed abstract class PsmState(key : String, n : Int) { def id: Int = n }
case class EmptyPsmState(empty : String) extends PsmState(empty, 1) // EMPTY
case class KeyInitEvictablePsmState(eKeyInit : String) extends PsmState(eKeyInit, 2) // KEY-INIT-E
case class KeyEvictablePsmState(eKey : String) extends PsmState(eKey, 3) // KEY-E
case class KeyPermanentPsmState(pKey : String) extends PsmState(pKey, 4) // KEY-P
case class RemovedPsmState(removed: String) extends PsmState(removed, 5) // REMOVED

object PermaCacherPsm {
 import com.gravity.logging.Logging._

  import PermaCacher.{cache, cacheAccessTimeInfiniteTTL, cacheAccessTimeNeverYet, cacheKeyMutex, cacheLastAccess, collectiveMutex, reloadOps}

  //
  // State consistency enforcement
  //

  private [cache] val nameLogHead = "PermaCacherPsm: "
  private [cache] val catKeyMutexMsg = nameLogHead + "entry in cacheKeyMutex not found for key: {0} at UTC timestamp: {1}"
  private [cache] val catKeyMutexMissing = nameLogHead + "(INTERNAL) missing (expected but not found) entry in cacheKeyMutex for key: {0} at UTC timestamp: {1}"
  private [cache] val catKeyMutexStray = nameLogHead + "(INTERNAL) stray (found but not expected) entry in cacheKeyMutex for key: {0} at UTC timestamp: {1}"
  private [cache] val catTimestampMissing = nameLogHead + "(INTERNAL) missing (expected but not found) entry in cacheLastAccess for key: {0} at UTC timestamp: {1}"
  private [cache] val catTimestampStray = nameLogHead + "(INTERNAL) stray (found but not expected) entry in cacheLastAccess for key: {0} at UTC timestamp: {1}"
  private [cache] val catCacheMissing = nameLogHead + "(INTERNAL) missing (expected but not found) entry in cache for key: {0} at UTC timestamp: {1}"
  private [cache] val catMissingKeyEvicted = nameLogHead + "(INTERNAL) key {0] for missing entry was evicted at UTC timestamp: {1}"
  private [cache] val catMissingKeyNotEvicted = nameLogHead + "(INTERNAL) failure (will try again) evicting key {0] for missing entry at UTC timestamp: {1}"
  private [cache] val catCacheStray = nameLogHead + "(INTERNAL) stray (found but not expected) entry in cache for key: {0} at UTC timestamp: {1}"
  private [cache] val catFactoryMissing = nameLogHead + "(INTERNAL) missing (expected but not found) entry in reloadOps for key: {0} at UTC timestamp: {1}"
  private [cache] val catFactoryStray = nameLogHead + "(INTERNAL) stray (found but not expected) entry in reloadOps for key: {0} at UTC timestamp: {1}"
  private [cache] val illegalStateMsg = nameLogHead + "(INTERNAL) embedded state machine is corrupt"

  private[cache] def stateBeforeGetOrRegister(key : String): Option[PsmState] = {
    val (isConsistent, state) = isConsistentBeforeGetOrRegister(key)
    if (!isConsistent) throw new IllegalStateException(illegalStateMsg)
    Some(state)
  }

  private[cache] def getPsmState( key : String ) : PsmState = {

    def getPsmState_(key : String, isInCache : Boolean, isInCkm : Boolean, isInReloadOps: Boolean, timeStamp : Option[AtomicLong]) : PsmState = {
      val retVal : PsmState = if (isInCache && isInCkm && isInReloadOps) timeStamp match {
        case Some(ts) =>
          if (cacheAccessTimeNeverYet == ts.get()) KeyInitEvictablePsmState(key)
          else if (cacheAccessTimeInfiniteTTL == ts.get()) KeyPermanentPsmState(key)
          else KeyEvictablePsmState(key)
        case None =>
          error(catTimestampMissing,key,System.currentTimeMillis.toString)
          throw new IllegalStateException(illegalStateMsg)
      } else if (!(isInCache || isInCkm || isInReloadOps)) timeStamp match {
        case Some(ts) =>
          error(catTimestampStray,key,System.currentTimeMillis.toString)
          throw new IllegalStateException(illegalStateMsg)
        case None =>
          if (cache.isEmpty && cacheLastAccess.isEmpty && cacheKeyMutex.isEmpty && reloadOps.isEmpty && tickingActors.isEmpty) EmptyPsmState(key)
          else RemovedPsmState(key)
      } else {
        if (isInCache) {
          error(catCacheStray,key,System.currentTimeMillis.toString)
        } else if (isInCkm) {
          error(catKeyMutexStray,key,System.currentTimeMillis.toString)
        } else if (isInReloadOps) {
          error(catFactoryStray,key,System.currentTimeMillis.toString)
        } else timeStamp match {
          case Some(ts) =>
            error(catTimestampStray,key,System.currentTimeMillis.toString)
            throw new IllegalStateException(illegalStateMsg)
          case None =>
        }
        throw new IllegalStateException(illegalStateMsg)
      }
      retVal
    }

    val ckmHolder = cacheKeyMutex.get(key)
    ckmHolder match {
      case Some(ckm) => ckm.synchronized {
        collectiveMutex.synchronized {
          getPsmState_(key, cache.contains(key), isInCkm = true, reloadOps.contains(key) || tickingActors.contains(key), cacheLastAccess.get(key))
        }
      }
      case None => collectiveMutex.synchronized {
        getPsmState_(key, cache.contains(key), isInCkm = false, reloadOps.contains(key) || tickingActors.contains(key), cacheLastAccess.get(key))
      }
    }
  }

  private[cache] def isPmsStateEmpty : Boolean = {
    collectiveMutex.synchronized {
      cache.isEmpty && cacheLastAccess.isEmpty && cacheKeyMutex.isEmpty && reloadOps.isEmpty && tickingActors.isEmpty
    }
  }

  // ---------------------------------------------------------------------------------
  // ** get ** transition contracts
  //
  // get can start from any state
  private[cache] def isConsistentBeforeGet(key : String) : (Boolean, PsmState) = (true, getPsmState(key))
  // after get
  // we are never in KEY-INIT-E
  // else we are always in the same state as before get
  private[cache] def isConsistentAfterGet(key : String, stateBefore : PsmState) : Boolean = {
    val currState = getPsmState(key)
    val isNever = currState match {
      case KeyInitEvictablePsmState(_) => false
      case _ => true
    }
    val isState = if (stateBefore.id == KeyInitEvictablePsmState(key).id) {
      KeyEvictablePsmState(key).id == currState.id
    } else {
      stateBefore.id == currState.id
    }
    isNever && isState
  }

  // ---------------------------------------------------------------------------------
  // ** getOrRegister ** transition contracts
  //
  // getOrRegister can start from any state
  //
  // BUT: if there is a getOrRegister in progress for key KK, and
  // that getOrRegister is a first-time getOrRegister, and in that
  // getOrRegister the factory hangs for a while, then it is possible
  // that during that while ANOTHER getOrRegister for key KK may be
  // called.  IF SO, the second getOrRegister will see the incomplete
  // execution-in-progress of the first getOrRegister.  In particular,
  // the second getOrRegister will throw an inconsistent state exception
  // from the getPsmState in the next line because there will be a stray
  // cacheKeyMutex in existence for key KK, but no other items (yet)
  //
  private[cache] def isConsistentBeforeGetOrRegister(key : String) : (Boolean, PsmState) = (true, getPsmState(key))
  // after getOrRegister
  // we are never in EMPTY or REMOVED (*)
  // if start is KEY-P or KEY-E then after is same state
  // if start is EMPTY then after is KEY-INIT-E or KEY-P
  // if start is REMOVED then after is KEY-INIT-E or KEY-P
  // if start is KEY-INIT-E then after is KEY-E
  //
  // (*) unless there was an error on the first getOrRegister ever
  private[cache] def isConsistentAfterGetOrRegister(key : String, stateBefore : PsmState) : Boolean = {
    val currState = getPsmState(key)
    val isNever = currState match {
      case EmptyPsmState(_) => false
      case RemovedPsmState(_) => false
      case _ => true
    }
    val isState = stateBefore match {
      case KeyEvictablePsmState(_) => stateBefore.id == currState.id
      case KeyPermanentPsmState(_) => stateBefore.id == currState.id
      case EmptyPsmState(_) => currState.id == KeyInitEvictablePsmState(key).id || currState.id == KeyPermanentPsmState(key).id
      case RemovedPsmState(_) => currState.id == KeyInitEvictablePsmState(key).id || currState.id == KeyPermanentPsmState(key).id
      case KeyInitEvictablePsmState(_) => currState.id == KeyEvictablePsmState(key).id
    }
    isNever && isState
  }

  // ---------------------------------------------------------------------------------
  // ** refresh ** transition contracts
  //
  // before refresh
  // we are never in EMPTY or REMOVED
  private[cache] def isConsistentBeforeRefresh(key : String) : (Boolean, PsmState) = {
    val currState = getPsmState(key)
    val isConsistent = currState match {
      case EmptyPsmState(_) => false
      case RemovedPsmState(_) => false
      case _ => true
    }
    (isConsistent, currState)
  }
  // after refresh:
  // we are never in EMPTY or REMOVED
  // else we are always in the same state as before refresh
  private[cache] def isConsistentAfterRefresh(key : String, stateBefore : PsmState) : Boolean = {
    val currState = getPsmState(key)
    val isNever = currState match {
      case EmptyPsmState(_) => false
      case RemovedPsmState(_) => false
      case _ => true
    }
    val isState = stateBefore.id == currState.id
    isNever && isState
  }

  // ---------------------------------------------------------------------------------
  // ** restart ** transition contracts
  //
  // restart can start from any state
  private[cache] def isConsistentBeforeRestart : Boolean = {
    true
  }
  // after restart
  // we are identically in EMPTY
  private[cache] def isConsistentAfterRestart : Boolean = {
    isPmsStateEmpty
  }

  // ---------------------------------------------------------------------------------
  // ** evict ** transition contracts
  //
  // evict can start from any state
  private[cache] def isConsistentBeforeEvict(key : String) : (Boolean, PsmState) = (true, getPsmState(key))
  // after evict
  // if start is EMPTY then after is EMPTY
  // if start is REMOVED then after is REMOVED
  // if start is KEY-INIT-E or KEY-E or KEY-P then after is REMOVED or EMPTY
  private[cache] def isConsistentAfterEvict(key : String, stateBefore : PsmState) : Boolean = {
    val currState = getPsmState(key)
    val isDestination = currState match {
      case RemovedPsmState(_) => true
      case EmptyPsmState(_) => true
      case _ => false
    }
    val isState = stateBefore match {
      case KeyEvictablePsmState(_) => currState.id == EmptyPsmState(key).id || currState.id == RemovedPsmState(key).id
      case KeyPermanentPsmState(_) => currState.id == EmptyPsmState(key).id || currState.id == RemovedPsmState(key).id
      case KeyInitEvictablePsmState(_) => currState.id == EmptyPsmState(key).id || currState.id == RemovedPsmState(key).id
      case EmptyPsmState(_) => currState.id == stateBefore.id
      case RemovedPsmState(_) => currState.id == stateBefore.id
    }
    isDestination && isState
  }
}

// ################################################################################
//
//               A S Y N C H R O N O U S   R E F R E S H E R
//
// ################################################################################

sealed abstract class Reloader()
protected[cache] case class ReloadOp(key: String, reloadOp: () => Any, reloadInSeconds: Long) extends Reloader()
protected[cache] case class ReloadOpConditional[T](key: String, reloadOp: Option[T] => StorageResult[T], reloadInSeconds: Long) extends Reloader()
protected[cache] case class ReloadOptionally(key: String, reloadOp: () => Option[Any], reloadInSeconds: Long) extends Reloader()

sealed abstract class StorageResult[+T]
case object StorageNOP extends StorageResult[Nothing]
case class StorageRefresh[T]( itemToBeStored : T ) extends StorageResult[T]

case class Refresher(system: ActorSystem, dispatcherName: String) {
  val actors: IndexedSeq[ActorRef] =  0 until PermaCacherAgent.refresherConcurrentActors map (_ => system.actorOf(Props[PermaCacherAgent].withDispatcher(dispatcherName)))
  val actorPaths: IndexedSeq[String] = actors.map(_.path.toString)
  val balancer: ActorRef = system.actorOf(RoundRobinGroup(actorPaths).props().withDispatcher(dispatcherName))
}

protected[cache] object PermaCacherAgent {
  import com.gravity.utilities.Counters._

  private [cache] val actorNameLogHead = "PermaCacherAgent: "
  private [cache] val strangeness = actorNameLogHead + " received strangeness"
  private [cache] val reloadingMessageIntro = actorNameLogHead + " reloading key: {0}"
  private [cache] val reloadedMessageIntro = actorNameLogHead + " reloaded key: {0}"
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
  private [cache] val runCounter = getOrMakeAverageCounter("PermaCacher", "Elemental Eviction Checks")
  private [cache] val unusedCounter = getOrMakeAverageCounter("PermaCacher", "Never-used Evictions")
  private [cache] val evictionCounter = getOrMakeAverageCounter("PermaCacher", "Timed Evictions")

  private val refresherSystems = new GrvConcurrentMap[String, Refresher]

  val refresherActorSystemName = "PermaCacherRefresher"

  // i.e. Hbase connections - 2; discussed with Chris, this is pretty critical so change only knowingly
  val refresherConcurrentActors = 3

  def dispatcherName(resourceType: String): String = "RefresherDispatcher-" + resourceType

  def getSystemConf(resourceType: String): Config = ConfigFactory.load(ConfigFactory.parseString(
    dispatcherName(resourceType) + """
    {
      type = Dispatcher
      mailbox-type = "com.gravity.utilities.grvakka.UnboundedFILOMailboxType"
      executor = "thread-pool-executor"
      thread-pool-executor {
        core-pool-size-min = """ + refresherConcurrentActors + """
        core-pool-size-max = """ + refresherConcurrentActors + """
      }
      throughput = """ + 1 + """
    }
    akka {
      daemonic = on
      log-dead-letters-during-shutdown = off
      logger-startup-timeout = 300s
      stdout-loglevel = "OFF"
      loglevel = "OFF"
      jvm-exit-on-fatal-error = off
    }
                                             """))


  private [cache] val defaultRefresher : Refresher = buildRefresher("")

  refresherSystems.update("", defaultRefresher)

  private [cache] def getRefresher(resourceType: String) : Refresher = refresherSystems.getOrElseUpdate(resourceType, buildRefresher(resourceType))

  def getMailboxSizes : String = {
   defaultRefresher.actors.toList.map(MeteredMailboxExtension.getMailboxSize(_)(defaultRefresher.system)).mkString("[", ",", "]")
  }

  def shutdown() {
    refresherSystems.values.foreach(_.system.shutdown())
  }

  private def buildRefresher(resourceType: String) : Refresher = {
    Refresher(ActorSystem(refresherActorSystemName + "-" + resourceType, getSystemConf(resourceType)), dispatcherName(resourceType))
  }

}

//class PermaCacherExecutor extends ExecutorServiceConfigurator {
//  override def createExecutorServiceFactory(id: String, threadFactory: ThreadFactory): ExecutorServiceFactory = ???
//}

protected[cache] class PCAgentHandler(
  reloadingMessageIntro : String,
  reloadedMessageIntro : String,
  unableMessageIntro : String,
  unableMessageIntroShort : String,
  reloadCounter : CounterT,
  reloadRejectCounter : CounterT,
  reloadingOptionIntro : String,
  reloadNoneIntro : String,
  reloadNoneCounter : CounterT,
  reloadedOptionIntro : String,
  unableOptionIntro : String,
  unableOptionIntroShort : String,
  runCounter : CounterT,
  unusedCounter : CounterT,
  evictionCounter : CounterT,
  nowRunningForKey : String = "",
  neverAccessedMessage : String = "",
  evictionMessage : String = "",
  evictionTroubleMessage : String = "",
  evictionInceonceivableMessage : String = "") {
 import com.gravity.logging.Logging._

  def processReloadOp (key : String, reloadInSeconds : Long, reloadOperation : () => Any): Any = {
    try {
      trace(reloadingMessageIntro, key)
      listeners.foreach(_.onBeforeKeyUpdated(key))

      val meta = metaOrDefault(key, reloadInSeconds)
      // does not mess with last-access time, etc. so the implicit
      // lock on cache will be enough for this op
      val result = cache.put(key, (reloadOperation(), meta.withCurrentTimestamp))
      reloadCounter.increment

      listeners.foreach(_.onAfterKeyUpdated(key, result))
      trace(reloadedMessageIntro, key)

    } catch {
      case ex: Exception =>
        listeners.foreach(_.onError(key, ex))
        error(ex, unableMessageIntro, key)
        ScalaMagic.printException(unableMessageIntroShort + key, ex)
    }
  }

  def processReloadOpConditional[T] (key : String, reloadInSeconds : Long, reloadOperation : Option[T] => StorageResult[T]): Any = {
    try {
      trace(reloadingMessageIntro, key)
      listeners.foreach(_.onBeforeKeyUpdated(key))

      val meta = metaOrDefault(key, reloadInSeconds)
      // does not mess with last-access time, etc. so
      // the implicit lock on cache will be enough
      val previous : Option[T] = cache.get(key).map(_._1).asInstanceOf[Option[T]]
      val result = reloadOperation(previous) match {
        case StorageRefresh(vvv) =>
          cache.put(key, (vvv, meta.withCurrentTimestamp))
          reloadCounter.increment
          Some(vvv)
        case StorageNOP =>
          warn(skipCachingMessage, key, "")
          reloadRejectCounter.increment
          None
      }
      listeners.foreach(_.onAfterKeyUpdated(key, result))
      result match {
        case Some(_) => trace(reloadedMessageIntro, key)
        case None => trace(unableMessageIntro, key)
      }

    } catch {
      case ex: Exception =>
        listeners.foreach(_.onError(key, ex))
        error(ex, unableMessageIntro, key)
        ScalaMagic.printException(unableMessageIntroShort + key, ex)
    }
  }

  def processReloadOptionally (key : String, reloadInSeconds : Long, reloadOperation : () => Option[Any]): Any = {
    try {
      trace(reloadingOptionIntro, key)
      listeners.foreach(_.onBeforeKeyUpdated(key))

      val meta = metaOrDefault(key, reloadInSeconds)
      // does not mess with last-access time, etc. so the implicit
      // lock on cache will be enough for this op
      val result = reloadOperation() match {
        case s@Some(v) =>
          reloadCounter.increment
          cache.put(key, (s, meta.withCurrentTimestamp))
        case None =>
          val prev = cache.get(key)
          prev.foreach(prevVal => {
            if (prevVal._1 != None) warn(reloadNoneIntro, key)
          })
          reloadNoneCounter.increment
          cache.put(key, (None, meta.withCurrentTimestamp))
      }

      listeners.foreach(_.onAfterKeyUpdated(key, result))
      trace(reloadedOptionIntro, key)

    } catch {
      case ex: Exception =>
        listeners.foreach(_.onError(key, ex))
        error(ex, unableOptionIntro, key)
        ScalaMagic.printException(unableOptionIntroShort + key, ex)
    }
  }

  def hasExpired(timeNow : Long, ttlVal : Long)(timeStamp : Long): Boolean = (timeNow-timeStamp) > ttlVal

  def processPceTimeToLiveKeyCheckOp(key : String, firstFound : mutable.Map[String, Long], ttlVal: Long = PermaCacherEvictor.msTimeToLiveDefault): Any = {
    try {
      runCounter.increment
      val timeNow = System.currentTimeMillis()
      def isExpired = hasExpired(timeNow, ttlVal) _
      trace(nowRunningForKey, timeNow)
      PermaCacher.cacheLastAccess.get(key) match {
        case Some(atomicTs) =>
          val myTs = atomicTs.get()
          val isEvict = if (myTs == PermaCacher.cacheAccessTimeInfiniteTTL) false // not evictable
          else { // evictable
            if (myTs == PermaCacher.cacheAccessTimeNeverYet) { // never accessed yet
              val foundTs = firstFound.getOrElseUpdate(key, timeNow) // did we see this before?
              if (isExpired(foundTs)) {
                firstFound.remove(key)
                info(neverAccessedMessage, key)
                unusedCounter.increment
                true // never accessed and now expired
              }
              else false // never accessed yet, not expired
            } else { // has been accessed at some point
              firstFound.remove(key)
              isExpired(myTs)
            }
          }
          if (isEvict) {
            PermaCacher.evict(key)
            evictionCounter.increment
            info(evictionMessage, key, timeNow)
          }

        case None =>
          error(evictionInceonceivableMessage,key)
      }
    } catch {
      case ex: Exception =>
        error(ex, evictionTroubleMessage, System.currentTimeMillis())
    }
  }

}

protected[cache] class PermaCacherAgent extends Actor {
 import com.gravity.logging.Logging._

  import PermaCacherAgent._

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
    evictionCounter
  )

  def receive: PartialFunction[Any, Unit] = {
    case ReloadOp(key, reloadOperation, reloadInSeconds) =>
      handler.processReloadOp(key,reloadInSeconds,reloadOperation)

    case ReloadOpConditional(key, reloadOperation, reloadInSeconds) =>
      handler.processReloadOpConditional(key,reloadInSeconds,reloadOperation)

    case ReloadOptionally(key, reloadOperation, reloadInSeconds) =>
      handler.processReloadOptionally(key,reloadInSeconds,reloadOperation)

    case "shutdown" => context.stop(self)
    case _ => warn(strangeness)

  }

}

// ################################################################################
//
//               A S Y N C H R O N O U S   E V I C T O R
//
// ################################################################################

sealed abstract class PceTimeToLive()
protected[cache] case class PceTimeToLiveCheckOp[V]
  (lastAccess : mutable.Map[String,AtomicLong], msTtl: Long = PermaCacherEvictor.msTimeToLiveDefault) extends PceTimeToLive()

private [cache] object PermaCacherEvictor {
  import com.gravity.utilities.Counters._
  //
  // private [cache] because the evicting behavior is completely optional,
  // and depends on the initialization (i.e. instantiation) of this private
  // object (which may or may not happen).  Limiting visibility to [cache]
  // encapsulates within the package any instantiation of PermaCacherEvictor
  // and therefore enables easier control of the overall behavior
  //

  private [cache] val actorNameLogHead = "PermaCacherEvictor: "
  private [cache] val strangeness = actorNameLogHead + "received strangeness"
  private [cache] val nowRunning = actorNameLogHead + "timed eviction check at UTC {0}"
  private [cache] val troubleMessage = actorNameLogHead + "trouble during timed eviction check at UTC {0}"
  private [cache] val evictionMessage = actorNameLogHead + "timed eviction of key {0} at UTC {1}"
  private [cache] val neverAccessedMessage = actorNameLogHead + "found now-expired never-accessed key {0}"
  private [cache] val nowExamined = actorNameLogHead + "timed eviction check completed for {0} cached items"
  private [cache] val evictionCounter = getOrMakeAverageCounter("PermaCacher", "Timed Evictions")
  private [cache] val unusedCounter = getOrMakeAverageCounter("PermaCacher", "Never-used Evictions")
  private [cache] val unusedYetCounter = getOrMakeAverageCounter("PermaCacher", "Never-used Yet")
  private [cache] val runCounter = getOrMakeAverageCounter("PermaCacher", "Eviction Check Runs")
  private [cache] val firstFound = new GrvConcurrentMap[String, Long]()

  private [cache] val evictorSystemConf = ConfigFactory.load(ConfigFactory.parseString(
    """  akka {
           daemonic = on
           actor {
             default-dispatcher {
               type = Dispatcher
               mailbox-type = "akka.dispatch.UnboundedMailbox"
             }
           }
         }"""))

  private [cache] val evictorActorSystemName = "PermaCacherEvictor"
  private [cache] val evictorConcurrentActor = 1 // do not increase
  private [cache] val msInTwoHours = 1000*60*60*2
  private [cache] val sInTenMinutes = 60*10
  private [cache] val msTimeToLiveDefault = msInTwoHours
  private [cache] val scheduledRunInterval = sInTenMinutes

  //
  // Akka actor pool setup for evict
  //
  private [cache] implicit val evictorSystem = ActorSystem(evictorActorSystemName, evictorSystemConf)
  // the following makes available the correct implicit ExecutionContext, taking care of this error:
  // Cannot find an implicit ExecutionContext, either require one yourself or import ExecutionContext.Implicits.global
  private [cache] implicit val executionContext = evictorSystem.dispatcher

  // protected [cache] for testing
  private [cache] val evictorActor = evictorSystem.actorOf(Props[PermaCacherEvictor])

  private [cache] def hasExpired(timeNow : Long, ttlVal : Long)(timeStamp : Long) = (timeNow-timeStamp) > ttlVal
  private [cache] def scheduleTtlCheckOp(msTtl : Long) : Cancellable = PermaCacherEvictor.evictorSystem.scheduler.
    schedule(scheduledRunInterval.seconds, scheduledRunInterval.seconds,PermaCacherEvictor.evictorActor,
    PceTimeToLiveCheckOp(PermaCacher.cacheLastAccess, msTtl))

  // this will start the scheduler with default ttl **if and when**  evaluated
  private [cache] val ttlDefaultCheckOpScheduler = scheduleTtlCheckOp(msTimeToLiveDefault)

  // dummy hook to trigger instantiation
  private[this] var beenHere = false
  private [cache] def triggerStart : Boolean = {
    /*
     Scala 2.10 Spec para. 5.4, Object Definition
     "Note that the value defined by an object definition is instantiated lazily. [... It is only ]
     evaluated the first time [...] dereferenced during execution of the program (which might be
     never at all)."
     */
    if (!beenHere) {
      beenHere = true
      println("PermaCacherEvictor instantiated")
    }
    beenHere
  }
}

protected[cache] class PermaCacherEvictor extends Actor {
 import com.gravity.logging.Logging._

  import PermaCacherEvictor._

  def receive: PartialFunction[Any, Unit] = {

    case PceTimeToLiveCheckOp(tsLiveMap, ttlVal) =>
      try {
        runCounter.increment
        val tsMap : immutable.Map[String,AtomicLong] = tsLiveMap.toMap
        val timeNow = System.currentTimeMillis()
        def isExpired = hasExpired(timeNow, ttlVal) _
        info(nowRunning, timeNow)
        for ((key, ts) <- tsMap) {
          val myTs = ts.get()
          val isEvict = if (myTs == PermaCacher.cacheAccessTimeInfiniteTTL) false
            else {
              if (myTs == PermaCacher.cacheAccessTimeNeverYet) {
                val foundTs = firstFound.getOrElseUpdate(key,timeNow)
                if (isExpired(foundTs)) {
                  firstFound.remove(key)
                  info(neverAccessedMessage, key)
                  unusedCounter.increment
                  true // never accessed and now expired
                }
                else false // never accessed yet, not expired
              } else {
                firstFound.remove(key)
                isExpired(myTs)
              }
            }
          if (isEvict) {
            PermaCacher.evict(key)
            evictionCounter.increment
            info(evictionMessage, key, timeNow)
          }
        }
        unusedYetCounter.set(firstFound.size)
        info(nowExamined, tsMap.size)
      } catch {
        case ex: Exception =>
          error(ex, troubleMessage, System.currentTimeMillis())
      }

    case _ => warn(strangeness)

  }

}

// ################################################################################
//
//               M E T A D A T A    R E P O R T E R
//
// ################################################################################

protected[cache] case class PermaCacherMeta(timestamps: Seq[Long], reloadInSeconds: Long) {
  require(0 <= reloadInSeconds)

  def withCurrentTimestamp: PermaCacherMeta = {
    val now = grvtime.currentMillis
    // if d is the time delta between now and the last time used (say 3 seconds, i.e.
    // this cache entry was last queried 3 seconds ago), and r is the max allowed
    // staleness between reloads, we have case (1) d > r [i.e. it is late] or
    // case (2) d < r [i.e. it is not late]
    //
    // if (1) [say, r is 2 seconds] then delay is 1 second (positive, late)
    // if (2) [say, r is 5 seconds] then delay is -2 seconds (negative, not late)
    //
    // when delay is a negative number, there is no problem;
    // if delay is positive, delay means "lateness"
    //
    // Probe: ========= (uncomment next two lines) =========
    // println("now:"+now+", reloadInSeconds (as ms):"+(reloadInSeconds*1000)+
    //   ", ts:"+timestamps.head+", now-ts:"+(now -timestamps.head))
    val delayms = timestamps.headOption.map(ts => now - ts - reloadInSeconds * 1000)
    // Increment a counter if delay surpasses our grace period
    // this is to create an audit record of the general trend
    val lates = delayms.filter(d => d.toDouble > PermaCacherMeta.gracePeriodMillis)
    lates.foreach(_ => PermaCacherMeta.getCounter.increment)
    val truncated = timestamps.take(PermaCacher.timestampHistoryLength)
    PermaCacherMeta(now +: truncated, reloadInSeconds)
  }
}

protected[cache] object PermaCacherMeta {
  import com.gravity.utilities.Counters._
  val counterCategory = "PermaCacherMeta"
  val delayCounterLabel = "delays"
  val gracePeriodMillis = 200

  def apply(reloadInSeconds: Long): PermaCacherMeta = {
    val currMillis = grvtime.currentMillis
    PermaCacherMeta(Seq(currMillis), reloadInSeconds)
  }

  private [cache] def getCounter = {
    getOrMakePerSecondCounter(counterCategory,delayCounterLabel)
  }
}

//
// Admin Page pretty-printer
//

case class PermaCacherMetaView(timestamps: Seq[String], reloadInSeconds: Long, nextLoad: String, millisUntilReload: Long)

object PermaCacherMetaView {
  val dtf: DateTimeFormatter = DateTimeFormat.forPattern("HH:mm:ss a")

  def apply(meta: PermaCacherMeta): PermaCacherMetaView = {
    val timeStrings = meta.timestamps.map(dtf.print)
    val nextLoad = meta.timestamps.headOption.map(ts => dtf.print(ts + meta.reloadInSeconds * 1000)).getOrElse("Could not determine last load time")
    val millisUntilReload = meta.timestamps.headOption.map(ts => (ts - grvtime.currentMillis) + meta.reloadInSeconds * 1000).getOrElse(Long.MinValue)
    PermaCacherMetaView(timeStrings, meta.reloadInSeconds, nextLoad, millisUntilReload)
  }
}

case class PermaCacherDisplayView(key: String, value: Any, meta: PermaCacherMetaView)
