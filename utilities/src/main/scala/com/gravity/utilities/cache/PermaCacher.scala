package com.gravity.utilities.cache

import java.text.SimpleDateFormat
import java.util.Date
import java.util.concurrent.atomic.AtomicLong

import akka.actor.{ActorRef, Cancellable}
import com.gravity.utilities.Bins._
import com.gravity.utilities._

import scala.annotation.tailrec
import scala.collection.mutable
import scala.concurrent.duration._
import scala.util.{Failure, Success, Try}


/*             )\._.,--....,'``.
 .b--.        /;   _.. \   _\  (`._ ,.
`=,-,-'~~~   `----(,_..'--(,_..'`-.;.'  */


/**
  * ###  this historical introduction is valuable for the perspective it offers, yet is obsolete ###
  *
  * A utility for caching items that should be reloaded from a background thread.
  * This is a permanent cache -- it will never be expunged.  It is good for caching things like lookup tables,
  * where you know the data will be refreshed at a particular time interval.
  *
  * NOTE PermaCacher is now evictable; see PermaCacherEcoSys.scala
  *
  * The reason to use this rather than put it in an LRU cache is that an LRU cache will drop the item from
  * the cache, thus rendering it temporarily unavailable.  During that time, if many threads wish to get the
  * item out of cache, they will simultaneously hit the database containing the data, thus gumming up the system.
  *
  * This cache performs well for a small number of largish items that should be reloaded every x seconds.
  */

// ################################################################################
//
//                 E V I C T A B L E   C A C H E
//
// ################################################################################

object PermaCacher {
 import com.gravity.logging.Logging._

  import Counters._
  import PermaCacherPsm.{catCacheMissing, catKeyMutexMsg, catMissingKeyEvicted, catMissingKeyNotEvicted, catTimestampMissing, illegalStateMsg, isConsistentAfterEvict, isConsistentAfterGet, isConsistentAfterGetOrRegister, isConsistentAfterRestart, isConsistentBeforeEvict, isConsistentBeforeGet, isConsistentBeforeGetOrRegister, isConsistentBeforeRestart, stateBeforeGetOrRegister}



  private val restartCallbacks = mutable.Buffer[() => Unit]()
  /**
    * Register a function that is called for when PermaCacher is restarted.  This is mainly for unit testing when people call PermaCacher.restart() and expect other things to reload as well.
 *
    * @param fx the function to be called on restart
    */
  def addRestartCallback(fx: () => Unit): Unit = {
    restartCallbacks += fx
  }

  //
  // for logging 
  //
  private [cache] val nameLogHead = "PermaCacher: "
  private [cache] val troubleFetching = "trouble fetching asset for priming or refresh at key: {0}, thread {1}"
  private [cache] val unableGetOrRegisterIntro = nameLogHead + troubleFetching
  private [cache] val getOrRegisterWithOptionSome = nameLogHead + "Loading item {0} for first time, " +
    "scheduling reload at interval of {1} seconds"
  private [cache] val getOrRegisterWithOptionNone = nameLogHead + "Initial call to factory returned `None`! " +
    "So `None` has been cached for key {0} but it will try to retrieve it again in {1} seconds."
  private [cache] val unableGetOrRegisterWithOptionIntro = nameLogHead + " (with Option) " + troubleFetching
  private [cache] val skipCachingMessage = nameLogHead+"unable to load key: {0} because Factory opined to disapprove"
  private [cache] val badCachingMessage = nameLogHead+"(INTERNAL) retrieved StorageNOP for key {0}, item {1}"
  private [cache] val evictionError = nameLogHead + "eviction attempt failed to cancel reload for key: {0} at UTC timestamp: {1}"
  private [cache] val evictionNopInfo = nameLogHead + "attempt to cancel non-existing scheduler for key: {0} at UTC timestamp: {1}"
  private [cache] val forceEvictionWarn = nameLogHead + "force attempt to evict for key: {0} at UTC timestamp: {1}"
  private [cache] val externalEvictionInfo = nameLogHead + "external eviction of key: {0} at UTC timestamp: {1}"
  private [cache] val cleanupAfterPartial = nameLogHead + "cleaning up removing remnants for key: {0} at UTC timestamp: {1}"
  private [cache] val cannotStartEvictor = nameLogHead + "cannot start evictor yet, will retry later"
  private [cache] val startedEvictor = nameLogHead + "evictor now started"
  private [cache] val uncachedItemMessage = nameLogHead + "not cached key {0} with reload interval 0 (pass-through)"

  //
  //==============================
  //
  // troubleshooting help
  //
  val isEnforceAssertions = false


  //
  // housekeeping
  //
  val timestampHistoryLength = 49
  val cacheAccessTimeInfiniteTTL: Long = Long.MaxValue
  val cacheAccessTimeNeverYet: Long = 0L
  private [cache] val cacheHitCounter = getOrMakeAverageCounter("PermaCacher", "Cache Hits")
  private [cache] val cachedItemsCounter = getOrMakeAverageCounter("PermaCacher", "Registered Items")
  private [cache] val evictableItemsCounter = getOrMakeAverageCounter("PermaCacher", "Evictable-only Registered")
  private [cache] val cachingFailureCounter = getOrMakeAverageCounter("PermaCacher", "Registration Cleanups")
  private [cache] val passThroughCounter = getOrMakeAverageCounter("PermaCacher", "Pass-through (uncached)")
  private [cache] val retryWHenBackedUpPSCounter= getOrMakePerSecondCounter("PermaCacher", "Factory retries p/s when backed up")
  private [cache] val retryWHenBackedUpAveCounter = getOrMakeAverageCounter("PermaCacher", "Factory retries ave when backed up")
  private [cache] val factoriesStartedCounter = getOrMakeAverageCounter("PermaCacher", "Factory starts")
  private [cache] val factoriesCompletedCounter = getOrMakeAverageCounter("PermaCacher", "Factory completions")
  private [cache] val factoriesErrorsCounter = getOrMakeAverageCounter("PermaCacher", "Factory errors")
  private [cache] val factoriesBusyCounter = getOrMakeAverageCounter("PermaCacher", "Factory in progress")

  //
  // shared caches; these are common to all the
  // Actor(s) and the API function users
  //

  // private [cache] for testing
  val thing: GrvConcurrentMap[Nothing, Nothing] = new GrvConcurrentMap
  private[cache] val cache = new GrvConcurrentMap[String, (Any, PermaCacherMeta)]()
  private[cache] val cacheLastAccess = new GrvConcurrentMap[String, AtomicLong]()
  // cache, cacheLastAccess are accessed/updated together and must remain consistent.  Correct operation
  // requires synchronization on a common, shared mutex.  To keep synchronization nimble we have a mutex
  // per key.  cacheKeyMutex protects the atomicity of [cache.get(key)/cacheLastAccess.put(key,timestamp)]
  // so that, say, key will not disappear from cache, yet cacheLastAccess still puts a new ts
  // ** ALWAYS acquire BEFORE collectiveMutex **
  private[cache] val cacheKeyMutex = new GrvConcurrentMap[String, AnyRef]()

  //
  // scheduled reloads; this is shared within and
  // common among the API function users
  //

  // private [cache] for testing
  private[cache] val reloadOps = new GrvConcurrentMap[String, Cancellable]()
  private[cache] val tickingActors = new GrvConcurrentMap[String, ActorRef]()
  // this additional mutex is needed when doing certain CRUD ops for reloadOps involving also cache and
  // cacheLastAccess consistency granularity is coarser (and accordingly simpler to execute and keep track
  // of) because occurrences of use are not that frequent
  // ** ALWAYS acquire AFTER cacheKeyMutex **
  private[cache] val collectiveMutex = new AnyRef()

  // the next line starts the evictor system; if no evictor wanted (i.e. PermaCacher will behave
  // as an immutable cache, same as what original behavior it manifested, then comment it out.
  // You can check the counters page to verify at runtime if there is any eviction behavior for
  // the currently running deployment
  private[this] var isEvictorStarted = startEvictor

  // factory timer
  // collects stats on the execution
  private[cache] val factoryTime = new GrvConcurrentMap[String, Long]()
  private val timeInSecBins = mkArbitraryBinStructureOne (Seq((0.0, 1.0),(1.0, 10.0),(10.0,100.0),(100.0,1000.0),(1000.0,HighestBoundOne)))
  private[cache] val execSecBinnery = new ConcurrentAscendingBinsOneAugmented[(String,String)](timeInSecBins,("(never)","(N/A)"))

  private[this] def startEvictor: Boolean = {
    Option(cacheLastAccess) match {
      case Some(_) =>
        info (startedEvictor)
        PermaCacherEvictor.triggerStart
      case None =>
        warn(cannotStartEvictor)
        false
    }
  }

  //
  // DRY code normalization (NOT general purpose)
  //

  private[this] def collectivePrimeAndRegister[T]
    (key: String, initialValue: T, reloadInSeconds: Long, mayBeEvicted: Boolean, reloader: Reloader, resourceType: String) {

    if (!isEvictorStarted) isEvictorStarted = startEvictor

    if (0 != reloadInSeconds) {


      //
      // ONLY => while holding cacheKeyMutex <=
      // also acquires collectiveMutex in here
      // DRY refactor: NOT a general purpose utility, beware
      //
      collectiveMutex.synchronized {

        //
        // HOLDING collectiveMutex
        //

        cache.put(key, (initialValue, PermaCacherMeta(reloadInSeconds)))
        primeLastAccess(key, mayBeEvicted)
        cachedItemsCounter.increment
        val theTicks : List[PermaCacherKeySystem.Tick] =
          if (mayBeEvicted) List ((PceTimeToLiveKeyCheckOp(key), PermaCacherKeyAgent.scheduledEvictionRunIntervalSec),(reloader,reloadInSeconds))
          else List((reloader, reloadInSeconds))
        tickingActors.put(key, PermaCacherKeySystem.createKeyActor(key,theTicks, resourceType))
      }
    } else {
      warn(uncachedItemMessage,key)
    }
  }

  private[this] def collectiveConsistencyCheck(key: String) {

    //
    // ONLY => while holding cacheKeyMutex <=
    // also acquires collectiveMutex in here
    // DRY refactor: NOT a general purpose utility, beware
    //

    collectiveMutex.synchronized {

      //
      // HOLDING collectiveMutex
      //

      val allOk = cache.contains(key) && cacheLastAccess.contains(key) && isSchedulingOk(key)
      if (!allOk) {
        warn(cleanupAfterPartial, key, System.currentTimeMillis().toString)
        cachingFailureCounter.increment
        cache.remove(key)
        cacheLastAccess.remove(key)
        reloadOps.remove(key)
        tickingActors.remove(key)
        cacheKeyMutex.remove(key)
      }
    } // collectiveMutex.synchronized
  }

  private[cache] def isSchedulingOk(key : String) : Boolean = {
    def oneActor : Boolean = reloadOps.isEmpty && tickingActors.nonEmpty
    def globSchedule : Boolean = reloadOps.nonEmpty && tickingActors.isEmpty
    oneActor && !globSchedule && tickingActors.contains(key)
  }

  private[this] def updateLastAccess(key: String) : Boolean = {

    //
    // ONLY => while holding cacheKeyMutex <=
    // DON'T acquire ANY mutex in here
    // DRY refactor: NOT a general purpose utility, beware
    //

    cacheLastAccess.get(key) match {
      case Some(ts) =>
        if (cacheAccessTimeInfiniteTTL != ts.get())
          ts.set(System.currentTimeMillis())
        true
      case None =>
        error(catTimestampMissing, key, System.currentTimeMillis().toString)
        false
    }
  }

  private[this] def primeLastAccess(key: String, mayBeEvicted: Boolean) = {

    //
    // ONLY => while holding cacheKeyMutex <=
    // so DON'T acquire ANY mutex in here
    // DRY refactor: NOT a general purpose utility, beware
    //

    val timeStamp = if (mayBeEvicted) {
      evictableItemsCounter.increment
      cacheAccessTimeNeverYet
    } else cacheAccessTimeInfiniteTTL
    cacheLastAccess.put(key, new AtomicLong(timeStamp))
  }

  //
  // utility methods/functions
  //

  private[cache] def metaOrDefault(key: String, reloadInSeconds: Long) = PermaCacher.cache.get(key).map {
    case (value, meta) => meta
  }.getOrElse(PermaCacherMeta(reloadInSeconds))


  // this will evict key no-matter-what, i.e. it is the burden of the
  // caller to establish business rules if key should be evicted.  There
  // is no business logic here.  Key, short of an error or other trouble,
  // will be evicted.  Only concern of relevance is the consistency of
  // the state machine.
  private[cache] def evict(key: String, force: Boolean = false): Boolean = {

    val stateBefore = if (isEnforceAssertions) {
      val (isConsistent, state) = isConsistentBeforeEvict(key)
      if (!isConsistent) throw new IllegalStateException(illegalStateMsg)
      state
    }

    // in case of corruption for cacheKeyMutex, this **may** be the way out
    val ckMxHolder = if (force) {
      warn(forceEvictionWarn,key,System.currentTimeMillis().toString)
      Some(new AnyRef())
    } else cacheKeyMutex.get(key)

    val isSuccess = ckMxHolder match {
      case Some(ckMx) => ckMx.synchronized {

        //
        // HOLDING cacheKeyMutex
        //

        collectiveMutex.synchronized {

          //
          // HOLDING collectiveMutex
          //

          val isSchedulingCancelled = try {
            // stop the actor for this key
            tickingActors.get(key) match {
              case Some(actor) =>
                // this is asynchronous!
                actor ! "shutdown"
                tickingActors.remove(key)
              case None => info(evictionNopInfo, key, System.currentTimeMillis().toString)
            }
            true
          } catch {
            case ex: Exception =>
              error(ex, evictionError, key, System.currentTimeMillis().toString)
              false
          }

          if (isSchedulingCancelled) {
            cache.remove(key)
            cacheLastAccess.remove(key)
          }
          isSchedulingCancelled
        } // collectiveMutex.synchronized
      } // cacheKeyMutex.synchronized
      case None => collectiveMutex.synchronized {

        //
        // HOLDING collectiveMutex
        //

        info(catKeyMutexMsg, key, System.currentTimeMillis().toString)
        val isRemnants = reloadOps.contains(key) || cache.contains(key) || cacheLastAccess.contains(key) || tickingActors.contains(key)
        !isRemnants
      }
    } //
    if (isSuccess) cacheKeyMutex.remove(key)

    if (isEnforceAssertions) {
      val isConsistent = isConsistentAfterEvict(key,stateBefore.asInstanceOf[PsmState])
      if (!isConsistent) throw new IllegalStateException(illegalStateMsg)
    }

    isSuccess
  }

  // =============================================================================================
  // this is the public API to IntelligenceSchemaServlet, strictly read-only
  // =============================================================================================

  def display: Iterable[PermaCacherDisplayView] = {
    // vestigial tails after reverting, will get rid of soon enough
    // import scala.collection.JavaConversions._
    // val auxMap : immutable.Map[String,(Any, PermaCacherMeta)] = cache.toMap

    cache.map {
      case (key, (value, meta)) => PermaCacherDisplayView(key, value, PermaCacherMetaView(meta))
    }
  }

  def numberOfTimers: Int = {
    reloadOps.size
  }

  def mailboxSize: String = {
    PermaCacherAgent.getMailboxSizes
  }

  def execSecReport: Seq[(String, Int)] = {
     execSecBinnery.byBucketReport
  }

  def augmentationCapacity : Int = {
    execSecBinnery.augmentationElements
  }

  def augmentationByBinReport(binIx : Int) : IndexedSeq[String]= {
    val st : IndexedSeq[(String,String)] = execSecBinnery.augmentationReport(binIx)
    // this appears in the scaml page, see permacacher.scaml and IntelligenceSchemaServlet.scala
    val ss = st.map(tup => tup._1+" "+tup._2)
    ss
  }

  def augmentationReport : IndexedSeq[IndexedSeq[String]] = {
    val ret = for (binIx <- 1 until execSecBinnery.size) yield augmentationByBinReport(binIx)
    ret
  }

  // =============================================================================================
  // listener public API, enables registration of listeners, else strictly read-only.  Listeners
  // MUST NOT mess with the internal state of PermaCacher or all bets are off.  Hopefully they
  // cannot...
  // =============================================================================================

  //val listeners: mutable.HashSet[PermaCacherListener] with mutable.SynchronizedSet[PermaCacherListener] = new mutable.HashSet[PermaCacherListener]() with mutable.SynchronizedSet[PermaCacherListener]
  val listeners: GrvConcurrentSet[PermaCacherListener] = new GrvConcurrentSet[PermaCacherListener]()

  listeners += new OnUpdateLockDiagnostic
  listeners += new FactoryExecutionTimer

  // =============================================================================================
  // general purpose public API, read-write
  // =============================================================================================

  //
  // Dumps the cache and restarts the scheduler
  //
  // protected [intelligence] for testing does not work as expected...
  def restart(): Unit = {
    if (isEnforceAssertions) {
      if (!isConsistentBeforeRestart) throw new IllegalStateException(illegalStateMsg)
    }

    // in lieu of acquiring, destroy.
    // May create errors in log, but
    // at this point, who cares...
    cacheKeyMutex.clear()

    collectiveMutex.synchronized {

      //
      // HOLDING collectiveMutex
      //

      // more vestigial tails
      // val ops = reloadOps.values
      // val opsIter = ops.iterator()
      // while (opsIter.hasNext) opsIter.next().cancel()
      for (reloadOp <- reloadOps) {
        reloadOp._2.cancel()
      }
      // the scheduler will NOT run again here, one hopes,
      // since reloadOps is protected with collectiveMutex and
      // therefore cannot be reinitialized asynchronously
      reloadOps.clear()

      for (tickingActor <- tickingActors) {
        tickingActor._2 ! "shutdown"
      }
      tickingActors.clear()

      // at this point there should be no further attempt to
      // update cache, so the next line is the last word
      cache.clear()
      // same here
      cacheLastAccess.clear()
    }

    // just in case something happened
    // asynchronously in the meantime
    cacheKeyMutex.clear()

    if (isEnforceAssertions) {
      if (!isConsistentAfterRestart) throw new IllegalStateException(illegalStateMsg)
    }

    //Clients can register simple callbacks for when restart is called.
    restartCallbacks.foreach {callback =>
        callback()
    }
  }

  def get[T](key: String): Option[T] = {

    val stateBefore = if (isEnforceAssertions) {
      val (isConsistent, state) = isConsistentBeforeGet(key)
      if (!isConsistent) throw new IllegalStateException(illegalStateMsg)
      state
    }

    def tryGetKey(key: String) : Try[Option[T]] = Try{ cacheKeyMutex.get(key) match {
        case Some(o) =>
          o.synchronized {

            //
            // HOLDING cacheKeyMutex
            //

            cache.get(key) match {
              case Some((value, _)) =>
                cacheHitCounter.increment
                if (!updateLastAccess(key)) throw new IllegalStateException(illegalStateMsg)
                Some(value.asInstanceOf[T])
              case None =>
                error(catCacheMissing, key, System.currentTimeMillis().toString)
                throw new IllegalStateException(illegalStateMsg)
            }
          }
        case None => None
      }
    }

    val tryRetVal : Option[T] = tryGetKey(key) match {
      case Success(retVal) =>
        retVal
      case Failure(getErr) =>
        val isLocalException = illegalStateMsg.equals(getErr.getMessage)
        val isKeyEvicted = if (isLocalException) {
          val isSuccess = evict(key)
          val isForcedSuccess = if (!isSuccess) evict(key, force = true) else false
          isSuccess || isForcedSuccess
        } else false
        if (isLocalException && isKeyEvicted) error(catMissingKeyEvicted, key, System.currentTimeMillis().toString)
        else if (isLocalException) error(catMissingKeyNotEvicted, key, System.currentTimeMillis().toString)
        else error(getErr,"")
        None
    }

    if (isEnforceAssertions) {
      val isConsistent = isConsistentAfterGet(key,stateBefore.asInstanceOf[PsmState])
      if (!isConsistent) throw new IllegalStateException(illegalStateMsg)
    }

    tryRetVal
  }

  def clearResultFromCache(key: String): Boolean = {
    info(externalEvictionInfo,key,System.currentTimeMillis().toString)
    evict(key)
  }

  def shutdown(): Unit = {
    PermaCacherAgent.shutdown()
  }

  def getOrRegisterConditional[T](key: String, reloadInSeconds: Long, mayBeEvicted: Boolean)
                                        (factory : (Option[T]) => StorageResult[T]): Option[T] = {
    getOrRegisterConditional(key, factory, reloadInSeconds)
  }

  /**
    * Will retrieve an item from the cache.  If the item does not exist, it will insert the item with a scheduled reload
    * after allowing the factory an opportunity to compare the current cache content vs. the new spawn from persistent
    * storage.  If the new spawn does appear, say, corrupted, then the factory may opine not to refresh, but rather to
    * reduce the refresh operation to a NOP and leave the cache content unaltered.  StorageResult case class is the
    * mechanism allowing the opining factoring to impress on PermaCacher what the course of action shall be.  Therefore,
    * PermaCacherAgent (see) becomes a dumb and faithful agent plainly carrying out the factory's all-knowing decision.
    */
  def getOrRegisterConditional[T](key: String, factory: (Option[T]) => StorageResult[T],
                              reloadInSeconds: Long, mayBeEvicted: Boolean = false, resourceType: String = ""): Option[T] = {


    // this is to reduce, but not eliminate, the window of opportunity for a race
    // condition if two first-time getOrRegister operations are concurrent; see
    // comment at isConsistentBeforeGetOrRegister for a longer explanation

    var stateBefore = if (isEnforceAssertions) {
      cacheKeyMutex.get(key) match {
        case None => stateBeforeGetOrRegister(key) // first time
        case Some(_) => None // not first time
      }
    } else None

    // here, reloadOp does not evaluate factory
    val reloadOp = factory

    val lockHolder = cacheKeyMutex.getOrElseUpdate(key, new AnyRef())
    // vestigial tails, likely boo-boos for post-mortem diagnostic at some time in the future
    // putIfAbsent, when absent, returns null
    // val lockHolder = Option(cacheKeyMutex.putIfAbsent(key, new AnyRef())) match {
    //  case Some(found) => found
    //  case None => cacheKeyMutex.get(key)
    //}

    var isNOP = false

    // cacheKeyMutex ALWAYS acquired BEFORE collectiveMutex
    val retVal = lockHolder.synchronized {

      stateBefore = if (isEnforceAssertions) stateBefore match {
        case None => stateBeforeGetOrRegister(key)
        case sb@Some(_) => sb
      } else None

      def runFactory: Option[T] = {
        try {
          listeners.foreach(_.onBeforeKeyUpdated(key))
          // if factory blows, it will do so here
          val vvv = factory(None) match {
            case StorageRefresh(itemToBeStored) =>
              collectivePrimeAndRegister(key, itemToBeStored, reloadInSeconds, mayBeEvicted, ReloadOpConditional(key, reloadOp, reloadInSeconds), resourceType)
              (Some(itemToBeStored), itemToBeStored)
            case nop@StorageNOP =>
              warn(skipCachingMessage, key)
              isNOP = true
              // collectiveConsistencyCheck(key) with cleanup will happen in the finally{} clause
              (None, None)
          }

          listeners.foreach(_.onAfterKeyUpdated(key, Some(vvv._2)))
          // case nop@StorageNOP() lost type info, so need to provide here again when None
          vvv._1.asInstanceOf[Option[T]]
        } catch {
          case ex: Exception =>
            listeners.foreach(_.onError(key, ex))
            error(ex, unableGetOrRegisterIntro, key, Thread.currentThread().getName)
            throw ex
        } finally {
          collectiveConsistencyCheck(key)
        }
      }

      //
      // HOLDING cacheKeyMutex
      //

      val aux = cache.get(key) match {
        case Some((result, _)) =>
          if (updateLastAccess(key)) {
            cacheHitCounter.increment
            val retVal = result match {
              case a@Some(_) => a
              case None => None
              case b => Some(b)
            }
            retVal.asInstanceOf[Option[T]]
          } else {
            // there is an object, but not an access time
            collectiveConsistencyCheck(key)
            runFactory
          }
        case None => runFactory
      } // cache.get(key) match
      aux
    } // cacheKeyMutex.synchronized

    if (isEnforceAssertions && 0!=reloadInSeconds) { stateBefore match {
      case Some(sb) =>
        val isConsistent = if (isNOP) {
          isConsistentBeforeGetOrRegister(key)._1
        } else {
          isConsistentAfterGetOrRegister(key, sb)
        }
        if (!isConsistent) throw new IllegalStateException(illegalStateMsg)
      case None => throw new RuntimeException("should NEVER get here")
    }}

    retVal
  }



  /**
    * Will retrieve an item from the cache.  If the item does not exist, it will insert the item with a scheduled reload.
    */
  def getOrRegister[T](key: String, factory: => T,
                              reloadInSeconds: Long, mayBeEvicted: Boolean = false, resourceType: String = ""): T = {

    // see comment earlier in the code
    var stateBefore = if (isEnforceAssertions) {
      cacheKeyMutex.get(key) match {
        case None => stateBeforeGetOrRegister(key) // first time
        case Some(_) => None // not first time
      }
    } else None

    // here, reloadOp does not evaluate factory
    val reloadOp = () => factory

    val lockHolder = cacheKeyMutex.getOrElseUpdate(key, new AnyRef())
    // vestigial tails, for post mortem diagnostic
    // putIfAbsent, when absent, returns null
    //val lockHolder = Option(cacheKeyMutex.putIfAbsent(key, new AnyRef())) match {
    //  case Some(found) => found
    //  case None => cacheKeyMutex.get(key)
    // }

    // cacheKeyMutex ALWAYS acquired BEFORE collectiveMutex
    val retVal = lockHolder.synchronized {

      stateBefore = if (isEnforceAssertions) stateBefore match {
        case None => stateBeforeGetOrRegister(key)
        case sb@Some(_) => sb
      } else None

      def runFactory: T = {
        try {
          listeners.foreach(_.onBeforeKeyUpdated(key))
          // if factory blows, it will do so here
          val initialValue = factory
          collectivePrimeAndRegister(key, initialValue, reloadInSeconds, mayBeEvicted, ReloadOp(key, reloadOp, reloadInSeconds), resourceType)
          listeners.foreach(_.onAfterKeyUpdated(key, Some(initialValue)))
          initialValue
        } catch {
          case ex: Exception =>
            listeners.foreach(_.onError(key, ex))
            error(ex, unableGetOrRegisterIntro, key, Thread.currentThread().getName)
            throw ex
        } finally {
          collectiveConsistencyCheck(key)
        }
      }

      //
      // HOLDING cacheKeyMutex
      //

      val aux = cache.get(key) match {
        case Some((result, _)) =>
          if (updateLastAccess(key)) {
            cacheHitCounter.increment
            result.asInstanceOf[T]
          } else {
            // there is an object, but not an access time
            collectiveConsistencyCheck(key)
            runFactory
          }
        case None => runFactory
      } // cache.get(key) match
      aux
    } // cacheKeyMutex.synchronized

    if (isEnforceAssertions && 0!=reloadInSeconds) { stateBefore match {
      case Some(sb) =>
        val isConsistent = isConsistentAfterGetOrRegister(key, sb)
        if (!isConsistent) throw new IllegalStateException(illegalStateMsg)
      case None => throw new RuntimeException("should NEVER get here")
    }}

    retVal
  }

  def getOrRegister[T](key: String, reloadInSeconds: Long, mayBeEvicted: Boolean)
    (factory: => T): T = getOrRegister(key, factory, reloadInSeconds, mayBeEvicted, "")

  def getOrRegisterWithResourceType[T](key: String, reloadInSeconds: Long, resourceType: String, mayBeEvicted: Boolean = false)
                      (factory: => T): T = getOrRegister(key, factory, reloadInSeconds, mayBeEvicted, resourceType)

  def getOrRegisterFactoryWithOption[T](key: String, reloadInSeconds: Long, mayBeEvicted: Boolean = false)
    (factory: => Option[T]): Option[T] = getOrRegisterFactoryWithOption(key, factory, reloadInSeconds, mayBeEvicted, "")

  def getOrRegisterWithOptionAndResourceType[T](key: String, reloadInSeconds: Long, mayBeEvicted: Boolean = false, resourceType: String)
    (factory: => Option[T]): Option[T] = getOrRegisterFactoryWithOption(key, factory, reloadInSeconds, mayBeEvicted, resourceType)

  /**
   * Will attempt to retrieve an item from the cache.  If the item does not exist,
   * it will call the factory method and set it with the key if Some[T] is the factory result
   */

  def getOrRegisterFactoryWithOption[T](key: String, factory: => Option[T],
                                        reloadInSeconds: Long, mayBeEvicted: Boolean, resourceType: String): Option[T] = {

    // see comment earlier in the code
    var stateBefore = if (isEnforceAssertions) {
      cacheKeyMutex.get(key) match {
        case None => stateBeforeGetOrRegister(key) // first time
        case Some(_) => None // not first time
      }
    } else None

    // here, reloadOp does not evaluate factory
    val reloadOp = () => factory

    val lockHolder = cacheKeyMutex.getOrElseUpdate(key, new AnyRef())
    // vestigial tails, for post mortem diagnostic
    // putIfAbsent, when absent, returns null
    //val lockHolder = Option(cacheKeyMutex.putIfAbsent(key, new AnyRef())) match {
    //  case Some(found) => found
    //  case None => cacheKeyMutex.get(key)
    //}

    // cacheKeyMutex ALWAYS acquired BEFORE collectiveMutex
    val retVal = lockHolder.synchronized {

      stateBefore = if (isEnforceAssertions) stateBefore match {
        case None => stateBeforeGetOrRegister(key)
        case sb@Some(_) => sb
      } else None

      def runFactory: Option[T] = {
        try {
          listeners.foreach(_.onBeforeKeyUpdated(key))
          val refresher = ReloadOptionally(key, reloadOp, reloadInSeconds)
          // mayhem, if any, would likely happen with factory
          val result = factory match {
            case initialValue@Some(vvv) =>
              info(getOrRegisterWithOptionSome, key, reloadInSeconds)
              collectivePrimeAndRegister(key, initialValue, reloadInSeconds, mayBeEvicted, refresher, resourceType)
              initialValue
            case None =>
              info(getOrRegisterWithOptionNone, key, reloadInSeconds)
              collectivePrimeAndRegister(key, None, reloadInSeconds, mayBeEvicted, refresher, resourceType)
              None
          }

          listeners.foreach(_.onAfterKeyUpdated(key, result))
          result
        }
        catch {
          case ex: Exception =>
            listeners.foreach(_.onError(key, ex))
            error(ex, unableGetOrRegisterWithOptionIntro, key, Thread.currentThread().getName)
            throw ex
        }
        finally {
          collectiveConsistencyCheck(key)
        }
      }

      //
      // HOLDING cacheKeyMutex
      //

      val aux = cache.get(key) match {
        case Some((result, _)) =>
          if (updateLastAccess(key)) {
            cacheHitCounter.increment
            // for getOrRegisterWitOption of getOrRegister item
            val retVal = result match {
              case a@Some(_) => a
              case None => None
              case b => Some(b)
            }
            retVal.asInstanceOf[Option[T]]
          } else {
            // there is an object, but not an access time
            collectiveConsistencyCheck(key)
            runFactory
          }
        case None => runFactory
      } // match
      aux
    } // cacheKeyMutex.synchronized

    if (isEnforceAssertions && 0!=reloadInSeconds) { stateBefore match {
      case Some(sb) =>
        val isConsistent = isConsistentAfterGetOrRegister(key, sb)
        if (!isConsistent) throw new IllegalStateException(illegalStateMsg)
      case None => throw new RuntimeException("should NEVER get here")
    }}

    retVal
  } // getOrRegisterWithOption

  @tailrec def retryUntilNoThrow[T](fun: () => T, retryOnFailSecs: Long = 15): T = {
    try {
      fun()
    } catch {
      case th: Throwable =>
        val funRef = fun.getClass.getSimpleName

        warn(th, s"$funRef failed, will retry in ${retryOnFailSecs}secs...")
        Thread.sleep(retryOnFailSecs * 1000L)

        info("Retrying $funRef...")
        retryUntilNoThrow(fun, retryOnFailSecs)
    }
  }
}

//
// listener API, enables registration of listeners
//

trait PermaCacherListener {

  def onBeforeKeyUpdated(key: String)

  def onAfterKeyUpdated[T](key: String, value: Option[T])

  def onError(key: String, complaint: Throwable)
}

class OnUpdateLockDiagnostic extends PermaCacherListener {
 import com.gravity.logging.Logging._

  import PermaCacher.{cache, cacheKeyMutex, cacheLastAccess, collectiveMutex, reloadOps, tickingActors}

  val head = "PermaCacher onUpdate Diagnostic"
  val missingItem: String = head+" key {0} is missing after update!"
  val untimelyReloadAfterEviction: String = head+" key {0} updated after eviction! Expect trouble (cache is in inconsistent state)"
  val potentiallyInconsistentReload: String = head+" key {0} update is potentially inconsistent"
  val onReload: String = head+" key {0} updated, locking appears consistent"
  info("OnUpdateLockDiagnostic is listening")

  def onBeforeKeyUpdated(key: String) {}

  def onAfterKeyUpdated[T](key: String, value: Option[T]) {
    val flag = collectiveMutex.synchronized {

      //
      // HOLDING collectiveMutex
      //

      val isItemFound = cache.contains(key)
      val isItemScheduled = reloadOps.contains(key) || tickingActors.contains(key)
      val hasAllPieces = isItemScheduled && cacheLastAccess.contains(key) && cacheKeyMutex.contains(key)
      val hasNoPieces = !(isItemScheduled || cacheLastAccess.contains(key) || cacheKeyMutex.contains(key))
      val isAfterEviction = isItemFound && hasNoPieces
      val isSomethingMissing = isItemFound && !hasAllPieces

      if (!isItemFound) 1
      else if (isAfterEviction) 2
      else if (isSomethingMissing) 3
      else 0
    }

    flag match {
      case 1 => error(missingItem, key)
      case 2 => error(untimelyReloadAfterEviction, key)
      case 3 =>
        val msg : StringBuilder = new StringBuilder()
        msg.append(potentiallyInconsistentReload)
        msg.append(' ')
        if (!(reloadOps.contains(key) || tickingActors.contains(key))) msg.append(" scheduled refresh (factory or actor) not found for this key!")
        if (!cacheLastAccess.contains(key)) msg.append(" cacheLastAccess not found for this key!")
        if (!cacheKeyMutex.contains(key)) msg.append(" cacheKeyMutex not found for key!")
        error(msg.toString(),key)
      case _ => trace(onReload,key)
    }


  }

  def onError(key: String, complaint: Throwable) {}
}

class FactoryExecutionTimer extends PermaCacherListener {
 import com.gravity.logging.Logging._

  import PermaCacher.{execSecBinnery, factoriesBusyCounter, factoriesCompletedCounter, factoriesErrorsCounter, factoriesStartedCounter, factoryTime, retryWHenBackedUpAveCounter, retryWHenBackedUpPSCounter}

  val head = "PermaCacher factory timer:"
  val backedUp: String = head+" key {0} is backed up: on retry, factory execution already pending for {1} seconds"
  val tStoredTooLarge: String = head+" stored start time {0} larger than now time {1} : Having fun with the clock?"
  val readyToExecute: String = head+" factory for key {0} about to execute at time {1}"
  val doneExecute: String = head+" factory for key {0} done executing at time {1}"
  val errorExecute: String = head+" factory for key {0} done executing at time {1}"
  val concurrentFactory: String = head+" factory for key {0} executed while another factory for same key was finishing"
  info("FactoryExecutionTimer is listening")

  def onBeforeKeyUpdated(key: String) {
    val tStart = System.currentTimeMillis()
    val tStored = factoryTime.getOrElseUpdate(key,tStart)
    factoriesBusyCounter.increment
    factoriesStartedCounter.increment
    if (tStart==tStored) {
      // we've just created the entry
      trace(readyToExecute,key,tStart.toString)
    } else {
      // entry existed already
      if (tStored > tStart) {
        error(tStoredTooLarge,tStored.toString,tStart.toString)
      } else {
        val tMilli : Double = tStart - tStored
        val tSec = tMilli / 1000.0
        warn(backedUp, key, tSec.toString)
      }
      retryWHenBackedUpAveCounter.increment
      retryWHenBackedUpPSCounter.increment
    }
  }
  def onAfterKeyUpdated[T](key: String, value: Option[T]) {
    factoriesBusyCounter.decrement
    factoriesCompletedCounter.increment
    factoryTime.get(key) match {
      case Some(tStored) =>
        val tEnd = System.currentTimeMillis()
        val sdf : SimpleDateFormat = new SimpleDateFormat("MMM-dd HH:mm:ss")
        val startDate : Date = new Date(tEnd)
        val stringDate : String = sdf.format(startDate)
        val tMilli : Double = tEnd - tStored
        val tSec = tMilli / 1000.0
        execSecBinnery.tallyOneAugmented(new SizeOne(tSec),(stringDate,key))
        trace(doneExecute,key,tEnd.toString)
        factoryTime.remove(key)
      case None => warn(concurrentFactory,key)
    }
  }

  def onError(key: String, complaint: Throwable) {
    factoriesBusyCounter.decrement
    factoriesErrorsCounter.increment
    factoryTime.remove(key)
  }
}