package com.gravity.utilities

import java.text.DecimalFormat
import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.{Executors, ScheduledExecutorService, TimeUnit}

import akka.actor.ActorSystem
import com.gravity.utilities.grvakka.Configuration._
import com.gravity.utilities.grvstrings._
import com.gravity.utilities.grvz._
import org.joda.time.DateTime
import play.api.libs.json.{Format, Json}

object Counters {
  import scala.concurrent.duration._

  private implicit val system: ActorSystem = ActorSystem("Counters", defaultConf)
  import system.dispatcher

  private var tickIntervalSeconds = 15

  private var tickCancel = system.scheduler.schedule(tickIntervalSeconds.seconds, tickIntervalSeconds.seconds)(tickPerSecond())
  system.scheduler.schedule(1.second, 1.second)(tickHitRatio())
  //system.scheduler.schedule(1.second, 1.second)(tickMovingAverage())

  def setTickIntervalSeconds(seconds: Int): Unit = {
    tickCancel.cancel()
    tickIntervalSeconds = seconds
    tickCancel = system.scheduler.schedule(tickIntervalSeconds.seconds, tickIntervalSeconds.seconds)(tickPerSecond())
  }

  def getTickIntervalSeconds : Int = tickIntervalSeconds

  private val allCounters = new GrvConcurrentMap[String, CounterT]()
  private val allCountersByCategory = new GrvConcurrentMap[String, GrvConcurrentMap[String, CounterT]]()
  private val loggedCounters = new GrvConcurrentMap[String, CounterT]()
  private val loggedCountersByCategory = new GrvConcurrentMap[String, GrvConcurrentMap[String, CounterT]]()

  private val perSecondCountersByCategory = new GrvConcurrentMap[String, GrvConcurrentMap[String, PerSecondCounter]]()
  private val averageCountersByCategory = new GrvConcurrentMap[String, GrvConcurrentMap[String, AverageCounter]]()
  private val maxCountersByCategory = new GrvConcurrentMap[String, GrvConcurrentMap[String, MaxCounter]]()

  //private val movingAvgCountersByCategory = new GrvConcurrentMap[String, GrvConcurrentMap[String, MovingAverageCounter]]()
  private val hitRatioCountersByCategory = new GrvConcurrentMap[String, GrvConcurrentMap[String, HitRatioCounter]]()

  def getCounterIfExists(category: String, name: String): Option[CounterT] = {
      allCountersByCategory.get(category) match {
        case Some(counters) =>
          counters.filterKeys(_.endsWith(category + "-" + name)).values.headOption
        case None =>
          None
      }
  }

  private def tickPerSecond(): Unit = {
    perSecondCountersByCategory.values.foreach(_.values.foreach(_.tick(tickIntervalSeconds)))
  }

  private def tickHitRatio(): Unit = {
    hitRatioCountersByCategory.values.foreach(_.values.foreach(_.tick(1)))
  }

//  private def tickMovingAverage(): Unit = {
//    movingAvgCountersByCategory.values.foreach(_.values.foreach(_.tick(1)))
//  }

  def getAll: Iterable[CounterT] = allCounters.values
  def getLogged: Iterable[CounterT] = loggedCounters.values
  def getAllByCategory(category: String): Iterable[CounterT] = allCountersByCategory(category).values
  def getLoggedByCategory(category: String): Iterable[CounterT] = allCountersByCategory(category).values

  def countPerSecond(category:String, name:String): Unit = {
    getOrMakePerSecondCounter(category, name, shouldLog = false).increment //if you're using the convenience method, we're going to assume it's not important enough to log
  }

  def countPerSecond(category:String, name:String, by: Long): Unit = {
    getOrMakePerSecondCounter(category, name, shouldLog = false).incrementBy(by) //if you're using the convenience method, we're going to assume it's not important enough to log
  }

  def clearCountPerSecond(category:String, name:String): Unit = {
    getOrMakePerSecondCounter(category, name, shouldLog = false).set(0) //if you're using the convenience method, we're going to assume it's not important enough to log
  }

  def setAverageCount(category:String, name:String, to: Long): Unit = {
    getOrMakeAverageCounter(category, name, shouldLog = false).set(to) //if you're using the convenience method, we're going to assume it's not important enough to log
  }

  def incrementAverageCount(category:String, name:String): Unit = {
    getOrMakeAverageCounter(category, name, shouldLog = false).increment //if you're using the convenience method, we're going to assume it's not important enough to log
  }

  def countHitRatioHit(category: String, name: String) : Unit = {
    getOrMakeHitRatioCounter(category, name).incrementHit()
  }

  def countHitRatioMiss(category: String, name: String) : Unit = {
    getOrMakeHitRatioCounter(category, name).incrementMiss()
  }

  def countHitRatioHitsAndMisses(category: String, name: String, hits: Long, misses: Long) : Unit = {
    getOrMakeHitRatioCounter(category, name).incrementHitsAndMisses(hits, misses)
  }

  def getOrMakePerSecondCounter(category:String, name:String, shouldLog: Boolean = false): PerSecondCounter = {
    perSecondCountersByCategory.getOrElseUpdate(category, {
      new GrvConcurrentMap[String, PerSecondCounter]
    }).getOrElseUpdate(name, {
      val newCounter = PerSecondCounter(category, name, shouldLog)
      allCounters.update(newCounter.key, newCounter)
      allCountersByCategory.getOrElseUpdate(category, new GrvConcurrentMap[String, CounterT]()).update(newCounter.key, newCounter)
      if(newCounter.shouldLog) {
        loggedCounters.update(newCounter.key, newCounter)
        loggedCountersByCategory.getOrElseUpdate(category, new GrvConcurrentMap[String, CounterT]()).update(newCounter.key, newCounter)
      }
      newCounter
    })
  }

  def getOrMakeAverageCounter(category:String, name:String, shouldLog: Boolean = false): AverageCounter = {
    averageCountersByCategory.getOrElseUpdate(category, {
      new GrvConcurrentMap[String, AverageCounter]
    }).getOrElseUpdate(name, {
      val newCounter = AverageCounter(category, name, shouldLog)
      allCounters.update(newCounter.key, newCounter)
      allCountersByCategory.getOrElseUpdate(category, new GrvConcurrentMap[String, CounterT]()).update(newCounter.key, newCounter)
      if(newCounter.shouldLog) {
        loggedCounters.update(newCounter.key, newCounter)
        loggedCountersByCategory.getOrElseUpdate(category, new GrvConcurrentMap[String, CounterT]()).update(newCounter.key, newCounter)
      }
      newCounter
    })
  }

  def getOrMakeHitRatioCounter(category:String, name:String, shouldLog: Boolean = false): HitRatioCounter = {
    hitRatioCountersByCategory.getOrElseUpdate(category, {
      new GrvConcurrentMap[String, HitRatioCounter]
    }).getOrElseUpdate(name, {
      val newCounter = new HitRatioCounter(category, name, shouldLog)
      allCounters.update(newCounter.key, newCounter)
      allCountersByCategory.getOrElseUpdate(category, new GrvConcurrentMap[String, CounterT]()).update(newCounter.key, newCounter)
      if(newCounter.shouldLog) {
        loggedCounters.update(newCounter.key, newCounter)
        loggedCountersByCategory.getOrElseUpdate(category, new GrvConcurrentMap[String, CounterT]()).update(newCounter.key, newCounter)
      }
      newCounter
    })
  }

  def getOrMakeMaxCounter(category:String, name:String, shouldLog: Boolean = false): MaxCounter = {
    maxCountersByCategory.getOrElseUpdate(category, {
      new GrvConcurrentMap[String, MaxCounter]
    }).getOrElseUpdate(name, {
      val newCounter = MaxCounter(category, name, shouldLog)
      allCounters.update(newCounter.key, newCounter)
      allCountersByCategory.getOrElseUpdate(category, new GrvConcurrentMap[String, CounterT]()).update(newCounter.key, newCounter)
      if(newCounter.shouldLog) {
        loggedCounters.update(newCounter.key, newCounter)
        loggedCountersByCategory.getOrElseUpdate(category, new GrvConcurrentMap[String, CounterT]()).update(newCounter.key, newCounter)
      }
      newCounter
    })
  }

  def countPerSecondWithDelta(category:String, name: String, by: Long, deltaInterval: Long, deltaUnits: TimeUnit, prefix: String,
                               deltaName: String = "", inspector: Option[(CounterT, Long) => Unit] = None,
                              shouldLogCounter : Boolean = false, shouldLogDelta: Boolean = true) {
    val sourceCounter = getOrMakePerSecondCounter(category, name, shouldLog = shouldLogCounter)
    sourceCounter.incrementBy(by)
    DeltaCounters.trackDelta(sourceCounter, deltaInterval, deltaUnits, prefix, deltaName, inspector, shouldLogDelta)
  }

//  val useMovingAverageCounters = false

//  def countMovingAverage(category: String, key: String, value: Double, periodInSecs: Int = 60, logged: Boolean = false) {
//    if(useMovingAverageCounters) {
//      getOrMakeMovingAverageCounter(category, key, periodInSecs, logged).sample(value)
//    }
//  }

//  def getOrMakeMovingAverageCounter(category: String, counter: String, periodInSecs: Int, logged: Boolean = false): MovingAverageCounter = {
//    movingAvgCountersByCategory.getOrElseUpdate(category, {
//      new GrvConcurrentMap[String, MovingAverageCounter]
//    }).getOrElseUpdate(counter, {
//      new MovingAverageCounter(counter, category, periodInSecs, logged)
//    })
//  }

  trait CounterT {
    def name: String

    def category: String

    def getInterval: Double

    def kind: String

    def shouldLog: Boolean

    def get: Long

    def set(to: Long): Unit

    def tick(intervalInSeconds: Long)

    def reset(): Unit

    def increment: Long

    def decrement: Long

    val key: String = kind + "-" + category + "-" + name

    protected val count = new AtomicLong()

    override def toString: String = {
      val df = new DecimalFormat("#,###,####,###")
      "Category : " + category + " : Name : " + name + " : Total : " + df.format(get) + " : " + kind + " : " + getInterval
    }
  }

  /** Interval value is the number of counts that occurred during the previous second. */
  case class PerSecondCounter protected[utilities] (category: String, name: String, shouldLog: Boolean) extends CounterT with Crementable {

    private var perSecond : Double = 0
    private val timeBuffer = new AtomicLong()

    def increment: Long =  {
      timeBuffer.incrementAndGet()
      count.incrementAndGet()
    }

    def incrementBy(amount: Long): Long = {
      val ret = count.getAndAdd(amount)
      timeBuffer.getAndAdd(amount)
      ret
    }

    def decrement: Long = {
      timeBuffer.decrementAndGet()
      count.decrementAndGet()
    }

    def set(value: Long): Unit = {
      count.set(value)
    }

    def get: Long = count.get()

    def getInterval : Double = perSecond

    def kind: String = "Per Second"

    def tick(intervalInSeconds : Long) {
      perSecond = timeBuffer.get().toFloat / intervalInSeconds.toFloat
      timeBuffer.set(0)
    }

    def reset() {
      count.set(0)
      timeBuffer.set(0)
      perSecond = 0
    }
  }

  /** Interval value is an average of all values counted since counter inception. */
  case class AverageCounter protected[utilities] (category: String, name: String, shouldLog: Boolean = false) extends CounterT with Crementable {
    import scala.actors.threadpool.locks.ReentrantReadWriteLock

    private val lock = new ReentrantReadWriteLock()
    private val writeLock = lock.writeLock()
    private val readLock = lock.readLock()

    private var sampleCount : Long = 0
    private var oldAverage : Double = 0
    private var newAverage : Double = 0
    private var oldVariance : Double = 0
    private var newVariance : Double = 0

    private def newValue(value: Long): Long = {
      try {
        writeLock.lock()
        sampleCount += 1
        if(sampleCount == 1) {
          oldAverage = value
          newAverage = value
        }
        else {
          newAverage = oldAverage + ((value - oldAverage) / sampleCount)
          newVariance = oldVariance + ((value - oldAverage) * (value - newAverage))
          oldAverage = newAverage
          oldVariance = newVariance
        }
        value
      }
      finally {
        writeLock.unlock()
      }
    }

    def increment: Long =  {
      newValue(count.incrementAndGet())
    }

    def incrementBy(amount: Long): Long = {
      newValue(count.getAndAdd(amount))
    }

    def decrement: Long = {
      newValue(count.decrementAndGet())
    }

    def set(value: Long): Unit = {
      count.set(value)
      newValue(value)
    }

    def get: Long = count.get()

    def getInterval : Double = {
      try {
        readLock.lock()
        newAverage
      }
      finally {
        readLock.unlock()
      }
    }

    def average: Double = getInterval
    def variance: Double = {
      try {
        readLock.lock()
        newVariance
      }
      finally {
        readLock.unlock()
      }
    }

    def kind: String = "Average"

    def tick(intervalInSeconds : Long) { }

    def reset() {
      try {
        writeLock.lock()
        count.set(0)
        newAverage = 0
        oldAverage = 0
        newVariance = 0
        oldVariance = 0
        sampleCount = 0
      }
      finally {
        writeLock.unlock()
      }
    }
  }

  /** Interval value is the max of all values counted since counter inception. */
  case class MaxCounter protected[utilities](category: String, name: String, shouldLog: Boolean = false) extends CounterT with Crementable {
    private var max : Long = 0l

    private def checkMax(cur: Long): Long = {
      //this can race but it's not critical
      if(cur > max) max = cur
      cur
    }

    def increment: Long =  {
      checkMax(count.incrementAndGet())
    }

    def incrementBy(amount: Long): Long = {
      checkMax(count.getAndAdd(amount))
    }

    def decrement: Long = {
      count.decrementAndGet()
    }

    def set(value: Long): Unit = {
      count.set(value)
      checkMax(value)
    }

    def get: Long = count.get()

    def getInterval : Double = max.toDouble

    def kind: String = "Max"

    def tick(intervalInSeconds : Long) { }

    def reset(): Unit = {
      count.set(0l)
    }
  }

  /** Interval value is a 0-100 percent (hits / attempts) hit ratio. */
  class HitRatioCounter protected[utilities] (val category: String, val name: String, val shouldLog: Boolean) extends CounterT {
    val kind = "Hit Ratio"

    val successBuffer: Array[AtomicLong] = new Array[AtomicLong](60)
    for(i <- 0 until 60) successBuffer(i) = new AtomicLong(0)
    val attemptsBuffer: Array[AtomicLong] = new Array[AtomicLong](60)
    for(i <- 0 until 60) attemptsBuffer(i) = new AtomicLong(0)

    var currentIndex: Int = new DateTime().getSecondOfMinute

    def set(to: Long): Unit = {
      //this doesn't really map to a hit ratio counter!
    }

    def reset(): Unit = {
      successBuffer.foreach(_.set(0))
      attemptsBuffer.foreach(_.set(0))
    }

    def increment : Long = {
      incrementHit()
      get
    }

    def decrement: Long = {
      incrementMiss()
      get
    }

    def incrementHit() {
      incrementHits(1)
    }

    def incrementHits(byHowMany: Long) {
      incrementHitsAndMisses(byHowMany, 0)
    }

    def incrementMiss() {
      incrementMisses(1)
    }

    def incrementMisses(byHowMany: Long) {
      incrementHitsAndMisses(0, byHowMany)
    }

    def incrementHitsAndMisses(hits: Long, misses: Long) {
      successBuffer(currentIndex).getAndAdd(hits)
      attemptsBuffer(currentIndex).getAndAdd(hits + misses)
    }

    def get: Long = attemptsBuffer.foldLeft(0L)(_ + _.get())

    def getInterval: Double = {
      val attempts = attemptsBuffer.foldLeft(0L)(_ + _.get())
      if(attempts == 0)
        0F
      else {
        val hits = successBuffer.foldLeft(0L)(_ + _.get())
        (hits.asInstanceOf[Float] / attempts.asInstanceOf[Float]) * 100F
      }
    }

    def getPerIntervalDescriptor = "%"

    def tick(intervalInSeconds : Long) {
      successBuffer((currentIndex + 1) % 60).set(0L)
      attemptsBuffer((currentIndex + 1) % 60).set(0L)
      currentIndex = new DateTime().getSecondOfMinute
    }
  }

  //  class MovingAverageCounter protected[utilities] (name: String, groupName: String, seconds: Int = 60, logStats: Boolean = false) extends CounterT {
//    private val movingAverageTotals = Array.fill[AtomicDouble](seconds)(new AtomicDouble())
//    private val movingAverageCounts = Array.fill[AtomicInteger](seconds)(new AtomicInteger())
//
//    val total = new AtomicDouble()
//    val count = new AtomicLong()
//
//    private val key = getName + "-" + groupName
//    private val currentIndex = new AtomicInteger()
//
//    def getKey: String = key
//
//    def shouldLogStats: Boolean = logStats
//
//    def getTotal: Long = count.get().ifThen(_ != 0)(t => (total.get() / t).toLong)
//
//    def sample(value: Double): Int = {
//      count.incrementAndGet()
//      total.addAndGet(value)
//      movingAverageTotals(currentIndex.get()).addAndGet(value)
//      movingAverageCounts(currentIndex.get()).incrementAndGet()
//    }
//
//    def getPerInterval: Float = {
//      val countTotal = movingAverageCounts.map(_.intValue()).sum
//      countTotal.toFloat.ifThen(_ != 0)(total => movingAverageTotals.map(_.floatValue()).sum / total)
//    }
//
//    def getPerIntervalDescriptor = s"avg last ${seconds}s"
//
//    def getName: String = name + " (" + getPerIntervalDescriptor + ")"
//
//    def getGroupName: String = groupName
//
//    def tick(intervalInSeconds: Long): Unit = {
//      // move to next bucket, wrap-around if necessary
//      if (currentIndex.get() >= movingAverageTotals.length - 1) {
//        currentIndex.set(0)
//      } else {
//        currentIndex.incrementAndGet()
//      }
//
//      movingAverageTotals(currentIndex.get()).set(0.0)
//      movingAverageCounts(currentIndex.get()).set(0)
//    }
//
//  }

  /** CounterT in JSON, which is information lossy. */
  case class CounterJson(group: String, name: String, value: String, perSecond: Double) extends CounterT {
    override val category: String = "CounterJsonCategory"
    override val getInterval: Double = 0.0
    override val kind: String = "CounterJsonKind"
    override val shouldLog: Boolean = false
    override val get: Long = value.replaceAllLiterally(",", "").tryToLong.getOrElse {
      throw new Exception(s"Counter value $value isn't Longable")
    }

    override def set(to: Long): Unit = throw new Exception("You can't set the value of a JsonCounter.")

    override def tick(intervalInSeconds: Long): Unit = throw new Exception("JsonCounter can't tick.")

    override def reset(): Unit = throw new Exception("JsonCounter can't reset.")

    override def increment: Long = throw new Exception("JsonCounter can't increment.")

    override def decrement: Long = throw new Exception("JsonCounter can't decrement.")
  }

  object CounterJson {
    implicit val jsonFormat: Format[CounterJson] = Json.format[CounterJson]
  }
}

trait Crementable {
  def increment : Long
  def decrement : Long
  def set(value: Long)
  def get : Long
}

/**
 * DeltaCounters.trackDelta() makes it easy to create delta counters of arbitrary periodicity, which is otherwise tedious.
 */
object DeltaCounters {
 import com.gravity.logging.Logging._
  class CounterSnapshot(totCounter: Counters.CounterT, deltaCounter: Counters.CounterT, logEvenIfZero: Boolean) {
    private val snapValue = new AtomicLong(totCounter.get)

    def update: Long = {
      this.synchronized {
        val newValue = totCounter.get
        val oldValue = snapValue.getAndSet(newValue)

        val deltaValue = newValue - oldValue

        deltaCounter.set(deltaValue)

        if (logEvenIfZero || deltaValue != 0)
          info(s"Delta Counter ${deltaCounter.name} value is $deltaValue")
      }

      deltaCounter.get
    }
  }

  private val scheduler: ScheduledExecutorService = Executors.newScheduledThreadPool(1)

  def deltaCounterName(prefix: String, deltaName: String, sourceCounter: Counters.CounterT): String =
    if (deltaName == "") newName(sourceCounter.name, prefix) else deltaName


  def trackDelta(sourceCounter: Counters.CounterT,
                 deltaInterval: Long,
                 deltaUnits: TimeUnit,
                 prefix: String,
                 deltaName: String = "",
                 inspector: Option[(Counters.CounterT, Long) => Unit] = None,
                 logEvenIfZero: Boolean = true): Counters.AverageCounter = {
    // Create the delta counter with a name based on the total counter.
    val name = deltaCounterName(prefix, deltaName, sourceCounter)
    val deltaCounter = Counters.getOrMakeAverageCounter(sourceCounter.category, name, sourceCounter.shouldLog)

    // Indicate an unset delta counter with -1.  Could also use a more unusual value, or could wait to create the delta counter until we have a value.
    deltaCounter.set(-1L)

    // Create the CounterSnapshot and update it at the requested interval. Consider using the similar akka facility for scheduling.
    val snapshot = new CounterSnapshot(sourceCounter, deltaCounter, logEvenIfZero)

    scheduler.scheduleAtFixedRate(new Runnable() {
      def run() {
        try {
          val newVal = snapshot.update

          tryToSuccess (
            { inspector.foreach(spect => spect(deltaCounter, newVal)) },
            { ex: Exception => warn(s"Failed running DeltaCounter inspector with ${ScalaMagic.formatException(ex)}") }
          )
        } catch {
          case ex: Exception => warn(s"Could not update `$name` : " + ScalaMagic.formatException(ex))
        }
      }
    }, deltaInterval, deltaInterval, deltaUnits)

    // Return the new delta counter.
    deltaCounter
  }

  def newName(totName: String, prefix: String): String = {
    val suffix = if (totName.toLowerCase.startsWith("total "))
      totName.substring(6)  // totName of "Total Yabba-Dabbas", prefix of "10-min"  => newName of e.g. "10-min Yabba-Dabbas"
    else
      totName               // totName of "Yabba-Dabbas"      , prefix of "10-min"  => newName of e.g. "10-min Yabba-Dabbas"

    s"$prefix $suffix"
  }
}
