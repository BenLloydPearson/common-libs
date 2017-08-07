package com.gravity.domain.recommendations

import java.io.FileNotFoundException

import com.googlecode.concurrentlinkedhashmap.{ConcurrentLinkedHashMap, EvictionListener}
import com.gravity.interests.jobs.hbase.HBaseConfProvider
import com.gravity.interests.jobs.intelligence.helpers.grvhadoop
import com.gravity.utilities._
import com.gravity.utilities.cache.{EhCacher, JobScheduler}
import com.gravity.utilities.components.FailureResult
import com.gravity.utilities.grvtime._
import org.joda.time.DateTime
import org.quartz._
import com.gravity.grvlogging._

import scala.collection.JavaConversions._
import scala.collection._
import scalaz.syntax.std.option._
import scalaz.{Failure, NonEmptyList, Success, ValidationNel}

/**
  * Created by IntelliJ IDEA.
  * Author: Robbie Coleman
  * Date: 12/9/13
  * Time: 10:51 AM
  */
object HdfsRecoReportBuilder {
 import com.gravity.logging.Logging._

  import com.gravity.utilities.Counters._

  @volatile var prefixDir: String = ""

  /**
    * Used for testing, to inject the temporary HDFS write dir.
    *
    * @param prefix directory to prepend to the passed in directory when this reader checks HDFS
    */
  def init(prefix: String): Unit = {
    prefixDir = prefix
  }

  val counterCategory: String = "HdfsRecoReportBuilder"

  private val reloadFrequency = grvtime.secondsFromHours(2)

  private val maxCacheBytes = if (com.gravity.service.grvroles.isDevelopmentRole) Bins.bytesForGigabyte(1) else Bins.bytesForGigabyte(3)

  private val cache = EhCacher.getOrCreateCache[String, Array[Byte]](counterCategory, ttlSeconds = reloadFrequency, persistToDisk = false, maxLocalBytes = maxCacheBytes)

  def reloadCache(keys: Iterable[HdfsRecoReportCacheKey[_]]): Unit = {
    var succeeded, failed: Int = 0

    countPerSecond(counterCategory, "HdfsRecoReportBuilder.reloadCache.called")

    keys.foreach {
      case key: HdfsRecoReportCacheKey[_] =>
        countPerSecond(counterCategory, "HdfsRecoReportBuilder.reloadCache.item")
        trace("HdfsRecoReportBuilder.reloadCache for dir: {0}", key.directory)
        try {
          getFromCache(key.directory, touchReloader = false)(key.parser)
          ifTrace(succeeded += 1)
          countPerSecond(counterCategory, "HdfsRecoReportBuilder.reloadCache.item.SUCCEEDED")
          trace("HdfsRecoReportBuilder.reloadCache SUCCEEDED for dir: {0}", key.directory)
        }
        catch {
          case ex: Exception =>
            ifTrace(failed += 1)
            countPerSecond(counterCategory, "HdfsRecoReportBuilder.reloadCache.item.FAILED")
            warn(ex, "HdfsRecoReportBuilder.reloadCache FAILED for dir: {0}", key.directory)
        }
    }

    trace("HdfsRecoReportBuilder.reloadCache completed with {0} succeeded and {1} failed out of {2} total.", succeeded, failed, succeeded + failed)
  }

  lazy val jobStartedUnit: Unit = HdfsRecoReportReloader.start(reloadFrequency, reloadCache _)

  private def getFromHdfs[R](
                              directory: String,
                              handleFailures: HdfsRecoReportFailedLine => Unit = _ => {},
                              parseRow: (String) => ValidationNel[FailureResult, R]): Option[HdfsRecoReportResult[R]] = {
    val rows = mutable.Buffer[R]()
    var failures = 0
    var lineCount = 0

    try {
      // Keep a running average of how much time we spend reading from HDFS.
      val sw = new Stopwatch()
      sw.start()

      grvhadoop.perHdfsDirectoryLine(HBaseConfProvider.getConf.fs, prefixDir + directory)(line => {
        lineCount += 1
        parseRow(line) match {
          case Success(row) =>
            countPerSecond(counterCategory, "Successfully Parsed HDFS Lines")
            rows += row

          case Failure(fails) =>
            countPerSecond(counterCategory, "Failed To Parse HDFS Lines")
            trace(fails, f"Failed to parse HDFS line #$lineCount%0,1d from directory: `$directory`")
            handleFailures(HdfsRecoReportFailedLine(line, fails))
            failures += 1
        }
      })

      val result = HdfsRecoReportResult(rows.toSeq, failures, lineCount, 0)

      cache.putItem(directory, grvio.serializeObject(result))
      countPerSecond(counterCategory, "HdfsRecoReportBuilder_EhCacher_Put")
      sw.stop()
      setAverageCount(counterCategory, "Average HDFS read duration", sw.getDuration)
      trace("HdfsRecoReportBuilder_EhCacher_Put for key: {0}", directory)
      result.some
    }
    catch {
      case fnf: FileNotFoundException =>
        warn(fnf, s"Zero logs found in directory: `$directory`")
        None
      case ex: Exception =>
        warn(ex, s"Failed to read and build from directory: `$directory`")
        None
    }
  }

  def getWithoutCache[R](
                          directory: String,
                          handleFailures: HdfsRecoReportFailedLine => Unit = _ => {})(parseRow: (String) => ValidationNel[FailureResult, R]): Option[HdfsRecoReportResult[R]] = {
    getFromHdfs(directory, handleFailures, parseRow)
  }

  def getFromCache[R](directory: String, handleFailures: HdfsRecoReportFailedLine => Unit = _ => {}, touchReloader: Boolean = true)(parseRow: (String) => ValidationNel[FailureResult, R]): Option[HdfsRecoReportResult[R]] = {
    // on first access of getFromCache, make sure we start the reloader
    jobStartedUnit

    if (touchReloader) HdfsRecoReportReloader.touchKey(directory, parseRow)

    cache.getItem(directory) match {
      case Some(bytes) =>
        countPerSecond(counterCategory, "HdfsRecoReportBuilder_EhCacher_Hit")
        trace("HdfsRecoReportBuilder_EhCacher_Hit for key: {0}", directory)
        grvio.deserializeObject[HdfsRecoReportResult[R]](bytes).some

      case None =>
        countPerSecond(counterCategory, "HdfsRecoReportBuilder_EhCacher_Miss")
        trace("HdfsRecoReportBuilder_EhCacher_Miss for key: {0}", directory)
        getFromHdfs(directory, handleFailures, parseRow)
    }
  }
}

case class HdfsRecoReportResult[R](rows: Seq[R], failedLines: Int, totalLines: Int, rowsFiltered: Int = 0) {
  def nonEmpty: Boolean = totalLines > 0 || rows.nonEmpty || failedLines > 0

  def isEmpty: Boolean = !nonEmpty

  override def toString: String = {
    if (isEmpty) {
      "HdfsRecoReportResult had ZERO Lines to process and is considered empty"
    } else {
      f"HdfsRecoReportResult has ${rows.size}%0,1d successfully parsed rows and ${failedLines}%0,1d failed lines with $rowsFiltered%0,1d rows filtered from a total of $totalLines%0,1d raw HDFS lines"
    }
  }
}

object HdfsRecoReportResult {
  private val genericEmpty = HdfsRecoReportResult[Any](Seq(), 0, 0)

  def empty[R]: HdfsRecoReportResult[R] = genericEmpty.asInstanceOf[HdfsRecoReportResult[R]]
}

case class HdfsRecoReportFailedLine(line: String, fails: NonEmptyList[FailureResult])

case class HdfsRecoReportFilter[R](key: String, filter: (R) => Boolean)

case class HdfsRecoReportCacheKey[R](directory: String, parser: (String) => ValidationNel[FailureResult, R]) {

  private lazy val _hashCode = directory.hashCode

  override def toString: String = directory

  override def hashCode(): Int = _hashCode

  override def equals(obj: scala.Any): Boolean = {
    obj match {
      case null => false
      case HdfsRecoReportCacheKey(that, _) => that.equals(directory)
      case _ =>
        warn("HdfsRecoReportCacheKey#equals called with obj: {0} of type: {1}", obj.toString, obj.getClass.getCanonicalName)
        false
    }
  }
}

object HdfsRecoReportReloader {
 import com.gravity.logging.Logging._
  import com.gravity.utilities.Counters._
  val counterCategory: String = "HdfsRecoReportReloader"

  private var expirySeconds = grvtime.secondsFromHours(2)

  val jobClazz: Class[HdfsRecoReportReloadJob] = classOf[HdfsRecoReportReloadJob]

  var reloadFactory: Option[(Iterable[HdfsRecoReportCacheKey[_]]) => Unit] = None

  def start(reloadAndExpirySeconds: Int, reloader: (Iterable[HdfsRecoReportCacheKey[_]]) => Unit): Unit = {
    expirySeconds = reloadAndExpirySeconds
    reloadFactory = reloader.some

    try {
      val job = JobBuilder.newJob(jobClazz).withIdentity("reloadRecoReportsJob", "hdfsCacheGroup").build()
      val trigger = TriggerBuilder.newTrigger().withIdentity("reloadRecoReportsTrigger", "hdfsCacheGroup")
        .startAt(grvtime.currentTime.toDate)
        .withSchedule(SimpleScheduleBuilder.simpleSchedule().withIntervalInSeconds(expirySeconds).repeatForever())
        .build()

      JobScheduler.scheduler.scheduleJob(job, trigger)
      JobScheduler.scheduler.start()

      countPerSecond(counterCategory, "Job Start")
    }
    catch {
      case ex: Exception =>
        countPerSecond(counterCategory, "Failed To Start")
        warn(ex, "HdfsRecoReportReloader: Failed to start Job Scheduler!")
    }
  }

  case class HdfsRecoReportTracker(lastAccess: DateTime = grvtime.currentTime) {

    def touch: HdfsRecoReportTracker = copy(lastAccess = grvtime.currentTime)

    def secondsOld: Int = {
      val lastAccessedSeconds = lastAccess.getSeconds
      val nowSeconds = grvtime.currentTime.getSeconds
      nowSeconds - lastAccessedSeconds
    }
  }

  class HdfsRecoReportReloadJob extends Job {
    def execute(context: JobExecutionContext): Unit = {
      reloadFactory.foreach(_ (getKeysToReload))
    }
  }

  private val listener = new EvictionListener[HdfsRecoReportCacheKey[_], Boolean] {
    import com.gravity.utilities.Counters._

    def onEviction(k: HdfsRecoReportCacheKey[_], v: Boolean): Unit = {
      trace("HdfsRecoReportReloader.evicted: {0}", k.directory)
      countPerSecond(counterCategory, "HdfsRecoReportReloader.evicted")
    }
  }

  private val trackingMap = new ConcurrentLinkedHashMap.Builder[HdfsRecoReportCacheKey[_], Boolean]()
    .maximumWeightedCapacity(300) // arbitrary number loosely based on a balance of most common used dirs
    .listener(listener)
    .build()

  private def getKeysToReload: Iterable[HdfsRecoReportCacheKey[_]] = trackingMap.keySet().toIterable

  def touchKey[R](directory: String, parser: (String) => ValidationNel[FailureResult, R]): Unit = {
    val key = HdfsRecoReportCacheKey(directory, parser)
    touchKey(key)
  }

  def touchKey[R](key: HdfsRecoReportCacheKey[R]): Unit = {
    Option(trackingMap.get(key)) match {
      case Some(tracker) =>
        trace("HdfsRecoReportReloader.hit: {0}", key.directory)
        countPerSecond(counterCategory, "HdfsRecoReportReloader.hit")
        trackingMap.update(key, true)

      case None =>
        trace("HdfsRecoReportReloader.miss: {0}", key.directory)
        countPerSecond(counterCategory, "HdfsRecoReportReloader.miss")
        trackingMap.putIfAbsent(key, true)
    }
  }
}
