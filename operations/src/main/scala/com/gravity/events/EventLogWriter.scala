package com.gravity.events

import java.io.File
import java.text.SimpleDateFormat
import java.util.Calendar
import java.util.concurrent.TimeUnit

import akka.actor.{Actor, ActorRef, Cancellable, Props}
import akka.util.Timeout
import com.gravity.events.Utilities._
import com.gravity.interests.jobs.hbase.HBaseConfProvider
import com.gravity.utilities.FieldConverters.SequenceFileLogLineKeyConverter
import com.gravity.utilities._
import com.gravity.utilities.grvfields._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path => HdfsPath}
import org.apache.hadoop.io.SequenceFile.Writer
import org.apache.hadoop.io.{BytesWritable, SequenceFile}
import org.joda.time.DateTime

import scala.concurrent.Await
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scalaz.syntax.validation._
import scalaz.{Failure, Success, Validation}



/**
 * what happens here...
 * 
 * After spending some time reverse engineering the lot of a beacon, here I'm sharing
 * what I understand happens.
 *
 * EventLogWriter will, on receiving LogMessage(msg), write (msg) to log and trigger an
 * asynchronous "flush" that, later on, will swipe the log file to some staging directory,
 * usually (but not necessarily) on HDFS.  The staging directory filesystem footprint is
 * time-coded, based not on the timestamp embedded in the message (msg), but on the
 * wall-clock time-of-day at the time of creation.
 *
 * A watchdog thread monitors the staging directory (see HDFSLogStageWatcher).  When
 * conditions are propitious, the watchdog will pick up the log file(s) from the staging
 * directory and, line by line, on the basis of the timestamp embedded in the (msg) line,
 * classify and write the beacon to its final time-coded destination, (again, this once
 * based on the embedded timestamp, not the wall-clock "now") whence it will be ingested and
 * digested for HBase persistification (such ingestion and digestion is not addressed here).
 * At the same time (see HDFSLogCollator) the beacon is also additionally processed to
 * ascertain that it is well-formed, writing an HDFS record of valid or invalid beacons.
 *
 * All the interesting HDFS collation (e.g. by HDFSLogCollator, which starts the watchdog)
 * is ignited and started in com.gravity.insights.archiver.logger.ArchiverRole
 *
 * At the time of this comment, significant paths in HDFS material to this activities are:
 * /user/gravity/stageLogs
 * /user/gravity/devStageLogs
 * for staging (see for instance hdfsStageRoot in Utilities), and
 * /user/gravity/logs
 * for the final time coded-destination (see for instance hdfsRoot in Utilities)
 *
 */

case class LogEvent[T](date: DateTime, event: T, converter: FieldConverter[T])

object EventLogWriter {
 import com.gravity.logging.Logging._
  import Counters._
  private val counterGroupName = "Event Logging"
  getOrMakePerSecondCounter(counterGroupName, "")
  protected[events] val filesMovedCounter =  getOrMakePerSecondCounter(counterGroupName, "Files moved")
  protected[events] val fileErrorsCounter =  getOrMakePerSecondCounter(counterGroupName, "File errors")
  protected[events] val sequenceLinesWrittenCounter =  getOrMakePerSecondCounter(counterGroupName, "Lines Written to Sequence")
  protected[events] val linesFailedCounter =  getOrMakePerSecondCounter(counterGroupName, "Lines Failed")
  protected[events] def count(name:String) { countPerSecond(counterGroupName, name) }

  private val logWriters = new GrvConcurrentMap[String, ActorRef]

  //create one actor per category, with a pinned dispatcher. have that actor manage one file at a time, dumping to the proper folder in the staging folder when done
  def writeLogEvent[T](category: String, date: DateTime, event: T)(implicit ev : FieldConverter[T]) {
    writeLogMessage(category, LogEvent(date, event, ev))
  }

  def flushAll() {
    logWriters.values.foreach(_ ! EventLogSystem.flushAction)
  }

  def shutdown(): Unit = {
    import akka.pattern.ask
    val responses = for {
      writer <- logWriters.values
    } yield writer.ask("shutdown")(Timeout(2, TimeUnit.MINUTES))
    responses.map(response => Await.result(response, Duration(2, TimeUnit.MINUTES)))
  }

  private def writeLogMessage[T](category: String, event: LogEvent[T]): Unit = {
    val aux = getActorFor(category)
    aux ! event
    count("Events Received - " + category)
  }

  private def getActorFor(categoryName: String) = {
    logWriters.getOrElseUpdate(categoryName, {
      trace("Creating log writing actor for " + categoryName)
      EventLogSystem.system.actorOf(Props(new LogWriteActor(categoryName)).withDispatcher(EventLogSystem.pinnedDispatcherName))
    })
  }

}

class LogWriteActor(categoryName: String) extends Actor {
  import com.gravity.logging.Logging._
  import com.gravity.events.EventLogWriter.{fileErrorsCounter, filesMovedCounter, linesFailedCounter, sequenceLinesWrittenCounter}
  import com.gravity.events.Utilities.fs

  private val hostname = EventLogSystem.hostname
  private val localSequenceWriteDir = EventLogSystem.eventBufferSequenceRoot + categoryName + "/"

  private val currentSequenceWriterLock = new Object()
  private var oldSequenceFileCheckCancellable : Option[Cancellable] = None

  moveOldSequenceFiles()
  deleteOldCRCFiles()

  private var currentSequenceWriterAndFilenameValidation: Validation[Throwable, (SequenceFile.Writer, String)] = createSequenceWriterAndFilenameValidation()
  private var rotateSequenceCancellable = EventLogSystem.system.scheduler.scheduleOnce(60.seconds)(rotateSequenceToHDFS())
  private val stateLock = new Object()
  private var started = true

  def receive: PartialFunction[Any, Unit] = {
    case e@LogEvent(date: DateTime, event: Any, converter: FieldConverter[_]) =>
      val logMessage = SequenceFileLogLine(SequenceFileLogLineKey(categoryName, date), converter.toBytes(event))
      log(logMessage)
    case a: SequenceFileLogLine =>
      log(a)
    case s: String =>
      s match {
        case EventLogSystem.flushAction =>
          trace("LogWriteActor received flush")
          rotateSequenceToHDFS()
        case "shutdown" =>
          rotateSequenceToHDFS()
          postStop()
          sender ! "ok"
        case unknown => warn("Unknown string command: {0}", unknown)
      }
  }

  private def log(msg: SequenceFileLogLine) {
    currentSequenceWriterLock.synchronized {
      currentSequenceWriterAndFilenameValidation match {
        case Success(tuple) => //there's a better way to pattern match this I'm sure
          try {
            val writer = tuple._1
            writer.append(new  BytesWritable(SequenceFileLogLineKeyConverter.toBytes(msg.key)), new BytesWritable(msg.logBytes))
            writer.hflush()
            sequenceLinesWrittenCounter.increment
            EventLogWriter.count("Sequence Lines Written - " + categoryName)
          }
          catch {
            case e: Exception =>
              warn("Exception writing sequence event log line: " + ScalaMagic.formatException(e))
              linesFailedCounter.increment
          }
        case Failure(e) =>
          warn("Could not write sequence event log line due to failure to create buffer file: " + ScalaMagic.formatException(e))
          linesFailedCounter.increment
      }
    }
  }

  private def deleteOldCRCFiles(): Unit = {
    currentSequenceWriterLock.synchronized {
      try {
        val files = for {
          file <- Option(new java.io.File(localSequenceWriteDir).listFiles) getOrElse Array.empty[java.io.File]
          if file.getName.startsWith(".")
          if file.getName.endsWith("crc")
          if file.getName.contains(hostname)
          if file.getName.contains(categoryName)
          if !currentSequenceOutputFileContains(file.getName)
        } yield file
        if (files.length > 0) {
          info("Deleting old crc files in category " + categoryName + ": " + files.length)
          files.foreach(_.delete())
        }
      }
      catch {
        case e: Exception => warn("Exception cleaning up old log files " + ScalaMagic.formatException(e))
      }
    }
  }

  private def rotateSequenceToHDFS(again: Boolean = true) {
    if(started) {
      try {
        val fileToMoveOption = currentSequenceWriterLock.synchronized {
          //first, if we HAVE a writer, flush it out and get a reference to it as a file
          val opt = currentSequenceWriterAndFilenameValidation match {
            case Success(tuple) => //there's a better way to pattern match this I'm sure
              val writer = tuple._1
              val fileName = tuple._2
              writer.close()
              trace("about to move " + fileName)
              Some(new File(fileName))
            case Failure(e) =>
              warn("No log file to flush because buffer creation failed with " + ScalaMagic.formatException(e))
              None
          }
          currentSequenceWriterAndFilenameValidation = createSequenceWriterAndFilenameValidation()
          opt
        }
        //doing this here to handle the actual copy to HDFS outside of the lock, so that writing can continue ASAP.
        //nothing else will try to touch this file, because a new writer was created inside the lock
        fileToMoveOption.map(fileToMove => moveSequenceFile(fileToMove))

      }
      catch {
        case e: Exception =>
          fileErrorsCounter.increment
          warn("Unable to flush: " + ScalaMagic.formatException(e))
      }
      finally {
        if (again && !EventLogSystem.system.isTerminated) rotateSequenceCancellable = EventLogSystem.system.scheduler.scheduleOnce(60.seconds)(rotateSequenceToHDFS()) //and now that we're done, schedule another flush in 20 seconds
      }
    }
  }


  private def currentSequenceOutputFileContains(fileName: String) : Boolean = {
    if (currentSequenceWriterAndFilenameValidation == null) false //this gets called on startup, which purposefully happens before creating a new log file
    else {
      currentSequenceWriterAndFilenameValidation match {
        //don't move the current file!
        case Success(tuple) => tuple._2.contains(fileName)
        case Failure(e) => false
      }
    }
  }

  private def moveOldSequenceFiles() {
    currentSequenceWriterLock.synchronized {
      //prevent changing the current file while we're doing this...
      try {
        val files = for {
          file <- Option(new java.io.File(localSequenceWriteDir).listFiles) getOrElse Array.empty[java.io.File]
          if file.getName.endsWith("log.seq")
          if file.getName.contains(hostname)
          if file.getName.contains(categoryName)
          if !currentSequenceOutputFileContains(file.getName)
        } yield file
        if (files.length > 0) {
          info("Cleaning up old sequence files in category " + categoryName + ": " + files.length)
          files.foreach((cur: File) => {
            moveSequenceFile(cur)
          })
        }
      }
      catch {
        case e: Exception => warn("Exception cleaning up old log files " + ScalaMagic.formatException(e))
      }
      finally {
        if (started)
          oldSequenceFileCheckCancellable = Some(EventLogSystem.system.scheduler.scheduleOnce(30.minutes)(moveOldSequenceFiles()))
      }
    }
  }

  private def moveSequenceFile(logFile: File) = {
    if (logFile.length() < 1) {
      logFile.delete()
    }
    else {
      if (copySequenceToHDFSStage(logFile)) {
        filesMovedCounter.increment
        trace("deleting " + logFile.getAbsolutePath)
        logFile.delete()
        try {
          val stupidHiddenFile = new File(logFile.getParent + "/." + logFile.getName + ".crc")
          if(stupidHiddenFile.exists()) {
            stupidHiddenFile.delete()
          }
        }
        catch {
          case e:Exception => warn(e, "Exception deleting stupid hidden crc file")
        }
      }
      else {
        trace("NOT copied to HDFS!")
        fileErrorsCounter.increment
      }
    }
  }

  private def copySequenceToHDFSStage(logFile: File) = {
    try {
      // the stage path "currency" is established through what wall-clock time
      // is "now" at the time of this call; also, the filename will reflect the
      // time at which the file is created (close to "now" above) rather than
      // the time at which the beacons were received.  The only timestamp that
      // carries the actual time when the event was received is in each line
      // inside the file itself, together with the content of the beacon
      val stagePathName = Utilities.getCurrentSequenceHdfsStageDir(categoryName) + logFile.getName
      val stagePath = new HdfsPath(stagePathName)

      if (fs.exists(stagePath)) {
        warn("Could not copy log file " + logFile.getAbsolutePath + " to hdfs stage because " + stagePath + " already exists!")
        false
      }
      else {
        val suc = org.apache.hadoop.fs.FileUtil.copy(logFile, FileSystem.get(hbaseConf.defaultConf), stagePath, false, hbaseConf.defaultConf)
        if(suc) trace("Event Logging moved file " + logFile.getAbsolutePath + " to " + stagePathName) else warn("Event Logging failed to move file to HDFS" + logFile.getAbsolutePath)

        suc
      }
    }
    catch {
      case e: Exception =>
        warn("Exception copying log file " + logFile.getAbsolutePath + " to hdfs stage: " + ScalaMagic.formatException(e))
        false
    }
  }

   private def createSequenceWriterAndFilenameValidation() : Validation[Throwable, (SequenceFile.Writer, String)] = {
    try {
      val conf: Configuration = HBaseConfProvider.getConf.defaultConf
      val cal = Calendar.getInstance
      val sdf = new SimpleDateFormat("yyyy-MM-dd-HH-mm-ss")
      val fileDate = sdf.format(cal.getTime)

      val outBufferFileName = "%s%s_%s_%s.log.seq".format(localSequenceWriteDir, categoryName, fileDate, hostname)

      val path = new HdfsPath("file://" + outBufferFileName)
      //trace("Creating writer for " + outBufferFileName)
      val writer = SequenceFile.createWriter(conf, Writer.file(path), Writer.keyClass(classOf[BytesWritable]), Writer.valueClass(classOf[BytesWritable]))

      (writer, outBufferFileName).success
    }
    catch {
      case e: Exception =>
        fileErrorsCounter.increment
        warn(e, "Error creating sequence file")
        e.failure
    }
  }

  override def postStop(): Unit = {
    if(started) {
      stateLock.synchronized {
        if(started) {
          started = false
          info("Stopping event log writer for " + categoryName)
          rotateSequenceToHDFS(again = false)
        }
      }
    }
  }
}


