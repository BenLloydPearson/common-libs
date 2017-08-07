package com.gravity.events

import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits.global
import akka.actor.Cancellable
import com.gravity.service.ZooCommon
import com.gravity.service.remoteoperations._
import com.gravity.utilities.ScalaMagic
import org.apache.hadoop.fs.{FileStatus, Path => HdfsPath}
import org.joda.time.DateTime
import com.gravity.utilities.Settings
/*
*     __         __
*  /"  "\     /"  "\
* (  (\  )___(  /)  )
*  \               /
*  /               \
* /    () ___ ()    \  erik 1/16/15
* |      (   )      |
*  \      \_/      /
*    \...__!__.../
*/

/*
each machine will have a watcher for each category, and each of those watchers will ask, in a delayed loop, "give me the lock for this category".
when they have it, they will look at each input segment folder, and for each segment folder that is older than 5 minutes and has not been modified
for more than 5 minutes (the current logic), it will check to see if a node indicating that the segment is being processed exists, and if not,
or if the existing node is more than an hour old, will fire off a remoting message saying "process this segment" to the role and create the node.
remoting sends are already a random selection process, so a random machine will queue that segment.

upon dequeueing the input segment process message, it will process the files in it. for each output segment the input segment has lines for,
it will write out to that segment as it does now, with the only difference being it will acquire a lock for the output segment while determining the output file name (the z element of the hour_xx_y_z name). once it has finished processing, it will acquire a lock on the input segement and remove the processing indication node.
 */



class DistributedLogStageWatcher(categoryName: String) {
  import EventLogSystem.system
  import com.gravity.logging.Logging._
  import Utilities.fs
  import com.gravity.utilities.Counters._
  val counterCategory = DistributedHDFSLogCollator.categoryName

  val zooClient = ZooCommon.getEnvironmentClient
  
  private val categorySequenceFolder = new HdfsPath(Utilities.getHdfsCategorySequenceStageDir(categoryName))

  info("Created log stage watcher for " + categorySequenceFolder)

  private val lockPath = Settings.getProperties.getProperty("recommendation.log.inputLockPath", "/logStage/inputLocks/") + categoryName
  private val lock = ZooCommon.buildLock(zooClient, lockPath)

  private val lastProcessedNodePath = Settings.getProperties.getProperty("recommendation.log.lastProcessedPath", "/logStage/lastProcessed/") + categoryName

  private val processingPath = Settings.getProperties.getProperty("recommendation.log.processingPath", "/logStage/processing/")

  private def longToBytes(v: Long) : Array[Byte] = {
    val bytes = new Array[Byte](8)
    bytes(0) = (v >>> 56).toByte
    bytes(1) = (v >>> 48).toByte
    bytes(2) = (v >>> 40).toByte
    bytes(3) = (v >>> 32).toByte
    bytes(4) = (v >>> 24).toByte
    bytes(5) = (v >>> 16).toByte
    bytes(6) = (v >>> 8).toByte
    bytes(7) = (v >>> 0).toByte
    bytes
  }

  private def bytesToLong(bytes: Array[Byte]) : Long = {
    (bytes(0).toLong << 56) +
      ((bytes(1) & 255).toLong << 48) +
      ((bytes(2) & 255).toLong << 40) +
      ((bytes(3) & 255).toLong << 32) +
      ((bytes(4) & 255).toLong << 24) +
      ((bytes(5) & 255) << 16) +
      ((bytes(6) & 255) << 8) +
      ((bytes(7) & 255) << 0)
  }

  private var nextLoop : Cancellable = system.scheduler.scheduleOnce(1.second)(processCategoryFolder())

  //private val oneHourInMilliseconds = 3600000l
  private val twoHoursInMilliseconds = 3600000l * 2l

  private val fiveMinutesInMilliseconds = 300000l

  def stop(): Unit = {
    nextLoop.cancel()
  }

  def processCategoryFolder() {
    try {
      trace("Trying to get lock " + lockPath)
      lock.acquire()
      val nowMillis = new DateTime().getMillis

      val doProcess: Boolean =
        ZooCommon.getNodeIfExists(zooClient, lastProcessedNodePath) match {
          case Some(bytes) =>
            val lastProcessedMillis = bytesToLong(bytes)
            if (nowMillis - fiveMinutesInMilliseconds > lastProcessedMillis) {
              //five minutes ago is after the last time it was processed
              ZooCommon.deleteNodeIfExists(zooClient, lastProcessedNodePath)
              true
            }
            else
              false
          case None =>
            true
        }

      if (doProcess) {
        ZooCommon.createNode(zooClient, lastProcessedNodePath, longToBytes(nowMillis))

        val dayFolders = if (fs.exists(categorySequenceFolder)) {
          fs.listStatus(categorySequenceFolder)
        }
        else {
          Array.empty[FileStatus]
        }

        val message = "Processing " + dayFolders.length + " days of staged logs in category " + categoryName

        if (dayFolders.length > 1)
          info(message)
        else
          trace(message)

        dayFolders.foreach { dayFolder =>
            val dayFolderPath = dayFolder.getPath
            val daySegments = fs.listStatus(dayFolderPath)

            if (daySegments.isEmpty) {
              try {
                fs.delete(dayFolderPath, false)
              }
              catch {
                case e: Exception => warn("Exception deleting empty day folder " + dayFolder + ": " + ScalaMagic.formatException(e))
              }
            }
            else {
              info("Currently inspecting " + daySegments.size + " segments in " + categoryName + " in day " + dayFolderPath.getName)

              daySegments.foreach {
                segment => {
                  val segmentPath = segment.getPath
                  val segmentName = segmentPath.getName
                  val segmentInterval = Utilities.getSegmentInterval(dayFolderPath.getName, segmentName)

                  segmentInterval match {
                    case Some(interval) =>
                      val segmentIntervalEnd = interval.getEnd
                      val now = new DateTime()
                      val segmentAge = now.getMillis - segmentIntervalEnd.getMillis
                      val segmentLastModified = segment.getModificationTime
                      val sinceSegmentModified = now.getMillis - segmentLastModified
                      //leaving the legacy .avro name here because transitioning would be a pain and it really isn't viewed by people anyway
                      val nodePath = processingPath + categoryName + ".avro/" + dayFolder.getPath.getName + "/" + segmentName

                      val sendSegment =
                        if (segmentAge > 300000 && sinceSegmentModified > 300000) {
                          //we know the segment is old enough, but let's check to see if it's been sent already!
                          val now = new DateTime().getMillis
                          ZooCommon.getNodeIfExists(zooClient, nodePath) match {
                            case Some(bytes) =>
                              val nodeCreated = bytesToLong(bytes)
                              val ageInMs = now - nodeCreated
                              if (ageInMs > twoHoursInMilliseconds) {
                                warn("Segment " + segmentPath + " was previously sent, but " + ageInMs + " ms ago so we suspect it's hung and will try again.")
                                true //if it was sent more than an hour ago, then chances are very high that it was lost. shit happens, try again.
                              }
                              else {
                                info("Segment " + segmentPath + " was previously sent, but only " + ageInMs + " ms ago so letting it process")
                                false
                              }
                            case None =>
                              ZooCommon.createNode(zooClient, nodePath, longToBytes(now)) //send now in milliseconds as bytes as the data
                              true
                          }
                        }
                        else {
                          info("Not sending segment, " + segmentPath + " segment age is " + segmentAge + " and sinceSegmentModified is " + sinceSegmentModified)
                          false //still being written to, leave it alone.
                        }

                      if (sendSegment) {
                        info("Sending segment " + segmentPath + " for processing.")
                        countPerSecond(counterCategory, "Segments sent for processing")
                        RemoteOperationsClient.clientInstance.send(ProcessSegmentPath(segmentPath.toString, categoryName, nodePath, isBinaryInput = true))

                      }

                    case None =>
                      countPerSecond(counterCategory, "Unparseable Paths : " + categoryName)
                      warn("Could not parse hour segment path " + segmentPath.toString + " for interval!")
                  }
                }
              }
            }
        }
      }
    }
    catch {
      case e: Exception => warn("Exception processing category " + categoryName + ": " + ScalaMagic.formatException(e))
    }
    finally {
      lock.release()
      trace("Released lock " + lockPath)
    }

    nextLoop = system.scheduler.scheduleOnce(2.minute)(processCategoryFolder())
  }
}
