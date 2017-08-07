package com.gravity.events

import com.gravity.domain.BeaconEvent
import com.gravity.domain.FieldConverters._
import com.gravity.events.DistributedHDFSLogCollator._
import com.gravity.events.TestEvent.TestEventConverter
import com.gravity.events.Utilities._
import com.gravity.interests.jobs.intelligence.operations.BeaconService.{FailedDomainCheck, FailedReferrerCheck, IgnoredAction, MissingField}
import com.gravity.interests.jobs.intelligence.operations.FieldConverters.CampaignBudgetOverageConverter
import com.gravity.interests.jobs.intelligence.operations.{BeaconService, ClickEvent, ClickFields, GrccableEvent}
import com.gravity.service.ZooCommon
import com.gravity.service.remoteoperations._
import com.gravity.utilities.FieldConverters.SequenceFileLogLineKeyConverter
import com.gravity.utilities._
import com.gravity.utilities.components.FailureResult
import com.gravity.utilities.eventlogging.{FieldRegistry, FieldValueRegistry}
import com.gravity.utilities.grvfields.FieldConverter
import org.apache.avro.generic.GenericData
import org.apache.curator.framework.CuratorFramework
import org.apache.hadoop.fs.{Path => HdfsPath}
import org.apache.hadoop.io.{BytesWritable, SequenceFile}
import org.apache.parquet.avro.AvroParquetWriter
import org.apache.parquet.hadoop.metadata.CompressionCodecName
import org.joda.time.DateTime

import scala.collection.{immutable, mutable}
import scalaz.syntax.validation._
import scalaz.{Failure, Success, ValidationNel}

/*
*     __         __
*  /"  "\     /"  "\
* (  (\  )___(  /)  )
*  \               /
*  /               \
* /    () ___ ()    \  erik 1/26/15
* |      (   )      |
*  \      \_/      /
*    \...__!__.../
*/

//object TestApp extends App {
//
//  val testPath = new HdfsPath("/user/gravity/logs.avro/clickEvent/2015_307/hour_16_5_0")
//  fs.delete(new HdfsPath("/user/gravity/logs.avro/clickEvent/"), true)
// // val testWriter = new AvroParquetLogLineWriter(testPath, "clickEvent")
////  println(converter.avroSchema.toString(true))
//
//}

object LogLineWriter {
 import com.gravity.logging.Logging._

  import com.gravity.utilities.Counters._
  val counterCategory: String = DistributedHDFSLogCollator.categoryName
  val additionalActionsByCategory : Map[String, Any => Unit ] = immutable.Map("beacon.new" -> beaconAction _)

  def beaconAction(beaconAny: Any): Unit = {
    beaconAny match {
      case beacon:BeaconEvent =>
        var clickDate = new DateTime(0)
        try {
          //beacon loader recreation
          BeaconService.validateBeacon(beacon, Seq.empty[String]) match {
            case Success(beaconData) =>
              if (beaconData.date.getYear >= 2010) {
                if (beaconData.action == "beacon") {
                  EventLogWriter.writeLogEvent("validatedBeacons.new", beaconData.date, beacon)
                }
              }
            case Failure(f) =>
              EventLogWriter.writeLogEvent("invalidBeacons.new", beacon.timestampDate, beacon)
              f match {
                case IgnoredAction(a) => countPerSecond("Invalid Beacons", "Ignored Action from " + beacon.siteGuidOpt.getOrElse("no siteGuid") + ": " + a)
                case FailedDomainCheck(domain, siteDomain) =>
                  countPerSecond("Invalid Beacons", "Failed domain check " + siteDomain)
                  trace("Beacon failed domain check. Domain " + domain + ", siteDomain " + siteDomain)
                //beaconsIgnored += 1
                case FailedReferrerCheck =>
                  countPerSecond("Invalid Beacons", "Failed referrer Check from " + beacon.siteGuidOpt.getOrElse("no siteGuid"))
                //println("Beacon did not pass referrer check: " + beacon.trimmedString)
                //beaconsIgnored += 1
                case mf: MissingField =>
                  countPerSecond("Invalid Beacons", "Missing Field from " + beacon.siteGuidOpt.getOrElse("no siteGuid") + ": " + mf.name)
                  trace("Beacon is missing field " + mf.name + ": " + BeaconEventConverter.toDelimitedFieldString(beacon))
                //beaconsIgnored += 1
              }
          }

          val eventOption: Option[ClickEvent] = {
            //first look for a grcc2, for our transition period
            val clickFieldsOpt = Some(ClickFields(clickDate.getMillis,
              beacon.pageUrl,
              beacon.userAgent,
              beacon.referrer,
              beacon.href,
              if (beacon.redirectClickHash.nonEmpty) Some(beacon.redirectClickHash) else None,
              if (beacon.conversionTrackingParamUuid.nonEmpty) Some(beacon.conversionTrackingParamUuid) else None
            ))

            val fromGrcc2 = for {
              grcc2 <- GrccableEvent.grcc2Re.findFirstIn(beacon.href)
              event <- GrccableEvent.fromGrcc2(grcc2, grcc2IsRaw = true, clickFieldsOpt)
            } yield {
              clickDate = beacon.timestampDate
              if (beacon.redirectClickHash.nonEmpty)
                countPerSecond(counterCategory, "Beacons with grcc and conversion hash: " + beacon.redirectClickHash)
              else
                countPerSecond(counterCategory, "Beacons with grcc without conversion hash")

              countPerSecond(counterCategory, "Grcc2 Tokens Found")
              event
            }

            if (fromGrcc2.isDefined)
              fromGrcc2 //hey we found one
            else {
              //look for a grcc3
              ClickEvent.grcc3Re.findFirstIn(beacon.href) match {
                case Some(grcc3) =>
                  val clickFieldsOpt = Some(
                    ClickFields(clickDate.getMillis,
                      beacon.pageUrl,
                      beacon.userAgent,
                      beacon.referrer,
                      beacon.href,
                      if (beacon.redirectClickHash.nonEmpty) Some(beacon.redirectClickHash) else None,
                      if (beacon.conversionTrackingParamUuid.nonEmpty) Some(beacon.conversionTrackingParamUuid) else None
                    ))

                  ClickEvent.fromGrcc3(grcc3, grcc3IsRaw = true, clickFieldsOpt) match {
                    case Success(event) =>
                      clickDate = beacon.timestampDate
                      if (beacon.redirectClickHash.nonEmpty)
                        countPerSecond(counterCategory, "Beacons with grcc and conversion hash: " + beacon.redirectClickHash)
                      else
                        countPerSecond(counterCategory, "Beacons with grcc without conversion hash")

                      countPerSecond(counterCategory, "Grcc3 Tokens Found")

                      Some(event)
                    case Failure(fails) =>
                      warn("Failed to parse grcc3 " + grcc3 + ": " + fails)
                      None
                  }
                case None =>
                  None
              }
            }
          }

          eventOption foreach {
            event =>
              countPerSecond(counterCategory, "Grcc Tokens Found")
              EventLogWriter.writeLogEvent("clickEvent-redirect", clickDate, event)
          }

        }
        catch {
          case ex: Exception => warn(ex, "Exception in beacon event processing")
        }
    }
  }
}

trait LogLineWriter {
  import LogLineWriter._
  import com.gravity.utilities.Counters._
  import com.gravity.logging.Logging._

  def writeLogLine(line: ArchiveLogLine)
  def writeLogLine(line: SequenceFileLogLine)
  def flush()
  def close()
  def moveToStorage(segmentId: SegmentId) : ValidationNel[FailureResult, HdfsPath]
  def ensureParentExists(path: HdfsPath): Unit = {
    val toFolder = path.getParent
    if(!fs.exists(toFolder)) {
      info("Making destination folder " + toFolder)
      fs.mkdirs(toFolder, folderPermission)
    }
  }

  def copyToAws(from: HdfsPath): ValidationNel[FailureResult, String] = {

    import com.amazonaws.auth.BasicAWSCredentials
    import com.amazonaws.services.s3.AmazonS3Client
    import com.amazonaws.services.s3.model.ObjectMetadata
    import com.gravity.interests.jobs.hbase.HBaseConfProvider

    try {
      val awsAccessKey = Settings.getProperty("gravity.eventlogging.key")
      val awsSecretKey = Settings.getProperty("gravity.eventlogging.secret")
      val s3Bucket = Settings.getProperty("gravity.eventlogging.bucket")

      //now copy that hdfs file to aws
      val awsCreds = {
        new BasicAWSCredentials(awsAccessKey, awsSecretKey)
      }

      val aws: AmazonS3Client = new AmazonS3Client(awsCreds)

      val fs = HBaseConfProvider.getConf.fs
      val keyName = Utilities.s3KeyFor(from)
      val omd = new ObjectMetadata()
      omd.setContentLength(fs.getFileStatus(from).getLen)
      val openFS = fs.open(from)
      val result = aws.putObject(s3Bucket, keyName, openFS, omd)
      result.getETag.successNel
    }
    catch {
      case e: Exception => FailureResult("Exception copying HDFS log file " + from.toString + " to S3", e).failureNel
    }
  }
}

object MoveTest extends App {
  //20:02:52 INFO   [BufferedLogLineWriter] Moving hdfs log file /user/gravity/devLogWriting/widgetDomReadyEvent/2016_034/hour_16_3_0 to storage location /user/gravity/devLogs/widgetDomReadyEvent/2016_034/hour_16_3_0
  val from: HdfsPath = new HdfsPath("/user/gravity/devLogWriting/widgetDomReadyEvent/2016_034/hour_16_3_0")
  val to: HdfsPath = new HdfsPath("/user/gravity/devLogs/widgetDomReadyEvent/2016_034/hour_16_3_0")
  val toFolder: HdfsPath = to.getParent
  println(toFolder)
  if(!fs.exists(toFolder)) {
   println("need to make " + toFolder )
    fs.mkdirs(toFolder, folderPermission)
  }
  println(fs.rename(from, to))
}

class AvroParquetLogLineWriter(outputPath : HdfsPath, category: String) extends LogLineWriter {
  import com.gravity.logging.Logging._
  private val compressionCodec: CompressionCodecName = CompressionCodecName.GZIP
  private val blockSize: Int = 8 * math.pow(1024, 2).toInt
  private val pageSize: Int = math.pow(1024, 2).toInt
  private val converter: FieldConverter[_] =
    LogProcessingActor.getFieldConverter(category) match {
      case Some(c) => c
      case None => throw new Exception("No converter set up for " + category)
    }
  private val actionOpt = LogLineWriter.additionalActionsByCategory.get(category)

  private val writerOption = {
    try {
      //noinspection ScalaDeprecation
      Some(new AvroParquetWriter[GenericData.Record](outputPath, converter.avroSchema, compressionCodec, blockSize, pageSize, true, hbaseConf.defaultConf))
    }
    catch {
      case t: Throwable =>
        warn("Exception creating parquet writer: " + ScalaMagic.formatException(t))
        None
    }
  }

  override def writeLogLine(line: ArchiveLogLine): Unit = {
    converter.getInstanceFromString(line.logMessage) match {
      case Success(objectToWrite) =>
        writerOption.foreach { writer =>
          val objectAsAvro = converter.toAvroRecordDangerous(objectToWrite)
          writer.write(objectAsAvro)
        }
        actionOpt.foreach(action => action(objectToWrite))

      case Failure(fails) =>
        warn("Error reading object in category " + line.category)
    }
  }


  override def writeLogLine(line: SequenceFileLogLine): Unit = {
    converter.getInstanceFromBytes(line.logBytes) match {
      case Success(objectToWrite) =>
        writerOption.foreach { writer =>
          val objectAsAvro = converter.toAvroRecordDangerous(objectToWrite)
          writer.write(objectAsAvro)
          actionOpt.foreach(action => action(objectToWrite))
        }
      case Failure(fails) =>
        warn("Error reading sequence object in category " + line.category)
    }
  }

  override def flush(): Unit = {}

  override def close(): Unit = writerOption.foreach(_.close())

  override def moveToStorage(segmentId: SegmentId): ValidationNel[FailureResult, HdfsPath] = {
    import LogProcessingActor.{outputLocksPath, zooClient}

    val lock = ZooCommon.buildLock(zooClient, outputLocksPath + segmentId.avroLockName)

    val destinationFileName = {
      try {
        lock.acquire()
        hdfsStoragePathFor(segmentId.year, segmentId.dayOfYear, segmentId.hourOfDay, segmentId.segmentNumber, category)
      }
      finally {
        lock.release()
      }
    }

    ensureParentExists(destinationFileName)

    info("Moving hdfs log file " + outputPath + " to storage location " + destinationFileName)

    try {
      if(fs.rename(outputPath, destinationFileName)) {
        destinationFileName.successNel
      }
      else {
        FailureResult("Filesystem rename returned false for " + outputPath.toString + " to " + destinationFileName.toString).failureNel
      }
    }
    catch {
      case e:Exception =>
        FailureResult("Exception moving hdfs log file " + outputPath + " to storage location " + destinationFileName, e).failureNel
    }
  }


}

//isBinaryInput is no longer used but this class is sent java serialized across the wire so changing it would require shutting down all archiver servers
case class ProcessSegmentPath(path: String, categoryName: String, processingNodePath: String, isBinaryInput: Boolean)

case class SegmentId(year: Int, dayOfYear: Int, hourOfDay: Int, segmentNumber: Int, categoryName: String) {
  val avroLockName : String = categoryName + ".avro" + "-" + year + "-" + dayOfYear + "-" + hourOfDay + "-" + segmentNumber
}

case class TestEvent(message: String) {
  def this(vals : FieldValueRegistry) = this(
    vals.getValue[String](0)
  )
}

object TestEvent {
  implicit object TestEventConverter extends FieldConverter[TestEvent] {
    val fields: FieldRegistry[TestEvent] = new FieldRegistry[TestEvent]("TestEvent")
    fields.registerUnencodedStringField("message", 0, "")

    def fromValueRegistry(reg: FieldValueRegistry): TestEvent = {
      new TestEvent(reg)
    }

    def toValueRegistry(o: TestEvent): FieldValueRegistry = {
      import o._
      new FieldValueRegistry(fields).registerFieldValue(0, message)
    }
  }
}

object LogProcessingActor {
 import com.gravity.logging.Logging._
  private val fieldConverterMap : Map[String, FieldConverter[_]] =
    Seq(
      "clickEvent-redirect" -> ClickEventConverter,
      "impressionServed" -> ImpressionEventConverter,
      "impressionViewedEvent" -> ImpressionViewedEventConverter,
      "test" -> TestEventConverter,
      "beacon.new" -> BeaconEventConverter,
      "invalidBeacons.new" -> BeaconEventConverter,
      "validatedBeacons.new" -> BeaconEventConverter,
      "CampaignBudgetOverage" -> CampaignBudgetOverageConverter,
      "ImpressionFailedEvent" -> ImpressionFailedEventConverter,
      "RssArticleIngestion" -> RssArticleIngestionConverter,
      "SyncUserEvent" -> SyncUserEventConverter,
      "auxiliaryClickEvent" -> AuxiliaryClickEventConverter,
      "widgetDomReadyEvent" -> WidgetDomReadyEventConverter,
      "RecoFailure" -> ImpressionFailedEventConverter,
      "RecogenEvent" -> RecoGenEventConverter,
      "auxiliaryClickEvent" -> AuxiliaryClickEventConverter,
      "SyncUserEvent" -> SyncUserEventConverter,
      "RssArticleIngestion" -> RssArticleIngestionConverter,
      AolBeaconConverterImpl.logCategory -> AolBeaconConverterImpl
    ).toMap

  fieldConverterMap.values.foreach{converter => trace("Got converter schema " + converter.avroSchema.toString(true))}

  def getFieldConverter(categoryName: String) : Option[FieldConverter[_]] = {
    fieldConverterMap.get(categoryName)
  }

  val outputLocksPath: String =  properties.getProperty("recommendation.log.outputLocksPath", "/logStage/outputLocks2/")
  val processingLockPath: String = properties.getProperty("recommendation.log.processingLockPath","/logStage/processingLocks/")

  val zooClient: CuratorFramework = ZooCommon.getEnvironmentClient
}

class LogProcessingActor extends ComponentActor {
  import LogProcessingActor._
  import com.gravity.utilities.Counters._
  import com.gravity.logging.Logging._

  val counterCategory: String = DistributedHDFSLogCollator.categoryName

  def handleMessage(w: MessageWrapper) {
    getPayloadObjectFrom(w) match {
      case ProcessSegmentPath(path, categoryName, processingNodePath, isBinaryInput) =>
        processPath(path, categoryName, processingNodePath)
    }
  }

  private def processPath(path: String, categoryName: String, processingNodePath: String) {

    val segmentPath = new HdfsPath(path)

    val processLockPath =  processingLockPath + categoryName + "/" + path.replace("/", "-")
    val processLock = ZooCommon.buildLock(zooClient, processLockPath)

    try {
      trace("Getting process lock for " + processLockPath)
      processLock.acquire()
      if (fs.exists(segmentPath)) {
        info("Processing stage folder " + segmentPath)

        val outputs = new mutable.HashMap[SegmentId, LogLineWriter]()

        var linesWritten = 0
        try {
          val segmentFiles = fs.listStatus(segmentPath)
          segmentFiles.foreach {
            segmentFile => {
              if (segmentFile.getLen > 0) {
                //can be either avro or sequence. sequence files end in .seq
                val isSequence = segmentFile.getPath.getName.endsWith("seq")
                if (isSequence) {
                  val sequenceReader = new SequenceFile.Reader(hbaseConf.defaultConf, SequenceFile.Reader.file(segmentFile.getPath))
                  var totalLines = 0
                  var linesErrored = 0
                  try {
                    val keyBytes = new BytesWritable()
                    val valueBytes = new BytesWritable()
                    var continue = true
                    while (continue) {
                      try {
                        if (sequenceReader.next(keyBytes, valueBytes)) {
                          totalLines += 1
                          SequenceFileLogLineKeyConverter.getInstanceFromBytes(keyBytes.copyBytes()) match {
                            case Success(sequenceFileLogLineKey) =>
                              val logLine = SequenceFileLogLine(sequenceFileLogLineKey, valueBytes.copyBytes())
                              val dateTime = logLine.timestamp
                              val segmentId = SegmentId(dateTime.getYear, dateTime.getDayOfYear, dateTime.getHourOfDay, Utilities.getSegmentNumber(dateTime.getMinuteOfHour), categoryName)
                              val output = outputs.getOrElseUpdate(segmentId, {
                                val lockName = segmentId.avroLockName
                                val lock = ZooCommon.buildLock(zooClient, outputLocksPath + lockName)
                                val outputPath =
                                  try {
                                    lock.acquire()
                                    hdfsWritingPathFor(segmentId.year, segmentId.dayOfYear, segmentId.hourOfDay, segmentId.segmentNumber, categoryName)
                                  }
                                  finally {
                                    lock.release()
                                  }

                                info("Segment path " + segmentPath + " has lines for output " + outputPath)

                                new AvroParquetLogLineWriter(outputPath, categoryName)

                              })
                              output.writeLogLine(logLine)
                              countPerSecond(counterCategory, "Lines written : " + categoryName)
                              linesWritten += 1
                            case Failure(fails) =>
                              linesErrored += 1
                              warn("Failure reading sequence log line from " + segmentFile.getPath + ": " + fails.toString)
                          }
                          continue = true
                        }
                        else {
                          continue = false
                        }
                      }
                      catch {
                        case e: Exception =>
                          warn(e, "Exception reading line from sequence file " + segmentFile.getPath)
                          totalLines += 1
                          linesErrored += 1
                      }
                    }
                  }
                  finally {
                    sequenceReader.close()
                  }
                  if (totalLines > 0 && totalLines == linesErrored) {
                    throw new Exception("All lines in sequence file sequence segment file " + segmentFile.getPath + " errored. Throwing to prevent file from being deleted.")
                  }
                }
                else {
                  //this shouldn't be happening any more
                  throw new Exception("Input file " + segmentFile.getPath + " is not marked as a seq file. Did something accidentally get staged as avro? Written from an old machine?")
                }


                outputs.values.foreach(_.flush())
                countPerSecond(counterCategory, "Stage files processed")
                countPerSecond(counterCategory, "Stage files processed : " + categoryName)
              }
              else {
                info("Ignoring zero length file " + segmentFile.getPath)
                //fs.delete(segmentFile.getPath, false)
              }
            }
          }
        }
        finally {
          outputs.foreach { case (segmentId, writer) =>
            writer.close()
            writer.moveToStorage(segmentId) match {
              case Success(destinationPath) =>
                countPerSecond(counterCategory, "Log files written")
                countPerSecond(counterCategory, "Log files written : " + categoryName)
                writer.copyToAws(destinationPath) match {
                  case Success(destPath) =>
                    countPerSecond(counterCategory, "S3 log file copies")
                    countPerSecond(counterCategory, "S3 log file copies: " + categoryName)
                  case Failure(fails) =>
                    warn("Failed to copy log file " + destinationPath + " to S3: " + fails.toString())
                    countPerSecond(counterCategory, "S3 log file fails")
                    countPerSecond(counterCategory, "S3 log file fails: " + categoryName)
                }
              case Failure(fails) =>
                countPerSecond(counterCategory, "Log file move failures")
                countPerSecond(counterCategory, "Log file move failures: " + categoryName)
                warn("Failed to move log file to storage " + segmentId + ": " + fails.toString)
            }
          }
        }

        fs.delete(segmentPath, true)

        ZooCommon.deleteNodeIfExists(zooClient, processingNodePath)
      }
      else {
        info("Path " + segmentPath + " did not exist after lock acquisition; it has already been processed")
      }
    }
    finally {
      processLock.release()
    }

  }
}

class LogProcessingComponent extends ServerComponent[LogProcessingActor]("LogProcessing", Seq(classOf[ProcessSegmentPath]), numActors = 12, numThreads = 12)

class DistributedLogProcessingServer(tcpPort : Int) extends RemoteOperationsServer(tcpPort, components = Seq(new LogProcessingComponent())) {
  import LogProcessingActor.{outputLocksPath, processingLockPath}
  import system._

  import scala.concurrent.duration._

  private val lockFolders : List[String] = { //TODO have this be updated regularly so new lock folders aren't ignored
    List(outputLocksPath) ++ ZooCommon.getChildPaths(ZooCommon.getEnvironmentClient, processingLockPath)
  }

  def cleanLocksFolders(): Unit = {
    lockFolders.foreach(lockFolder => {
      ZooCommon.cleanLockFolder(ZooCommon.getEnvironmentClient, lockFolder.toString)
    })
    if(started) system.scheduler.scheduleOnce(12.hours)(cleanLocksFolders())
  }

  system.scheduler.scheduleOnce(30.seconds)(cleanLocksFolders())
}
//
//object LockCleanTestApp extends App {
//  import LogProcessingActor.{outputLocksPath, processingLockPath}
//
//  private val lockFolders : List[String] = {
//    List(outputLocksPath) ++ ZooCommon.getChildPaths(ZooCommon.awsClient, processingLockPath)
//  }
//
//  def cleanLocksFolders(): Unit = {
//    lockFolders.foreach(lockFolder => {
//      ZooCommon.cleanLockFolder(ZooCommon.prodClient, lockFolder)
//    })
//  }
//
//  cleanLocksFolders()
//
//}
