package com.gravity.interests.jobs.intelligence.jobs

import java.io._

import cascading.flow.hadoop2.Hadoop2MR1FlowConnector
import cascading.flow.{Flow, FlowDef, FlowListener}
import com.gravity.domain.FieldConverters.BeaconEventConverter
import cascading.pipe.Pipe
import cascading.scheme.hadoop.TextLine
import cascading.tap.hadoop.Hfs
import cascading.tuple.{Fields, Tuple}
import com.gravity.domain.BeaconEvent
import com.gravity.hbase.mapreduce._
import com.gravity.hbase.schema._
import com.gravity.interests.jobs.intelligence.helpers.{ReducerCountConfInternal, grvcascading}
import com.gravity.interests.jobs.intelligence.helpers.grvcascading._
import com.gravity.interests.jobs.intelligence.operations.{MetricsEvent, SiteService}
import com.gravity.utilities.analytics.{DateMidnightRange, TimeSliceResolutions}
import com.gravity.utilities.components.FailureResult
import com.gravity.utilities.grvtime._
import com.gravity.utilities.time.GrvDateMidnight
import org.apache.avro.generic.GenericData
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hbase.client.Mutation
import org.apache.hadoop.io.{LongWritable, NullWritable, Text}
import org.apache.hadoop.mapred.JobConf
import org.apache.hadoop.mapreduce.Job
import org.joda.time.format.DateTimeFormat
import org.joda.time.{DateTime, DurationFieldType, PeriodType}
import com.gravity.utilities.grvfields._

import scala.collection.mutable.Map
import scalaz.{Failure, Success}

/*             )\._.,--....,'``.
 .b--.        /;   _.. \   _\  (`._ ,.
`=,-,-'~~~   `----(,_..'--(,_..'`-.;.'  */

object JobHelpers {
  private val defaultReducerSize = 30
  private val maxReducerSize = 200
  val defaultReducers = ReducerCountConfInternal(defaultReducerSize)
  val zeroBytesWritable = makeWritable(_.writeByte(0))
  val avroPathFmt = "/user/gravity/logs.avro/validatedBeacons.new/%d_%03d/*"

  val hdfsLogBase = "/user/gravity/logs/"
  val hdfsLogAvroBase = "/user/gravity/logs.avro/"
  val hdfsCleansedReportParquetBase = "/user/gravity/reports/parquet/cleansing/"

  val recoMetricsImpressionsParquetBasePaths = Seq(hdfsLogAvroBase + "impressionServed/")
  val recoMetricsClicksParquetBasePaths = Seq(hdfsLogAvroBase + "clickEvent-redirect/")
  val recoMetricsImpressionsViewedParquetBasePaths = Seq(hdfsLogAvroBase + "impressionViewedEvent/")

  val recoMetricsCleansedClicksParquetBasePaths = hdfsCleansedReportParquetBase + "click/"
  val recoMetricsCleansedImpressionsParquetBasePaths = hdfsCleansedReportParquetBase + "impressionServed/"
  val recoMetricsCleansedImpressionsViewedParquetBasePaths = hdfsCleansedReportParquetBase + "impressionViewed/"

  val datePrintFormatter = DateTimeFormat.forPattern("MM/dd/yyyy")
  val singleDayKey = "single.day"
  val singleDaySettings = StandardJobSettings.get(true, scala.collection.mutable.Map(singleDayKey -> true))
  val timePeriodKey = "time.period"
  val dateRangeKey = "date.range"

  // PH
  val similarArticlesJobMap = Map("siteguid" -> SiteService.YAHOONEWSGUID, "lastNdays" -> "7")
  val similarArticlesJobSettings = StandardJobSettings.get(false, Map.empty, similarArticlesJobMap)
  // PH

  def optimalReducers(howManyYouWant: => Int): HConfigLet = {
    val optimalSize = math.min(maxReducerSize, math.max(defaultReducerSize, howManyYouWant))
    ReducerCountConfInternal(optimalSize)
  }

  def getSiteSettings(siteKey: String, siteGuid: String): StandardJobSettings = {
    val map = Map(siteKey -> siteGuid)
    StandardJobSettings.get(false, Map.empty, map)
  }

  def getNumDaysFromSettings(settings: StandardJobSettings) = {

    val numDays = settings.stringBag.get(timePeriodKey) match {
      case Some(tp) => {
        TimeSliceResolutions.parse(tp) match {
          case Some(period) => period.interval.toPeriod(PeriodType.days()).getDays
          case None => throw new RuntimeException("Invalid time.period passed to settings. Value received: " + tp)
        }
      }
      case None => if (settings.incremental) {
        if (settings.isFlagPresentAndEnabled(singleDayKey)) 1 else 2
      } else {
        9
      }
    }
    numDays
  }

  def isBeforeYesterday(day: DateTime): Boolean = {
    val yesterday = new GrvDateMidnight().minusDays(1)
    new GrvDateMidnight(day).isBefore(yesterday)
  }

  def  buildCleansedRecoParquetPathsFromDays(days: Seq[DateTime]): Map[String, String] = {
    println(days.partition(isBeforeYesterday))
    val (daysBeforeYesterday, yesterdayAndOn) = days.partition(isBeforeYesterday)

    val logFolders =
      (for {
      //          day <- daysBeforeYesterday
        day <- days
        dayStr = day.toYearDayString
      } yield {
        Map(
          recoMetricsCleansedClicksParquetBasePaths + dayStr + "/cleansed" -> "parquet-click",
          recoMetricsCleansedImpressionsParquetBasePaths + dayStr + "/cleansed/unit" -> "parquet-imp",
          recoMetricsCleansedImpressionsViewedParquetBasePaths + dayStr + "/cleansed" -> "parquet-view"
        )
      }).reduce(_ ++ _)

    logFolders
  }

  def buildRecoParquetPathsFromDays(days: Seq[DateTime]): Map[String, String] = {
    val (daysBeforeYesterday, yesterdayAndOn) = days.partition(isBeforeYesterday)

    val logFolders = {
      val cleansedPaths = if (daysBeforeYesterday.nonEmpty) {
        (for {
          day <- daysBeforeYesterday
          dayStr = day.toYearDayString
        } yield {
            Map(
              recoMetricsCleansedClicksParquetBasePaths + dayStr + "/cleansed" -> "parquet-click",
              recoMetricsCleansedImpressionsParquetBasePaths + dayStr + "/cleansed/unit" -> "parquet-imp",
              recoMetricsCleansedImpressionsViewedParquetBasePaths + dayStr + "/cleansed" -> "parquet-view"
            )
          }).reduce(_ ++ _)
      } else {
        Map[String, String]()
      }

      val origPaths = if (yesterdayAndOn.nonEmpty) {
        (for {
          day <- yesterdayAndOn
          dayStr = day.toYearDayString
        } yield {
            recoMetricsClicksParquetBasePaths.map(_ + dayStr -> "parquet-click").toMap ++
              recoMetricsImpressionsParquetBasePaths.map(_ + dayStr -> "parquet-imp").toMap ++
              recoMetricsImpressionsViewedParquetBasePaths.map(_ + dayStr -> "parquet-view")
          }).reduce(_ ++ _)
      } else {
        Map[String, String]()
      }

      cleansedPaths ++ origPaths
    }

    logFolders
  }

  // ignore is.click and return paths for clicks, served, & viewed, use parquet for cleansed
  def buildRecoParquetPathsFromSettings(settings: StandardJobSettings): Map[String, String] = {
    val days = getDaysFromSettings(settings)
    buildRecoParquetPathsFromDays(days)
  }

  def buildRecoParquetPathsFromDay(day: DateTime): Map[String, String] = {
    buildRecoParquetPathsFromDays(Seq(day))
  }


  def getUnitImpressionParquetPathFromDay(day: DateTime): String = {
    JobHelpers.buildRecoParquetPathsFromDay(day).find(_._2 == "parquet-imp").get._1 // this is intentionally a get
  }

  def buildAvroBeaconPaths(daysToRun: Int, fromDateInclusive: DateTime = new DateTime()): Seq[String] = {
    (0 until daysToRun) map {i =>
      val date = fromDateInclusive.minusDays(i)
      avroPathFmt.format(date.getYear, date.getDayOfYear)
    }
  }

  def buildLogFileGlob(daysToRun: Int, folder: String, fromDateInclusive: DateTime = new DateTime()): String = {
    val terminatedHdfdBase  = if (hdfsLogBase.endsWith("/")) hdfsLogBase else hdfsLogBase + "/"
    val terminatedLogFolder = if (folder.endsWith("/")) folder else folder + "/"

    val dateGlob = (for (rdt <- expandDaysBefore(daysToRun, fromDateInclusive)) yield rdt.toYearDayString)
      .mkString("{", ",", "}")

    s"$terminatedHdfdBase$terminatedLogFolder$dateGlob"
  }

  def buildAvroPaths(daysToRun: Int, folder: String, fromDateInclusive: DateTime = new DateTime()): Seq[String] = {
    val terminatedHdfdBase  = if (hdfsLogAvroBase.endsWith("/")) hdfsLogAvroBase else hdfsLogAvroBase + "/"
    val terminatedLogFolder = if (folder.endsWith("/")) folder else folder + "/"

    for (rdt <- expandDaysBefore(daysToRun, fromDateInclusive)) yield s"$terminatedHdfdBase$terminatedLogFolder${rdt.toYearDayString}"
  }

  def buildAvroBeaconPath(day: Int, year: Int) : String = {
    avroPathFmt.format(year, day)
  }

  def getDaysFromSettings(settings: StandardJobSettings): Seq[DateTime] = {
    settings.stringBag.get(timePeriodKey) match {
      case Some(timePeriodString) => TimeSliceResolutions.parse(timePeriodString) match {
        case Some(period) => expandDaysBefore(period.interval.toPeriod(PeriodType.days()).getDays)
        case None => throw new RuntimeException("Invalid " + timePeriodKey + " passed to settings. Value received: " + timePeriodString)
      }
      case None => settings.stringBag.get(dateRangeKey) match {
        case Some(dateRangeString) => DateMidnightRange.parse(dateRangeString) match {
          case Some(range) => range.daysWithin.toSeq.map(_.toDateTime)
          case None => throw new RuntimeException("Invalid " + dateRangeKey + " passed to settings. Value received: " + dateRangeString)
        }
        case None => {
          val numDays = if (settings.incremental) {
            if (settings.isFlagPresentAndEnabled(singleDayKey)) 1 else 2
          } else {
            9
          }
          expandDaysBefore(numDays)
        }
      }
    }
  }

  def expandDaysBefore(daysToRun: Int, fromDateInclusive: DateTime = new DateTime()): Seq[DateTime] = {
    for {
      i <- 0 until daysToRun
      dt = fromDateInclusive.minusDays(i)
    } yield dt
  }

  def buildClickAndImpressionAndImpressionViewedAvroPathsWithDays(endDay: DateTime = new DateTime(), daysBefore:Int = 7) = {
    val startTime: DateTime = endDay.toDateHour.asMidnightHour.minusDays(daysBefore).toDateTime
    val endTime: DateTime = startTime.toDateTime.withFieldAdded(DurationFieldType.days(), daysBefore).minusDays(1)
    val days = expandDays(startTime, endTime)
    //    buildRecoParquetPathsFromDays(days)
    buildCleansedRecoParquetPathsFromDays(days)
  }


  def expandDays(startTime: DateTime, endTime: DateTime): Seq[DateTime] = {

    var days = collection.mutable.Seq[DateTime]()

    var day = endTime

    while(day.getMillis >= startTime.getMillis) {

      days = days :+ day

      day = day.minusDays(1)
    }

    days
  }

  def buildClickAndImpressionAndImpressionViewedAvroPathsForAMonth(endDay: DateTime = new DateTime(), monthAgo:Int = 0) = {
    val startTime: DateTime = endDay.minusMonths(monthAgo).toDateHour.asMonthHour.toDateTime
    val endTime: DateTime = startTime.toDateTime.withFieldAdded(DurationFieldType.months(), 1).minusDays(1)
    val days = expandDays(startTime, endTime)
    //    buildRecoParquetPathsFromDays(days)
    buildCleansedRecoParquetPathsFromDays(days)
  }

  def buildDelimitedString(delimiter: String, items: Any*): String = {
    val sb = new StringBuilder()
    var pastFirst = false
    for (item <- items) {
      sb.append(item)
      if (pastFirst) {
        sb.append(delimiter)
      } else {
        pastFirst = true
      }
    }
    sb.toString()
  }


}

trait StandardSettings {
  type SettingsClass = StandardJobSettings
}

/** This is a trait because it implements the first half of the mapper (the input key and value) -- it doesn't care what the output is, so it can be mixed in anywhere.
  *
  */
trait MagellanMapper {
  this: HMapper[LongWritable, GenericData.Record, _, _] =>

  def perBeacon(beacon: BeaconEvent)

  private var incaseOfEmergencyBreakGlass = new Array[Byte](1000000)

  override def map() {
    try {
      context.getCurrentValue.getSchema.getName match {
        case "BeaconEvent" => {
          getInstanceFromAvroRecord[BeaconEvent](context.getCurrentValue) match {
            case Success(event: BeaconEvent) => {
              perBeacon(event)
            }
            case Failure(fails) => ctr("Couldn't get instance of beacon event.")
          }
        }
      }
    } catch {
      case utfEx: UTFDataFormatException => {
        println("ERROR::UTFDataFormatException => Failed to serialize MetricsData generated from beacon String: " + value.toString)
        ctr("Failures caused by java.io.UTFDataFormatException", 1l)
      }
      case ex: Exception => ctr("Failures caused by " + ex.getClass.getCanonicalName, 1l)
      case err: java.lang.OutOfMemoryError => { ctr("OOM - very bad") ; incaseOfEmergencyBreakGlass = null ; System.gc() ; incaseOfEmergencyBreakGlass = new Array[Byte](1000000) ; println("ERROR:OOM! " + this.context.getInputSplit.getLocations.mkString(",")) }
    }
  }
}

trait MetricsEventMapper {
  this: HMapper[LongWritable, Text, _, _] =>

  var incaseOfEmergencyBreakGlass = new Array[Byte](1000000)

  def perEventSuccess(value: MetricsEvent, line: String)

  def perEventFailure(failure: FailureResult) {
    failure.printError()
    ctr("FieldWriterMapper failures not handled")
  }

  override def map() {
    var line: String = ""
    try {
      line = context.getCurrentValue.toString
      MetricsEvent.getMetricsEventFromString(line) match {
        case Success(value) => perEventSuccess(value, line)
        case Failure(fails) => fails.list.map(f => FailureResult(f.message + "\nFailed line:\n" + line, f.exceptionOption)).foreach(perEventFailure)
      }
    }
    catch {
      case ex: Exception => perEventFailure(FailureResult("Failed line:\n" + line, ex))
      case err: java.lang.OutOfMemoryError => {
        ctr("OOM - very bad")
        incaseOfEmergencyBreakGlass = null
        System.gc()
        incaseOfEmergencyBreakGlass = new Array[Byte](1000000)
        println("ERROR:OOM! Size: " + context.getCurrentValue.asInstanceOf[Text].getLength)
      }
    }
  }
}

abstract class ToTableMapper[MK, MV, T <: HbaseTable[T, R, RR], R, RR <: HRow[T, R]](mk: Class[MK], mv: Class[MV], table: HbaseTable[T, R, RR]) extends HMapper[MK, MV, NullWritable, Mutation] with ToTableWritable[T, R, RR] {
}

abstract class FromTableToTextMapper[T <: HbaseTable[T, R, RR], R, RR <: HRow[T, R]](table: HbaseTable[T, R, RR])
  extends FromTableMapper[T, R, RR, NullWritable, Text](table, classOf[NullWritable], classOf[Text]) {
  def writeln(line: String) {
    write(NullWritable.get(), new Text(line))
  }

  def writetabs(items: Any*) {
    val text = JobHelpers.buildDelimitedString("\t", items)
    write(NullWritable.get(), new Text(text))
  }
}


case object LogHbaseScannerUsage extends HConfigLet {
  override def configure(job: Job) {
    job.getConfiguration.set("hbase.client.log.scanner.activity", "true")
  }
}

case object CompressMapOutputIfAvailable extends HConfigLet {
  //com.hadoop.compression.lzo.LzoCodec
  //Set code to check if available
  override def configure(job: Job) {
    if (com.gravity.utilities.Settings.isProductionServer) {
      job.getConfiguration.set("mapred.compress.map.output", "true")
      job.getConfiguration.set("mapreduce.map.output.compress", "true")
      job.getConfiguration.set("mapred.output.compression.type", "BLOCK")
      job.getConfiguration.set("mapreduce.output.fileoutputformat.compress.type", "BLOCK")
      //job.getConfiguration.set("mapred.map.output.compression.codec", "com.hadoop.compression.lzo.LzoCodec")
      job.getConfiguration.set("mapred.map.output.compression.codec", "org.apache.hadoop.io.compress.SnappyCodec")
      job.getConfiguration.set("mapred.output.compression.codec", "org.apache.hadoop.io.compress.SnappyCodec")
      //
    } else {
      println("Will not compress output of mapper")
    }
  }
}

case object TemporaryMemoryOverrideConf extends HConfigLet {
  override def configure(job: Job) {
    val conf = job.getConfiguration
    conf.set("mapred.map.child.java.opts", "-Xmx768m -server -Djava.net.preferIPv4Stack=true")
    conf.set("mapreduce.map.java.opts", "-Xmx768m -server -Djava.net.preferIPv4Stack=true")
    conf.set("mapred.reduce.child.java.opts", "-Xmx768m -server -Djava.net.preferIPv4Stack=true")
    conf.set("mapreduce.reduce.java.opts", "-Xmx768m -server -Djava.net.preferIPv4Stack=true")
    conf.set("mapred.child.java.opts", "-Xmx768m -server -Djava.net.preferIPV4Stack=true")
    conf.set("mapreduce.java.opts", "-Xmx768m -server -Djava.net.preferIPV4Stack=true")
    conf.setInt("mapred.job.map.memory.mb", 2048)
    conf.setInt("mapreduce.map.memory.mb", 2048)
    conf.setInt("mapred.job.reduce.memory.mb", 2048)
    conf.setInt("mapreduce.reduce.memory.mb", 2048)
  }
}

case class ParameterizedMemoryOverrideConf(mapMemMB: Int = 2048, reduceMemMB: Int = 2048, taskTimeout: Int = Int.MaxValue) extends HConfigLet {
  override def configure(job: Job) {
    val conf = job.getConfiguration

    // setup up these using parameters if needed
    conf.set("mapred.map.child.java.opts", "-Xmx"+ mapMemMB +"m -server -Djava.net.preferIPv4Stack=true")
    conf.set("mapreduce.map.java.opts", "-Xmx"+ mapMemMB +"m -server -Djava.net.preferIPv4Stack=true")
    conf.set("mapred.reduce.child.java.opts", "-Xmx"+ reduceMemMB +"m -server -Djava.net.preferIPv4Stack=true")
    conf.set("mapreduce.reduce.java.opts", "-Xmx"+ reduceMemMB +"m -server -Djava.net.preferIPv4Stack=true")
    conf.set("mapred.child.java.opts", "-Xmx" + Math.max(mapMemMB, reduceMemMB) + "m -server -Djava.net.preferIPV4Stack=true")
    conf.set("mapreduce.java.opts", "-Xmx" + Math.max(mapMemMB, reduceMemMB) + "m -server -Djava.net.preferIPV4Stack=true")

    // give 20% more to hadoop than java vm size
    val map = (mapMemMB * 1).ceil.toInt
    val reduce = (reduceMemMB * 1).ceil.toInt

    conf.setInt("mapred.job.map.memory.mb", map)
    conf.setInt("mapreduce.map.memory.mb", map)
    conf.setInt("mapred.job.reduce.memory.mb", reduce)
    conf.setInt("mapreduce.reduce.memory.mb", reduce)
    conf.setInt("mapred.task.timeout", taskTimeout)
    conf.setInt("mapreduce.task.timeout", taskTimeout)

  }
}

case class JobAuditConfig(auditOutputMethod: Option[String] = None,
                          auditOutputPath: Option[String] = None) {
}

object JobAuditConfig {
  def apply(config: Map[String, String]) = {
    val auditOutputMethod = config.get("auditOutputMethod")
    val auditOutputPath = config.get("auditOutputPath")
    new JobAuditConfig(auditOutputMethod, auditOutputPath)
  }
}

trait JobAuditing {
  def flowName = "unnamed-cascading-job"

  def defaultInput: TapAlias
  def defaultAssembly: Pipe
  def defaultOutput: String => TapAlias

  def getOutput: String => String => TapAlias = { outputMethod =>
    defaultOutput
  }

  def getFlowConnector: Hadoop2MR1FlowConnector = {
    grvcascading.flow
  }

  def runFlow(outputDir: String, auditConfig: JobAuditConfig, flowName: String = flowName) = {
    val flow = setupFlow(outputDir, auditConfig, flowName)
    flow.complete()
  }

  def setupFlow(outputDir: String, auditConfig: JobAuditConfig, flowName: String = flowName) = {
    val assembly = defaultAssembly
    val standardOutputPipe = grvcascading.pipe("standardOutput", assembly)

    val flowDef = FlowDef.flowDef()
      .setName(flowName)
      .addSource(assembly, defaultInput)
      .addTailSink(standardOutputPipe, defaultOutput(outputDir))

    addAuditOutput(flowDef, assembly, auditConfig)

    getFlowConnector.connect(flowDef)
  }

  def addAuditOutput(flowDef: FlowDef, auditedPipe: Pipe, auditConfig: JobAuditConfig) = {
    auditConfig match {
      case JobAuditConfig(None, _) =>
      case JobAuditConfig(Some(auditOutputMethod), Some(auditOutputPath)) => {
        val auditOutputPipe = grvcascading.pipe("auditOutput", auditedPipe)
        val auditOutputTap = getOutput(auditOutputMethod)(auditOutputPath)
        flowDef.addTailSink(auditOutputPipe, auditOutputTap)
      }
    }
  }
}

trait JobSuccessMarker extends JobAuditing {
  import org.apache.hadoop.fs.FileSystem
  override def runFlow(outputDir: String, auditConfig: JobAuditConfig, flowName: String = flowName): Unit = {
    val flow = setupFlow(outputDir, auditConfig, flowName)
    flow.addListener(new FlowListener {
      def writeSuccessDir(flow: Flow[JobConf], targetPath: String) {
        val tap = new Hfs(new TextLine(new Fields("line")), targetPath)
        val collector = tap.openForWrite(flow.getFlowProcess)
        collector.add(new Tuple("success"))
        collector.close()
      }
      def writeSuccessFile(flow: Flow[JobConf], targetPath: String) {
        val fs: FileSystem = FileSystem.get(flow.getConfig)
        //val br: BufferedWriter = new BufferedWriter(new OutputStreamWriter(fs.create(new Path(targetPath))))
        val os = fs.create(new Path(targetPath))
        os.write("success\n".getBytes)
        os.close()
      }
      override def onCompleted(flow: Flow[_]): Unit = {
        val jobFlow = flow.asInstanceOf[Flow[JobConf]]
        val targetDir = new Path(outputDir).toString + "-status/_SUCCESS"
        //flow.getFlowStats.isSuccessful
        writeSuccessDir(jobFlow, targetDir)
        writeSuccessFile(jobFlow, targetDir+".file")
      }
      override def onThrowable(flow: Flow[_], e: Throwable): Boolean = {
        true
      }
      override def onStarting(flow: Flow[_]) {

      }
      override def onStopping(flow: Flow[_]) {

      }
    })
    flow.complete()
  }
}



