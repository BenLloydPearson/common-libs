package com.gravity.interests.jobs.intelligence.operations

import java.io.{File, FileWriter}
import java.text.NumberFormat

import cascading.pipe.Each
import com.csvreader.CsvWriter
import com.gravity.algorithms.model.{FeatureSettings, Variable, VariableResult}
import com.gravity.interests.jobs.hbase.HBaseConfProvider
import com.gravity.interests.jobs.intelligence.helpers.grvcascading._
import com.gravity.interests.jobs.intelligence.helpers.{grvcascading, grvhadoop}
import com.gravity.interests.jobs.intelligence.jobs.StandardJobSettings
import com.gravity.interests.jobs.intelligence.{CampaignKey, CampaignRow, CampaignStatus, Schema}
import com.gravity.utilities.grvcsv._
import com.gravity.utilities.grvstrings._
import com.gravity.utilities.EmailUtility
import org.apache.hadoop.fs.{FileUtil, Path}
import org.joda.time.DateTime

import scala.collection.mutable.ListBuffer
import scalaz.{Failure, Index => _, _}
import Scalaz._

object CampaignAuditInfo {
  def settingForCampaign(featureVar: Variable, ck: CampaignKey): VariableResult = {
    val scopes = Seq(ck.toScopedKey, ck.siteKey.toScopedKey, FeatureSettings.universalKeyScope)

    FeatureSettings.getScopedVariable(featureVar, scopes)
  }

  def fromCampaignRow(row: CampaignRow): CampaignAuditInfo = {
    val recentSize: Int = row.recentArticleSettings(skipCache = true).size

    val dateTimeSet = row.columnTimestamp(_.siteGuid).toSet

    val createdDateTime = dateTimeSet.reduceOption { (x, y) => if (x.getMillis < y.getMillis) x else y }
    val recentDateTime  = row.recentArticlesByDate.keySet.reduceOption { (x, y) => if (x.getMillis > y.getMillis) x else y }

    CampaignAuditInfo(row.campaignKey,
      row.status, row.columnTimestamp(_.status).filter(_ => !CampaignIngestion.isActiveForIngestion(row.status)),
      createdDateTime, recentDateTime, row.recentArticlesMaxAge, row.recentArticlesSizeSnap,
      recentSize,
      row.recentArticlesSizeSnap.map(recentSize - _).filter(_ > 0),
      row.siteCampaignName)
  }

  def reportHeader: Seq[String] = {
    Seq(
      "Campaign Key",
      "Status",
      "Recent Articles Size",
      "MaxAge TTL",
      "Created-Ish",
      "Recent Article Date",
      "Stopped When",
      "Saved Completed Size",
      "Zombie Articles",
      "SiteName -> CampName"
    )
  }

  def reportDetail(campInfo: CampaignAuditInfo): Seq[String] = {
    Seq(
      campInfo.ck.toString,
      campInfo.status.toString,
      NumberFormat.getIntegerInstance.format(campInfo.recentArticlesSize),
      campInfo.recentArticlesMaxAge.map(NumberFormat.getIntegerInstance.format(_)).getOrElse(""),
      campInfo.minDateTime.map(_.toLocalDate).getOrElse("").toString,
      campInfo.recentDateTime.map(_.toLocalDate).getOrElse("").toString,
      campInfo.stoppedTimeStamp.map(_.toLocalDate).getOrElse("").toString,
      campInfo.recentArticlesSizeSnap.map(NumberFormat.getIntegerInstance.format(_)).getOrElse(""),
      campInfo.recentArticlesZombies.filter(_ > campInfo.recentArticlesZombieSlack).map(NumberFormat.getIntegerInstance.format(_)).getOrElse(""),
      campInfo.siteCampaignName
    )
  }

  val tabDelimTextFldNames: Array[String] = (1 to 10).map(i => s"fld$i").toArray

  def toTabDelimText(ci: CampaignAuditInfo): Seq[String] = {
    Seq(
      ci.ck,
      ci.status,
      ci.stoppedTimeStamp.map(_.getMillis),
      ci.minDateTime.map(_.getMillis),
      ci.recentDateTime.map(_.getMillis),
      ci.recentArticlesMaxAge,
      ci.recentArticlesSizeSnap,
      ci.recentArticlesSize,
      ci.recentArticlesZombies,
      ci.siteCampaignName
    ).map {
      case Some(value) => value.toString
      case None => ""
      case value => value.toString
    }
  }

  def fromTabDelimText(line: String): Option[CampaignAuditInfo] = line.splitBoringly("\t", maxTokens = 10) match {
    case Array(ckStr, statusStr, stoppedTimeStampStr, minDateTimeStr, recentDateTimeStr,
    raMaxAgeStr, raSizeSnapStr, raSizeStr, raZombiesStr, siteCampaignName) =>
      val ckOpt     = CampaignKey.parse(ckStr)
      val statusOpt = CampaignStatus.get(statusStr)
      val stoppedTimeStamp = stoppedTimeStampStr.tryToLong.map(new DateTime(_))
      val minDateTime = minDateTimeStr.tryToLong.map(new DateTime(_))
      val recentDateTime = recentDateTimeStr.tryToLong.map(new DateTime(_))
      val recentArticlesMaxAge = raMaxAgeStr.tryToInt
      val recentArticlesSizeSnap = raSizeSnapStr.tryToInt
      val recentArticlesSizeOpt = raSizeStr.tryToInt
      val recentArticlesZombies = raZombiesStr.tryToInt

      (ckOpt, statusOpt, recentArticlesSizeOpt) match {
        case (Some(ck), Some(status), Some(recentArticlesSize)) =>
          CampaignAuditInfo(
            ck = ck,
            status = status,
            stoppedTimeStamp = stoppedTimeStamp,
            minDateTime = minDateTime,
            recentDateTime = recentDateTime,
            recentArticlesMaxAge = recentArticlesMaxAge,
            recentArticlesSizeSnap = recentArticlesSizeSnap,
            recentArticlesSize = recentArticlesSize,
            recentArticlesZombies = recentArticlesZombies,
            siteCampaignName = siteCampaignName
          ).some

        case _ =>
          None
      }

    case _ => None
  }
}

case class CampaignAuditInfo(ck: CampaignKey,
                             status: CampaignStatus.Type,
                             stoppedTimeStamp: Option[DateTime],
                             minDateTime: Option[DateTime],
                             recentDateTime: Option[DateTime],
                             recentArticlesMaxAge: Option[Int],
                             recentArticlesSizeSnap: Option[Int],
                             recentArticlesSize: Int,
                             recentArticlesZombies: Option[Int],
                             siteCampaignName: String) {
  val recentArticlesSizeRedLimit: Int = CampaignAuditInfo.settingForCampaign(FeatureSettings.recentArticleSizeAlertRed   , ck).value.toInt
  val recentArticlesSizeYellowLimit: Int = CampaignAuditInfo.settingForCampaign(FeatureSettings.recentArticleSizeAlertYellow, ck).value.toInt

  // Zombie Slack makes us more tolerant when some of the aged-out articles in a completed campaign may also appear in an active campaign, and receive a pubDate update.
  val recentArticlesZombieSlack: Int = CampaignAuditInfo.settingForCampaign(FeatureSettings.recentArticleZombieSlack    , ck).value.toInt
}

object CampaignAuditReport {
 import com.gravity.logging.Logging._
  val (levelIsGreen, levelIsYellow, levelIsRed) = (0, 1, 2)

  def emailReportIfAppropriate(campInfos: Seq[CampaignAuditInfo], csvFile: File): Unit = {
    // Find the CampInfos that have problems.
    val redAlertCis = campInfos.filter(ci => ci.recentArticlesSize > ci.recentArticlesSizeRedLimit).sortBy(-_.recentArticlesSize)
    val yelAlertCis = campInfos.filter(ci => ci.recentArticlesSize > ci.recentArticlesSizeYellowLimit && ci.recentArticlesSize <= ci.recentArticlesSizeRedLimit).sortBy(-_.recentArticlesSize)
    val zombieCis   = campInfos.filter(ci => ci.recentArticlesZombies.getOrElse(0) > ci.recentArticlesZombieSlack).sortBy(-_.recentArticlesZombies.getOrElse(0))

    // Use scarier subject for red-alert failures.
    val alertLevel = if (redAlertCis.nonEmpty)
      Option("WARNING")
    else if (yelAlertCis.nonEmpty || zombieCis.nonEmpty)
      Option("NOTICE")
    else
      None

    // Only send the e-mail if the results are sufficiently scary.
    if (alertLevel.isDefined) {
      // Create the Subject line for the e-mail.
      val subject = {
        val whyBuffer = ListBuffer[String]()

        if (redAlertCis.nonEmpty || yelAlertCis.nonEmpty)
          whyBuffer += s"Too Many Articles"

        if (zombieCis.nonEmpty)
          whyBuffer += "Possible Zombies"

        val why = whyBuffer.mkString(", ")

        s"${alertLevel.getOrElse("INFO")} from CampaignAuditReport ($why)"
      }

      // Create the message body for the e-mail.
      val body = {
        val bodyBuffer = ListBuffer[String]()

        if (redAlertCis.nonEmpty) {
          bodyBuffer += s"${NumberFormat.getIntegerInstance.format(redAlertCis.size)} campaign(s) exceed their red-alert size:"

          for (ci <- redAlertCis)
            bodyBuffer += s"  ${NumberFormat.getIntegerInstance.format(ci.recentArticlesSize)} > ${NumberFormat.getIntegerInstance.format(ci.recentArticlesSizeRedLimit)} in ${ci.ck}."

          bodyBuffer += ""
        }

        if (yelAlertCis.nonEmpty) {
          bodyBuffer += s"${NumberFormat.getIntegerInstance.format(yelAlertCis.size)} campaign(s) exceed their yellow-alert size:"

          for (ci <- yelAlertCis)
            bodyBuffer += s"  ${NumberFormat.getIntegerInstance.format(ci.recentArticlesSize)} > ${NumberFormat.getIntegerInstance.format(ci.recentArticlesSizeYellowLimit)} in ${ci.ck}."

          bodyBuffer += ""
        }

        if (zombieCis.nonEmpty) {
          bodyBuffer += s"${NumberFormat.getIntegerInstance.format(zombieCis.size)} campaign(s) have possible zombie articles:"

          for (ci <- zombieCis)
            bodyBuffer += s"  ${ci.recentArticlesZombies.getOrElse(0)} zombies in ${ci.ck}."

          bodyBuffer += ""
        }

        bodyBuffer += "See enclosed report."

        bodyBuffer.mkString("", "\n", "\n")
      }

      EmailUtility.send(
        "accountmanagers@gravity.com,tchappell@gravity.com",
        "alerts@gravity.com",
        subject,
        body,
        attachFileOption = Some(csvFile)
      )
    }
  }

  def runReport(campInfos: Seq[CampaignAuditInfo]): Boolean = {
    val csvFile = File.createTempFile("CampaignAuditReport", ".csv", new File("/tmp"))

    val ioWriter  = new FileWriter(csvFile)
    val csvWriter = new CsvWriter(ioWriter, ',')

    try {
      csvWriter.record(CampaignAuditInfo.reportHeader:_*)

      campInfos.sortBy(ci => (
        -1 * Math.max(0, ci.recentArticlesZombies.getOrElse(0) - ci.recentArticlesZombieSlack),
        -1 * ci.recentArticlesSize,
        ci.status.toString)
      ).foreach { campInfo =>
        csvWriter.record(CampaignAuditInfo.reportDetail(campInfo):_*)
      }
    } finally {
      csvWriter.close()
      ioWriter.close()
    }

    CampaignAuditReport.emailReportIfAppropriate(campInfos, csvFile)

    val copiedOk = copyReportToHdfs(csvFile)

    if (copiedOk) {
      info(s"CampaignAuditReport updating campaign size snapshots...")

      for (ci <- campInfos) {
        if (ci.status == CampaignStatus.completed && ci.recentArticlesSizeSnap.isEmpty) {
          CampaignService.put(Schema.Campaigns.put(ci.ck).value(_.recentArticlesSizeSnap, ci.recentArticlesSize)) match {
            case Failure(fails) => warn(fails, s"Failure updating campaign size snapshot for ${ci.ck}")
            case _ =>
          }
        } else if (ci.status != CampaignStatus.completed && ci.recentArticlesSizeSnap.isDefined) {
          info(s"CampaignAuditReport removing non-completed campaign ${ci.ck}'s size snapshot.")

          CampaignService.delete(Schema.Campaigns.delete(ci.ck).values(_.meta, Set(Schema.Campaigns.recentArticlesSizeSnap.columnName))) match {
            case Failure(fails) => warn(fails, s"Failure removing campaign size snapshot for ${ci.ck}")
            case _ =>
          }
        }
      }
    }

    copiedOk
  }

  def copyReportToHdfs(csvFile: File): Boolean = {
    val hdfsFile  = "/user/gravity/reports/CampaignAuditReport.csv"
    val hdfsFile1 = "/user/gravity/reports/CampaignAuditReport1.csv"

    val deleteSource  = true
    val overwrite     = true

    try {
      FileUtil.copy(
        HBaseConfProvider.getConf.fs, new Path(hdfsFile),
        HBaseConfProvider.getConf.fs, new Path(hdfsFile1),
        deleteSource, overwrite, HBaseConfProvider.getConf.defaultConf)
    } catch {
      case ex: Exception =>
        info(s"CampaignAuditReport failed to make copy of old report from $hdfsFile to $hdfsFile1 (${ex.getMessage}), continuing...", ex)
    }

    try {
      HBaseConfProvider.getConf.fs.delete(new Path(hdfsFile), false)
    } catch {
      case ex: Exception =>
        info(s"CampaignAuditReport failed to delete old report at $hdfsFile (${ex.getMessage}), continuing...", ex)
    }

    try {
      if (FileUtil.copy(csvFile, HBaseConfProvider.getConf.fs, new Path(hdfsFile), deleteSource, HBaseConfProvider.getConf.defaultConf)) {
        info(s"CampaignAuditReport copied to $hdfsFile")
        true
      } else {
        warn(s"CampaignAuditReport failed to copy report to $hdfsFile")
        false
      }
    } catch {
      case ex: Exception =>
        warn(ex, s"CampaignAuditReport failed to copy report to $hdfsFile (${ex.getMessage})")
        false
    }
  }
}

/**
 * This should never have to have been implemented as a map/reduce job,
 * but asking for all of the recentArticles from within a normal job keeps
 * clobbering the HBase region servers, even if there are only 40,000 recent articles.
 */
class CampaignAuditReportJob(outputDirectory: String, settings: StandardJobSettings) extends Serializable {
  val counterGroup = "Custom - CampaignAuditReportJob"

  val source: _root_.com.gravity.interests.jobs.intelligence.helpers.grvcascading.TapAlias = grvcascading.fromTable(Schema.Campaigns) {
    _.withFamilies(_.meta, _.recentArticles)
  }

  val allFields: Seq[String] = CampaignAuditInfo.reportHeader

  val pipe: Each = grvcascading.pipe("campaign-audit-report-pipe").each((flow, call) => {
    val campRow = call.getRow(Schema.Campaigns)

    flow.increment(counterGroup, "CampaignRows Read", 1)

    val campInfo = CampaignAuditInfo.fromCampaignRow(campRow)

    call.write(CampaignAuditInfo.toTabDelimText(campInfo):_*)
  })(CampaignAuditInfo.tabDelimTextFldNames:_*)

  val sink: _root_.com.gravity.interests.jobs.intelligence.helpers.grvcascading.TapAlias = grvcascading.toTextDelimited(outputDirectory)(CampaignAuditInfo.tabDelimTextFldNames:_*)

  grvcascading.flowWithOverrides(mapperMB=4000, mapperVM=6000, reducerMB = 1400, reducerVM = 2200).connect("grv-map-report-flow", source, sink, pipe).complete()

  // Combine output of the run into a single sequence of CampaignAuditInfos.
  val campInfos: Seq[CampaignAuditInfo] = grvhadoop.perHdfsDirectoryLineToSeq(HBaseConfProvider.getConf.fs, outputDirectory)(CampaignAuditInfo.fromTabDelimText).flatten

  // And now run the report, given the laboriously-acquired CampInfos.
  CampaignAuditReport.runReport(campInfos)
}

