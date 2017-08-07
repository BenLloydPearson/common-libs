package com.gravity.interests.jobs.intelligence.operations

import com.gravity.interests.jobs.hbase.HBaseConfProvider
import com.gravity.interests.jobs.intelligence.CampaignStatus.Type
import com.gravity.interests.jobs.intelligence.operations.audit.{AuditService, OperationsAuditLoggingEvents}
import com.gravity.interests.jobs.intelligence.{CampaignKey, CampaignRow, CampaignStatus, Schema}
import com.gravity.utilities.components.FailureResult
import com.gravity.utilities.{EmailUtility, Settings2}
import org.apache.hadoop.conf.Configuration
import org.joda.time.DateTime

import scalaz.Scalaz._
import scalaz.{Index => _, _}

class CampaignAutoCompleteJob(dryRun: Boolean) {
 import com.gravity.logging.Logging._
  import OperationsAuditLoggingEvents._
  implicit val conf: Configuration = HBaseConfProvider.getConf.defaultConf
  private val root = Settings2.webRoot
  def adminLink(ck: CampaignKey) =
    s"http://admintool$root/admin/campaign/edit?campaignKey=$ck"

  def dashboardLink(sg: String, ck: CampaignKey) =
    s"https://dashboard.gravity.com/campaigns/campaign/edit?id=$ck&sg=$sg&sort=-publish_timestamp"

  def newStatus(beforeRow: CampaignRow): Type = if (beforeRow.isSponsored) CampaignStatus.completed else CampaignStatus.paused

  // beforeRow must have been read with at least CampaignService.loggingFetchQuerySpecWithoutRecentArticles
  def completeOrPauseCampaign(beforeRow: CampaignRow): ValidationNel[FailureResult, CampaignRow] = {
    val ck = beforeRow.campaignKey

    for {
      afterRow <- CampaignService.modifyPutRefetch(ck) (put => put.value(_.status, newStatus(beforeRow))) (CampaignService.loggingFetchQuerySpecWithoutRecentArticles)

      _ = AuditService.logAllChanges(ck, -1, Option(beforeRow), afterRow, Seq("CampaignAutoComplete")) valueOr { _.failureNel }
    } yield {
      afterRow
    }
  }

  def runReport(allCampRows: Seq[CampaignRow]): Unit = {
    val campRows   = allCampRows.filter(_.isSponsored)

    val oldAgeDays = 60
    val oldAgeDt   = new DateTime().minusDays(oldAgeDays)
    val epochDt    = new DateTime(0L)

    val badStatus  = Set(CampaignStatus.paused)

    val workRows   = campRows
      .filter { cr => cr.columnTimestamp(_.status).getOrElse(epochDt).getMillis < oldAgeDt.getMillis && badStatus.contains(cr.status) && cr.status != newStatus(cr) }

    val newCampRows = if (dryRun) workRows else workRows.map(completeOrPauseCampaign).flatMap(_.toList)

    if (dryRun || newCampRows.nonEmpty) {
      val dryRunLabel = if (dryRun) "[Dry Run] " else ""
      val verbLeader  = if (dryRun) "would be" else "have been"
      val statuses    = badStatus.map(str => s"'$str'").mkString(" or ")

      val introStr = s"${dryRunLabel}The following sponsored campaign(s) were in $statuses status for the last $oldAgeDays days and $verbLeader automatically set to 'completed':"

      val bodySeq = introStr +: newCampRows.map { row =>
        val ck    = row.campaignKey
        val optSg = SiteService.siteGuid(ck.siteKey)

        val dashboardLine = optSg.map(sg => s"  Dashboard : ${dashboardLink(sg, ck)}\n").getOrElse("")

        s"${row.siteCampaignName}:\n$dashboardLine  Admin Page: ${adminLink(row.campaignKey)}"
      }.sorted

      val toStr   = if (dryRun) "tchappell@gravity.com" else "accountmanagers@gravity.com,tchappell@gravity.com,richard@gravity.com"
      val fromStr = "alerts@gravity.com"
      val subjStr = s"$dryRunLabel${workRows.size} Campaign(s) Auto-Completed"
      val bodyStr = bodySeq.mkString("", "\n\n", "\n")

      EmailUtility.send(toStr, fromStr, subjStr, bodyStr)
    }
  }

  try {
    info("Running CampaignAutoComplete...")

    val campRows = CampaignService.loggingFetchQuerySpecWithoutRecentArticles(Schema.Campaigns.query2).scanToIterable(identity).toList

    runReport(campRows)

    info(s"CampaignAutoComplete completed.")
  } catch {
    case th: Throwable =>
      warn(th, "CampaignAutoComplete threw an exception.")
  }
}
