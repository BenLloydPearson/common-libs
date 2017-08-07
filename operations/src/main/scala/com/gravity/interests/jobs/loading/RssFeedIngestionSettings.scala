package com.gravity.interests.jobs.loading

import com.gravity.interests.jobs.intelligence.{RssFeedSettings, CampaignKey, CampaignArticleStatus}
import com.gravity.interests.jobs.intelligence.operations.ScrubberEnum

/**
 * Created by IntelliJ IDEA.
 * Author: Robbie Coleman
 * Date: 11/26/12
 * Time: 11:20 AM
 */
case class RssFeedIngestionSettings(siteGuid: String, feedUrl: String, skipAlreadyCrawled: Boolean = true, ignoreMissingImages: Boolean = false, initialArticleStatus: CampaignArticleStatus.Type = CampaignArticleStatus.inactive, campaignKeyOption: Option[CampaignKey] = None, scrubberEnum: ScrubberEnum.Type = ScrubberEnum.defaultValue, initialArticleBlacklisted: Boolean = true) {
  override lazy val toString: String = {
    val sb = new StringBuilder
    val dq = '"'
    val cas = ", "
    val cos = ": "

    sb.append(dq).append("feedSettings").append(dq).append(cos).append("{ ")

    sb.append(dq).append("siteGuid").append(dq).append(cos).append(dq).append(siteGuid).append(dq).append(cas)
    sb.append(dq).append("feedUrl").append(dq).append(cos).append(dq).append(feedUrl).append(dq).append(cas)
    sb.append(dq).append("skipAlreadyCrawled").append(dq).append(cos).append(skipAlreadyCrawled).append(cas)
    sb.append(dq).append("ignoreMissingImages").append(dq).append(cos).append(ignoreMissingImages).append(cas)
    sb.append(dq).append("initialArticleStatus").append(dq).append(cos).append(dq).append(initialArticleStatus.toString).append(dq).append(cas)
    sb.append(dq).append("scrubberEnum").append(dq).append(cos).append(dq).append(scrubberEnum.toString).append(dq).append(cas)
    sb.append(dq).append("initialBlacklisted").append(dq).append(cos).append(initialArticleBlacklisted)
    campaignKeyOption.foreach(ck => {
      sb.append(cas).append(dq).append("campaignKey").append(dq).append(cos).append(dq).append(ck.toString).append(dq)
    })

    sb.append(" }")

    sb.toString()
  }
}

object RssFeedIngestionSettings {
  def apply(siteGuid: String, campaignKey: CampaignKey, feedUrl: String, feedSettings: RssFeedSettings): RssFeedIngestionSettings = RssFeedIngestionSettings(
    siteGuid = siteGuid,
    feedUrl = feedUrl,
    skipAlreadyCrawled = feedSettings.skipAlreadyIngested,
    ignoreMissingImages = feedSettings.ignoreMissingImages,
    initialArticleStatus = feedSettings.initialArticleStatus,
    campaignKeyOption = Some(campaignKey),
    scrubberEnum = feedSettings.scrubberEnum,
    initialArticleBlacklisted = feedSettings.initialArticleBlacklisted
  )
  def apply(siteGuid: String, feedUrl: String, feedSettings: RssFeedSettings): RssFeedIngestionSettings = RssFeedIngestionSettings(
    siteGuid = siteGuid,
    feedUrl = feedUrl,
    skipAlreadyCrawled = feedSettings.skipAlreadyIngested,
    ignoreMissingImages = feedSettings.ignoreMissingImages,
    initialArticleStatus = feedSettings.initialArticleStatus,
    campaignKeyOption = None,
    scrubberEnum = feedSettings.scrubberEnum,
    initialArticleBlacklisted = feedSettings.initialArticleBlacklisted
  )
}
