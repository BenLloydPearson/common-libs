package com.gravity.domain.recommendations

import com.gravity.interests.jobs.intelligence.{ArticleKey, CampaignKey}
import com.gravity.utilities.time.{DateHour, GrvDateMidnight}

/**
 * Created by tdecamp on 7/14/15.
 * {{insert neat ascii diagram here}}
 */
case class AdvertiserCampaignReportDayArticleCampaignKey(
  day: GrvDateMidnight,
  articleId: Long,
  advertiserGuid: String,
  campaignId: Long) {
  lazy val campaignKey: CampaignKey = CampaignKey(advertiserGuid, campaignId)
  lazy val articleKey: ArticleKey = ArticleKey(articleId)

  override def toString: String = s"day:$day - articleId:$articleId => ${advertiserGuid}_$campaignId"
}
