package com.gravity.domain.recommendations

import com.gravity.utilities.time.GrvDateMidnight
import com.gravity.interests.jobs.intelligence.CampaignKey

/**
 * Created by IntelliJ IDEA.
 * Author: Robbie Coleman
 * Date: 2/12/14
 * Time: 11:21 AM
 *            _ 
 *          /  \
 *         / ..|\
 *        (_\  |_)
 *        /  \@'
 *       /     \
 *   _  /  \   |
 * \\/  \  | _\
 *   \   /_ || \\_
 *    \____)|_) \_)
 *
 */
case class AdvertiserCampaignReportDayCampaignKey(day: GrvDateMidnight, advertiserGuid: String, campaignId: Long) {
  lazy val campaignKey: CampaignKey = CampaignKey(advertiserGuid, campaignId)
  override def toString: String = s"${day.toString("yyyy-MM-dd")} => ${advertiserGuid}_$campaignId"
}
