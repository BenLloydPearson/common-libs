package com.gravity.domain.recommendations

import com.gravity.interests.jobs.intelligence.{CampaignKey, ArticleKey}

/**
 * Created by IntelliJ IDEA.
 * Author: Robbie Coleman
 * Date: 2/19/14
 * Time: 3:21 PM
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
case class AdvertiserCampaignReportArticleCampaignKey(articleId: Long, advertiserGuid: String, campaignId: Long) {
  lazy val campaignKey: CampaignKey = CampaignKey(advertiserGuid, campaignId)
  lazy val articleKey: ArticleKey = ArticleKey(articleId)
  override def toString: String = s"articleId:$articleId => ${advertiserGuid}_$campaignId"
}
