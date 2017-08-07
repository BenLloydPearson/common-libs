package com.gravity.domain.recommendations

import com.gravity.interests.jobs.intelligence.{CampaignKey, SiteKey}
import com.gravity.interests.jobs.intelligence.schemas.DollarValue
import com.gravity.utilities.eventlogging.FieldValueRegistry
import org.joda.time.DateTime

/**
 * Created by IntelliJ IDEA.
 * Author: Robbie Coleman
 * Date: 2/18/14
 * Time: 10:07 AM
 *            _ 
 *          /  \
 *         / ..|\
 *        (_\  |_)
 *        /  \@'
 *       /     \
 *   _  /  \   |
 *  \\/  \  | _\
 *   \   /_ || \\_
 *    \____)|_) \_)
 *
 */
case class AdvertiserCampaignArticleReportRow(
     logDate: DateTime,
     advertiserGuid: String,
     campaignId: Long,
     articleId: Long,
     articleImpressionsClean: Long,
     articleImpressionsDiscarded: Long,
     articleImpressionsViewedClean: Long,
     articleImpressionsViewedDiscarded: Long,
     clicksClean: Long,
     clicksDiscarded: Long,
     advertiserSpendClean: DollarValue,
     advertiserSpendDiscarded: DollarValue,
     conversionsClean: Long = 0l,
     conversionsDiscarded: Long = 0l) {

  lazy val siteKey: SiteKey = SiteKey(advertiserGuid)

  lazy val campaignKey: CampaignKey = CampaignKey(siteKey, campaignId)

  def articleImpressionsRaw: Long = articleImpressionsClean + articleImpressionsDiscarded
  def articleImpressionsViewedRaw: Long = articleImpressionsViewedClean + articleImpressionsViewedDiscarded
  def clicksRaw: Long = clicksClean + clicksDiscarded
  def advertiserSpendRaw: Long = advertiserSpendClean.pennies + advertiserSpendDiscarded.pennies
  def conversionsRaw: Long = conversionsClean + conversionsDiscarded
}

object AdvertiserCampaignArticleReportRow {
  def apply(reg: FieldValueRegistry): AdvertiserCampaignArticleReportRow = {
    AdvertiserCampaignArticleReportRow(
      RdsDateCustomFieldValueConverter.getValue(0, reg),
      reg.getValue[String](1),
      reg.getValue[Long](2),
      reg.getValue[Long](3),
      reg.getValue[Long](4),
      reg.getValue[Long](5),
      reg.getValue[Long](6),
      reg.getValue[Long](7),
      reg.getValue[Long](8),
      reg.getValue[Long](9),
      DollarValue(reg.getValue[Long](10)),
      DollarValue(reg.getValue[Long](11)),
      reg.getValue[Long](12),
      reg.getValue[Long](13)
    )
  }
}