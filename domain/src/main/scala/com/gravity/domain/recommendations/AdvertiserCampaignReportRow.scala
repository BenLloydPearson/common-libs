package com.gravity.domain.recommendations

import com.gravity.interests.jobs.intelligence.schemas.DollarValue
import com.gravity.utilities.grvtime._
import com.gravity.utilities.time.DateHour
import com.gravity.interests.jobs.intelligence.{CampaignKey, SiteKey}
import com.gravity.utilities.eventlogging.FieldValueRegistry

/**
 * Created by IntelliJ IDEA.
 * Author: Robbie Coleman
 * Date: 2/10/14
 * Time: 9:38 AM
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
case class AdvertiserCampaignReportRow(
    logHour: DateHour,
    advertiserGuid: String,
    campaignId: Long,
    articleImpressionsClean: Long,
    articleImpressionsDiscarded: Long,
    articleImpressionsViewedClean: Long,
    articleImpressionsViewedDiscarded: Long,
    clicksClean: Long,
    clicksDiscarded: Long,
    advertiserSpendClean: DollarValue,
    advertiserSpendDiscarded: DollarValue,
    conversionsClean: Long,
    conversionsDiscarded: Long) {

  lazy val siteKey: SiteKey = SiteKey(advertiserGuid)

  lazy val campaignKey: CampaignKey = CampaignKey(siteKey, campaignId)

  def articleImpressionsRaw: Long = articleImpressionsClean + articleImpressionsDiscarded
  def articleImpressionsViewedRaw: Long = articleImpressionsViewedClean + articleImpressionsViewedDiscarded
  def clicksRaw: Long = clicksClean + clicksDiscarded
  def advertiserSpendRaw: Long = advertiserSpendClean.pennies + advertiserSpendDiscarded.pennies
  def conversionsRaw: Long = conversionsClean + conversionsDiscarded
}

object AdvertiserCampaignReportRow {
  def apply(reg: FieldValueRegistry): AdvertiserCampaignReportRow = {
    AdvertiserCampaignReportRow(
      RdsDateCustomFieldValueConverter.getValue(0, reg).withHourOfDay(reg.getValue[Int](1)).toDateHour,
      reg.getValue[String](2),
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
