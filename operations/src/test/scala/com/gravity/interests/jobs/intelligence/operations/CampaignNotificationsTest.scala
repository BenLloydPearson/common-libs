package com.gravity.interests.jobs.intelligence.operations

import com.gravity.interests.jobs.intelligence._
import com.gravity.test.operationsTesting
import com.gravity.utilities.BaseScalaTest
import org.junit.Assert._
import com.gravity.interests.jobs.intelligence.hbase.HBaseTestEnvironment
import com.gravity.test.operationsTesting
import org.junit.Assert._
import org.junit.Test

/**
 * Created with IntelliJ IDEA.
 * User: ahiniker
 * Date: 7/3/13
 */
class CampaignNotificationsTest extends BaseScalaTest with operationsTesting {

  val siteGuid = makeSiteGuid(getClass.getName)
  var campaignKey: CampaignKey = null

  override def onBefore {
    makeSite(siteGuid)
    campaignKey = makeCampaign("Test Campaign", siteGuid)
  }

  override def onAfter {
    Schema.Campaigns.delete(campaignKey).execute()
    Schema.Sites.delete(SiteKey(siteGuid)).execute()
  }

  test("SendBudgetNotification") {

    val notification = CampaignBudgetExceededNotification(campaignKey, BudgetResult(false, MaxSpendTypes.total))

    def getLastNotification = {
      Schema.Campaigns.query2.withKey(campaignKey).withFamilies(_.meta).singleOption().flatMap(_.campaignBudgetNotificationTimestamp)
    }

    // assert we get None on first call
    assertEquals(None, getLastNotification)

    CampaignNotifications.sendCampaignBudgetExceededNotification(notification)

    val lastNotification = getLastNotification

    // assert we now have a notification time set
    assert(lastNotification.isDefined)

    CampaignNotifications.sendCampaignBudgetExceededNotification(notification)

    // assert that we didn't change the notification time (ie: didn't send an email)
    assertEquals(lastNotification, getLastNotification)

  }


}
