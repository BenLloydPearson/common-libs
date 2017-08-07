package com.gravity.interests.jobs.intelligence.operations.sites

import com.gravity.interests.jobs.intelligence.operations.sponsored.SponsoredStoryService
import com.gravity.interests.jobs.intelligence.operations.{ArticleService, CampaignDateRequirements, CampaignService, SiteService}
import com.gravity.interests.jobs.intelligence.schemas.DollarValue
import com.gravity.interests.jobs.intelligence.{CampaignArticleSettings, _}
import com.gravity.test.operationsTesting
import com.gravity.utilities.components.FailureResult
import com.gravity.utilities.thumby.ThumbyMode
import com.gravity.utilities.time.GrvDateMidnight
import com.gravity.utilities.{BaseScalaTest, ScalaMagic}
import com.gravity.utilities.grvz._

import scala.collection._
import scalaz.Scalaz._
import scalaz.{Failure, Success, ValidationNel}

/*             )\._.,--....,'``.
 .b--.        /;   _.. \   _\  (`._ ,.
`=,-,-'~~~   `----(,_..'--(,_..'`-.;.'  */


class SponsoredStoriesTest extends BaseScalaTest with operationsTesting {

  /*
  Sponsor is marked with the pools they participate in.
  Sponsee is marked with the pools they participate in.

  Each one is a Site --> so, sponsorPools and sponseePools
  A Pool has a isSponsoredArticlesPool boolean setting
   */
  test("Sponsor Creation") {

    val sponsor = makeSiteGuid("SponsorSite")
    val sponsee = makeSiteGuid("SponseeSite")
    val sponsorUrl = "http://sponsor.com"
    val sponseeUrl = "http://sponsee.com"
    val pool1Guid = makeSiteGuid("SponsoredPool1")
    val pool2Guid = makeSiteGuid("SponsoredPool2")
    val article1Url = sponsorUrl + "/article1.html"
    val article2Url = sponsorUrl + "/article2.html"
    val article3Url = sponsorUrl + "/article3.html"

    val today = new GrvDateMidnight()
    val nextMonth = today.plusMonths(1)
    val sponsorOneCampaignOneDateParams = CampaignDateRequirements.createNonOngoing(today, nextMonth)

    val maxBid = DollarValue(100)
    val maxBidFloorOpt = Some(DollarValue(99))
    val maxSpend = DollarValue(100 * 100)
    val maxSpendType = MaxSpendTypes.daily
    val useCachedImages = Option(false)
    val thumbyMode = Option(ThumbyMode.off)
    val isGravityOptimized = false

    val activeCampaignArticleSettings: Option[CampaignArticleSettings] = CampaignArticleSettings(CampaignArticleStatus.active, isBlacklisted = false).some

    def cleanup() {
      deleteSite(pool1Guid)

      deleteSite(sponsor)
      deleteSite(sponsee)
      deleteSite(pool2Guid)

    }

    cleanup()

    (for{
      poolSite <- SiteService.makeSite(pool1Guid, "SponsoredPool1", "http://nourl.com") //Make the Pool (which is a Site)
      poolSite2 <- SiteService.makeSite(pool2Guid, "SponsoredPool2", "http://nourl2.com") //Make a second pool
      _ <- SponsoredStoryService.setIsSponsoredStoryPool(poolSite.siteKey, isSponsoredStoryPool = true) //Set the Pool to be a Sponsored Stories Pool
      _ <- SponsoredStoryService.setIsSponsoredStoryPool(poolSite2.siteKey, isSponsoredStoryPool = true) //Set the Pool to be a Sponsored Stories Pool for the second pool
      sponsorSite <- SiteService.makeSite(sponsor, "SponsorSite", sponsorUrl) //Make the Sponsor Site
      sponsorPool <- SiteService.addSponsorToPool(sponsorSite.siteKey, poolSite.siteKey, -1l) //Add the Sponsor to the Sponsors list in the Pool
      sponsorPool2 <- SiteService.addSponsorToPool(sponsorSite.siteKey, poolSite2.siteKey, -1l) //Add the Sponsor to the Sponsors list in the Pool
      sponseeSite <- SiteService.makeSite(sponsee, "SponseeSite", sponseeUrl) //Make the Sponsee Site
      sponseeSiteAgain <- SiteService.addSponseeToPool(sponseeSite.siteKey, poolSite.siteKey, -1L)() //Add the Sponsee to the Pool
      _ <- SiteService.addSponseeToPool(sponseeSiteAgain.siteKey, poolSite2.siteKey, -1L)() //Add the Sponsee to the second pool

      // Create a single campaign to house sponsored articles
      campaign <- CampaignService.createCampaign(sponsor, "Sponsor Creation",
              CampaignType.sponsored.some, sponsorOneCampaignOneDateParams, maxBid, maxBidFloorOpt, isGravityOptimized,
              Some(BudgetSettings(maxSpend, maxSpendType)), useCachedImages, thumbyMode)
      _ <- CampaignService.updateStatus(campaign.campaignKey, CampaignStatus.active) // Activate the campaign
      article1 <- ArticleService.makeArticle(article1Url, siteGuid=sponsor) //Make the first article
      article2 <- ArticleService.makeArticle(article2Url, siteGuid=sponsor) //Make the second article
      article3 <- ArticleService.makeArticle(article3Url, siteGuid=sponsor) //Make the third article
      _ <- CampaignService.addArticleKey(campaign.campaignKey, article1.articleKey, settings = activeCampaignArticleSettings) //Add the first article to the campaign
      _ <- CampaignService.addArticleKey(campaign.campaignKey, article2.articleKey, settings = activeCampaignArticleSettings) //Add the second article to the campaign
      _ <- CampaignService.addArticleKey(campaign.campaignKey, article3.articleKey, settings = activeCampaignArticleSettings) //Add the third article to the campaign
      sponsoredArticlesForSponsor <- SponsoredStoryService.fetchSponsoredArticleKeysForSponseeFromCampaigns(sponseeSite.siteKey) //Grab the sponsored articles for the Sponsee
    } yield {
      sponsoredArticlesForSponsor
    }).fold(failList=>{
      println("Failed!")
      for(fail <- failList.list) {
        println("Fail: " + fail)
      }
      fail("Failed because of the following problems: " + failList.list.mkString(","))
    },success=>{
      println("Success!  Below are the sponsored articles for the Sponsee")
      success.foreach{println}
      success.size should be (3)
    })

    println("Sponsor1: " + SiteKey(pool1Guid))
    println("Sponsor2: " + SiteKey(pool2Guid))

    for {
      sponseeRow <- SiteService.fetchSiteForManagement(SiteKey(sponsee))
      result <- SiteService.removeSponseeFromPool(sponseeRow.siteKey, SiteKey(pool1Guid), -1L)
      sponseeAgain <- SiteService.fetchSiteForManagement(SiteKey(sponsee))
    } {
      println("PRE REMOVAL")
      sponseeRow.prettyPrint()
      println("POST REMOVAL")
      sponseeAgain.prettyPrint()
    }

    cleanup()
//    makeSite(pool1Guid, "SponsoredPool1", "http://nourl.com")
//    makeSite(pool2Guid, "SponsoredPool2", "http://nourl.com")


  }

  test("Fetch Sponsored Articles For Sponsee From Campaigns") {
    val campaignKeysToDelete = mutable.Set[CampaignKey]()

    def registerCampaignDelete(ck: CampaignKey): ValidationNel[FailureResult, CampaignKey] = {
      campaignKeysToDelete += ck
      ck.successNel
    }

    val nameOrKeyPrefix = "SponsoredStoriesTest.testFetchSponsoredArticlesForSponseeFromCampaigns-"

    val sponsorOneSiteGuid = makeSiteGuid(nameOrKeyPrefix + "sponsor1")
    val sponsorTwoSiteGuid = makeSiteGuid(nameOrKeyPrefix + "sponsor2")
    val sponseeSiteGuid = makeSiteGuid(nameOrKeyPrefix + "sponsee")
    val poolGuid = makeSiteGuid(nameOrKeyPrefix + "sponsoredPool")
    val poolSiteKey = SiteKey(poolGuid)

    val sponsorOneUrl = "http://sponsorone.com"
    val sponsorTwoUrl = "http://sponsortwo.com"
    val sponseeUrl = "http://sponsee.com"

    val sponsorOneArticle1url = sponsorOneUrl + "/article1.html"
    val sponsorOneArticle2url = sponsorOneUrl + "/article2.html"
    val sponsorOneArticle3url = sponsorOneUrl + "/article3.html"
    val sponsorOneArticle4url = sponsorOneUrl + "/article4.html"
    val sponsorOneArticle5url = sponsorOneUrl + "/article5.html"
    val sponsorOneArticle6url = sponsorOneUrl + "/article6.html"

    val sponsorTwoArticle1url = sponsorTwoUrl + "/article1.html"
    val sponsorTwoArticle2url = sponsorTwoUrl + "/article2.html"
    val sponsorTwoArticle3url = sponsorTwoUrl + "/article3.html"
    val sponsorTwoArticle4url = sponsorTwoUrl + "/article4.html"
    val sponsorTwoArticle5url = sponsorTwoUrl + "/article5.html"
    val sponsorTwoArticle6url = sponsorTwoUrl + "/article6.html"

    val sponsorOneCampaignOneName = nameOrKeyPrefix + "sponsor1-campaign1"
    val sponsorOneCampaignTwoName = nameOrKeyPrefix + "sponsor1-campaign2"

    val sponsorTwoCampaignOneName = nameOrKeyPrefix + "sponsor2-campaign1"
    val sponsorTwoCampaignTwoName = nameOrKeyPrefix + "sponsor2-campaign2"

    val today = new GrvDateMidnight()
    val nextMonth = today.plusMonths(1)
    val sponsorOneCampaignOneDateParams = CampaignDateRequirements.createNonOngoing(today, nextMonth)
    val sponsorOneCampaignTwoDateParams = CampaignDateRequirements.createOngoing(today)
    val sponsorTwoCampaignOneDateParams = CampaignDateRequirements.createNonOngoing(today, nextMonth)
    val sponsorTwoCampaignTwoDateParams = CampaignDateRequirements.createOngoing(today)

    val maxBid = DollarValue(100)
    val maxBidFloorOpt = Some(DollarValue(99))
    val maxSpend = DollarValue(100 * 100)
    val maxSpendType = MaxSpendTypes.daily
    val useCachedImages = Option(false)
    val thumbyMode = Option(ThumbyMode.off)
    val isGravityOptimized = false

    val activeCampaignArticleSettings: Option[CampaignArticleSettings] = CampaignArticleSettings(CampaignArticleStatus.active, isBlacklisted = false).some
    val inActiveCampaignArticleSettings: Option[CampaignArticleSettings] = CampaignArticleSettings(CampaignArticleStatus.inactive, isBlacklisted = false).some

    (for {
      poolSite <- SiteService.makeSite(poolGuid, "SponsoredPool", "http://nourl.com")
      _ <- SponsoredStoryService.setIsSponsoredStoryPool(poolSite.rowid, isSponsoredStoryPool = true)
      sponsorOneSite <- SiteService.makeSite(sponsorOneSiteGuid, "Sponsor1", sponsorOneUrl)
      _ <- SiteService.addSponsorToPool(sponsorOneSite.siteKey, poolSiteKey, -1l)
      sponsorTwoSite <- SiteService.makeSite(sponsorTwoSiteGuid, "Sponsor2", sponsorTwoUrl)
      sponsorPool <- SiteService.addSponsorToPool(sponsorTwoSite.siteKey, poolSiteKey, -1l)
      sponseeSite <- SiteService.makeSite(sponseeSiteGuid, "Sponsee", sponseeUrl)
      _ <- SiteService.addSponseeToPool(sponseeSite.siteKey, poolSiteKey, -1L)()
      sponsorOneArticle1 <- ArticleService.makeArticle(sponsorOneArticle1url, sponsorOneSiteGuid)
      sponsorOneArticle2 <- ArticleService.makeArticle(sponsorOneArticle2url, sponsorOneSiteGuid)
      sponsorOneArticle3 <- ArticleService.makeArticle(sponsorOneArticle3url, sponsorOneSiteGuid)
      sponsorOneArticle4 <- ArticleService.makeArticle(sponsorOneArticle4url, sponsorOneSiteGuid)
      sponsorOneArticle5 <- ArticleService.makeArticle(sponsorOneArticle5url, sponsorOneSiteGuid)
      sponsorOneArticle6 <- ArticleService.makeArticle(sponsorOneArticle6url, sponsorOneSiteGuid)
      sponsorTwoArticle1 <- ArticleService.makeArticle(sponsorTwoArticle1url, sponsorTwoSiteGuid)
      sponsorTwoArticle2 <- ArticleService.makeArticle(sponsorTwoArticle2url, sponsorTwoSiteGuid)
      sponsorTwoArticle3 <- ArticleService.makeArticle(sponsorTwoArticle3url, sponsorTwoSiteGuid)
      sponsorTwoArticle4 <- ArticleService.makeArticle(sponsorTwoArticle4url, sponsorTwoSiteGuid)
      sponsorTwoArticle5 <- ArticleService.makeArticle(sponsorTwoArticle5url, sponsorTwoSiteGuid)
      sponsorTwoArticle6 <- ArticleService.makeArticle(sponsorTwoArticle6url, sponsorTwoSiteGuid)
      sponsorOneCampaignOne <- CampaignService.createCampaign(sponsorOneSiteGuid, sponsorOneCampaignOneName,
        CampaignType.sponsored.some, sponsorOneCampaignOneDateParams, maxBid, maxBidFloorOpt, isGravityOptimized,
        Some(BudgetSettings(maxSpend, maxSpendType)), useCachedImages, thumbyMode)
      sponsorOneCampaignOneKey <- registerCampaignDelete(sponsorOneCampaignOne.rowid)
      _ <- CampaignService.updateStatus(sponsorOneCampaignOne.rowid, CampaignStatus.active)
      sponsorOneCampaignTwo <- CampaignService.createCampaign(sponsorOneSiteGuid, sponsorOneCampaignTwoName,
        CampaignType.sponsored.some, sponsorOneCampaignTwoDateParams, maxBid, maxBidFloorOpt, isGravityOptimized,
        Some(BudgetSettings(maxSpend, maxSpendType)), useCachedImages, thumbyMode)
      sponsorOneCampaignTwoKey <- registerCampaignDelete(sponsorOneCampaignTwo.rowid)
      sponsorTwoCampaignOne <- CampaignService.createCampaign(sponsorTwoSiteGuid, sponsorTwoCampaignOneName,
        CampaignType.sponsored.some, sponsorTwoCampaignOneDateParams, maxBid, maxBidFloorOpt, isGravityOptimized,
        Some(BudgetSettings(maxSpend, maxSpendType)), useCachedImages, thumbyMode)
      sponsorTwoCampaignOneKey <- registerCampaignDelete(sponsorTwoCampaignOne.rowid)
      _ <- CampaignService.updateStatus(sponsorTwoCampaignOne.rowid, CampaignStatus.active)
      sponsorTwoCampaignTwo <- CampaignService.createCampaign(sponsorTwoSiteGuid, sponsorTwoCampaignTwoName,
        CampaignType.sponsored.some, sponsorTwoCampaignTwoDateParams, maxBid, maxBidFloorOpt, isGravityOptimized,
        Some(BudgetSettings(maxSpend, maxSpendType)), useCachedImages, thumbyMode)
      sponsorTwoCampaignTwoKey <- registerCampaignDelete(sponsorTwoCampaignTwo.rowid)
      _ <- CampaignService.addArticleKey(sponsorOneCampaignOneKey, sponsorOneArticle1.rowid, settings = activeCampaignArticleSettings)
      _ <- CampaignService.addArticleKey(sponsorOneCampaignOneKey, sponsorOneArticle2.rowid, settings = activeCampaignArticleSettings)
      _ <- CampaignService.addArticleKey(sponsorOneCampaignOneKey, sponsorOneArticle3.rowid, settings = inActiveCampaignArticleSettings)
      _ <- CampaignService.addArticleKey(sponsorOneCampaignTwoKey, sponsorOneArticle4.rowid, settings = activeCampaignArticleSettings)
      _ <- CampaignService.addArticleKey(sponsorOneCampaignTwoKey, sponsorOneArticle5.rowid, settings = activeCampaignArticleSettings)
      _ <- CampaignService.addArticleKey(sponsorOneCampaignTwoKey, sponsorOneArticle6.rowid, settings = inActiveCampaignArticleSettings)
      _ <- CampaignService.addArticleKey(sponsorTwoCampaignOneKey, sponsorTwoArticle1.rowid, settings = activeCampaignArticleSettings)
      _ <- CampaignService.addArticleKey(sponsorTwoCampaignOneKey, sponsorTwoArticle2.rowid, settings = activeCampaignArticleSettings)
      _ <- CampaignService.addArticleKey(sponsorTwoCampaignOneKey, sponsorTwoArticle3.rowid, settings = inActiveCampaignArticleSettings)
      _ <- CampaignService.addArticleKey(sponsorTwoCampaignTwoKey, sponsorTwoArticle4.rowid, settings = activeCampaignArticleSettings)
      _ <- CampaignService.addArticleKey(sponsorTwoCampaignTwoKey, sponsorTwoArticle5.rowid, settings = activeCampaignArticleSettings)
      _ <- CampaignService.addArticleKey(sponsorTwoCampaignTwoKey, sponsorTwoArticle6.rowid, settings = inActiveCampaignArticleSettings)
      sponsoredArticles <- SponsoredStoryService.fetchSponsoredArticleKeysForSponseeFromCampaigns(sponseeSite.rowid)
    } yield sponsoredArticles) match {
      case Success(sponsoredArticles) =>
        println("Received " + sponsoredArticles.size + " sponsored articles")
        sponsoredArticles.foreach(sa => println("Article: {key: '" + sa._1 + "'; campaignKey: '" + sa._2.campaignKey + "'}"))

        sponsoredArticles should have size 4
      case Failure(failList) =>
        println("Failed!")
        for (fail <- failList.list) {
          println("Fail: " + fail)
        }
        fail("Failed because of the following problems: " + failList.list.mkString(","))
    }

    deleteSite(sponsorOneSiteGuid)
    deleteSite(sponsorTwoSiteGuid)
    deleteSite(sponseeSiteGuid)
    deleteSite(poolGuid)

    for (ck <- campaignKeysToDelete) {
      try {
        Schema.Campaigns.delete(ck).execute()
      } catch {
        case ex: Exception =>
          println("Unable to delete campaign: " + ck + " during post test cleanup due to:")
          ScalaMagic.printException("", ex)
      }
    }
  }

}
