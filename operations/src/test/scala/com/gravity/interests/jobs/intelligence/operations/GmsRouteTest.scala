package com.gravity.interests.jobs.intelligence.operations

import com.gravity.domain.aol.AolDynamicLeadChannels
import com.gravity.domain.gms.{GmsAlgoSettings, GmsRoute}
import com.gravity.interests.jobs.intelligence.{ArtGrvMap, CampaignKey, SiteKey}
import com.gravity.test.operationsTesting
import com.gravity.utilities.BaseScalaTest
import com.gravity.utilities.analytics.articles.{AolMisc, ArticleWhitelist}

import scalaz.syntax.std.option._

object GmsRouteTest {
  import AolDynamicLeadChannels._

  private val aolSiteKey = SiteKey("ea5ddf3148ee6dad41c76e96cb00e1a9")

  // This is now only used to populate allDlugCampaignKeys, which is only used to implement isDlugCampaign(ck).
  private val campaignMap = Map(
    NotSet.id -> CampaignKey(aolSiteKey, 7875655411765477568L)
    ,Entertainment.id -> CampaignKey(aolSiteKey, 8019880681320880563L)
    ,Finance.id -> CampaignKey(aolSiteKey, 6816930029769684917L)
    ,Lifestyle.id -> CampaignKey(aolSiteKey, -2788523670700937658L)
    ,Sports.id -> CampaignKey(aolSiteKey, 4563041315169303405L)
    ,News.id -> CampaignKey(aolSiteKey, 3868351738489925248L)
    ,Home.id -> CampaignKey(aolSiteKey, 8309200088894437379L)
    ,Style.id -> CampaignKey(aolSiteKey, 9213203217009655771L)
    ,Beauty.id -> CampaignKey(aolSiteKey, 6211566437176367995L)
    ,Food.id -> CampaignKey(aolSiteKey, 3579187152396865379L)
    ,Wellness.id -> CampaignKey(aolSiteKey, 2347896285248326811L)
    ,Travel.id -> CampaignKey(aolSiteKey, 6666938451665974988L)
    ,Save.id -> CampaignKey(aolSiteKey, 7829828752961793545L)
    ,Invest.id -> CampaignKey(aolSiteKey, -6428663033850833670L)
    ,Plan.id -> CampaignKey(aolSiteKey, 1354588217077367609L)
    ,Learn.id -> CampaignKey(aolSiteKey, 3810708098685482656L)
    ,Jobs.id -> CampaignKey(aolSiteKey, -2216305733614678406L)
    ,HomeSub.id -> CampaignKey(aolSiteKey, 4925542220467354621L)
    ,Tech.id -> CampaignKey(aolSiteKey, 5212671139956893513L)
  )

  val allDlugCampaignKeys = campaignMap.values.toSet

  val aolCks = allDlugCampaignKeys + AolMisc.dynamicLeadDLUGCampaignKey
  val allCks = aolCks

  val aolSks = Set(AolMisc.aolSiteKey)

  val nonAolSks = ArticleWhitelist.Partners.partnerToSiteGuid.values
    .map(SiteKey(_))
    .filterNot(aolSks contains _).toSet

  val allSks = nonAolSks ++ aolSks
}

class GmsRouteTest extends BaseScalaTest with operationsTesting {
  import GmsRouteTest._

  def shouldBeDlugRouteIfEnabled(gmsRoute: GmsRoute) = {
//    if (GmsAlgoSettings.aolComDlugUsesMultisiteGms) {
      shouldBeNonDlugRoute(gmsRoute)
//    } else {
//      gmsRoute.isDlugScopeKey should be (true)
//      gmsRoute.oneScopeKey._2 should be ("")
//
//      gmsRoute.siteKey     should be (AolMisc.aolSiteKey)
//      gmsRoute.oneScopeKey should be (AolMisc.dlugOneScopeKey)
//    }
  }

  def shouldBeNonDlugRoute(gmsRoute: GmsRoute) = {
    gmsRoute.isDlugScopeKey should be (false)
    gmsRoute.oneScopeKey._2 should be ("GMS")
  }

  test("A GmsRoute created from any of the well-known DLUG campaigns should produce the DLUG GmsRoute") {
    for (ck <- aolCks) {
      shouldBeDlugRouteIfEnabled(GmsRoute(ck))
    }
  }

  test("A GmsRoute created from the Aol.com SiteKey should produce the DLUG GmsRoute") {
    for (sk <- aolSks) {
      shouldBeDlugRouteIfEnabled(GmsRoute(sk))
    }
  }

  test("A GmsRoute created from any non-Aol.com SiteKey should produce a non-DLUG GmsRoute") {
    for (sk <- nonAolSks) {
      if (sk != AolMisc.aolSiteKey) {
        shouldBeNonDlugRoute(GmsRoute(sk))
      }
    }
  }

  test("We should be able to round-trip GmsRoutes using GmsRoute.fromOneScopeKey") {
    val gmsRoutes1 = allCks.map(ck => GmsRoute(ck)) ++ allSks.map(sk => GmsRoute(sk))
    val gmsRoutes2 = gmsRoutes1.map(gmsRoute => GmsRoute.fromOneScopeKey(gmsRoute.oneScopeKey)).filter(_.isDefined).map(_.get)

    // If we're no longer doing AOL.com DLUG the old way, then we don't want to see those old-style records.
    val gmsExpected = gmsRoutes1.filter { gmsRoute =>
      /* GmsAlgoSettings.aolComDlugInSingleSiteProduction || */ gmsRoute.isDlugScopeKey == false
    }

    gmsRoutes2 should be (gmsExpected)
  }

  test("If a grv:map has no keys, dlugIfAvailableElseHeadGmsRoute should return None") {
    GmsRoute.forTestsDlugIfAvailableElseHeadGmsRoute(ArtGrvMap.emptyAllScopesMap) should be (None)
  }

  test("If a grv:map has no GmsRoute keys, dlugIfAvailableElseHeadGmsRoute should return None") {
    val allGrvMap = Map(
      ArtGrvMap.toOneScopeKey(CampaignKey(SiteKey(12345L), 67890L), "") -> ArtGrvMap.emptyOneScopeMap
    )

    GmsRoute.forTestsDlugIfAvailableElseHeadGmsRoute(allGrvMap) should be (None)
  }

  test("When a grv:map has the DLUG OneScopeKey, dlugIfAvailableElseHeadGmsRoute should return the DLUG GmsRoute") {
    val allGrvMap = Map(
      AolMisc.dlugOneScopeKey -> ArtGrvMap.emptyOneScopeMap
    )

    // If we're no longer doing AOL.com DLUG the old way, then we don't want to see those old-style records.
    val expected = // if (GmsAlgoSettings.aolComDlugInSingleSiteProduction)
//      GmsRoute(AolMisc.dynamicLeadDLUGCampaignKey).some
//    else
      None

    GmsRoute.forTestsDlugIfAvailableElseHeadGmsRoute(allGrvMap) should be (expected)
  }

  test("When a grv:map has the DLUG OneScopeKey and other GMS OneScopeKeys, dlugIfAvailableElseHeadGmsRoute should np longer return the DLUG GmsRoute") {
    val gmsRoutes = allSks.map(sk => GmsRoute(sk))

    val allGrvMap = gmsRoutes.map(_.oneScopeKey -> ArtGrvMap.emptyOneScopeMap).toMap

//    if (GmsAlgoSettings.aolComDlugInSingleSiteProduction)
//      GmsRoute.forTestsDlugIfAvailableElseHeadGmsRoute(allGrvMap) should be (GmsRoute.fromOneScopeKey(AolMisc.dlugOneScopeKey))
//    else
      GmsRoute.forTestsDlugIfAvailableElseHeadGmsRoute(allGrvMap) should not be (GmsRoute.fromOneScopeKey(AolMisc.dlugOneScopeKey))
  }

  test("When a grv:map has a non-DLUG oneScopeKey, dlugIfAvailableElseHeadGmsRoute should return its GmsRoute") {
    val gmsRoutes = nonAolSks.map(sk => GmsRoute(sk))

    val allGrvMap = gmsRoutes.map(_.oneScopeKey -> ArtGrvMap.emptyOneScopeMap).toMap
    val expRoute  = GmsRoute.fromOneScopeKey(allGrvMap.keys.head).get

    GmsRoute.forTestsDlugIfAvailableElseHeadGmsRoute(allGrvMap) should be (expRoute.some)
  }
}

