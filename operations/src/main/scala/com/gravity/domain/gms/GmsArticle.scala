package com.gravity.domain.gms

import com.gravity.algorithms.model.FeatureSettings
import com.gravity.domain.GrvDuration
import com.gravity.domain.recommendations.ContentGroup
import com.gravity.interests.jobs.intelligence._
import com.gravity.interests.jobs.intelligence.operations.GmsService
import com.gravity.utilities.analytics.articles.AolMisc
import com.gravity.utilities.grvtime
import org.joda.time.DateTime
import play.api.libs.json.{JsString, Writes}

import scalaz.syntax.std.option._

/**
 * Created by robbie on 08/05/2015.
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

object GmsAlgoSettings {
  // An easy override to tell GmsAlgoSettings to ignore the FeatureSettings. Leave set to None for production.
  private val optHammer: Option[Int] = None // 3.some      // Index [0-3] (old-way, maint/old, maint/new, new-way)

  private val everythingScopedKey = Seq(EverythingKey.toScopedKey)

  // If this is true, we are in Aol.com DLUG-Transition mode, and should try to be quiescent.
  def aolComDlugInMaintenanceMode = optHammer match {
    case Some(hammer) => hammer >= 1 && hammer <= 2
    case None => FeatureSettings.getScopedSwitch(FeatureSettings.gmsDlugInMaintenanceMode, everythingScopedKey).value
  }

//  // If this is true, Aol.com DLUG should use the new multi-site GMS.
//  def aolComDlugUsesMultisiteGms = optHammer match {
//    case Some(hammer) => hammer >= 2
//    case None => FeatureSettings.getScopedSwitch(FeatureSettings.gmsDlugUsesMultisiteGms, everythingScopedKey).value
//  }
//
//  // If this is true, we're not in DLUG-Transition maintenance mode, and we're still running Aol.com using old-style single-site DLUG software.
//  def aolComDlugInSingleSiteProduction = !aolComDlugInMaintenanceMode && !aolComDlugUsesMultisiteGms
//
//  // If this is true, start returning the ContentGroups in to AolDynamicLeadChannels.contentGroupIds as GMS-managed
//  // as soon as aolComDlugUsesMultisiteGms goes true, no matter what the ConfigDb.ContentPools.isGmsManaged value is.
//  // This is really a just a kludge to help some of my tests.  The better way is to use the Testing ConfigDb Database.
//  def aggressiveAolComDlugUsesMultisiteGms = if (optHammer.isDefined) aolComDlugUsesMultisiteGms else false

  val maintenanceModeMailTo = Seq("robbie@gravity.com", "tchappell@gravity.com", "tony.huynh@gravity.com")
}

trait GmsArticle {
  def url: String
  def articleKey: ArticleKey
  def title: String
  def startDate: Option[DateTime]
  def endDate: Option[DateTime]
  def duration: Option[GrvDuration]
  def status: GmsArticleStatus.Type
  def author: Option[String]

  def isWithinDateRange: Boolean = {
    val now = grvtime.currentTime

    startDate match {
      case Some(notStartedYet) if now.isBefore(notStartedYet) => false

      case Some(actualStartDate) =>
        duration.map(_.fromTime(actualStartDate)).orElse(endDate) match {
          case Some(actualEndDate) => actualEndDate.isAfter(now)
          case None => true
        }

      case None => false
    }
  }
}

case class UniArticleId(siteKey: SiteKey, articleKey: ArticleKey, forDlug: Boolean) {
  lazy val toKeyString = s"GMS^${siteKey.siteId}^${articleKey.articleId}"

  lazy val forGms = !forDlug
}

object UniArticleId {
  def apply(articleKey: ArticleKey, gmsRoute: GmsRoute): UniArticleId =
    UniArticleId(gmsRoute.siteKey, articleKey, gmsRoute.isDlugScopeKey)

  def forDlug(articleKey: ArticleKey) =
    UniArticleId(AolMisc.aolSiteKey, articleKey, forDlug = true)

  def forGms(siteKey: SiteKey, articleKey: ArticleKey) =
    UniArticleId(siteKey, articleKey, forDlug = false)

  implicit val jsonWrites = Writes[UniArticleId](id => {
    JsString(id.toKeyString)
  })
}

case class GmsRoute(siteKey: SiteKey, oneScopeKey: ArtGrvMap.OneScopeKey) {
  lazy val isDlugScopeKey = oneScopeKey == AolMisc.dlugOneScopeKey
}

object GmsRoute {
  def fromContentGroups(contentGroups: Seq[ContentGroup]): Seq[GmsRoute] =
    contentGroups.filter(_.isGmsManaged).flatMap(_.sourceKey.tryToTypedKey[CampaignKey].toOption).map(GmsRoute(_))  // GmsRoute(ck) is safe here.

  // Returns tuple of GmsRoute Seqs, as (forDlug, !forDlug)
  def forTestsDlugAndGmsRoutes(allArtGrvMap: ArtGrvMap.AllScopesMap): (Seq[GmsRoute], Seq[GmsRoute]) =
    allRoutes(allArtGrvMap).partition(_.oneScopeKey == AolMisc.dlugOneScopeKey) // In forTestsDlugAndGmsRoutes

  def allRoutes(allArtGrvMap: ArtGrvMap.AllScopesMap): Seq[GmsRoute] =
    allArtGrvMap.keys.toList.flatMap(oneScopeKey => fromOneScopeKey(oneScopeKey))

  // This is currently only used to flatMap OneScopedKeys in allRoutes above.
  def fromOneScopeKey(oneScopeKey: ArtGrvMap.OneScopeKey): Option[GmsRoute] = {
    if (oneScopeKey == AolMisc.dlugOneScopeKey) {
//      if (GmsAlgoSettings.aolComDlugInSingleSiteProduction)
//        new GmsRoute(AolMisc.aolSiteKey, AolMisc.dlugOneScopeKey).some
//      else
        None
    } else {
      oneScopeKey match {
        case (Some(scopedKey), "GMS") =>
          scopedKey.tryToTypedKey[SiteKey].toOption.map(sk => GmsRoute(sk))

        case _ =>
          None
      }
    }
  }

  // For remaining context-free accesses the DLUG/GMS info, use DLUG if available, else fall back to any available GMS.
  def forTestsDlugIfAvailableElseHeadGmsRoute(allArtGrvMap: ArtGrvMap.AllScopesMap): Option[GmsRoute] = {
    // Find all available GmsRoutes in the grv:map OneScopeKeys
    val (dlugRoutes, gmsRoutes) = GmsRoute.forTestsDlugAndGmsRoutes(allArtGrvMap)

    // Use the DLUG GmsRoute if it exists, otherwise use the first GmsRoute
    dlugRoutes.headOption orElse gmsRoutes.headOption
  }

  // Where shall we look for DLUG/GMS article info? Might be None if this is not a GMS-managed Content Group!
  def optGmsRoute(ck: CampaignKey): Option[GmsRoute] = {
    if (GmsService.isGmsManaged(ck))
      GmsRoute(ck).some  // GmsRoute(ck) is safe here.
    else
      None
  }

  // TODO-FIXGMS-3-IMPROVE: This should really be a function that returns an Option[GmsRoute],
  // so that we can return None for content groups that are not isGmsManaged.
  // We'll have to change all the callers, though, so this won't be an immediate change.
  //
  // Phase out most uses of this in favor of GmsRoute.optGmsRoute(ck).
  def apply(ck: CampaignKey): GmsRoute = {
//    if (!GmsAlgoSettings.aolComDlugUsesMultisiteGms && AolDynamicLeadChannels.isDlugCampaign(ck))
//      new GmsRoute(AolMisc.aolSiteKey, AolMisc.dlugOneScopeKey)
//    else
      new GmsRoute(ck.siteKey, gmsOnlySiteKeyToGrvMapKey(ck.siteKey))
  }

  // TODO-FIXGMS-3-IMPROVE: Note that this call does not have the advantage of knowing the campaign.
  // It should only be called in cases where we're dealing with aggregate operations, e.g when
  // building the GmsArticleIndex or similar.  Review these calls.
  def apply(sk: SiteKey): GmsRoute = sk match {
//    case AolMisc.aolSiteKey if !GmsAlgoSettings.aolComDlugUsesMultisiteGms =>
//      new GmsRoute(AolMisc.aolSiteKey, AolMisc.dlugOneScopeKey)

    case _ =>
      new GmsRoute(sk, gmsOnlySiteKeyToGrvMapKey(sk))
  }

  def gmsOnlySiteKeyToGrvMapKey(siteKey: SiteKey) =
    ArtGrvMap.toOneScopeKey(siteKey.toScopedKey.some, "GMS")
}

