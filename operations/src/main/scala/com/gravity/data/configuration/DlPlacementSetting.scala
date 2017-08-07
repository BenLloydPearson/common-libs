package com.gravity.data.configuration

import com.gravity.algorithms.model.FeatureSettings
import com.gravity.data.configuration.DlPlacementSetting.SettingName
import com.gravity.domain.aol.AolDynamicLeadChannels
import com.gravity.interests.jobs.intelligence.operations.GmsService
import com.gravity.interests.jobs.intelligence.{ArticleKey, ContentGroupKey, EverythingKey}
import com.gravity.interests.jobs.intelligence.hbase.ScopedKey
import com.gravity.logging.Logstashable
import com.gravity.utilities.analytics.articles.AolMisc
import com.gravity.utilities.Settings2
import com.gravity.utilities.grvstrings._
import com.gravity.valueclasses.ValueClassesForDomain.SitePlacementId

import scalaz.{Failure, Success}

/*
              ___...---''
  ___...---'\'___
''       _.-''  _`'.______\\.
        /_.)  )..-  __..--'\\
       (    __..--''
        '-''\@


 Ⓐ Ⓐ Ⓐ Ⓐ Ⓐ Ⓐ Ⓐ Ⓐ Ⓐ Ⓐ Ⓐ
*/
case class DlPlacementSetting(settingName: SettingName, settingValue: String)

object DlPlacementSetting extends ((String, String) => DlPlacementSetting) {
  type SettingName = String
  
  private def q = ConfigurationQueryService.queryRunner

  import com.gravity.logging.Logstashable._

  case class GmsAdUnitAndArticleCollision(articleKey: ArticleKey, slot: Int, contentGroupId: Long) extends Logstashable {
    override def getKVs: Seq[(String, String)] = {
      val message = s"Found article key $articleKey collision with ad unit in slot $slot. Ad unit takes precedence."
      Seq(Logstashable.ArticleKey -> articleKey.toString, "Slot" -> slot.toString, "ContentGroup" -> contentGroupId.toString, Message -> message)
    }
  }

//  case class AdUnitAndArticleCollision(articleKey: ArticleKey, slot: Int, channel: AolDynamicLeadChannels.Type) extends Logstashable {
//    import Logstashable._
//    override def getKVs: Seq[(String, String)] = {
//      val message = s"Found article key $articleKey collision with ad unit in slot $slot. Ad unit takes precedence."
//      Seq(Logstashable.ArticleKey -> articleKey.toString, "Slot" -> slot.toString, "Channel" -> channel.name, Message -> message)
//    }
//  }

  def scopes(optContentGroupId: Option[Long], optSitePlacementId: Option[SitePlacementId]): Seq[ScopedKey] = {
    Seq(
      optSitePlacementId.map(_.sitePlacementIdKey.toScopedKey).toList,
      optContentGroupId.map(ContentGroupKey(_).toScopedKey).toList,
      Seq(EverythingKey.toScopedKey)
    ).flatten
  }

//  def adUnits(channel: AolDynamicLeadChannels.Type = AolDynamicLeadChannels.Home,
//              sitePlacementId: SitePlacementId = AolMisc.aolDlMainPlacementId,    // in adUnits, whose use is avoided by aolComDlugUsesMultisiteGms algoSetting.
//              skipCache: Boolean = !useCache): AdUnits = {
//    val prefix = if (sitePlacementId == AolMisc.aolDlMainBonanzaPlacementId)      // in adUnits, whose use is avoided by aolComDlugUsesMultisiteGms algoSetting.
//      "adUnitSlot_bonanza"
//    else
//      "adUnitSlot_classic"
//
//    val firstLabel = s"${prefix}_${channel.name}"
//    val secondLabel = s"${prefix}2_${channel.name}"
//
//    val first = getSetting(firstLabel, skipCache).flatMap(_.settingValue.tryToInt).map(p => AdUnit("adUnitSlot", p))
//    val second = getSetting(secondLabel, skipCache).flatMap(_.settingValue.tryToInt).map(p => AdUnit("adUnitSlot2", p))
//
//    AdUnits(first, second)
//  }

  def gmsAdUnits(optContentGroupId: Option[Long], optSitePlacementId: Option[SitePlacementId], skipCache: Boolean = !useCache): AdUnits = {
    val v1 = FeatureSettings.getScopedVariable(FeatureSettings.gmsAdUnitSlot , scopes(optContentGroupId, optSitePlacementId), skipCache)
    val a1 = Option(v1.value.asInstanceOf[Int]).filter(_ >= 0).map(p => AdUnit("adUnitSlot", p))

    val v2 = FeatureSettings.getScopedVariable(FeatureSettings.gmsAdUnitSlot2, scopes(optContentGroupId, optSitePlacementId), skipCache)
    val a2 = Option(v2.value.asInstanceOf[Int]).filter(_ >= 0).map(p => AdUnit("adUnitSlot2", p))

    AdUnits(a1, a2)
  }

  private val bonanzaAdUnitSlot: Option[Int] = Some(6)
  private val defaultAdUnitSlot: Option[Int] = Some(4)

//  def adUnitSlot(channel: AolDynamicLeadChannels.Type = AolDynamicLeadChannels.Home, sitePlacementId: SitePlacementId = AolMisc.aolDlMainPlacementId): Option[Int] = {  // In adUnitSlot, whose use is avoided by aolComDlugUsesMultisiteGms algoSetting.
//    if (sitePlacementId == AolMisc.aolDlMainBonanzaPlacementId)   // In adUnitSlot, whose use is avoided by aolComDlugUsesMultisiteGms algoSetting.
//      return bonanzaAdUnitSlot
//
//    getSetting(SETTING_NAME_AD_UNIT_SLOT_PREFIX + channel.name, skipCache = !useCache).flatMap(_.settingValue.tryToInt).orElse(defaultAdUnitSlot)
//  }

  def gmsAdUnitSlot(optContentGroupId: Option[Long], optSitePlacementId: Option[SitePlacementId], skipCache: Boolean = !useCache): Option[Int] = {
    val adUnitSlot = FeatureSettings.getScopedVariable(
      FeatureSettings.gmsAdUnitSlot , scopes(optContentGroupId, optSitePlacementId), skipCache).value.asInstanceOf[Int]

    Option(adUnitSlot)
  }

  private def getSetting(settingName: String, skipCache: Boolean): Option[DlPlacementSetting] = {
    if (skipCache)
      q.getAolDlSettingWithoutCaching(settingName)
    else
      q.allAolDlSettings.get(settingName)
  }

//  /** @return Seconds timestamp when a DL admin last requested "force users to slide 1." */
//  def forceUsersToSlide1LastTouchedTime(channel: AolDynamicLeadChannels.Type): Long = {
//    val settingName = SETTING_NAME_FORCE_USERS_TO_SLIDE1_PREFIX + channel.name
//    val setting = getSetting(settingName, skipCache = !useCache)
//
//    setting.flatMap(_.settingValue.tryToLong).getOrElse(defaultForceUsersToSlide1)
//  }
//
//  def forceUsersToSlide1LastTouchedTime(sitePlacementId: SitePlacementId): Long = {
//    AolDynamicLeadChannels.sitePlacementIdToChannel.get(sitePlacementId)            // In forceUsersToSlide1LastTouchedTime, which SHOULD be obsolete.
//      .map(forceUsersToSlide1LastTouchedTime).getOrElse(defaultForceUsersToSlide1)
//  }

  def gmsForceUsersToSlide1LastTouchedTime(optCgId: Option[Long], optSpId: Option[SitePlacementId], skipCache: Boolean = !useCache): Long = {
    // This variable is always accessed at the content-group scope.
    val useScopes = (optCgId, optSpId) match {
      case (Some(cgId), _) =>
        scopes(optCgId, None)

      case (_, Some(spId)) =>
        GmsService.getManagedContentGroupsForSitePlacement(spId, skipCache) match {
          case Success(cgrList) => scopes(cgrList.headOption.map(_.id), None)
          case Failure(boo) => return -1L
        }

      case (None, None) =>
        scopes(None, None)
    }

    FeatureSettings.getScopedVariable(FeatureSettings.gmsForceUsersToSlide1, useScopes, skipCache).value.asInstanceOf[Long]
  }

  def gmsSetForceUsersToSlide1LastTouchedTime(cgId: Long, secs: Int) = {
    FeatureSettings.setScopedVariable(FeatureSettings.gmsForceUsersToSlide1, ContentGroupKey(cgId).toScopedKey, secs.toDouble)
  }

  def gmsMinArticles(optCgId: Option[Long], optSpId: Option[SitePlacementId], skipCache: Boolean = !useCache): Int = {
    FeatureSettings.getScopedVariable(
      FeatureSettings.gmsMinArticles, scopes(optCgId, optSpId), skipCache).value.asInstanceOf[Int]
  }

  private val SETTING_NAME_AD_UNIT_SLOT_PREFIX = "adUnitSlot_"
  private val SETTING_NAME_FORCE_USERS_TO_SLIDE1_PREFIX = "forceUsersToSlide1_"
  private val SETTING_NAME_MIN_ARTICLES_PREFIX = "minArticles_"

  private val defaultForceUsersToSlide1 = 0L
  private val defaultMinArticles = 4

  private val useCache = Settings2.getBooleanOrDefault("recommendation.widget.configuration.dlPlacementSettings.cache",
    default = true)
}

case class AdUnit(name: String, position: Int) {
  def asTuple: (String, Int) = name -> position
}

case class AdUnits(firstPosition: Option[AdUnit], secondPosition: Option[AdUnit]) {
  def isEmpty: Boolean = firstPosition.isEmpty && secondPosition.isEmpty

  def fields: List[(String, Int)] = {
    if (isEmpty) return List.empty[(String, Int)]

    List(firstPosition, secondPosition).flatten.map(_.asTuple)
  }
}

object AdUnits {
  val empty = AdUnits(None, None)
}
