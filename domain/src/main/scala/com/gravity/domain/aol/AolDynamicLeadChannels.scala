package com.gravity.domain.aol

import com.gravity.interests.jobs.intelligence.{CampaignKey, SiteKey}
import com.gravity.utilities.analytics.articles.AolMisc
import com.gravity.utilities.grvenum.GrvEnum
import com.gravity.utilities.grvcoll._
import com.gravity.utilities.grvjson._
import com.gravity.utilities.swagger.adapter.DefaultValueWriter
import com.gravity.valueclasses.ValueClassesForDomain.SitePlacementId
import play.api.libs.json._

import scala.collection._
import scalaz.syntax.std.option._

/**
 * Created by IntelliJ IDEA.
 * Author: Robbie Coleman
 * Date: 11/4/14
 * Time: 2:42 PM
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
object AolDynamicLeadChannels extends GrvEnum[Byte] {
  case class Type(i: Byte, n: String) extends ValueTypeBase(i, n)

  def mkValue(id: Byte, name: String): Type = Type(id, name)

  val NotSet : AolDynamicLeadChannels.Type = Value(0, "NOT_SET")

  val Entertainment : AolDynamicLeadChannels.Type  = Value(1, "entertainment")

  val Finance : AolDynamicLeadChannels.Type  = Value(2, "finance")

  val Lifestyle : AolDynamicLeadChannels.Type  = Value(3, "lifestyle")

  val Sports : AolDynamicLeadChannels.Type  = Value(4, "sports")

  val News : AolDynamicLeadChannels.Type  = Value(5, "news")

  // values 6 - 18 should no longer be used, but must remain to match existing data
  val Home : AolDynamicLeadChannels.Type  = Value(6, "home")

  val Style : AolDynamicLeadChannels.Type  = Value(7, "style")

  val Beauty : AolDynamicLeadChannels.Type  = Value(8, "beauty")

  val Food : AolDynamicLeadChannels.Type  = Value(9, "food")

  val Wellness : AolDynamicLeadChannels.Type  = Value(10, "wellness")

  val Travel : AolDynamicLeadChannels.Type  = Value(11, "travel")

  val HomeSub : AolDynamicLeadChannels.Type  = Value(12, "home-sub") // Not to be confused with the Home[page] channel (id: 6) above

  val Save : AolDynamicLeadChannels.Type  = Value(13, "save")

  val Invest : AolDynamicLeadChannels.Type  = Value(14, "invest")

  val Plan : AolDynamicLeadChannels.Type  = Value(15, "plan")

  val Learn : AolDynamicLeadChannels.Type  = Value(16, "learn")

  val Jobs : AolDynamicLeadChannels.Type  = Value(17, "jobs")

  val Tech : AolDynamicLeadChannels.Type  = Value(18, "tech")

  def defaultValue: Type = NotSet

  val aolComHomeContentGroupId = 1996L

//  val contentGroupIdMap: Map[Byte, Option[Long]] = Map(
//    NotSet.id -> None,
//    Beauty.id -> 2006L.some,
//    Entertainment.id -> 1486L.some,
//    Finance.id -> 1488L.some,
//    Food.id -> 2007L.some,
//    Home.id -> 1996L.some,
//    HomeSub.id -> 2016L.some,
//    Invest.id -> 2011L.some,
//    Jobs.id -> 2014L.some,
//    Learn.id -> 2013L.some,
//    Lifestyle.id -> 1487L.some,
//    News.id -> 1677L.some,
//    Plan.id -> 2012L.some,
//    Save.id -> 2010L.some,
//    Sports.id -> 1485L.some,
//    Style.id -> 2005L.some,
//    Tech.id -> 2072L.some,
//    Travel.id -> 2009L.some,
//    Wellness.id -> 2008L.some
//  )
//
//  val sitePlacementIdToChannel: Map[SitePlacementId, AolDynamicLeadChannels.Type] = Map(
//    // Channel DLs
//    AolMisc.entertainmentChannelDlSitePlacementId -> Entertainment,
//    AolMisc.financeChannelDlSitePlacementId -> Finance,
//    AolMisc.lifestyleChannelDlSitePlacementId -> Lifestyle,
//    AolMisc.sportsChannelDlSitePlacementId -> Sports,
//    AolMisc.newsChannelDlSitePlacementId -> News,
//    AolMisc.aolDlMainPlacementId -> Home,
//
//    // Hnav
//    AolMisc.entertainmentChannelHnavSpId -> Entertainment,
//    AolMisc.newsChannelHnavSpId -> News,
//    AolMisc.lifestyleChannelHnavSpId -> Lifestyle,
//    AolMisc.sportsChannelHnavSpId -> Sports,
//    AolMisc.financeChannelHnavSpId -> Finance
//  )
//
//  private val channelToDlPlacementMap: Map[AolDynamicLeadChannels.Type, SitePlacementId] = Map(
//    Home -> AolMisc.aolDlMainPlacementId,
//    Entertainment -> AolMisc.entertainmentChannelDlSitePlacementId,
//    Finance -> AolMisc.financeChannelDlSitePlacementId,
//    Lifestyle -> AolMisc.lifestyleChannelDlSitePlacementId,
//    Sports -> AolMisc.sportsChannelDlSitePlacementId,
//    News -> AolMisc.newsChannelDlSitePlacementId
//  )
//
//  val dlPlacementToChannelMap = channelToDlPlacementMap.map(kv => kv._2 -> kv._1) ++
//    Map(AolMisc.aolDlMainBonanzaPlacementId -> Home, AolMisc.aolDlBroadbandSitePlacementId -> Home)
//
//  private val channelToSitePlacementMap = sitePlacementIdToChannel.map(kv => kv._2 -> kv._1)
//
//  private val channelToAllSitePlacementsMap = {
//    val res = mutable.Map[Type, mutable.Set[SitePlacementId]]()
//
//    sitePlacementIdToChannel.foreach {
//      case (spid: SitePlacementId, channel: Type) =>
//        val itm = res.getOrElseUpdate(channel, mutable.Set[SitePlacementId]())
//        itm.add(spid)
//    }
//
//    res.toMap
//  }
//
//  implicit class WrappedEnumInstance(val enumInstance: Type) extends AnyVal {
//    def campaignKey: CampaignKey = campaignMap(enumInstance.id)
//
//    def allBoundSitePlacementIds: Set[SitePlacementId] = channelToAllSitePlacementsMap.get(enumInstance).fold(Set.empty[SitePlacementId])(_.toSet)
//
//    def isEnabled: Boolean = enumInstance.id > 0 && enumInstance.id < 6
//  }

  implicit val jsonFormat: Format[AolDynamicLeadChannels.Type] = makeJsonFormat[Type]

  implicit val articleChannelToPinFormat: Format[Map[Type, Int]] = Format[Map[Type, Int]](
    Reads[Map[Type, Int]](
      _.validate[Map[String, Int]].flatMap(_.toSeq.map {
        case (channelName, pinNum) => valuesByName.get(channelName).map(channel => (channel, pinNum))
                                         .toJsResult(JsError(s"No such channel '$channelName'"))
      }.extrude.map(_.toMap))
    ),

    Writes[Map[Type, Int]](typedMap => JsObject(typedMap.map(kv => (kv._1.name, Json.toJson(kv._2))).toSeq))
  )

//  def isDlugCampaign(ck: CampaignKey): Boolean = allDlugCampaignKeys.contains(ck)

//  val channelValuesJson: String = Json.stringify(JsArray(values.diff(Seq(defaultValue)).map(v => JsString(v.n))))

  implicit val defaultValueWriter : DefaultValueWriter[AolDynamicLeadChannels.Type] = makeDefaultValueWriter[Type]
}

case class ChannelToPinnedSlot(channel: AolDynamicLeadChannels.Type, slot: Int)

object ChannelToPinnedSlot {
  implicit val channelsJsonFormat: Format[AolDynamicLeadChannels.Type] = AolDynamicLeadChannels.jsonFormat
  implicit val jsonFormat: Format[ChannelToPinnedSlot] = Json.format[ChannelToPinnedSlot]
  implicit val jsonListFormat: Format[List[ChannelToPinnedSlot]] = Format(Reads.list(jsonFormat), Writes.list(jsonFormat))
}
