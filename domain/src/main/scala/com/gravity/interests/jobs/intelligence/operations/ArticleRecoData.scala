package com.gravity.interests.jobs.intelligence.operations

import com.gravity.interests.jobs.intelligence.operations.GrccableEvent._
import com.gravity.interests.jobs.intelligence.{ArticleKey, CampaignKey}
import com.gravity.utilities.analytics.URLUtils
import com.gravity.utilities.grvtime
import org.joda.time.DateTime

/*
*     __         __
*  /"  "\     /"  "\
* (  (\  )___(  /)  )
*  \               /
*  /               \
* /    () ___ ()    \  erik 4/16/14
* |      (   )      |
*  \      \_/      /
*    \...__!__.../
*/
@SerialVersionUID(1l)
case class ArticleRecoData(key: ArticleKey, rawUrl: String, recoGenerationDate: Long, placementId: Int, sitePlacementId: Long, var why: String,
                           recoAlgo: Int, currentAlgo: Int, recoBucket: Int, currentBucket: Int, recommenderId: Long,
                           sponseeGuid: String, sponsorGuid: String, sponsorPoolGuid: String, auctionId: String,
                           campaignKey: String, costPerClick: Long, campaignType: Int, displayIndex: Int,
                           contentGroupId: Long, sourceKey: String, algoSettingsData: Seq[AlgoSettingsData], articleSlotsIndex: Int, exchangeGuid: String) {

  import ImpressionEvent.checkWhyFor

  //if (contentGroupId == -1) ArticleRecoData.voidContentGroupIdAll.increment else ArticleRecoData.validContentGroupIdAll.increment

  lazy val articleUrl: String = URLUtils.normalizeUrl(rawUrl)
  lazy val hash: String = sitePlacementId.toString + recoBucket.toString + getRecoGenerationDate + rawUrl + costPerClick + campaignKey + key.toString + displayIndex.toString

  def ck: Option[CampaignKey] = CampaignKey.parse(campaignKey)
  def getRecoGenerationDate = new DateTime(recoGenerationDate)

  why = checkWhyFor(FIELD_DELIM)(checkWhyFor(LIST_DELIM)(checkWhyFor(GRCC_DELIM)(why)))

  def toDisplayString : String = {
    val sb = new StringBuilder()
    sb.append(" ArticleRecoData  Key: ").append(key).append(" rawUrl: ").append(rawUrl).append(" recoGenerationDate: ").append(getRecoGenerationDate).append(" placementId: ").append(placementId).append(" sitePlacementId: ").append(sitePlacementId).append(" why: ").append(why)
    sb.append(" recoAlgo: ").append(recoAlgo).append(" currentAlgo: ").append(currentAlgo).append(" recoBucket: ").append(recoBucket).append(" currentBucket: ").append(currentBucket).append(" recommenderId: ").append(recommenderId)
    sb.append(" sponseeGuid: ").append(sponseeGuid).append(" sponsorGuid: ").append(sponsorGuid).append(" sponsorPoolGuid: ").append(sponsorPoolGuid).append(" auctionId: ").append(auctionId)
    sb.append(" campaignKey: ").append(campaignKey).append(" cpc: ").append(costPerClick).append(" campaignType: ").append(campaignType).append(" displayIndex: ").append(displayIndex)
    sb.append(" contentGroupId: ").append(contentGroupId).append(" sourceKey: ").append(sourceKey)
    sb.append(" algoSettingsData: ")
    algoSettingsData.map(setting => sb.append(setting).append(", "))
    sb.append(" articleSlotsIndex: ").append(articleSlotsIndex).append(" exchangeGuid: ").append(exchangeGuid)
    sb.toString()
  }
}

object ArticleRecoData {
  def apply(
             key: ArticleKey,
             rawUrl: String,
             recoGenerationDateTime: DateTime,
             placementId: Int,
             sitePlacementId: Long,
             why: String,
             recoAlgo: Int,
             currentAlgo: Int,
             recoBucket: Int,
             currentBucket: Int,
             recommenderId: Long,
             sponseeGuid: String,
             sponsorGuid: String,
             sponsorPoolGuid: String,
             auctionId: String,
             campaignKey: String,
             costPerClick: Long,
             campaignType: Int,
             displayIndex: Int,
             contentGroupId: Long,
             sourceKey: String,
             algoSettingsData: Seq[AlgoSettingsData],
             articleSlotsIndex: Int,
             exchangeGuid: String
           ) =
    new ArticleRecoData(
      key = key,
      rawUrl = rawUrl,
      recoGenerationDate = recoGenerationDateTime.getMillis,
      placementId = placementId,
      sitePlacementId = sitePlacementId,
      why = why,
      recoAlgo = recoAlgo,
      currentAlgo = currentAlgo,
      recoBucket = recoBucket,
      currentBucket = currentBucket,
      recommenderId = recommenderId,
      sponseeGuid = sponseeGuid,
      sponsorGuid = sponsorGuid,
      sponsorPoolGuid = sponsorPoolGuid,
      auctionId = auctionId,
      campaignKey = campaignKey,
      costPerClick = costPerClick,
      campaignType = campaignType,
      displayIndex = displayIndex,
      contentGroupId = contentGroupId,
      sourceKey = sourceKey,
      algoSettingsData = algoSettingsData,
      articleSlotsIndex = articleSlotsIndex,
      exchangeGuid = exchangeGuid
    )
//  // version 37 of grcc is the first version with content group id
//
//  private val voidContentGroupIdAll = new Counter("void content group id all", "Event_ArticleRecoData", true, CounterType.PER_SECOND)
//  private val validContentGroupIdAll = new Counter("valid content group id all", "Event_ArticleRecoData", true, CounterType.PER_SECOND)
//  private [operations] val forClickEvent = new Counter("grccable click event (v36-)", "Event_ArticleRecoData", true, CounterType.PER_SECOND)
//  private [operations] val forImpressionEvent = new Counter("grccable impression event (v36-)", "Event_ArticleRecoData", true, CounterType.PER_SECOND)
//  private [operations] val forClickEvent37Plus = new Counter("grccable click event (v37+)", "Event_ArticleRecoData", true, CounterType.PER_SECOND)
//  private [operations] val forImpressionEvent37Plus = new Counter("grccable impression event (v37+)", "Event_ArticleRecoData", true, CounterType.PER_SECOND)
//  private [operations] val byClickEvent = new Counter("by click event", "Event_ArticleRecoData", true, CounterType.PER_SECOND)
//  private [operations] val byImpressionEvent = new Counter("by impression event", "Event_ArticleRecoData", true, CounterType.PER_SECOND)

  val empty: ArticleRecoData = ArticleRecoData(ArticleKey.empty, "", grvtime.epochDateTime, -1, -1, "empty", -1, -1, -1, -1, -1l, "", "", "", "", "", 0l, 0, 0, 0, "", List.empty[AlgoSettingsData], -1, "")
}
