package com.gravity.domain.aol

import com.gravity.domain.gms.{GmsArticle, GmsArticleStatus, UniArticleId}
import com.gravity.domain.rtb.gms.ContentGroupIdToPinnedSlot
import com.gravity.domain.{GrvDuration, grvmetrics}
import com.gravity.interests.jobs.intelligence.ArtGrvMap.AllScopesMap
import com.gravity.interests.jobs.intelligence._
import com.gravity.interests.jobs.intelligence.hbase.ScopedKey
import com.gravity.utilities.analytics.articles.AolMisc
import com.gravity.utilities.grvcoll._
import com.gravity.utilities.grvstrings._
import com.gravity.utilities.grvtime
import com.gravity.utilities.grvtime._
import com.gravity.utilities.grvz._
import com.gravity.utilities.time.DateMinute
import org.apache.commons.lang3.builder.{EqualsBuilder, HashCodeBuilder}
import org.joda.time.DateTime
import play.api.libs.functional.syntax._
import play.api.libs.json._

import scala.collection._
import scalaz.NonEmptyList
import scalaz.std.option._
import scalaz.syntax.apply._
import scalaz.syntax.std.option._


/**
 * Created by IntelliJ IDEA.
 * Author: Robbie Coleman
 * Date: 5/1/14
 * Time: 1:51 PM
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
 * @param aolImage An image cropped and stored by Gravity, served via our CDN.
 * @param highResImage A version of [[aolImage]] suitable for retina devices (typically double resolution).
 * @param narrowBandImage A version of [[aolImage]] with same resolution but higher compression for slow connections.
 * @param channelImage This may be a completely different crop and resolution from [[aolImage]] to be served for special
 *                     channel DL units.
 */

class AolDynamicLeadArticle(
  url: String,
  articleId: String,
  title: String,
  metrics: Map[String, AolDynamicLeadMetrics],
  dlArticleStatus: String,
  dlArticleLastGoLiveTime: Int,
  dlArticleSubmittedTime: Int,
  dlArticleSubmittedUserId: Int,
  aolCategory: String,
  aolCategoryLink: String,
  aolCategoryId: Option[Int],
  aolCategorySlug: Option[String],
  aolSource: String,
  aolSourceLink: String,
  aolSummary: String,
  aolHeadline: String,
  aolSecondaryHeader: String,
  aolSecondaryLinks: List[AolLink],
  aolImage: String,
  showVideoIcon: Boolean,
  updatedTime: Int,
  val aolCampaign: Option[AolDynamicLeadCampaign],
  startDate: Option[DateTime],
  endDate: Option[DateTime],
  duration: Option[GrvDuration],
  dlArticleApprovedRejectedByUserId: Option[Int],
  aolRibbonImageUrl: Option[String],
  aolImageSource: Option[String],
  publishOnDate: Int,
  channelImage: Option[String],
  val channels: Set[AolDynamicLeadChannels.Type],
  channelImageSource: Option[String],
  narrowBandImage: Option[String],
  val channelsToPinned: List[ChannelToPinnedSlot],
  val subChannelRibbon: Option[AolChannelRibbon],
  minutelyMetrics: List[AolDynamicLeadMinutelyMetrics],
  channelPageTitle: Option[String],
  highResImage: Option[String])

  extends AolUniArticle(
    url,
    articleId,
    title,
    metrics,
    dlArticleStatus,
    dlArticleLastGoLiveTime,
    dlArticleSubmittedTime,
    dlArticleSubmittedUserId,
    aolCategory,
    aolCategoryLink,
    aolCategoryId,
    aolCategorySlug,
    aolSource,
    aolSourceLink,
    aolSummary,
    aolHeadline,
    aolSecondaryHeader,
    aolSecondaryLinks,
    aolImage,
    showVideoIcon,
    updatedTime,
    startDate,
    endDate,
    duration,
    dlArticleApprovedRejectedByUserId,
    aolRibbonImageUrl,
    aolImageSource,
    publishOnDate,
    channelImage,
    channelImageSource,
    narrowBandImage,
    minutelyMetrics,
    channelPageTitle,
    highResImage,
    None, None, None, Map.empty[String, String], None) {
  override def toString: String = Json.stringify(Json.toJson(this)(AolDynamicLeadArticle.jsonWrites))

  override def equalsExceptMetrics(obj: scala.Any): Boolean = {
    if (super.equalsExceptMetrics(obj) == false)
      return false

    if (obj == null || !obj.isInstanceOf[AolDynamicLeadArticle]) return false

    val that = obj.asInstanceOf[AolDynamicLeadArticle]

    return this.metaDataEquals(that)
  }

  def metaDataEquals(that: AolDynamicLeadArticle): Boolean = {
    if (super.metaDataEquals(that) == false) {
      false
    } else {
      // The `channels`, `channelsToPinned`, and `subChannelRibbon` fields do not affect metaDataEquals or isEquals (compatibility with previous code).
      new EqualsBuilder()
        .append(aolCampaign, that.aolCampaign)
        .isEquals
    }
  }

  override val siteKey: SiteKey = AolMisc.aolSiteKey

  override lazy val uniArticleId: UniArticleId = UniArticleId.forDlug(articleKey)

  override val deliveryMethod: AolDeliveryMethod.Type = if (channelsToPinned.isEmpty) AolDeliveryMethod.personalized else AolDeliveryMethod.pinned

  override def toOneScopeGrvMap: ArtGrvMap.OneScopeMap = {
    val F = AolDynamicLeadFieldNames

    // This method does not populate the grv:map with the `channelsToPinned` field (compatibility with previous code),
    // but search for uses of AolDynamicLeadFieldNames.ChannelsToPin.
    val optionalFields = List(
      F.AolDynamicLeadCampaign -> aolCampaign,
      F.ChannelLabel -> subChannelRibbon,
      F.Channels -> channels.toList.toNel
    )

    val optionalValueMap: ArtGrvMap.OneScopeMap = (for {
      (k1, value) <- optionalFields
      (key, metaValue) <- AolDynamicLeadArticle.toGrvMapKeyVal(k1, value)
    } yield {
      key -> metaValue
    }).toMap

    super.toOneScopeGrvMap ++ optionalValueMap
  }

  def getPinnedSlot(channel: AolDynamicLeadChannels.Type = AolDynamicLeadChannels.Home): Option[Int] = {
    channelsToPinned.find(_.channel == channel).map(_.slot)
  }
}

class AolGmsArticle(val siteKey: SiteKey,
                    val contentGroupIds: Set[Long],
                    val contentGroupsToPinned: List[ContentGroupIdToPinnedSlot],
                    url: String,
                    articleId: String,
                    title: String,
                    metrics: Map[String, AolDynamicLeadMetrics],
                    dlArticleStatus: String,
                    dlArticleLastGoLiveTime: Int,
                    dlArticleSubmittedTime: Int,
                    dlArticleSubmittedUserId: Int,
                    aolCategory: String,
                    aolCategoryLink: String,
                    aolCategoryId: Option[Int],
                    aolCategorySlug: Option[String],
                    aolSource: String,
                    aolSourceLink: String,
                    aolSummary: String,
                    aolHeadline: String,
                    aolSecondaryHeader: String,
                    aolSecondaryLinks: List[AolLink],
                    aolImage: String,
                    showVideoIcon: Boolean,
                    updatedTime: Int,
                    startDate: Option[DateTime],
                    endDate: Option[DateTime],
                    duration: Option[GrvDuration],
                    dlArticleApprovedRejectedByUserId: Option[Int],
                    aolRibbonImageUrl: Option[String],
                    aolImageSource: Option[String],
                    publishOnDate: Int,
                    channelImage: Option[String],
                    channelImageSource: Option[String],
                    narrowBandImage: Option[String],
                    minutelyMetrics: List[AolDynamicLeadMinutelyMetrics],
                    channelPageTitle: Option[String],
                    highResImage: Option[String],
                    altTitleOption: Option[String],
                    altImageOption: Option[String],
                    altImageSourceOption: Option[String],
                    trackingParams: immutable.Map[String, String],
                    author: Option[String])
  extends AolUniArticle(
    url,
    articleId,
    title,
    metrics,
    dlArticleStatus,
    dlArticleLastGoLiveTime,
    dlArticleSubmittedTime,
    dlArticleSubmittedUserId,
    aolCategory,
    aolCategoryLink,
    aolCategoryId,
    aolCategorySlug,
    aolSource,
    aolSourceLink,
    aolSummary,
    aolHeadline,
    aolSecondaryHeader,
    aolSecondaryLinks,
    aolImage,
    showVideoIcon,
    updatedTime,
    startDate,
    endDate,
    duration,
    dlArticleApprovedRejectedByUserId,
    aolRibbonImageUrl,
    aolImageSource,
    publishOnDate,
    channelImage,
    channelImageSource,
    narrowBandImage,
    minutelyMetrics,
    channelPageTitle,
    highResImage,
    altTitleOption,
    altImageOption,
    altImageSourceOption,
    trackingParams,
    author) {
  override def toString: String = Json.stringify(Json.toJson(this)(AolGmsArticle.jsonWrites))

  override def equalsExceptMetrics(obj: scala.Any): Boolean = {
    if (super.equalsExceptMetrics(obj) == false)
      return false

    if (obj == null || !obj.isInstanceOf[AolGmsArticle]) return false

    val that = obj.asInstanceOf[AolGmsArticle]

    return this.metaDataEquals(that)
  }

  def metaDataEquals(that: AolGmsArticle): Boolean = {
    if (super.metaDataEquals(that) == false) {
      false
    } else {
      // Like AolDynamicLeadArticle.channels, AolGmsArticle.contentGroupIds does not affect metaDataEquals or isEquals.
      // GMS-FIELD-UPDATE location
      new EqualsBuilder()
        .append(contentGroupIds, that.contentGroupIds)
        .isEquals
    }
  }

  override lazy val uniArticleId: UniArticleId = UniArticleId.forGms(siteKey, articleKey)

  override val deliveryMethod: AolDeliveryMethod.Type = if (contentGroupsToPinned.isEmpty) AolDeliveryMethod.personalized else AolDeliveryMethod.pinned

  override def toOneScopeGrvMap: ArtGrvMap.OneScopeMap = {
    val F = AolGmsFieldNames

    // This method does not populate the grv:map with the `contentGroupsToPinned` field (compatibility with previous code),
    // but search for uses of AolGmsFieldNames.ContentGroupsToPinned.
    val gmsFields = List(
      F.SiteId -> siteKey.siteId.some,
      F.ContentGroupIds -> contentGroupIds.toList.toNel
    )

    val gmsValueMap: ArtGrvMap.OneScopeMap = (for {
      (k1, value) <- gmsFields
      (key, metaValue) <- AolGmsArticle.toGrvMapKeyVal(k1, value)
    } yield {
      key -> metaValue
    }).toMap

    super.toOneScopeGrvMap ++ gmsValueMap
  }
}

abstract class AolUniArticle(
                              val url: String,
                              val articleId: String,
                              val title: String,
                              val metrics: Map[String, AolDynamicLeadMetrics],
                              val dlArticleStatus: String,
                              val dlArticleLastGoLiveTime: Int,
                              val dlArticleSubmittedTime: Int,
                              val dlArticleSubmittedUserId: Int,
                              val aolCategory: String,
                              val aolCategoryLink: String,
                              val aolCategoryId: Option[Int],
                              val aolCategorySlug: Option[String],
                              val aolSource: String,
                              val aolSourceLink: String,
                              val aolSummary: String,
                              val aolHeadline: String,
                              val aolSecondaryHeader: String,
                              val aolSecondaryLinks: List[AolLink],
                              val aolImage: String,
                              val showVideoIcon: Boolean,
                              val updatedTime: Int,
                              val startDate: Option[DateTime],
                              val endDate: Option[DateTime],
                              val duration: Option[GrvDuration],
                              val dlArticleApprovedRejectedByUserId: Option[Int],
                              val aolRibbonImageUrl: Option[String],
                              val aolImageSource: Option[String],
                              val publishOnDate: Int,
                              val channelImage: Option[String],
                              val channelImageSource: Option[String],
                              val narrowBandImage: Option[String],
                              val minutelyMetrics: List[AolDynamicLeadMinutelyMetrics],
                              val channelPageTitle: Option[String],
                              val highResImage: Option[String],
                              val altTitleOption: Option[String],
                              val altImageOption: Option[String],
                              val altImageSourceOption: Option[String],
                              val trackingParams: immutable.Map[String, String],
                              val author: Option[String]) extends AolArticleFields with GmsArticle {

  val uniArticleId: UniArticleId

  val siteKey: SiteKey

  val articleKey: ArticleKey = ArticleKey(url)

  val deliveryMethod: AolDeliveryMethod.Type

  val articleStatus: GmsArticleStatus.Type = GmsArticleStatus.getOrDefault(dlArticleStatus)

  def getArticleStatus: Option[GmsArticleStatus.Type] = GmsArticleStatus.get(dlArticleStatus)

  def updatedDateTime: DateTime = updatedTime.secondsFromEpoch

  def submittedDateTime: DateTime = dlArticleSubmittedTime.secondsFromEpoch

  def category: AolLink = AolLink(aolCategoryLink, aolCategory)

  def source: AolLink = AolLink(aolSourceLink, aolSource)

  def status: GmsArticleStatus.Type = GmsArticleStatus.getOrDefault(articleStatus.id)

  def headline: String = aolHeadline

  def secondaryLinks: List[AolLink] = aolSecondaryLinks

  def categoryText: String = category.text

  def categoryUrl: String = category.url

  def sourceText: String = source.text

  def sourceUrl: String = source.url

  def secondaryHeader: String = aolSecondaryHeader

  val isActive: Boolean = articleStatus == GmsArticleStatus.Live

  def summary: String = aolSummary

  def imageUrl: String = aolImage

  def effectiveEndDate: Option[DateTime] = {
    duration.flatMap(dur => startDate.map(dur.fromTime)).orElse(endDate)
  }

  def secondsRemainingLive: Option[Long] = {
    articleStatus match {
      case GmsArticleStatus.Live => effectiveEndDate.map(ed => grvtime.currentTime.durationUntil(ed).toSeconds)
      case _ => None
    }
  }

  def toGrvMap(oneScopeKey: ArtGrvMap.OneScopeKey): ArtGrvMap.AllScopesMap =
    Map(oneScopeKey -> toOneScopeGrvMap)

  def toOneScopeGrvMap: ArtGrvMap.OneScopeMap = {
    object F extends AolUniFieldNames

    val nonOptionalValueMap: ArtGrvMap.OneScopeMap = Map(
      F.Title -> ArtGrvMapPublicMetaVal(title)
      , F.Status -> ArtGrvMapPublicMetaVal(dlArticleStatus)
      , F.CategoryText -> ArtGrvMapPublicMetaVal(aolCategory)
      , F.CategoryLink -> ArtGrvMapPublicMetaVal(aolCategoryLink)
      , F.SourceText -> ArtGrvMapPublicMetaVal(aolSource)
      , F.SourceLink -> ArtGrvMapPublicMetaVal(aolSourceLink)
      , F.Summary -> ArtGrvMapPublicMetaVal(aolSummary)
      , F.Headline -> ArtGrvMapPublicMetaVal(aolHeadline)
      , F.SecondaryHeader -> ArtGrvMapPublicMetaVal(aolSecondaryHeader)
      , F.Image -> ArtGrvMapPublicMetaVal(aolImage)
      , F.ShowVideoIcon -> ArtGrvMapPublicMetaVal(showVideoIcon)
      , F.UpdatedTime -> ArtGrvMapPublicMetaVal(updatedTime)
      , F.LastGoLiveTime -> ArtGrvMapPublicMetaVal(dlArticleLastGoLiveTime)
      , F.SubmittedTime -> ArtGrvMapPublicMetaVal(dlArticleSubmittedTime)
      , F.SubmittedUserId -> ArtGrvMapPublicMetaVal(dlArticleSubmittedUserId)
      , F.GravityCalculatedPlid -> ArtGrvMapPublicMetaVal(articleKey.intId)
      , F.ChannelPageTitle -> ArtGrvMapPublicMetaVal(channelPageTitle.getOrElse(title))
    )

    def optionalFields = List(
      F.CategoryId -> aolCategoryId
      , F.CategorySlug -> aolCategorySlug
      , F.SecondaryLinks -> aolSecondaryLinks.toNel
      , F.StartDate -> startDate
      , F.EndDate -> endDate
      , F.Duration -> duration
      , F.ApprovedRejectedUserId -> dlArticleApprovedRejectedByUserId
      , F.RibbonImageUrl -> aolRibbonImageUrl
      , F.ImageCredit -> aolImageSource
      , F.ChannelImage -> channelImage
      , F.ChannelImageSource -> channelImageSource
      , F.NarrowBandImage -> narrowBandImage
      , F.HighResImage -> highResImage
      , F.AltTitle -> altTitleOption
      , F.AltImage -> altImageOption
      , F.AltImageSource -> altImageSourceOption
      , F.TrackingParams -> (if (trackingParams.isEmpty) None else trackingParams.some)
    )

    val optionalValueMap: ArtGrvMap.OneScopeMap = (for {
      (k1, value) <- optionalFields
      (key, metaValue) <- AolUniArticle.toGrvMapKeyVal(k1, value)
    } yield {
      key -> metaValue
    }).toMap

    nonOptionalValueMap ++ optionalValueMap
  }

  private lazy val hashCodeValue = new HashCodeBuilder()
    .append(siteKey.siteId)
    .append(url)
    .append(articleId)
    .append(title)
    .append(dlArticleStatus).toHashCode

  override def hashCode(): Int = hashCodeValue

  override def equals(obj: scala.Any): Boolean = {
    if (obj == null || !obj.isInstanceOf[AolUniArticle]) return false

    val that = obj.asInstanceOf[AolUniArticle]

    new EqualsBuilder()
      .append(true, equalsExceptMetrics(that))
      .append(metrics, that.metrics)
      .isEquals
  }

  def equalsExceptMetrics(obj: scala.Any): Boolean = {
    if (obj == null || !obj.isInstanceOf[AolUniArticle]) return false

    val that = obj.asInstanceOf[AolUniArticle]

    new EqualsBuilder()
      .append(hashCode(), that.hashCode())
      .append(true, metaDataEquals(that))
      .append(updatedTime, that.updatedTime)
      .isEquals
  }

  def metaDataEquals(that: AolUniArticle): Boolean = {
    new EqualsBuilder()
      .append(siteKey, that.siteKey)
      .append(url, that.url)
      .append(articleId, that.articleId)
      .append(title, that.title)
      .append(dlArticleStatus, that.dlArticleStatus)
      .append(dlArticleLastGoLiveTime, that.dlArticleLastGoLiveTime)
      .append(dlArticleSubmittedTime, that.dlArticleSubmittedTime)
      .append(dlArticleSubmittedUserId, that.dlArticleSubmittedUserId)
      .append(aolCategory, that.aolCategory)
      .append(aolCategoryLink, that.aolCategoryLink)
      .append(aolCategoryId, that.aolCategoryId)
      .append(aolCategorySlug, that.aolCategorySlug)
      .append(aolSource, that.aolSource)
      .append(aolSourceLink, that.aolSourceLink)
      .append(aolSummary, that.aolSummary)
      .append(aolHeadline, that.aolHeadline)
      .append(aolSecondaryHeader, that.aolSecondaryHeader)
      .append(aolSecondaryLinks, that.aolSecondaryLinks)
      .append(aolImage, that.aolImage)
      .append(showVideoIcon, that.showVideoIcon)
      .append(startDate, that.startDate)
      .append(endDate, that.endDate)
      .append(dlArticleApprovedRejectedByUserId, that.dlArticleApprovedRejectedByUserId)
      .isEquals
  }
}

object AolGmsArticle {
  implicit val jsonWrites: Writes[AolGmsArticle] = Writes[AolGmsArticle] { a =>
    // GMS-FIELD-UPDATE location
    AolUniArticle.toJsObject(a) ++ Json.obj(
      AolGmsFieldNames.SiteId -> a.siteKey.siteId.toString, // JSON numerics cannot hold Long values.
      AolGmsFieldNames.ContentGroupIds -> Json.toJson(a.contentGroupIds.map(_.toString).toSet)(Writes.set[String]),
      AolGmsFieldNames.ContentGroupsToPinned -> Json.toJson(a.contentGroupsToPinned)(ContentGroupIdToPinnedSlot.jsonListFormat)
    )
  }

  def empty(siteKey: SiteKey): AolGmsArticle {def toGrvMap(oneScopeKey: (Option[ScopedKey], String)): AllScopesMap} = new AolGmsArticle(
    siteKey, Set.empty[Long], Nil, emptyString, emptyString, emptyString, Map.empty[String, AolDynamicLeadMetrics], emptyString, 0, 0, 0, emptyString,
    emptyString, None, None, emptyString, emptyString, emptyString, emptyString, emptyString, Nil, emptyString, false,
    0, None, None, None, None, None, None, 0, None, None, None, Nil, None, None, None, None, None, Map.empty[String, String], None
  ) {
    override def toGrvMap(oneScopeKey: (Option[ScopedKey], String)): AllScopesMap = Map(oneScopeKey -> ArtGrvMap.emptyOneScopeMap)
  }

  def apply(siteKey: SiteKey,
            contentGroupIds: Set[Long],
            contentGroupsToPinned: List[ContentGroupIdToPinnedSlot],
            url: String,
            articleId: String,
            title: String,
            metrics: Map[String, AolDynamicLeadMetrics],
            dlArticleStatus: String,
            dlArticleLastGoLiveTime: Int,
            dlArticleSubmittedTime: Int,
            dlArticleSubmittedUserId: Int,
            aolCategory: String,
            aolCategoryLink: String,
            aolCategoryId: Option[Int],
            aolCategorySlug: Option[String],
            aolSource: String,
            aolSourceLink: String,
            aolSummary: String,
            aolHeadline: String,
            aolSecondaryHeader: String,
            aolSecondaryLinks: List[AolLink],
            aolImage: String,
            showVideoIcon: Boolean,
            updatedTime: Int,
            startDate: Option[DateTime],
            endDate: Option[DateTime],
            duration: Option[GrvDuration],
            dlArticleApprovedRejectedByUserId: Option[Int],
            aolRibbonImageUrl: Option[String],
            aolImageSource: Option[String],
            channelImage: Option[String],
            channelImageSource: Option[String],
            narrowBandImage: Option[String],
            minutelyMetrics: List[AolDynamicLeadMinutelyMetrics],
            channelPageTitle: Option[String],
            highResImage: Option[String],
            altTitleOption: Option[String],
            altImageOption: Option[String],
            altImageSourceOption: Option[String],
            trackingParams: immutable.Map[String, String] = Map.empty[String, String],
            author: Option[String] = None): AolGmsArticle = {

    val pubOnDate = startDate.map(_.getSeconds).getOrElse {
      if (dlArticleLastGoLiveTime > 0) {
        dlArticleLastGoLiveTime
      }
      else {
        dlArticleSubmittedTime
      }
    }

    new AolGmsArticle(
      siteKey, contentGroupIds, contentGroupsToPinned, url, articleId, title, metrics, dlArticleStatus,
      dlArticleLastGoLiveTime, dlArticleSubmittedTime, dlArticleSubmittedUserId, aolCategory, aolCategoryLink,
      aolCategoryId, aolCategorySlug, aolSource, aolSourceLink, aolSummary, aolHeadline, aolSecondaryHeader,
      aolSecondaryLinks, aolImage, showVideoIcon, updatedTime, startDate, endDate, duration,
      dlArticleApprovedRejectedByUserId, aolRibbonImageUrl, aolImageSource, pubOnDate, channelImage, channelImageSource,
      narrowBandImage, minutelyMetrics, channelPageTitle, highResImage, altTitleOption, altImageOption,
      altImageSourceOption, trackingParams, author
    )
  }

  val toGrvMapKeyVal: (String, Option[Any]) => Option[(String, ArtGrvMapMetaVal)] = (name: String, optionalValue: Option[Any]) => {
    // GMS-FIELD-UPDATE location
    optionalValue.collect {
      case contentGroupIds: NonEmptyList[_] if name == AolGmsFieldNames.ContentGroupIds =>
        name -> ArtGrvMapPublicMetaVal(contentGroupIds.list.mkString("[\"", "\",\"", "\"]"))
    } orElse AolUniArticle.toGrvMapKeyVal(name, optionalValue)
  }
}

object AolDynamicLeadArticle {
  private implicit val campaignFormat = AolDynamicLeadCampaign.jsonFormat
  private implicit val subChannelRibbonOptionalFormat = AolChannelRibbon.jsonOptionFormat

  implicit val jsonWrites: Writes[AolDynamicLeadArticle] = Writes[AolDynamicLeadArticle] { a =>
    AolUniArticle.toJsObject(a) ++ Json.obj(
      "aolCampaign" -> Json.toJson(a.aolCampaign),
      "channels" -> Json.toJson(a.channels.toSet)(Writes.set[AolDynamicLeadChannels.Type](AolDynamicLeadChannels.jsonFormat)),
      AolDynamicLeadFieldNames.ChannelsToPin -> Json.toJson(a.channelsToPinned)(ChannelToPinnedSlot.jsonListFormat),
      AolDynamicLeadFieldNames.ChannelLabel -> a.subChannelRibbon
    )
  }

  def apply(
             url: String,
             articleId: String,
             title: String,
             metrics: Map[String, AolDynamicLeadMetrics],
             dlArticleStatus: String,
             dlArticleLastGoLiveTime: Int,
             dlArticleSubmittedTime: Int,
             dlArticleSubmittedUserId: Int,
             aolCategory: String,
             aolCategoryLink: String,
             aolCategoryId: Option[Int],
             aolCategorySlug: Option[String],
             aolSource: String,
             aolSourceLink: String,
             aolSummary: String,
             aolHeadline: String,
             aolSecondaryHeader: String,
             aolSecondaryLinks: List[AolLink],
             aolImage: String,
             showVideoIcon: Boolean,
             updatedTime: Int,
             aolCampaign: Option[AolDynamicLeadCampaign],
             startDate: Option[DateTime],
             endDate: Option[DateTime],
             duration: Option[GrvDuration],
             dlArticleApprovedRejectedByUserId: Option[Int],
             aolRibbonImageUrl: Option[String],
             aolImageSource: Option[String],
             channelImage: Option[String],
             channels: Set[AolDynamicLeadChannels.Type],
             channelImageSource: Option[String],
             narrowBandImage: Option[String],
             channelsToPinned: List[ChannelToPinnedSlot],
             subChannelRibbon: Option[AolChannelRibbon],
             minutelyMetrics: List[AolDynamicLeadMinutelyMetrics],
             channelPageTitle: Option[String],
             highResImage: Option[String]): AolDynamicLeadArticle = {

    val pubOnDate = startDate.map(_.getSeconds).getOrElse {
      if (dlArticleLastGoLiveTime > 0) {
        dlArticleLastGoLiveTime
      }
      else {
        dlArticleSubmittedTime
      }
    }

    new AolDynamicLeadArticle(
      url, articleId, title, metrics, dlArticleStatus, dlArticleLastGoLiveTime,
      dlArticleSubmittedTime, dlArticleSubmittedUserId, aolCategory, aolCategoryLink,
      aolCategoryId, aolCategorySlug, aolSource, aolSourceLink, aolSummary,
      aolHeadline, aolSecondaryHeader, aolSecondaryLinks, aolImage,
      showVideoIcon, updatedTime, aolCampaign, startDate, endDate, duration,
      dlArticleApprovedRejectedByUserId, aolRibbonImageUrl, aolImageSource,
      pubOnDate, channelImage, channels, channelImageSource, narrowBandImage,
      channelsToPinned, subChannelRibbon, minutelyMetrics, channelPageTitle,
      highResImage
    )
  }

  val toGrvMapKeyVal: (String, Option[Any]) => Option[(String, ArtGrvMapMetaVal)] = (name: String, optionalValue: Option[Any]) => {
    // The `channelsToPinned` field is not handled here (compatibility with existing code)
    optionalValue.collect {
      case channels: NonEmptyList[_] if name == AolDynamicLeadFieldNames.Channels => name -> ArtGrvMapPublicMetaVal(channels.list.collect {
        case channel: AolDynamicLeadChannels.Type => channel.toString
      }.mkString("[\"", "\",\"", "\"]"))
      case dlCamp: AolDynamicLeadCampaign => name -> ArtGrvMapPublicMetaVal(dlCamp.toString)
      case subChannelRibbon: AolChannelRibbon => name -> ArtGrvMapPublicMetaVal(subChannelRibbon.toString)
    } orElse AolUniArticle.toGrvMapKeyVal(name, optionalValue)
  }

  val empty: AolDynamicLeadArticle {def toGrvMap(oneScopeKey: (Option[ScopedKey], String)): AllScopesMap} = new AolDynamicLeadArticle(
    emptyString, emptyString, emptyString, Map.empty[String, AolDynamicLeadMetrics], emptyString, 0, 0, 0, emptyString,
    emptyString, None, None, emptyString, emptyString, emptyString, emptyString, emptyString, Nil, emptyString, false,
    0, None, None, None, None, None, None, None, 0, None, Set.empty[AolDynamicLeadChannels.Type], None, None, Nil, None,
    Nil, None, None
  ) {
    override def toGrvMap(oneScopeKey: (Option[ScopedKey], String)): AllScopesMap = Map(oneScopeKey -> ArtGrvMap.emptyOneScopeMap)
  }
}

object AolUniArticle {
  private implicit val minutelyMetricsWrites = AolDynamicLeadMinutelyMetrics.jsonListFormat

  def toJsObject(a: AolUniArticle): JsObject = Json.obj(
    "url" -> a.url,
    "articleId" -> a.articleId,
    "title" -> a.title,
    AolUniFieldNames.ChannelPageTitle -> a.channelPageTitle,
    "metrics" -> JsObject(a.metrics.map(kv => kv._1 -> Json.toJson(kv._2)(AolDynamicLeadMetrics.jsonWrites)).toSeq),
    "dlArticleStatus" -> a.dlArticleStatus,
    "dlArticleLastGoLiveTime" -> a.dlArticleLastGoLiveTime,
    "dlArticleSubmittedTime" -> a.dlArticleSubmittedTime,
    "dlArticleSubmittedUserId" -> a.dlArticleSubmittedUserId,
    "aolCategory" -> a.aolCategory,
    "aolCategoryLink" -> a.aolCategoryLink,
    "aolCategoryId" -> a.aolCategoryId,
    "aolCategorySlug" -> a.aolCategorySlug,
    "aolSource" -> a.aolSource,
    "aolSourceLink" -> a.aolSourceLink,
    "aolSummary" -> a.aolSummary,
    "aolHeadline" -> a.aolHeadline,
    "aolSecondaryHeader" -> a.aolSecondaryHeader,
    "aolSecondaryLinks" -> Json.toJson(a.aolSecondaryLinks)(AolLink.internalJsonListFormat),
    "aolImage" -> a.aolImage,
    "showVideoIcon" -> a.showVideoIcon,
    "updatedTime" -> a.updatedTime,
    "startDate" -> a.startDate.map(_.toString()),
    "endDate" -> a.endDate.map(_.toString()),
    AolUniFieldNames.Duration -> a.duration.map(_.toString),
    "dlArticleApprovedRejectedByUserId" -> a.dlArticleApprovedRejectedByUserId,
    "aolRibbonImageUrl" -> a.aolRibbonImageUrl,
    "aolImageSource" -> a.aolImageSource,
    "publishOnDate" -> a.publishOnDate,
    "channelImage" -> a.channelImage,
    "channelImageSource" -> a.channelImageSource,
    AolUniFieldNames.NarrowBandImage -> a.narrowBandImage,
    AolUniFieldNames.HighResImage -> a.highResImage,
    "minutelyMetrics" -> a.minutelyMetrics,
    "secondsRemainingLive" -> a.secondsRemainingLive,
    AolUniFieldNames.AltTitle -> a.altTitleOption,
    AolUniFieldNames.AltImage -> a.altImageOption,
    AolUniFieldNames.AltImageSource -> a.altImageSourceOption,
    AolUniFieldNames.TrackingParams -> Json.toJson(a.trackingParams)
  )

  val dlArticleFeedWrites: Writes[AolDynamicLeadArticle] = Writes[AolDynamicLeadArticle](a => Json.obj(
    "displayUrl" -> a.url,
    "data" -> ArtGrvMap.grvMapAsJson(a.toOneScopeGrvMap.some)
  ))

  val dlArticleFeedListWrites: Writes[List[AolDynamicLeadArticle]] = Writes[List[AolDynamicLeadArticle]](l => Json.obj("articles" -> Json.toJson(l)(Writes.list(dlArticleFeedWrites))))

  val uniArticleFeedWrites: Writes[AolUniArticle] = Writes[AolUniArticle](a => {
    a.author match {
      case Some(author) =>
        Json.obj(
          "displayUrl" -> a.url,
          "author" -> author,
          "data" -> ArtGrvMap.grvMapAsJson(a.toOneScopeGrvMap.some)
        )

      case None =>
        Json.obj(
          "displayUrl" -> a.url,
          "data" -> ArtGrvMap.grvMapAsJson(a.toOneScopeGrvMap.some)
        )

    }

  })

  val uniArticleFeedListWrites: Writes[List[AolUniArticle]] = Writes[List[AolUniArticle]](l => Json.obj("articles" -> Json.toJson(l)(Writes.list(uniArticleFeedWrites))))

  val toGrvMapKeyVal: (String, Option[Any]) => Option[(String, ArtGrvMapMetaVal)] = (name: String, optionalValue: Option[Any]) => {
    optionalValue.collect {
      case intVal: Int => name -> ArtGrvMapPublicMetaVal(intVal)
      case longVal: Long => name -> ArtGrvMapPublicMetaVal(longVal.toString)  // JSON numerics can't hold Long values.
      case strVal: String => name -> ArtGrvMapPublicMetaVal(strVal)
      case links: NonEmptyList[_] if name == AolUniFieldNames.SecondaryLinks => name -> ArtGrvMapPublicMetaVal(links.list.collect {
        case link: AolLink => link.toString
      }.mkString("[", ",", "]"))
      case dateTime: DateTime => name -> ArtGrvMapPublicMetaVal(dateTime.getSeconds)
      case grvDuration: GrvDuration => name -> ArtGrvMapPublicMetaVal(grvDuration.toString)
      case dlStatus: GmsArticleStatus.Type => name -> ArtGrvMapPublicMetaVal(dlStatus.toString)
      case map: immutable.Map[_, _] if map.headOption.exists(kv => kv._1.isInstanceOf[String] && kv._2.isInstanceOf[String]) =>
        name -> ArtGrvMapPublicMetaVal(Json.stringify(Json.toJson(map.asInstanceOf[immutable.Map[String, String]])))
    }
  }
}

object AolGmsFieldNames extends AolUniFieldNames {
  // GMS-FIELD-UPDATE location (look for other instances of this comment when adding fields!)

  val SiteId = "siteId"
  val ContentGroupIds = "contentGroupIds"
  val ContentGroupsToPinned = "contentGroupsToPinned"

  override val defined = base_defined ++ Set(SiteId, ContentGroupIds, ContentGroupsToPinned)
}

object AolDynamicLeadFieldNames extends AolUniFieldNames {
  // The following fields are DLUG-only.
  val Channels = "channels"
  val ChannelsToPin = "channelToPin"

  // The following fields are not even used by the front end, as far as we know, but are still supported.
  val AolDynamicLeadCampaign = "aolDynamicLeadCampaign"
  val ChannelLabel = "channelLabel"

  override val defined = base_defined ++ Set(Channels, ChannelsToPin, AolDynamicLeadCampaign, ChannelLabel)
}

object AolUniFieldNames extends AolUniFieldNames

trait AolUniFieldNames {
  val Title = "title"
  val ChannelPageTitle = "channelPageTitle"
  val Status = "status"
  val CategoryText = "category"
  val CategoryLink = "categoryLink"
  val SourceText = "source"
  val SourceLink = "sourceLink"
  val Summary = "summary"
  val Headline = "headline"
  val SecondaryHeader = "secondaryHeader"
  val Image = "image"
  val ShowVideoIcon = "showVideoIcon"
  val UpdatedTime = "updatedTime"
  val LastGoLiveTime = "lastGoLiveTime"
  val SubmittedTime = "submittedTime"
  val SubmittedUserId = "submittedUserId"
  val CategoryId = "categoryId"
  val CategorySlug = "categorySlug"
  val SecondaryLinks = "secondaryLinks"
  val StartDate = "startDate"
  val EndDate = "endDate"
  val ApprovedRejectedUserId = "approvedRejectedUserId"
  val RibbonImageUrl = "ribbonImageUrl"
  val ImageCredit = "imageCredit"
  val ChannelImage = "channelImage"
  val ChannelImageSource = "channelImageSource"
  val NarrowBandImage = "narrowBandImage"
  val GravityCalculatedPlid = "gravityCalculatedPlid"
  val Duration = "duration"
  val HighResImage = "highResImage"
  val IsPinned = "isPinned"
  val AltTitle = "altTitle"
  val AltImage = "altImage"
  val AltImageSource = "altImageSource"
  val TrackingParams = "trackingParams"

  val base_defined = Set(
    Title, ChannelPageTitle, Status, CategoryText, CategoryLink, SourceText, SourceLink, Summary, Headline,
    SecondaryHeader, Image, ShowVideoIcon, UpdatedTime, LastGoLiveTime, SubmittedTime, SubmittedUserId,
    CategoryId, CategorySlug, SecondaryLinks, StartDate, EndDate, ApprovedRejectedUserId, RibbonImageUrl,
    ImageCredit, ChannelImage, ChannelImageSource, NarrowBandImage, GravityCalculatedPlid, Duration,
    HighResImage, IsPinned, AltTitle, AltImage, AltImageSource, TrackingParams)
  val defined = base_defined
}

case class MetricsBase(clicks: Long, impressions: Long, ctr: Double) {
  def +(that: MetricsBase): MetricsBase = {
    val totalClicks = this.clicks + that.clicks
    val totalImps = this.impressions + that.impressions
    val totalCTR = grvmetrics.safeCTR(totalClicks, totalImps)

    MetricsBase(totalClicks, totalImps, totalCTR)
  }

  override def toString: String = Json.stringify(Json.toJson(this)(MetricsBase.jsonWrites))
}

object MetricsBase {
  implicit val jsonWrites: Writes[MetricsBase] = Writes[MetricsBase](m => Json.obj("clicks" -> m.clicks, "impressions" -> m.impressions, "ctr" -> m.ctr))

  val empty: MetricsBase = MetricsBase(0L, 0L, 0D)

  def apply(clicks: Long, impressions: Long): MetricsBase = MetricsBase(
    clicks, impressions, grvmetrics.safeCTR(clicks, impressions)
  )
}

case class ClicksImps(clicks: Long, imps: Long) {
  def +(that: ClicksImps): ClicksImps = ClicksImps(this.clicks + that.clicks, this.imps + that.imps)
  def plusClicks(clicks: Long): ClicksImps = ClicksImps(this.clicks + clicks, this.imps)
  def plusImps(imps: Long): ClicksImps = ClicksImps(this.clicks, this.imps + imps)
  def toMetricsLite: AolDynamicLeadMetricsLite = AolDynamicLeadMetricsLite(imps, grvmetrics.safeCTR(clicks, imps))
  def toMinutelyMetrics(minute: DateMinute): AolDynamicLeadMinutelyMetrics = AolDynamicLeadMinutelyMetrics(minute.getMillis.toString, imps, grvmetrics.safeCTR(clicks, imps))
}

object ClicksImps {
  val empty: ClicksImps = ClicksImps(0L, 0L)
  def forClicks(clicks: Long): ClicksImps = ClicksImps(clicks, 0L)
  def forImpressions(imps: Long): ClicksImps = ClicksImps(0L, imps)
}

case class AolDynamicLeadMetrics(lifetime: MetricsBase, thirtyMinute: MetricsBase) {
  def +(that: AolDynamicLeadMetrics): AolDynamicLeadMetrics = {

    val totalLifetime = this.lifetime + that.lifetime
    val total30minute = this.thirtyMinute + that.thirtyMinute

    AolDynamicLeadMetrics(totalLifetime, total30minute)
  }

  override def toString: String = Json.stringify(Json.toJson(this)(AolDynamicLeadMetrics.jsonWrites))
}

object AolDynamicLeadMetrics {
  implicit val baseWrites: Writes[MetricsBase] = MetricsBase.jsonWrites
  implicit val jsonWrites: Writes[AolDynamicLeadMetrics] = Writes[AolDynamicLeadMetrics](m => Json.obj("lifetime" -> m.lifetime, "thirtyMinutes" -> m.thirtyMinute))

  val empty: AolDynamicLeadMetrics = AolDynamicLeadMetrics(MetricsBase.empty, MetricsBase.empty)
}

case class AolDynamicLeadMetricsLite(impressions: Long, ctr: Double) {
  override def toString: String = Json.stringify(toJson)

  def toJson: JsValue = Json.toJson(this)(AolDynamicLeadMetricsLite.jsonFormat)
}

object AolDynamicLeadMetricsLite {
  implicit val jsonFormat: Format[AolDynamicLeadMetricsLite] = Json.format[AolDynamicLeadMetricsLite]

  val empty: AolDynamicLeadMetricsLite = AolDynamicLeadMetricsLite(0L, 0D)
}

case class AolDynamicLeadMinutelyMetrics(minuteMillis: String, impressions: Long, ctr: Double)

object AolDynamicLeadMinutelyMetrics {
  implicit val jsonFormat: Format[AolDynamicLeadMinutelyMetrics] = Json.format[AolDynamicLeadMinutelyMetrics]
  implicit val jsonListFormat: Format[List[AolDynamicLeadMinutelyMetrics]] = Format(Reads.list(jsonFormat), Writes.list(jsonFormat))
  val empty: AolDynamicLeadMinutelyMetrics = AolDynamicLeadMinutelyMetrics(emptyString, 0L, 0D)
}

case class AolDynamicLeadMetricsWithMinutely(metrics: Map[String, AolDynamicLeadMetrics], minutely: List[AolDynamicLeadMinutelyMetrics])

object AolDynamicLeadMetricsWithMinutely {
  def apply(metrics: Map[String, AolDynamicLeadMetrics], minutelyClickImpsMap: Map[DateMinute, ClicksImps]): AolDynamicLeadMetricsWithMinutely = {
    AolDynamicLeadMetricsWithMinutely(metrics, minutelyClickImpsMap.toList.sortBy(_._1.getMillis).map {
      case (minute: DateMinute, clicksImps: ClicksImps) => clicksImps.toMinutelyMetrics(minute)
    })
  }

  def emptyMetrics = new AolDynamicLeadMetricsWithMinutely(Map.empty, Nil)
}

case class AolDynamicLeadCampaign(productId: Int, planName: String) {
  override def toString: String = s"""{"productId":$productId,"planName":"$planName"}"""
}

object AolDynamicLeadCampaign {
  def parse(productIdOpt: Option[Int], planNameOpt: Option[String]): Option[AolDynamicLeadCampaign] = {
    (productIdOpt tuple planNameOpt).map {
      case (prodId: Int, planName: String) => AolDynamicLeadCampaign(prodId, planName)
    }
  }

  implicit val jsonFormat: Format[AolDynamicLeadCampaign] = Json.format[AolDynamicLeadCampaign]
}

case class AolChannelRibbon(channel: AolDynamicLeadChannels.Type, imageUrl: String, linkUrl: String) {
  override def toString: String = Json.stringify(Json.toJson(this)(AolChannelRibbon.jsonFormat))
}

object AolChannelRibbon {

  implicit val jsonWrites: Writes[AolChannelRibbon] = Json.writes[AolChannelRibbon]
  implicit val jsonReads: Reads[AolChannelRibbon] = (
    (__ \ "channel").read[AolDynamicLeadChannels.Type].orElse(
      (__ \ "linkUrl").read[String].map(AolChannelRibbon.findChannelByClickUrl).filter(_.nonEmpty).map(_.get)
    ) and
      (__ \ "imageUrl").read[String] and
      (__ \ "linkUrl").read[String]
    )(AolChannelRibbon.apply _)


  implicit val jsonFormat: Format[AolChannelRibbon] = Format(jsonReads, jsonWrites)
  implicit val jsonOptionFormat: Format[Option[AolChannelRibbon]] = Format(Reads.optionWithNull(jsonReads), Writes.optionWithNull(jsonWrites))

  private val baseImageUrl = "http://www.aol.com/channel/image/"
  private val baseClickUrl = "http://www.aol.com/"

  def buildChannelRibbon(channel: AolDynamicLeadChannels.Type): (AolDynamicLeadChannels.Type, AolChannelRibbon) = {
    val linkUrl = channel match {
      case AolDynamicLeadChannels.HomeSub => baseClickUrl + "home"
      case _ => baseImageUrl + channel.name + ".jpg"
    }
    channel -> AolChannelRibbon(channel, baseImageUrl + channel.name + ".jpg", linkUrl)
  }

  private val channelToRibbonMap = AolDynamicLeadChannels.values.map(buildChannelRibbon).toMap

  def get(channel: AolDynamicLeadChannels.Type): Option[AolChannelRibbon] = channelToRibbonMap.get(channel)

  def findChannelByClickUrl(linkUrl: String): Option[AolDynamicLeadChannels.Type] =
    channelToRibbonMap.values.find(ribbon => {
      ribbon.linkUrl == linkUrl || ribbon.linkUrl + ".jpg" == linkUrl ||
        "http://www.aol.com/channel/" + ribbon.channel.name + ".jpg" == linkUrl
    }).map(_.channel)
}
