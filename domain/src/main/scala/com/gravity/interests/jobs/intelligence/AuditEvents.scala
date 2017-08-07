package com.gravity.interests.jobs.intelligence

import com.gravity.utilities.grvenum.GrvEnum
import com.gravity.utilities.grvjson.EnumNameSerializer
import com.gravity.utilities.swagger.adapter.DefaultValueWriter
import play.api.libs.json.Format

import scala.util.matching.Regex

@SerialVersionUID(-1932043529859269178l)
object AuditEvents extends GrvEnum[Short] with DlugAuditEventMeta {
  case class Type(i: Short, n: String) extends ValueTypeBase(i, n)
  override def mkValue(id: Short, name: String): Type = Type(id, name)

  /** Campaign related events are reserved in the 1 - 99 range */
  val unknown: Type = Value(0, "unknown")
  val campaignCreation: Type = Value(1, "campaignCreation")
  val campaignNameUpdate: Type = Value(2, "campaignNameUpdate")
  val campaignStatusChange: Type = Value(3, "campaignStatusChange")
  val startDateUpdate: Type = Value(4, "startDateUpdate")
  val endDateUpdate: Type = Value(5, "endDateUpdate")
  val bidUpdate: Type = Value(6, "bidUpdate")
  val budgetAmountUpdate: Type = Value(7, "budgetAmountUpdate")
  val budgetTypeUpdate: Type = Value(8, "budgetTypeUpdate")
  val trackingParamUpdate: Type = Value(9, "trackingParamUpdate")
  val rssFeedListAdd: Type = Value(10, "rssFeedListAdd")
  val rssFeedStatusUpdate: Type = Value(11, "rssFeedStatusUpdate")
  val rssFeedSettingsUpdate: Type = Value(12, "rssFeedSettingsUpdate")
  val articleUrlListAdd: Type = Value(13, "articleUrlListAdd")
//  val articleSettingsUpdate = Value(14, "articleSettingsUpdate")
  val algoChanges: Type = Value(15, "algoChanges")
//  val bucketChanges = Value(16, "bucketChanges")
//  val segmentPlacementUpdate = Value(17, "segmentPlacementUpdate")
//  val countryRestrictionsAdd = Value(18, "countryRestrictionsAdd")
//  val deviceRestrictionsAdd = Value(19, "deviceRestrictionsAdd")

//  val countryRestrictionsDelete = Value(20, "countryRestrictionsDelete")
//  val deviceRestrictionsDelete = Value(21, "deviceRestrictionsDelete")
  val rssFeedListDelete: Type = Value(22, "rssFeedListDelete")
  val articleUrlListDelete: Type = Value(23, "articleUrlListDelete")
  val displayDomainUpdate: Type = Value(24, "displayDomainUpdate")
  val campaignTypeChange: Type = Value(25, "campaignTypeChange")
  val campaignUseThumbyUpdate: Type = Value(26, "campaignUseThumbyUpdate")
  val recentArticlesMaxAgeUpdate: Type = Value(27, "recentArticlesMaxAgeUpdate")
  val budgetSettingsUpdate: Type = Value(28, "budgetSettingsUpdate")
  val campIngestRulesUpdate: Type = Value(29, "campIngestRulesUpdate")
  val bidFloor: Type = Value(30, "bidFloor")
  val trackingParamAdd: Type = Value(31, "trackingParamAdd")
  val trackingParamDelete: Type = Value(32, "trackingParamDelete")
  val campaignUseCachedImagesUpdate: Type = Value(33, "campaignUseCachedImagesUpdate")
  val campaignThumbyModeUpdate: Type = Value(34, "campaignThumbyModeUpdate")
  val campaignScheduleChange: Type = Value(35, "campaignScheduleChange")
  val campaignEnableAuxiliaryClickEventUpdate: Type = Value(36, "campaignEnableAuxiliaryClickEventUpdate")
  val campaignContentCategoriesChange: Type = Value(37, "campaignContentCategoriesChange")
  val campaignContentRatingChange: Type = Value(38, "campaignContentRatingChange")
  val campaignIoIdUpdate: Type = Value(39, "campaignIoIdUpdate")
  val campaignEnableComscorePixelUpdate: Type = Value(40, "enableComscorePixel")
  val campRecoRequirementsChange: Type = Value(41, "campRecoRequirementsChange")
  val campaignGenderRestrictions: Type = Value(42, "campaignGenderRestrictions")
  val campaignAgeGroupRestrictions: Type = Value(43, "campaignAgeGroupRestrictions")
  val countryRestrictions: Type = Value(44, "countryRestrictions")
  val deviceRestrictions: Type = Value(45, "deviceRestrictions")
  val campaignMobileOsRestrictions: Type = Value(46, "campaignMobileOsRestrictions")

  val algoOverrideDeleted: Type = Value(47, "algoOverrideDeleted")

  /** Campaign Article range 100-199 */
  val campaignArticleSettingsUpdate: Type = Value(100, "campaignArticleSettingsUpdate")

  /** Article range 200-299 */
  val articleReviewStatusChanged: Type = Value(200, "articleReviewStatusChanged")
  // These events should note which 'parent' ArticleKey caused these to be rejected.
  val articleReviewRejectedForTitle: Type = Value(201, "articleReviewRejectedForTitle")
  val articleReviewRejectedForImage: Type = Value(202, "articleReviewRejectedForImage")
  val articleReviewRejectedForUrl: Type = Value(203, "articleReviewRejectedForUrl")
  val articleReviewCheckedIn: Type = Value(204, "articleReviewCheckedIn")
  val articleReviewCheckedOut: Type = Value(205, "articleReviewCheckedOut")
  val campaignArticleStatusChanged: Type = Value(206, "campaignArticleStatusChanged")

  /** DLUG range 300-349 */
  val dlugUnitCreated: Type = Value(300, "dlugUnitCreated")
  val dlugUnitApproved: Type = Value(301, "dlugUnitApproved")
  val dlugUnitRejected: Type = Value(302, "dlugUnitRejected")
  val dlugUnitRemoved: Type = Value(303, "dlugUnitRemoved")
  val dlugImageRecropped: Type = Value(304, "dlugImageRecropped")
  val dlugPlanNameChanged: Type = Value(305, "dlugPlanNameChanged")
  val dlugProductIdChanged: Type = Value(306, "dlugProductIdChanged")
  val dlugPublishDateChanged: Type = Value(307, "dlugPublishDateChanged")
  val dlugTtlChanged: Type = Value(308, "dlugTtlChanged")
  // currently not using the "dlugImageChanged" event as it is treated as "dlugImageRecropped"
//  val dlugImageChanged = Value(309, "dlugImageChanged")
  val dlugRibbonChanged: Type = Value(310, "dlugRibbonChanged")
  val dlugCategoryNameChanged: Type = Value(311, "dlugCategoryNameChanged")
  val dlugCategoryUrlChanged: Type = Value(312, "dlugCategoryUrlChanged")
  val dlugSourceNameChanged: Type = Value(313, "dlugSourceNameChanged")
  val dlugSourceUrlChanged: Type = Value(314, "dlugSourceUrlChanged")
  val dlugIsVideoChanged: Type = Value(315, "dlugIsVideoChanged")
  val dlugTitleChanged: Type = Value(316, "dlugTitleChanged")
  val dlugArticleUrlChanged: Type = Value(317, "dlugArticleUrlChanged")
  val dlugSummaryChanged: Type = Value(318, "dlugSummaryChanged")
  val dlugSecondaryTitleChanged: Type = Value(319, "dlugSecondaryTitleChanged")
  val dlugMoreLinksHeaderChanged: Type = Value(320, "dlugMoreLinksHeaderChanged")
  val dlugMoreLinksAdded: Type = Value(321, "dlugMoreLinksAdded")
  val dlugMoreLinksRemoved: Type = Value(322, "dlugMoreLinksRemoved")
  val dlugChannelImageRecropped: Type = Value(323, "dlugChannelImageRecropped")
  val dlugChannelAdded: Type = Value(324, "dlugChannelAdded")
  val dlugChannelRemoved: Type = Value(325, "dlugChannelRemoved")
  val dlugContentGroupAdded: Type = Value(326, "dlugContentGroupAdded")
  val dlugContentGroupRemoved: Type = Value(327, "dlugContentGroupRemoved")
  val dlugAltTitleChanged: Type = Value(328, "dlugAltTitleChanged")
  val dlugAltImageChanged: Type = Value(329, "dlugAltImageChanged")
  val dlugAltImageSourceChanged: Type = Value(330, "dlugAltImageSourceChanged")

  /** revenue model updates 350-360  */
  val revenueModelSetNew: Type = Value(350, "revenueModelSetNew")
  val revenueModelChangeDate: Type = Value(351, "revenueModelChangeDate")
  val revenueModelDelete: Type = Value(352, "revenueModelDelete")

  /** Pools 400-499 */
  val addSponseeToPool: Type = Value(400, "addSponseeToPool")
  val removeSponseeFromPool: Type = Value(401, "removeSponseeFromPool")
  val addSponsorToPool: Type = Value(402, "addSponsorToPool")
  val removeSponsorFromPool: Type = Value(403, "removeSponsorFromPool")

  def defaultValue: Type = unknown

  /** Plugins 500-599 */
  val pluginNameChanged: Type = Value(500, "pluginNameChanged")
  val pluginStatusChanged: Type = Value(501, "pluginStatusChanged")

  val pluginBucketAdded: Type = Value(525, "pluginBucketAdded")
  val pluginBucketModified: Type = Value(526, "pluginBucketModified")
  val pluginBucketRemoved: Type = Value(527, "pluginBucketRemoved")
  val pluginBucketStatusChanged: Type = Value(528, "pluginBucketStatusChanged")

  val camelCaseRegex: Regex = "(?<=[a-z])(?=[A-Z])".r
  val camelCaseToSpaces: (String) => String = (s: String) => camelCaseRegex.split(s).mkString(" ")
  val sentenceCase: (String) => String = (s: String) => s.toList match {
    case head::tail => head.toUpper + tail.mkString.toLowerCase
    case Nil => ""
  }
  val camelCaseToSentenceCase: (String) => String = sentenceCase compose camelCaseToSpaces

  val jsonSerializer: EnumNameSerializer[AuditEvents.type] = new EnumNameSerializer(this)

  implicit val jsonFormat: Format[Type] = makeJsonFormat[Type]
  implicit val defaultValueWriter: DefaultValueWriter[Type] = makeDefaultValueWriter[Type]

  /** Dashboard Entities 600-699 */
  val userCreated: Type = Value(600, "userCreated")
  val userFirstNameChanged: Type = Value(601, "userFirstNameChanged")
  val userLastNameChanged: Type = Value(602, "userLastNameChanged")
  val userEmailChanged: Type = Value(603, "userEmailChanged")
  val userDisabledChanged: Type = Value(604, "userDisabledChanged")
  val userIsHiddenChanged: Type = Value(605, "userIsHiddenChanged")
  val userAccountLockedChanged: Type = Value(606, "userAccountLockedChanged")
  val userAssignedSiteAdded: Type = Value(607, "userAssignedSiteAdded")
  val userAssignedSiteRemoved: Type = Value(608, "userAssignedSiteRemoved")
  val userPluginPermissionAdded: Type = Value(609, "userPluginPermissionAdded")
  val userPluginPermissionRemoved: Type = Value(610, "userPluginPermissionRemoved")
  val userRoleAdded: Type = Value(611, "userRoleAdded")
  val userRoleRemoved: Type = Value(612, "userRoleRemoved")

  /** Sites 700-749 */
  val siteCreated: Type = Value(700, "siteCreated")
  val siteNameChanged: Type = Value(701, "siteNameChanged")
  val urlChanged: Type = Value(702, "urlChanged")
  val feedUrlChanged: Type = Value(703, "feedUrlChanged")
  val redirectOrganicsChanged: Type = Value(704, "redirectOrganicsChanged")
  val recommendationsRequireImagesChanged: Type = Value(705, "recommendationsRequireImagesChanged")
  val requiresOutboundTrackingParamsChanged: Type = Value(706, "requiresOutboundTrackingParamsChanged")
  val redirectOrganicsUsingHttpStatusChanged: Type = Value(707, "redirectOrganicsUsingHttpStatusChanged")
  val ignoreDomainCheckChanged: Type = Value(708, "ignoreDomainCheckChanged")
  val isEnabledChanged: Type = Value(709, "isEnabledChanged")

  /** Exchanges 750-849 */
  val exchangeCreated: Type = Value(750, "exchangeCreated")
  val exchangeNameChanged: Type = Value(751, "exchangeNameChanged")
  val exchangeStatusChanged: Type = Value(752, "exchangeStatusChanged")
  val exchangeGoalChanged: Type = Value(753, "exchangeGoalChanged")
  val exchangeThrottleChanged: Type = Value(754, "exchangeThrottleChanged")
  val exchangeSitesChanged: Type = Value(755, "exchangeSitesChanged")
  val exchangeContentSourcesChanged: Type = Value(756, "exchangeContentSourcesChanged")
  val exchangeDisabledSitesChanged: Type = Value(757, "exchangeDisabledSitesChanged")
  val exchangeScheduleStartTimeChanged: Type = Value(758, "exchangeScheduleStartTimeChanged")
  val exchangeScheduleEndTimeChanged: Type = Value(759, "exchangeScheduleEndTimeChanged")
  val exchangeOmnitureTrackingParamsAllSites: Type = Value(760, "exchangeOmnitureTrackingParamsAllSites")
  val exchangeTrackingParamsAllSites: Type = Value(761, "exchangeTrackingParamsAllSites")

}


