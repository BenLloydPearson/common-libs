package com.gravity.interests.jobs.intelligence.operations.audit

import com.gravity.domain.DashboardUser
import com.gravity.domain.aol.{AolGmsArticle, AolDynamicLeadArticle}
import com.gravity.domain.gms.GmsArticleStatus
import com.gravity.interests.jobs.intelligence._
import com.gravity.interests.jobs.intelligence.operations.audit.ChangeDetectors._
import com.gravity.utilities.grvjson._
import com.gravity.valueclasses.ValueClassesForDomain.SiteGuid
import org.joda.time.DateTime

import scala.collection._

/*
 *    __   _         __         
 *   / /  (_)__  ___/ /__  ___ _
 *  / _ \/ / _ \/ _  / _ \/ _  /
 * /_//_/_/_//_/\_,_/\___/\_, / 
 *                       /___/
 * May 23, 2014
 */


/*
  Type-class for things that can have audit log events
 */
trait CanBeAuditLogged[T] {
  val events: Map[AuditEvents.Type, ChangeDetector[T]]
}

/*
  Type-class for things that can have audit log events which must include the forSiteGuid
 */
trait CanBeAuditLoggedWithForSiteGuid[T] {
  val events: Map[AuditEvents.Type, ChangeDetector[T]]
}

/*
  Evidences for the things that can have audit log events
 */
object OperationsAuditLoggingEvents {

  /* Campaign Events */
  implicit object CampaignAuditLoggingEvents extends CanBeAuditLogged[CampaignRow] {

    override val events = Map[AuditEvents.Type, ChangeDetector[CampaignRow]](
      // Don't watch budgetSettingsUpdate, it's audited in CampaignService.setBudget() so manual updates are tracked.
      AuditEvents.campaignCreation -> CreationDetector((_: CampaignRow).status)
      , AuditEvents.campaignNameUpdate -> SimpleChangeDetector((_: CampaignRow).nameOrNotSet, "name")
      , AuditEvents.campaignStatusChange -> SimpleChangeDetector((_: CampaignRow).status, "status")
      , AuditEvents.startDateUpdate -> SimpleChangeDetector((_: CampaignRow).startDateMillis, "startDate")
      , AuditEvents.endDateUpdate -> SimpleChangeDetector((_: CampaignRow).endDateMillis, "endDate")
      , AuditEvents.bidUpdate -> SimpleChangeDetector((_: CampaignRow).bidOrMaxBid, "bidOrMaxBid")
      , AuditEvents.bidFloor -> SimpleChangeDetector((_: CampaignRow).maxBidFloor, "maxBidFloor")
      , AuditEvents.trackingParamUpdate -> MapChangeDetector[CampaignRow, String]((_: CampaignRow).trackingParams.toMap, fieldName = "trackingParams")
      , AuditEvents.trackingParamAdd -> MapAdditionDetector[CampaignRow, String]((_: CampaignRow).trackingParams.toMap, fieldName = "trackingParams")
      , AuditEvents.trackingParamDelete -> MapDeletionDetector[CampaignRow, String]((_: CampaignRow).trackingParams.toMap, fieldName = "trackingParams")
      , AuditEvents.displayDomainUpdate -> SimpleChangeDetector((_: CampaignRow).displayDomain, "displayDomain")

      //      ,AuditEvents.countryRestrictionsAdd ->  AdditionDetector((_: CampaignRow).countryRestrictions, "countryRestrictions")
      //      ,AuditEvents.countryRestrictionsDelete ->  RemovalDetector((_: CampaignRow).countryRestrictions, "countryRestrictions")
      //      ,AuditEvents.deviceRestrictionsAdd ->  AdditionDetector((_: CampaignRow).deviceRestrictions, "deviceRestrictions")
      //      ,AuditEvents.deviceRestrictionsDelete ->  RemovalDetector((_: CampaignRow).deviceRestrictions, "deviceRestrictions")
      , AuditEvents.rssFeedListAdd -> AdditionDetector((_: CampaignRow).feedUrls.toSeq, "rssFeedList")
      , AuditEvents.rssFeedListDelete -> RemovalDetector((_: CampaignRow).feedUrls.toSeq, "rssFeedList")
      , AuditEvents.rssFeedSettingsUpdate -> MapChangeDetector((_: CampaignRow).feeds.toMap, fieldName = "rssFeedSettings")
      , AuditEvents.rssFeedStatusUpdate -> MapChangeDetector((_: CampaignRow).feeds.mapValues(_.feedStatus).toMap, fieldName = "rssFeedSettings")
      , AuditEvents.campaignTypeChange -> SimpleChangeDetector((_: CampaignRow).campaignType, "campaignType")
      , AuditEvents.campaignUseCachedImagesUpdate -> SimpleChangeDetector((_: CampaignRow).useCachedImages, "useCachedImages")
      , AuditEvents.campaignThumbyModeUpdate -> SimpleChangeDetector((_: CampaignRow).thumbyMode, "thumbyMode")
      , AuditEvents.recentArticlesMaxAgeUpdate -> SimpleChangeDetector((_: CampaignRow).recentArticlesMaxAge, "recentArticlesMaxAge")
      , AuditEvents.campIngestRulesUpdate -> SimpleChangeDetector((_: CampaignRow).campIngestRules, "campIngestRules")
      , AuditEvents.campaignScheduleChange -> SimpleChangeDetector((_: CampaignRow).schedule.map(s => CampaignScheduleAuditEvent.fromWeekSchedule(s)), "schedule")
      , AuditEvents.campaignEnableAuxiliaryClickEventUpdate -> SimpleChangeDetector((_: CampaignRow).enableAuxiliaryClickEvent, "enableAuxiliaryClickEvent")
      , AuditEvents.campaignIoIdUpdate -> SimpleChangeDetector((_: CampaignRow).ioId, "ioId")
      , AuditEvents.campaignEnableComscorePixelUpdate -> SimpleChangeDetector((_: CampaignRow).enableComscorePixel, "enableComscorePixel")
      , AuditEvents.campaignContentCategoriesChange -> SimpleChangeDetector((_: CampaignRow).contentCategoriesOpt, "campaignContentCategories")
      , AuditEvents.campaignContentRatingChange -> SimpleChangeDetector((_: CampaignRow).contentRatingOpt, "campaignContentRating")
      , AuditEvents.campRecoRequirementsChange -> SimpleChangeDetector((_: CampaignRow).campRecoRequirementsOpt, "campRecoRequirementsChange")
      , AuditEvents.campaignGenderRestrictions -> SimpleChangeDetector((_: CampaignRow).genderRestrictions, "genderRestrictions")
      , AuditEvents.campaignMobileOsRestrictions -> SimpleChangeDetector((_: CampaignRow).mobileOsRestrictions, "mobileOsRestrictions")
      , AuditEvents.campaignAgeGroupRestrictions -> SimpleChangeDetector((_: CampaignRow).ageGroupRestrictions, "ageGroupRestrictions")
      , AuditEvents.countryRestrictions -> SimpleChangeDetector((_: CampaignRow).countryRestrictions, "countryRestrictions")
      , AuditEvents.deviceRestrictions -> SimpleChangeDetector((_: CampaignRow).deviceRestrictions, "deviceRestrictions")
    )

  }

  implicit object DashboardUserCanBeAuditLogged extends CanBeAuditLogged[DashboardUser] {

    val events: Map[AuditEvents.Type, ChangeDetector[DashboardUser]] = Map[AuditEvents.Type, ChangeDetector[DashboardUser]](
      AuditEvents.userFirstNameChanged -> SimpleChangeDetector((_: DashboardUser).firstName, AuditEvents.userFirstNameChanged.name),
      AuditEvents.userLastNameChanged -> SimpleChangeDetector((_: DashboardUser).lastName, AuditEvents.userLastNameChanged.name),
      AuditEvents.userEmailChanged -> SimpleChangeDetector((_: DashboardUser).email, AuditEvents.userEmailChanged.name),
      AuditEvents.userDisabledChanged -> SimpleChangeDetector((_: DashboardUser).disabled, AuditEvents.userDisabledChanged.name),
      AuditEvents.userIsHiddenChanged -> SimpleChangeDetector((_: DashboardUser).isHidden, AuditEvents.userIsHiddenChanged.name),
      AuditEvents.userAccountLockedChanged -> SimpleChangeDetector((_: DashboardUser).accountLocked, AuditEvents.userAccountLockedChanged.name),
      AuditEvents.userRoleAdded -> AdditionDetector((_: DashboardUser).roles.getOrElse(List.empty[String]), AuditEvents.userRoleAdded.name),
      AuditEvents.userRoleRemoved -> RemovalDetector((_: DashboardUser).roles.getOrElse(List.empty[String]), AuditEvents.userRoleRemoved.name),
      AuditEvents.userAssignedSiteAdded -> AdditionDetector((_: DashboardUser).assignedSites.getOrElse(List.empty[SiteGuid]), AuditEvents.userAssignedSiteAdded.name),
      AuditEvents.userAssignedSiteRemoved -> RemovalDetector((_: DashboardUser).assignedSites.getOrElse(List.empty[SiteGuid]), AuditEvents.userAssignedSiteRemoved.name),
      AuditEvents.userPluginPermissionAdded -> AdditionDetector((_: DashboardUser).pluginIds.getOrElse(List.empty[Int]), AuditEvents.userPluginPermissionAdded.name),
      AuditEvents.userPluginPermissionRemoved -> RemovalDetector((_: DashboardUser).pluginIds.getOrElse(List.empty[Int]), AuditEvents.userPluginPermissionRemoved.name)
    )

  }

  /* Exchange Events */
  implicit object ExchangeAuditLoggingEvents extends CanBeAuditLogged[ExchangeRow] {
    import com.gravity.interests.jobs.intelligence.ExchangeGoalConverters._
    import com.gravity.interests.jobs.intelligence.ExchangeThrottleConverters._

    override val events = Map[AuditEvents.Type, ChangeDetector[ExchangeRow]](
      // Don't watch disabledSites, it's audited in ExchangeService.setDisabledSitesWithAuditLog() so manual updates are tracked.
      AuditEvents.exchangeCreated -> CreationDetector((_: ExchangeRow).exchangeName)
      , AuditEvents.exchangeNameChanged -> SimpleChangeDetector((_: ExchangeRow).exchangeName, "exchangeName")
      , AuditEvents.exchangeStatusChanged -> SimpleChangeDetector((_: ExchangeRow).exchangeStatus, "exchangeStatus")
      , AuditEvents.exchangeGoalChanged -> SimpleChangeDetector((_: ExchangeRow).goal, "goal")
      , AuditEvents.exchangeThrottleChanged -> SimpleChangeDetector((_: ExchangeRow).throttle, "throttle")
      , AuditEvents.exchangeSitesChanged -> SimpleChangeDetector((_: ExchangeRow).exchangeSites.toList.sortBy(_.siteId), "exchangeSites")
      , AuditEvents.exchangeContentSourcesChanged -> SimpleChangeDetector((_: ExchangeRow).contentSources.toList.sortBy(_.contentGroupId), "ContentSources")
      , AuditEvents.exchangeScheduleStartTimeChanged -> SimpleChangeDetector((_: ExchangeRow).scheduledStartTime, "scheduledStart")
      , AuditEvents.exchangeScheduleEndTimeChanged -> SimpleChangeDetector((_: ExchangeRow).scheduledEndTime, "scheduledEnd")
      , AuditEvents.exchangeOmnitureTrackingParamsAllSites -> SimpleChangeDetector((_: ExchangeRow).enableOmnitureTrackingParamsForAllSites, "exchangeOmnitureTrackingParamsAllSites")
      , AuditEvents.exchangeTrackingParamsAllSites -> SimpleChangeDetector((_: ExchangeRow).trackingParamsForAllSites.toList.sortBy(_._1), "exchangeTrackingParamsAllSites")
    )
  }

  implicit object ExchangeStatusAndDisabledSitesAuditLoggingEvents extends CanBeAuditLogged[(Set[SiteKey], ExchangeStatus.Type)] {
    override val events = Map[AuditEvents.Type, ChangeDetector[(Set[SiteKey], ExchangeStatus.Type)]](
      AuditEvents.exchangeDisabledSitesChanged -> SimpleChangeDetector({ case ((disabledSites: Set[SiteKey], _)) => disabledSites.toList.sortBy(_.siteId)}, "disabledSites")
      , AuditEvents.exchangeStatusChanged -> SimpleChangeDetector({ case ((_, exchangeStatus: ExchangeStatus.Type)) => exchangeStatus}, "exchangeStatus")
    )
  }
}

object AolUniArticleAuditLoggingEvents {

  implicit object AolDynamicLeadArticleCanBeAuditLogged extends CanBeAuditLoggedWithForSiteGuid[AolDynamicLeadArticle] {
    val events: Map[AuditEvents.Type, ChangeDetector[AolDynamicLeadArticle]] = Map[AuditEvents.Type, ChangeDetector[AolDynamicLeadArticle]](
      AuditEvents.dlugUnitCreated -> CreationDetector((_: AolDynamicLeadArticle).dlArticleStatus)
      ,AuditEvents.dlugUnitRemoved -> CustomChangeDetector((_: AolDynamicLeadArticle).articleStatus, {
        case (from: AolDynamicLeadArticle, to: AolDynamicLeadArticle) =>
          from.articleStatus != to.articleStatus && to.articleStatus == GmsArticleStatus.Deleted
      }, DlArticleAuditEventFields.dlStatus.name)
      ,AuditEvents.dlugUnitApproved -> CustomChangeDetector((_: AolDynamicLeadArticle).dlArticleStatus, {
        case (from: AolDynamicLeadArticle, to: AolDynamicLeadArticle) =>
          from.articleStatus != to.articleStatus && (to.articleStatus == GmsArticleStatus.Approved || to.articleStatus == GmsArticleStatus.Live)
      }, DlArticleAuditEventFields.dlStatus.name)
      ,AuditEvents.dlugUnitRejected -> CustomChangeDetector((_: AolDynamicLeadArticle).dlArticleStatus, {
        case (from: AolDynamicLeadArticle, to: AolDynamicLeadArticle) =>
          from.articleStatus != to.articleStatus && to.articleStatus == GmsArticleStatus.Rejected
      }, DlArticleAuditEventFields.dlStatus.name)
      ,AuditEvents.dlugImageRecropped -> SimpleChangeDetector((_: AolDynamicLeadArticle).aolImage, DlArticleAuditEventFields.dlImage.name)
      ,AuditEvents.dlugPlanNameChanged -> SimpleChangeDetector((_: AolDynamicLeadArticle).aolCampaign.fold("")(_.planName), DlArticleAuditEventFields.dlPlanName.name)
      ,AuditEvents.dlugProductIdChanged -> SimpleChangeDetector((_: AolDynamicLeadArticle).aolCampaign.fold(0)(_.productId), DlArticleAuditEventFields.dlProductId.name)
      ,AuditEvents.dlugPublishDateChanged -> OptionalFieldChangeDetector[AolDynamicLeadArticle, DateTime, Option[DateTime]](_.startDate, _.startDate, fieldName = DlArticleAuditEventFields.dlPublishDate.name)
      ,AuditEvents.dlugTtlChanged -> OptionalFieldChangeDetector[AolDynamicLeadArticle, DateTime, Option[DateTime]](_.endDate, _.endDate, fieldName = DlArticleAuditEventFields.dlTtl.name)
      ,AuditEvents.dlugCategoryNameChanged -> SimpleChangeDetector((_: AolDynamicLeadArticle).aolCategory, DlArticleAuditEventFields.dlCategoryName.name)
      ,AuditEvents.dlugCategoryUrlChanged -> SimpleChangeDetector((_: AolDynamicLeadArticle).aolCategoryLink, DlArticleAuditEventFields.dlCategoryUrl.name)
      ,AuditEvents.dlugSourceNameChanged -> SimpleChangeDetector((_: AolDynamicLeadArticle).aolSource, DlArticleAuditEventFields.dlSourceName.name)
      ,AuditEvents.dlugSourceUrlChanged -> SimpleChangeDetector((_: AolDynamicLeadArticle).aolSourceLink, DlArticleAuditEventFields.dlSourceUrl.name)
      ,AuditEvents.dlugIsVideoChanged -> SimpleChangeDetector((_: AolDynamicLeadArticle).showVideoIcon, DlArticleAuditEventFields.dlIsVideo.name)
      ,AuditEvents.dlugTitleChanged -> SimpleChangeDetector((_: AolDynamicLeadArticle).title, DlArticleAuditEventFields.dlTitle.name)
      ,AuditEvents.dlugArticleUrlChanged -> SimpleChangeDetector((_: AolDynamicLeadArticle).url, DlArticleAuditEventFields.dlArticleUrl.name)
      ,AuditEvents.dlugSummaryChanged -> SimpleChangeDetector((_: AolDynamicLeadArticle).aolSummary, DlArticleAuditEventFields.dlSummary.name)
      ,AuditEvents.dlugSecondaryTitleChanged -> SimpleChangeDetector((_: AolDynamicLeadArticle).aolHeadline, DlArticleAuditEventFields.dlSecondaryTitle.name)
      ,AuditEvents.dlugMoreLinksHeaderChanged -> SimpleChangeDetector((_: AolDynamicLeadArticle).aolSecondaryHeader, DlArticleAuditEventFields.dlMoreLinksHeader.name)
      ,AuditEvents.dlugMoreLinksAdded -> AdditionDetector((_: AolDynamicLeadArticle).aolSecondaryLinks.toSeq, DlArticleAuditEventFields.dlMoreLinksHeader.name)
      ,AuditEvents.dlugMoreLinksRemoved -> RemovalDetector((_: AolDynamicLeadArticle).aolSecondaryLinks.toSeq, DlArticleAuditEventFields.dlMoreLinksHeader.name)
      ,AuditEvents.dlugChannelImageRecropped -> SimpleChangeDetector((_: AolDynamicLeadArticle).channelImage, DlArticleAuditEventFields.dlChannelImage.name)
      ,AuditEvents.dlugChannelAdded -> AdditionDetector((_: AolDynamicLeadArticle).channels.toSeq, DlArticleAuditEventFields.dlChannels.name)
      ,AuditEvents.dlugChannelRemoved -> RemovalDetector((_: AolDynamicLeadArticle).channels.toSeq, DlArticleAuditEventFields.dlChannels.name)
    )
  }

  implicit object AolGmsArticleCanBeAuditLogged extends CanBeAuditLoggedWithForSiteGuid[AolGmsArticle] {
    // GMS-FIELD-UPDATE location
    val events: Map[AuditEvents.Type, ChangeDetector[AolGmsArticle]] = Map[AuditEvents.Type, ChangeDetector[AolGmsArticle]](
      AuditEvents.dlugUnitCreated -> CreationDetector((_: AolGmsArticle).dlArticleStatus)
      ,AuditEvents.dlugUnitRemoved -> CustomChangeDetector((_: AolGmsArticle).articleStatus, {
        case (from: AolGmsArticle, to: AolGmsArticle) =>
          from.articleStatus != to.articleStatus && to.articleStatus == GmsArticleStatus.Deleted
      }, DlArticleAuditEventFields.dlStatus.name)
      ,AuditEvents.dlugUnitApproved -> CustomChangeDetector((_: AolGmsArticle).dlArticleStatus, {
        case (from: AolGmsArticle, to: AolGmsArticle) =>
          from.articleStatus != to.articleStatus && (to.articleStatus == GmsArticleStatus.Approved || to.articleStatus == GmsArticleStatus.Live)
      }, DlArticleAuditEventFields.dlStatus.name)
      ,AuditEvents.dlugUnitRejected -> CustomChangeDetector((_: AolGmsArticle).dlArticleStatus, {
        case (from: AolGmsArticle, to: AolGmsArticle) =>
          from.articleStatus != to.articleStatus && to.articleStatus == GmsArticleStatus.Rejected
      }, DlArticleAuditEventFields.dlStatus.name)
      ,AuditEvents.dlugImageRecropped -> SimpleChangeDetector((_: AolGmsArticle).aolImage, DlArticleAuditEventFields.dlImage.name)
//      ,AuditEvents.dlugPlanNameChanged -> SimpleChangeDetector((_: AolGmsArticle).aolCampaign.fold("")(_.planName), DlArticleAuditEventFields.dlPlanName.name)
//      ,AuditEvents.dlugProductIdChanged -> SimpleChangeDetector((_: AolGmsArticle).aolCampaign.fold(0)(_.productId), DlArticleAuditEventFields.dlProductId.name)
      ,AuditEvents.dlugPublishDateChanged -> OptionalFieldChangeDetector[AolGmsArticle, DateTime, Option[DateTime]](_.startDate, _.startDate, fieldName = DlArticleAuditEventFields.dlPublishDate.name)
      ,AuditEvents.dlugTtlChanged -> OptionalFieldChangeDetector[AolGmsArticle, DateTime, Option[DateTime]](_.endDate, _.endDate, fieldName = DlArticleAuditEventFields.dlTtl.name)
      ,AuditEvents.dlugCategoryNameChanged -> SimpleChangeDetector((_: AolGmsArticle).aolCategory, DlArticleAuditEventFields.dlCategoryName.name)
      ,AuditEvents.dlugCategoryUrlChanged -> SimpleChangeDetector((_: AolGmsArticle).aolCategoryLink, DlArticleAuditEventFields.dlCategoryUrl.name)
      ,AuditEvents.dlugSourceNameChanged -> SimpleChangeDetector((_: AolGmsArticle).aolSource, DlArticleAuditEventFields.dlSourceName.name)
      ,AuditEvents.dlugSourceUrlChanged -> SimpleChangeDetector((_: AolGmsArticle).aolSourceLink, DlArticleAuditEventFields.dlSourceUrl.name)
      ,AuditEvents.dlugIsVideoChanged -> SimpleChangeDetector((_: AolGmsArticle).showVideoIcon, DlArticleAuditEventFields.dlIsVideo.name)
      ,AuditEvents.dlugTitleChanged -> SimpleChangeDetector((_: AolGmsArticle).title, DlArticleAuditEventFields.dlTitle.name)
      ,AuditEvents.dlugArticleUrlChanged -> SimpleChangeDetector((_: AolGmsArticle).url, DlArticleAuditEventFields.dlArticleUrl.name)
      ,AuditEvents.dlugSummaryChanged -> SimpleChangeDetector((_: AolGmsArticle).aolSummary, DlArticleAuditEventFields.dlSummary.name)
      ,AuditEvents.dlugSecondaryTitleChanged -> SimpleChangeDetector((_: AolGmsArticle).aolHeadline, DlArticleAuditEventFields.dlSecondaryTitle.name)
      ,AuditEvents.dlugMoreLinksHeaderChanged -> SimpleChangeDetector((_: AolGmsArticle).aolSecondaryHeader, DlArticleAuditEventFields.dlMoreLinksHeader.name)
      ,AuditEvents.dlugMoreLinksAdded -> AdditionDetector((_: AolGmsArticle).aolSecondaryLinks.toSeq, DlArticleAuditEventFields.dlMoreLinksHeader.name)
      ,AuditEvents.dlugMoreLinksRemoved -> RemovalDetector((_: AolGmsArticle).aolSecondaryLinks.toSeq, DlArticleAuditEventFields.dlMoreLinksHeader.name)
      ,AuditEvents.dlugChannelImageRecropped -> SimpleChangeDetector((_: AolGmsArticle).channelImage, DlArticleAuditEventFields.dlChannelImage.name)
      ,AuditEvents.dlugContentGroupAdded -> AdditionDetector((_: AolGmsArticle).contentGroupIds.toSeq, DlArticleAuditEventFields.contentGroupIds.name)
      ,AuditEvents.dlugContentGroupRemoved -> RemovalDetector((_: AolGmsArticle).contentGroupIds.toSeq, DlArticleAuditEventFields.contentGroupIds.name)
      ,AuditEvents.dlugAltTitleChanged -> OptionalFieldChangeDetector[AolGmsArticle, String, Option[String]](_.altTitleOption, _.altTitleOption, fieldName = DlArticleAuditEventFields.altTitle.name)
      ,AuditEvents.dlugAltImageChanged -> OptionalFieldChangeDetector[AolGmsArticle, String, Option[String]](_.altImageOption, _.altImageOption, fieldName = DlArticleAuditEventFields.altImage.name)
      ,AuditEvents.dlugAltImageSourceChanged -> OptionalFieldChangeDetector[AolGmsArticle, String, Option[String]](_.altImageSourceOption, _.altImageSourceOption, fieldName = DlArticleAuditEventFields.altImageSource.name)
    )
  }

}