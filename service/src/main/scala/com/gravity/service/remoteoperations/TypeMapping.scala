package com.gravity.service.remoteoperations

import com.gravity.service.grvroles

import scala.collection._

/*
*     __         __
*  /"  "\     /"  "\
* (  (\  )___(  /)  )
*  \               /
*  /               \
* /    () ___ ()    \  erik 6/26/14
* |      (   )      |
*  \      \_/      /
*    \...__!__.../
*/

trait TypeMapper {
  val types: Seq[TypeInfo]
  private var typeMapping : Option[Map[String, TypeInfo]] = None
  //This could, as of right now, just be a map of names to roles, but I want to leave open the possibility of more  //type metadata

  private def getTypeMapping : Map[String, TypeInfo] = {
    if(typeMapping.isEmpty) //this could happen twice but that doesn't really matter
      typeMapping = Some(types.map{typeInfo => typeInfo.name -> typeInfo }.toMap)
    typeMapping.get
  }

  def getInfoFor[T](implicit m:Manifest[T]) : Option[TypeInfo] = {
    val name = m.runtimeClass.getCanonicalName
    getTypeMapping.get(name)
  }

  def getInfoFor(typeKey: String) : Option[TypeInfo] = {
    getTypeMapping.get(typeKey)
  }
}

object ProductionMapper extends TypeMapper {
  val metricRole = "INTEREST_INTELLIGENCE_OPERATIONS"

  val types: scala.Seq[TypeInfo] = Seq(
    TypeInfo("com.gravity.interests.jobs.intelligence.operations.graphing.LiveGraphingRequest", Seq("ONTOLOGY_MANAGEMENT")),
    TypeInfo("com.gravity.interests.jobs.intelligence.operations.graphing.GraphArticleRemoteMessage", Seq("ONTOLOGY_MANAGEMENT")),
    TypeInfo("com.gravity.interests.jobs.intelligence.operations.graphing.RegraphArticleRemoteMessage", Seq("ONTOLOGY_MANAGEMENT")),
    TypeInfo("com.gravity.interests.jobs.intelligence.operations.graphing.GraphHighlighterLinksRemoteMessage", Seq("ONTOLOGY_MANAGEMENT")),
    TypeInfo("com.gravity.interests.jobs.intelligence.operations.graphing.GraphFacebookLikesRemoteMessage", Seq("ONTOLOGY_MANAGEMENT")),
    TypeInfo("com.gravity.interests.jobs.intelligence.operations.graphing.NodeIdToUriNameResolverMessage", Seq("ONTOLOGY_MANAGEMENT")),
    TypeInfo("com.gravity.interests.jobs.intelligence.operations.CampaignBudgetExceededNotification",  Seq(grvroles.CAMPAIGN_MANAGEMENT)),
    TypeInfo("com.gravity.interests.jobs.intelligence.operations.CampaignStatusChangedNotification",  Seq(grvroles.CAMPAIGN_MANAGEMENT)),
    TypeInfo("com.gravity.interests.jobs.intelligence.operations.ValidateCampaignBudgetAndSchedule",  Seq(grvroles.CAMPAIGN_MANAGEMENT)),
    TypeInfo("com.gravity.interests.jobs.intelligence.operations.ValidateCampaigns",  Seq(grvroles.CAMPAIGN_MANAGEMENT)),
    TypeInfo("com.gravity.interests.jobs.intelligence.operations.recommendations.RecommendationRequest", Seq(grvroles.REMOTE_RECOS)),
    TypeInfo("com.gravity.interests.jobs.intelligence.operations.recommendations.model.RecommendationContextForRemoting", Seq(grvroles.RECO_STORAGE)),
    TypeInfo("com.gravity.interests.jobs.intelligence.operations.recommendations.model.SlotContext", Seq(grvroles.RECO_STORAGE)),
    TypeInfo("com.gravity.interests.jobs.intelligence.operations.ImpressionViewedEvent", Seq("INTEREST_INTELLIGENCE_OPERATIONS", "METRICS_SCOPED", "METRICS_SCOPED_INFERENCE")),
    TypeInfo("com.gravity.interests.jobs.intelligence.operations.ImpressionEvent", Seq("INTEREST_INTELLIGENCE_OPERATIONS", "METRICS_SCOPED", "METRICS_SCOPED_INFERENCE")),
    TypeInfo("com.gravity.interests.jobs.intelligence.operations.ClickEvent", Seq("INTEREST_INTELLIGENCE_OPERATIONS", "METRICS_SCOPED", "METRICS_SCOPED_INFERENCE")),
    TypeInfo("com.gravity.interests.jobs.intelligence.operations.recommendations.model.ImpressionFailedEvent", Seq("METRICS_SCOPED")),
    TypeInfo("com.gravity.interests.jobs.intelligence.operations.RssIngestHeartbeat", Seq("DATAFEEDS")),
    TypeInfo("com.gravity.interests.jobs.intelligence.operations.RssIngestMessage", Seq("DATAFEEDS")),
    TypeInfo("com.gravity.interests.jobs.intelligence.operations.CacheImageMessage", Seq("DATAFEEDS")),
    TypeInfo("com.gravity.interests.jobs.intelligence.operations.CacheImagesMessage", Seq("DATAFEEDS")),
    TypeInfo("com.gravity.domain.BeaconEvent", Seq(metricRole)),
    TypeInfo("com.gravity.interests.jobs.intelligence.operations.recommendations.RequestRecoGeneration", Seq(grvroles.RECOGENERATION)),
    TypeInfo("com.gravity.events.ProcessSegmentPath", Seq("ARCHIVER")),
    TypeInfo("com.gravity.domain.SyncUserEvent", Seq(grvroles.USER_SYNC)),
    TypeInfo("com.gravity.interests.jobs.intelligence.operations.ArticleDataLiteRequest", Seq("REMOTE_RECOS")),
    TypeInfo("com.gravity.interests.jobs.intelligence.operations.SiteMetaRequest", Seq("REMOTE_RECOS")),
    TypeInfo("com.gravity.interests.jobs.intelligence.operations.CampaignMetaRequest", Seq("REMOTE_RECOS")),
    TypeInfo("com.gravity.interests.jobs.intelligence.operations.SitePlacementMetaRequest", Seq("REMOTE_RECOS")),
    TypeInfo("com.gravity.interests.jobs.intelligence.operations.urlvalidation.UrlValidationRequestMessage", Seq(grvroles.URL_VALIDATION)),
    TypeInfo("com.gravity.interests.jobs.intelligence.operations.urlvalidation.ImageUrlValidationRequestMessage", Seq(grvroles.URL_VALIDATION)),
    TypeInfo("com.gravity.interests.jobs.intelligence.operations.user.UserClickstreamRequest", Seq(grvroles.USERS_ROLE)),
    TypeInfo("com.gravity.interests.jobs.intelligence.operations.user.UserInteractionClickStreamRequest", Seq(grvroles.USERS_ROLE)),
    TypeInfo("com.gravity.interests.jobs.intelligence.operations.user.UserCacheRemoveRequest", Seq(grvroles.USERS_ROLE)),
    TypeInfo("com.gravity.interests.jobs.intelligence.operations.metric.LiveMetricRequest", Seq(grvroles.METRICS_LIVE)),
    TypeInfo("com.gravity.interests.jobs.intelligence.operations.metric.LiveMetricResponse", Seq(grvroles.METRICS_LIVE)),
    TypeInfo("com.gravity.interests.jobs.intelligence.operations.metric.LiveMetricUpdate", Seq(grvroles.METRICS_LIVE))
  )

}

object TestMapper extends TypeMapper {
  val testRole: scala.Seq[String] =  Seq("TEST_ROLE")

  val types: scala.Seq[TypeInfo] = Seq(
    TypeInfo("com.gravity.service.remoteoperations.BasicMessage", testRole),
    TypeInfo("com.gravity.service.remoteoperations.FailureMessage", testRole),
    TypeInfo("com.gravity.service.remoteoperations.HashHintedMessage", testRole),
    TypeInfo("com.gravity.service.remoteoperations.HintedMessage", testRole),
    TypeInfo("com.gravity.service.remoteoperations.TestSplittableMessage", testRole),
    TypeInfo("com.gravity.interests.jobs.intelligence.operations.graphing.LiveGraphingRequest", Seq("LIVE_GRAPHING_TEST")),
    TypeInfo("com.gravity.interests.jobs.intelligence.operations.recommendations.RecommendationRequest", testRole),
    TypeInfo("com.gravity.interests.jobs.intelligence.operations.graphing.NodeIdToUriNameResolverMessage", Seq("NODE_ID_RESOLVER_TEST")),
    TypeInfo("com.gravity.insights.rtb.saturn.AdRenderLog", Seq("RTB_REMOTE_TEST")),
    TypeInfo("com.gravity.insights.rtb.saturn.SaturnBidRequest", Seq("RTB_API")),
    TypeInfo("com.gravity.interests.jobs.intelligence.operations.graphing.GraphFacebookLikesRemoteMessage", Seq("ONTOLOGY_MANAGEMENT")),
    TypeInfo("com.gravity.service.remoteoperations.TimeoutMessage", testRole),
    TypeInfo("com.gravity.interests.jobs.intelligence.operations.recommendations.RecoEcho", testRole),
    TypeInfo("com.gravity.service.remoteoperations.FieldSerializedMessage", testRole),
    TypeInfo("com.gravity.domain.BeaconEvent", testRole),
    TypeInfo("com.gravity.interests.jobs.intelligence.operations.recommendations.RequestRecoGeneration", testRole),
    TypeInfo("com.gravity.interests.jobs.intelligence.operations.recommendations.model.RecommendationContextForRemoting", testRole),
    TypeInfo("com.gravity.interests.jobs.intelligence.operations.recommendations.model.SlotContext", testRole),
    TypeInfo("com.gravity.events.ProcessSegmentPath", testRole),
    TypeInfo("com.gravity.interests.jobs.intelligence.operations.ArticleDataLiteRequest", Seq("REMOTE_RECOS")),
    TypeInfo("com.gravity.interests.jobs.intelligence.operations.SiteMetaRequest", Seq("REMOTE_RECOS")),
    TypeInfo("com.gravity.interests.jobs.intelligence.operations.CampaignMetaRequest", Seq("REMOTE_RECOS")),
    TypeInfo("com.gravity.interests.jobs.intelligence.operations.SitePlacementMetaRequest", Seq("REMOTE_RECOS")),
    TypeInfo("com.gravity.interests.jobs.intelligence.operations.urlvalidation.UrlValidationRequestMessage", Seq(grvroles.URL_VALIDATION)),
    TypeInfo("com.gravity.interests.jobs.intelligence.operations.urlvalidation.ImageUrlValidationRequestMessage", Seq(grvroles.URL_VALIDATION)),
    TypeInfo("com.gravity.interests.jobs.intelligence.operations.user.UserClickstreamRequest", Seq(grvroles.USERS_ROLE)),
    TypeInfo("com.gravity.interests.jobs.intelligence.operations.user.UserInteractionClickStreamRequest", Seq(grvroles.USERS_ROLE)),
    TypeInfo("com.gravity.interests.jobs.intelligence.operations.user.UserCacheRemoveRequest", Seq(grvroles.USERS_ROLE)),
    TypeInfo("com.gravity.interests.jobs.intelligence.operations.ImpressionEvent", Seq(grvroles.METRICS_LIVE)),
    TypeInfo("com.gravity.interests.jobs.intelligence.operations.ClickEvent", Seq(grvroles.METRICS_LIVE)),
    TypeInfo("com.gravity.interests.jobs.intelligence.operations.metric.LiveMetricRequest", Seq(grvroles.METRICS_LIVE)),
    TypeInfo("com.gravity.interests.jobs.intelligence.operations.metric.LiveMetricResponse", Seq(grvroles.METRICS_LIVE)),
    TypeInfo("com.gravity.interests.jobs.intelligence.operations.metric.LiveMetricUpdate", Seq(grvroles.METRICS_LIVE))
  )
}

case class TypeInfo(name: String, roles: Seq[String])