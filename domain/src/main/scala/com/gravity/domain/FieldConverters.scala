package com.gravity.domain

import com.gravity.domain.fieldconverters._
import com.gravity.utilities.grvfields._

import scala.collection.Set

/*
*     __         __
*  /"  "\     /"  "\
* (  (\  )___(  /)  )
*  \               /
*  /               \
* /    () ___ ()    \  erik 4/2/14
* |      (   )      |
*  \      \_/      /
*    \...__!__.../
*/

object FieldConverters extends Serializable
with ImpressionEventConverter
with ImpressionFailedEventConverter
with ClickEventConverter
with ArticleRecoDataConverter
with AlgoSettingsDataConverter
with BeaconEventConverter
with OrdinalArticleKeyPairConverter
with AdvertiserCampaignArticleReportRowConverter
with AdvertiserCampaignReportRowConverter
with DiscardImpressionEventConverter
with DiscardClickEventConverter
with ImpressionViewedEventConverter
with DiscardImpressionViewedEventConverter
with AuxiliaryClickEventConverter
with ScopedKeyConverter
with SampleNestedEventConverter
with SampleEventConverter
with SyncUserEventConverter
with RecoDataMartRowConverter
with ArticleDataMartRowConverter
with UnitImpressionDataMartRowConverter
with CampaignAttributesDataMartRowConverter
with DiscardBeaconEventConverter
with EventDetailRowConverter
with EventDetailAlgoSettingsDimConverter
with RssArticleIngestionConverter
with RssArticleConverter
with ArtChRgInfoConverter
with ArtRgEntityConverter
with ArtRgSubjectConverter
with AuthorConverter
with AuthorDataConverter
with ProbabilityDistributionConverter
with UserCacheRemoveRequestConverter
with UserCacheRemoveResponseConverter
with UserClickstreamConverter
with UserInteractionClickStreamRequestConverter
with UserClickstreamRequestConverter
with AllScopesMapConverter
with ArtGrvMapMetaValConverter
with ClickstreamEntryConverter
with OneScopeKeyConverter
with OneScopeMapConverter
with SectionPathConverter
with WidgetDomReadyEventConverter
with RecoGenEventConverter
with ArticleAggDataConverter
with LiveMetricConverter
with LiveMetricRequestConverter
with LiveMetricResponseConverter
with ScopedMetricsFromToKeyConverter
with LiveMetricsConverter
with LiveMetricUpdateConverter
with LiveMetricUpdateBundleConverter
with ImpressionSlugFieldConverter
with AolBeaconConverter
{
  // thanks, type erasure
  private val allConverters : Set[FieldConverter[_]] = Set(
    ImpressionEventConverter,
    ImpressionFailedEventConverter,
    ClickEventConverter,
    ArticleRecoDataConverter,
    AlgoSettingsDataConverter,
    BeaconEventConverter,
    OrdinalArticleKeyPairConverter,
    AdvertiserCampaignArticleReportRowConverter,
    AdvertiserCampaignReportRowConverter,
    DiscardImpressionEventConverter,
    DiscardClickEventConverter,
    ImpressionViewedEventConverter,
    DiscardImpressionViewedEventConverter,
    AuxiliaryClickEventConverter,
    ScopedKeyConverter,
    SampleNestedEventConverter,
    SampleEventConverter,
    SyncUserEventConverter,
    RecoDataMartRowConverter,
    ArticleDataMartRowConverter,
    UnitImpressionDataMartRowConverter,
    CampaignAttributesDataMartRowConverter,
    DiscardBeaconEventConverter,
    EventDetailRowConverter,
    EventDetailAlgoSettingsDimConverter,
    RssArticleIngestionConverter,
    RssArticleConverter,
    ArtChRgInfoConverter,
    ArtRgEntityConverter,
    ArtRgSubjectConverter,
    AuthorConverter,
    AuthorDataConverter,
    ProbabilityDistributionConverter,
    RecoGenEventConverter,
    ArticleAggDataConverter,
    LiveMetricConverter,
    LiveMetricRequestConverter,
    LiveMetricResponseConverter,
    ScopedMetricsFromToKeyConverter,
    LiveMetricsConverter,
    LiveMetricUpdateConverter,
    LiveMetricUpdateBundleConverter,
    ImpressionSlugFieldConverter,
    AolBeaconConverterImpl
  )
  private val convertersByName = Map(allConverters.groupBy(_.fields.getCategoryName).toSeq.map(itm => (itm._1, itm._2.head)) :_*)
  def converterByName(name:String) : FieldConverter[_] = convertersByName.getOrElse(name, throw new RuntimeException("Attempted to get converter " + name + " and failed.  Maybe this should be added to the allConverters list?"))
}
