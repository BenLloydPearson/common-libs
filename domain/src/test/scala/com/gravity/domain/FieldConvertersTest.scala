package com.gravity.domain

import com.gravity.api.livetraffic.ApiRequestEvent.ApiRequestEventConverter
import com.gravity.api.livetraffic.ClickEventWrapper.ClickEventWrapperConverter
import com.gravity.api.livetraffic.ImpressionViewedEventWrapper
import com.gravity.api.livetraffic.ImpressionViewedEventWrapper.ImpressionViewedEventWrapperConverter
import com.gravity.domain.FieldConverters._
import com.gravity.interests.jobs.intelligence.hbase.{ScopedKey, ScopedKeyTypes}
import com.gravity.interests.jobs.intelligence.operations._
import com.gravity.interests.jobs.intelligence.operations.metric.{LiveMetricRequest, LiveMetricResponse, LiveMetricUpdate}
import com.gravity.interests.jobs.intelligence.operations.recommendations.model.RecoGenEvent
import com.gravity.interests.jobs.intelligence.operations.user.{UserCacheRemoveRequest, UserCacheRemoveResponse}
import com.gravity.interests.jobs.intelligence.{ArticleKey, MetricsEventTest}
import com.gravity.utilities.eventlogging.VersioningTestHelpers
import com.gravity.utilities.{BaseScalaTest, grvfields}
import com.gravity.valueclasses.ValueClassesForDomain.UserGuid

import scalaz.syntax.std.option._

/*
*     __         __
*  /"  "\     /"  "\
* (  (\  )___(  /)  )
*  \               /
*  /               \
* /    () ___ ()    \  erik 4/24/14
* |      (   )      |
*  \      \_/      /
*    \...__!__.../
*/

object FieldConvertersTest {

    val convertersToTest : Seq[(grvfields.FieldConverter[_], Option[_])] = Seq(
      (AdvertiserCampaignArticleReportRowConverter, None),
      (AdvertiserCampaignReportRowConverter, None),
      (AlgoSettingsDataConverter, None),
      (ArticleRecoDataConverter, None),
      (ImpressionEventConverter, Some(MetricsEventTest.testEvent2)),
      (ClickEventConverter, Some(MetricsEventTest.testClickEvent)),
      (OrdinalArticleKeyPairConverter, None),
      (ImpressionViewedEventConverter, None),
      (ApiRequestEventConverter, None),
      (ImpressionViewedEventWrapperConverter, Some(ImpressionViewedEventWrapper.empty)),
      (ClickEventWrapperConverter, None),
      (ScopedKeyConverter, Some(ScopedKey(ArticleKey.max, ScopedKeyTypes.ZERO))),
      (SyncUserEventConverter, Option(SyncUserEvent.testSyncUserEvent)),
      (BeaconEventConverter, Some(MetricsEventTest.testBeaconEvent)),
      (RecoDataMartRowConverter, None),
      (AuxiliaryClickEventConverter, None),
      (ProbabilityDistributionConverter, Some(ProbabilityDistribution(List[ValueProbability](ValueProbability(1, 10),ValueProbability(2, 20), ValueProbability(3, 100))))),
      (UserCacheRemoveRequestConverter, Some(UserCacheRemoveRequest(UserGuid("df6ad847323ac16fd707f3aeb77481e5")))),
      (UserCacheRemoveResponseConverter, Some(UserCacheRemoveResponse(wasCached = true))),
      (RecoGenEventConverter, Some(RecoGenEvent.testRecoGenEvent)),
      (LiveMetricRequestConverter, Some(LiveMetricRequest.testObject)),
      (LiveMetricResponseConverter, Some(LiveMetricResponse.testObject)),
      (LiveMetricUpdateConverter, Some(LiveMetricUpdate.testObject)),
      (ArtChRgInfoConverter, Some(ArtChRgInfo(
        (Integer.MAX_VALUE.asInstanceOf[Long] + 10).some,
        List(
          ArtRgEntity("ent1", Integer.MAX_VALUE.asInstanceOf[Long] + 100, 2.0, "ent1-disambig".some, Map("Title" -> 2, "Body" -> 3), true, List(CRRgNodeType("school", 84), CRRgNodeType("location", 30))),
          ArtRgEntity("ent2", Integer.MAX_VALUE.asInstanceOf[Long] + 101, 3.0, "ent2-disambig".some, Map("Title" -> 3, "Body" -> 4), false, List(CRRgNodeType("state", 32), CRRgNodeType("moviePerson", 14)))
        ),
        List(
          ArtRgSubject("sub1", Integer.MAX_VALUE.asInstanceOf[Long] + 200, 4.0, "sub1-disambig".some, true),
          ArtRgSubject("sub2", Integer.MAX_VALUE.asInstanceOf[Long] + 201, 5.0, "sub2-disambig".some, false)
        )
      ))),
      (ImpressionSlugFieldConverter, None)
    )
}

class FieldConvertersTest extends BaseScalaTest {
  test("Domain Converters Test") {
    FieldConvertersTest.convertersToTest.foreach{case(converter, testObjectOption) => VersioningTestHelpers.testConverter(converter, testObjectOption)}
  }
}

object FieldConvertersTestStorer extends App {
//  val converter = ImpressionSlugFieldConverter
//  VersioningTestHelpers.delete(converter.fields.getCategoryName, converter.fields.getVersion)
//  VersioningTestHelpers.store(converter, None)
}
