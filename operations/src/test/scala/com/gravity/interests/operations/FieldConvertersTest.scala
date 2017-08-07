package com.gravity.interests.operations

import com.gravity.algorithms.model.AlgoSettingNamespace
import com.gravity.domain.articles.{ContentGroupSourceTypes, ContentGroupStatus}
import com.gravity.domain.recommendations.ContentGroup
import com.gravity.interests.jobs.intelligence._
import com.gravity.interests.jobs.intelligence.hbase.{ScopedKey, ScopedKeyTypes}
import com.gravity.interests.jobs.intelligence.operations.{AlgoSettingsData, ProbabilityDistribution, ValueProbability}
import com.gravity.interests.jobs.intelligence.operations.FieldConverters.{AlgoSettingNamespaceConverter, ArticleRecommendationFieldConverter, CampaignArticleSettingsFieldConverter, ContentGroupFieldConverter}
import com.gravity.interests.jobs.intelligence.operations.recommendations.CampaignRecommendationData
import com.gravity.interests.jobs.intelligence.schemas.DollarValue
import com.gravity.utilities.eventlogging.VersioningTestHelpers
import com.gravity.utilities.{BaseScalaTest, grvfields}
import com.gravity.valueclasses.ValueClassesForDomain.ExchangeGuid

import scalaz.Scalaz._

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
  val convertersToTest: Seq[(grvfields.FieldConverter[_], Option[_])] = Seq(
    // If you have more than one round-trip test, put the one that tests the new fields supported by the new version first, because that's the one that'll be saved by the storer.
    (
      CampaignArticleSettingsFieldConverter,
      CampaignArticleSettings(CampaignArticleStatus.inactive, isBlacklisted = false).some
    )
    ,(
      CampaignArticleSettingsFieldConverter,
      CampaignArticleSettings(CampaignArticleStatus.active, isBlacklisted = true, Some("http://example.com/clickUrl"),
        Some("title"), Some("http://example.com/image"), Some("displayDomain"), isBlocked = Some(true),
        Some(Set(BlockedReason.EMPTY_AUTHOR, BlockedReason.EMPTY_TAGS)), Some("<img src='http://example.com/imp-pixel' />"),
        Some("<img src='http://example.com/click-pixel' />"),
        Map("foo" -> "bar", "baz" -> "bat")
      ).some
    )
    , (
      ArticleRecommendationFieldConverter,
      ArticleRecommendation(ArticleKey(5L), 0.1, "because!",
        Option(CampaignRecommendationData(CampaignKey(SiteKey("siteguid1"),23456L), DollarValue(0l), CampaignType.sponsored, Some(CampaignArticleSettings(CampaignArticleStatus.active, isBlacklisted = false)))),
        List( AlgoSettingsData("setting1", 3, "setting1Value"), AlgoSettingsData("setting2", 1, "true"), AlgoSettingsData("setting3", 2, "1.5")),
        Option(ContentGroup(123456789L, "CgName2", ContentGroupSourceTypes.campaign  , CampaignKey(SiteKey("siteguid1"), 23456L).toScopedKey, "siteguid1", ContentGroupStatus.inactive, isGmsManaged = false)),
        Option(ExchangeGuid("exchangeGuid1"))
      ).some
    )
    , (
      ArticleRecommendationFieldConverter,
      ArticleRecommendation(ArticleKey(5L), 0.1, "because!",
        Option(CampaignRecommendationData(CampaignKey(SiteKey("siteguid1"),23456L), DollarValue(0l), CampaignType.sponsored, Some(CampaignArticleSettings(CampaignArticleStatus.active, isBlacklisted = false)))),
        List( AlgoSettingsData("setting1", 3, "setting1Value"), AlgoSettingsData("setting2", 1, "true"), AlgoSettingsData("setting3", 2, "1.5")),
        Option(ContentGroup(123456789L, "CgName2", ContentGroupSourceTypes.campaign  , CampaignKey(SiteKey("siteguid1"), 23456L).toScopedKey, "siteguid1", ContentGroupStatus.inactive, isGmsManaged = false))
      ).some
    )

    ,(
      ContentGroupFieldConverter,
      ContentGroup(123450L, "CgName", ContentGroupSourceTypes.advertiser, CampaignKey(SiteKey("siteguid"), 23456L).toScopedKey, "siteguid", ContentGroupStatus.active, false, true, "client1", "channel1", "feed1").some
    )
    ,(
      ContentGroupFieldConverter,
      ContentGroup(123450L, "CgName", ContentGroupSourceTypes.advertiser, CampaignKey(SiteKey("siteguid"), 23456L).toScopedKey, "siteguid", ContentGroupStatus.active, true).some
    )
    ,(
      ContentGroupFieldConverter,
      ContentGroup(12345L, "CgName", ContentGroupSourceTypes.advertiser, CampaignKey(SiteKey("siteguid"), 23456L).toScopedKey, "siteguid", ContentGroupStatus.active).some
    )

    ,(AlgoSettingNamespaceConverter, Some(
      AlgoSettingNamespace(
        Map[ScopedKey, Double](
          ScopedKey(ArticleKey.max, ScopedKeyTypes.ZERO) -> 69.2,
          ScopedKey(CampaignKey.empty, ScopedKeyTypes.CAMPAIGN) -> 98.6
        ),
        Map[ScopedKey, Boolean](
          ScopedKey(ArticleKey.min, ScopedKeyTypes.ARTICLE) -> true,
          ScopedKey(CampaignKey.maxValue, ScopedKeyTypes.CAMPAIGN) -> false
        ),
        Map[ScopedKey, String](
          ScopedKey(CampaignKey.minValue, ScopedKeyTypes.CAMPAIGN) -> "snurgle",
          ScopedKey(ArticleKey.emptyUrl, ScopedKeyTypes.ARTICLE) -> "bargle"
        ),
        Map[ScopedKey, ProbabilityDistribution](
          ScopedKey(SiteKey.maxValue, ScopedKeyTypes.SITE) -> ProbabilityDistribution(List[ValueProbability](ValueProbability(1, 10),ValueProbability(2, 20), ValueProbability(3, 100))),
          ScopedKey(SiteKey.minValue, ScopedKeyTypes.SITE) -> ProbabilityDistribution(List[ValueProbability](ValueProbability(3, 15),ValueProbability(7, 30), ValueProbability(3, 100)))
        )
      )
    ))
  )
}

//object StoreDataForFieldConvertersTest extends App {
//  FieldConvertersTest.convertersToTest.foreach{ case (converter, testObjectOption) =>
//    VersioningTestHelpers.store(converter, testObjectOption)}
//}

class FieldConvertersTest extends BaseScalaTest {
  test("operations converters") {
    FieldConvertersTest.convertersToTest.foreach {
      case (converter, testObjectOption) =>
        VersioningTestHelpers.testConverter(converter, testObjectOption)
    }
  }
}