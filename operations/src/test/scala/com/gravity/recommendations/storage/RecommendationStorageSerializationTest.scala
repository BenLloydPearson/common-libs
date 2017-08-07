package com.gravity.recommendations.storage

import com.google.common.primitives.UnsignedBytes
import com.gravity.domain.recommendations.ContentGroup
import com.gravity.interests.jobs.intelligence.operations.recommendations.model.{AlgoStateKey, CandidateSetQualifier, RecommendedScopeKey}
import com.gravity.interests.jobs.intelligence.operations.{AlgoSettingType, AlgoSettingsData}
import com.gravity.interests.jobs.intelligence.{ArticleKey, SiteKey}
import com.gravity.recommendations.storage.RecommendationsStorageConverters.RecommendedScopeKeyConverter
import com.gravity.utilities.BaseScalaTest
import com.gravity.valueclasses.ValueClassesForDomain.ExchangeGuid
import org.junit.Assert

/*             )\._.,--....,'``.
.b--.        /;   _.. \   _\  (`._ ,.
`=,-,-'~~~   `----(,_..'--(,_..'`-.;.'  */
class RecommendationStorageSerializationTest extends BaseScalaTest { //} with operationsTesting {

  test("testCandidateScopeKey") {
    val sitePlacement = Some(6)
    val bucket = Some(3)
    val slotIndex = Some(7)
    val deviceType: Option[Int] = None
    val geoLocationId: Option[Int] = None
    val algoid: Option[Int] = Some(300)

    val key = CandidateSetQualifier(
      sitePlacement,
      bucket,
      slotIndex,
      deviceType,
      geoLocationId
    )

    val keyBytes = RecommendationsStorageConverters.CandidateSetQualifierConverter.toBytes(key)
    val newKey = RecommendationsStorageConverters.CandidateSetQualifierConverter.fromBytes(keyBytes)

    Assert.assertEquals(key, newKey)
  }

  test("testSortingOfKey") {
    val algoId = Some(136)
    val scores = Seq(3.6, -3.7, 3.8, -3.9, 10.6, -10.8, 12.5, -13434.3434, 13434.343434, -134345.0, 394.394343434, -1.0e6, Double.MaxValue, Double.MinValue, Double.MinPositiveValue)
    val articleKey = ArticleKey(200)

    val values = (for ((score, idx) <- scores.zipWithIndex) yield {
      val key = RecommendedArticleKey(
        score,
        articleKey
      )
      val keyBytes = RecommendationsStorageConverters.RecommendedArticleKeyConverter.toBytes(key)
      val newKey = RecommendationsStorageConverters.RecommendedArticleKeyConverter.fromBytes(keyBytes)
      val newKey2 = RecommendedArticleKey(
        score,
        articleKey
      )

      println("Testing " + key + " against " + newKey)
      Assert.assertTrue(Set(key, newKey, newKey2).size == 1) //Verifies that serialization is compatible and serializes the results in the appropriate order

      (newKey2 -> keyBytes)
    })

    values.foreach { case (key: RecommendedArticleKey, bytes: Array[Byte]) =>
      println("Bytes : " + bytes.mkString(",") + " : Key: " + key)
    }

    println("SORTED")
    val ordering = Ordering.comparatorToOrdering(UnsignedBytes.lexicographicalComparator())
    values.sortBy(_._2)(ordering).foreach { case (key: RecommendedArticleKey, bytes: Array[Byte]) =>
      println("Score : " + key.score + " : Key: " + key)
    }
  }

  test("testSerializationOfKey") {


    val algoId = Some(136)

    val score = 3.6
    val articleKey = ArticleKey(200)

    val key = RecommendedArticleKey(
      score,
      articleKey
    )

    val keyBytes = RecommendationsStorageConverters.RecommendedArticleKeyConverter.toBytes(key)
    val newKey = RecommendationsStorageConverters.RecommendedArticleKeyConverter.fromBytes(keyBytes)

    val newKey2 = RecommendedArticleKey(
      score,
      articleKey
    )

    Assert.assertTrue(Set(key, newKey, newKey2).size == 1) //Verifies that serialization is compatible and serializes the results in the appropriate order

  }

  test("testSerializationOfData1") {
    val settingName = "kittens"
    val settingType = 2
    val settingData = "kittensTwo"
    val settingsData = AlgoSettingsData(settingName, settingType, settingData)
    val why = "because we like you!"

    val value = RecommendedArticleData(why, Seq(settingsData), None, 3, None, None, None)

    val valueBytes = RecommendationsStorageConverters.RecommendedArticleDataConverter.toBytes(value)
    val newValue = RecommendationsStorageConverters.RecommendedArticleDataConverter.fromBytes(valueBytes)

    Assert.assertEquals(value, newValue)

  }

  test("testSerializationOfData2") {
    val settingName = "kittens"
    val settingType = 2
    val settingData = "kittensTwo"
    val settingsData = AlgoSettingsData(settingName, settingType, settingData)
    val why = "because we like you!"

    val value = RecommendedArticleData(why, Seq(settingsData), None, 3, Option("feefoofum"), Option(ContentGroup.default), None)

    val valueBytes = RecommendationsStorageConverters.RecommendedArticleDataConverter.toBytes(value)
    val newValue = RecommendationsStorageConverters.RecommendedArticleDataConverter.fromBytes(valueBytes)

    Assert.assertEquals(value, newValue)

  }

    test("testSerializationOfData3 with Transient AlgoSettingsData") {
      val settingName = "kittens"
      val settingType = AlgoSettingType.TransientSetting
      val settingData = "kittensTwo"
      val settingsData = AlgoSettingsData(settingName, settingType, settingData)
      val why = "because we like you!"

      val value = RecommendedArticleData(why, Seq(settingsData), None, 3, Option("feefoofum"), Option(ContentGroup.default), None)

      val valueBytes = RecommendationsStorageConverters.RecommendedArticleDataConverter.toBytes(value)
      val newValue = RecommendationsStorageConverters.RecommendedArticleDataConverter.fromBytes(valueBytes)

      Assert.assertEquals(value, newValue)

    }

  test("testSerializationOfData4 with ExchangeGuid") {
    val settingName = "kittens"
    val settingType = AlgoSettingType.TransientSetting
    val settingData = "kittensTwo"
    val settingsData = AlgoSettingsData(settingName, settingType, settingData)
    val why = "because we like you!"

    val value = RecommendedArticleData(why, Seq(settingsData), None, 3, Option("feefoofum"), Option(ContentGroup.default), Option(ExchangeGuid.generateGuid))

    val valueBytes = RecommendationsStorageConverters.RecommendedArticleDataConverter.toBytes(value)
    val newValue = RecommendationsStorageConverters.RecommendedArticleDataConverter.fromBytes(valueBytes)

    Assert.assertEquals(value, newValue)

  }

  test("test RecommendedScopeKey") {
    val sitePlacement = Some(6)
    val bucket = Some(3)
    val slotIndex = Some(7)
    val deviceType : Option[Int] = None
    val geoLocationId : Option[Int] = None
    val algoid : Option[Int] = Some(300)

    val csq = CandidateSetQualifier(
      sitePlacement,
      bucket,
      slotIndex,
      deviceType,
      geoLocationId
    )

    val ask = AlgoStateKey(Some(1), Some(1l), Some(2l))
    val rsk = RecommendedScopeKey(SiteKey("test site guid").toScopedKey, csq, ask)

    val bytes = RecommendedScopeKeyConverter.toBytes(rsk)
    val newRsk = RecommendedScopeKeyConverter.fromBytes(bytes)
    Assert.assertEquals(rsk, newRsk)
  }
}
