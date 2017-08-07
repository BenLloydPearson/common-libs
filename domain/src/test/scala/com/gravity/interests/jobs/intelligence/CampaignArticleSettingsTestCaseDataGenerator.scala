package com.gravity.interests.jobs.intelligence

import com.gravity.interests.jobs.intelligence.SchemaTypes._
import com.gravity.test.SerializationTesting
import com.gravity.utilities.grvjson
import org.apache.commons.codec.binary.Base64

/*
              ___...---''
  ___...---'\'___
''       _.-''  _`'.______\\.
        /_.)  )..-  __..--'\\
       (    __..--''
        '-''\@


 Ⓐ Ⓐ Ⓐ Ⓐ Ⓐ Ⓐ Ⓐ Ⓐ Ⓐ Ⓐ Ⓐ
*/
object CampaignArticleSettingsTestCaseDataGenerator extends SerializationTesting {
  def generateAllPossibleVersions(baseOnCas: CampaignArticleSettings): Seq[VersionedTestCase] = {
    generateVersionedCases(baseOnCas, 1 to CampaignArticleSettingsConverter.maxReadableVersion)
  }

  def generateVersionedCases(baseOnCas: CampaignArticleSettings, versions: Seq[Int]): Seq[VersionedTestCase] = {
    for (ver <- versions) yield {
      // since the amount of data serialized is variable based on version, we need to first serialize each
      // version and then read it out from the version serialized bytes to have the correctly populated instance.
      // e.g. Even if the passed `baseOnCas` contains nonEmpty trackingParams, when it's serialized to versions < 7
      // it will be stripped of this field's data and the resulting instance will have an empty trackingParams
      val convertedBytes = CampaignArticleSettingsConverter.serializeToBytes(baseOnCas, ver)
      val expectedInstance = CampaignArticleSettingsConverter.fromBytes(convertedBytes)
      VersionedTestCase(
        ver,
        expectedInstance,
        grvjson.jsonStr(expectedInstance),
        Base64.encodeBase64String(convertedBytes),
        Base64.encodeBase64String(javaSerObjToBytes(expectedInstance))
      )
    }
  }

  def printOutAllVersions(baseOnCas: CampaignArticleSettings): Unit = {
    generateAllPossibleVersions(baseOnCas) foreach println
  }
}

object CampaignArticleSettingsTestCaseDataGeneratorApp extends App {
    val cas = CampaignArticleSettings(
      status = CampaignArticleStatus.inactive,
      isBlacklisted = false,
      clickUrl = Some("http://example.com/click/url/example"),
      title = Some("Example Title Override"),
      image = Some("http://example.com/image/override"),
      displayDomain = Some("Example.com"),
      isBlocked = Some(true),
      blockedReasons = Some(Set(BlockedReason.IMAGE_NOT_CACHED)),
      articleImpressionPixel = Some("<img src='http://example.com/impression/pixel.gif' />"),
      articleClickPixel = Some("<img src='http://example.com/click/pixel.gif' />"),
      trackingParams = Map("utm_source" -> "test", "utm_tag" -> "foo")
    )

//  val cas = CampaignArticleSettings(CampaignArticleStatus.inactive, isBlacklisted = false)

  CampaignArticleSettingsTestCaseDataGenerator.printOutAllVersions(cas)
}

case class VersionedTestCase(version: Int, expectedInstance: CampaignArticleSettings, jsonStringified: String, byteConverterBase64Bytes: String, javaBase64Bytes: String) {
  def isRoundTripByteConverterTestingSupported: Boolean = version <= CampaignArticleSettingsConverter.writingVersion
}

/**
  * Remember to adjust [[CampaignArticleSettingsConverter.writingVersion]] prior to running this as needed.
  */
object LegacyCampaignArticleSettingsTestCaseDataGenerator extends App with SerializationTesting with CampaignArticleSettingsTestResources {
  //  val cas = CampaignArticleSettings(
  //    CampaignArticleStatus.inactive,
  //    isBlacklisted = false,
  //    clickUrl = Some("http://example.com/click/url/example"),
  //    title = Some("Example Title Override"),
  //    image = Some("http://example.com/image/override"),
  //    displayDomain = Some("Example.com"),
  //    isBlocked = None,
  //    blockedReasons = Some(Set(BlockedReason.IMAGE_NOT_CACHED)),
  //    articleImpressionPixel = Some("<img src='http://example.com/impression/pixel.gif' />"),
  //    articleClickPixel = Some("<img src='http://example.com/click/pixel.gif' />"),
  //    trackingParams = Map("utm_source" -> "test", "utm_tag" -> "foo")
  //  )

  val cas = CampaignArticleSettings(CampaignArticleStatus.inactive, isBlacklisted = false)

  println("Lift JSON: " + grvjson.serialize(cas))
  println("Play JSON: " + grvjson.jsonStr(cas))
  println("ByteConverter bytes: " + Base64.encodeBase64String(CampaignArticleSettingsConverter.toBytes(cas)))
  println("Java bytes: " + Base64.encodeBase64String(javaSerObjToBytes(cas)))
  println()
}