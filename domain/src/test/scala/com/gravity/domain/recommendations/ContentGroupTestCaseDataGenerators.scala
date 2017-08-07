package com.gravity.interests.jobs.intelligence

import com.gravity.domain.articles.{ContentGroupStatus, ContentGroupSourceTypes}
import com.gravity.domain.recommendations.ContentGroup
import com.gravity.domain.recommendations.ContentGroup.{ContentGroupSeqByteConverter, ContentGroupJsonSerializer, ContentGroupByteConverter}
import com.gravity.interests.jobs.intelligence.SchemaTypes._
import com.gravity.test.SerializationTesting
import com.gravity.utilities.{ArticleReviewStatus, BaseScalaTest, grvjson}
import com.gravity.utilities.grvz._
import net.liftweb.json.Formats
import org.apache.commons.codec.binary.Base64
import org.junit.Assert._
import play.api.libs.json.{JsError, JsSuccess, Json}

// import scalaz._, Scalaz._
import scalaz.syntax.validation._

/**
  * Remember to adjust [[com.gravity.domain.recommendations.ContentGroup.ContentGroupByteConverter.writingVersion]] prior to running this as needed.
  */
object ContentGroupTestCaseDataGenerator1 extends App with SerializationTesting with ContentGroupTestResources {
  val values = Seq(
    ContentGroup(12345L    , "CgName1", ContentGroupSourceTypes.advertiser, SiteKey("siteguid1").toScopedKey                     , "siteguid1", ContentGroupStatus.active),
    ContentGroup(123456789L, "CgName2", ContentGroupSourceTypes.campaign  , CampaignKey(SiteKey("siteguid2"), 23456L).toScopedKey, "siteguid2", ContentGroupStatus.inactive),
    ContentGroup(123456789L, "CgName3", ContentGroupSourceTypes.campaign, CampaignKey(SiteKey("siteguid2"), 23456L).toScopedKey, "siteguid2", ContentGroupStatus.inactive, true),
    ContentGroup(123456789L, "CgName4", ContentGroupSourceTypes.campaign, CampaignKey(SiteKey("siteguid2"), 23456L).toScopedKey, "siteguid2", ContentGroupStatus.inactive, false),
    ContentGroup(123456789L, "CgName5", ContentGroupSourceTypes.campaign, CampaignKey(SiteKey("siteguid2"), 23456L).toScopedKey, "siteguid2", ContentGroupStatus.inactive, false, false, "client1", "chan1", "feed1"),
    ContentGroup(123456789L, "CgName6", ContentGroupSourceTypes.campaign, CampaignKey(SiteKey("siteguid2"), 23456L).toScopedKey, "siteguid2", ContentGroupStatus.inactive, false, true, "client2", "chan2", "feed2")
  )

  for (value <- values) {
    println(value.name + ":")
    //  println("Lift JSON: " + grvjson.serialize(value))
    println("Play JSON: " + grvjson.jsonStr(value))
    println("ByteConverter bytes: " + Base64.encodeBase64String(ContentGroupByteConverter.toBytes(value)))
    println("Java bytes: " + Base64.encodeBase64String(javaSerObjToBytes(value)))
    println()
  }
}

object ContentGroupTestCaseDataGenerator2 extends App with SerializationTesting with ContentGroupTestResources {
  val values = Seq(
    List(
      ContentGroup(12345L    , "CgName1", ContentGroupSourceTypes.advertiser, SiteKey("siteguid1").toScopedKey                     , "siteguid1", ContentGroupStatus.active),
      ContentGroup(123456789L, "CgName2", ContentGroupSourceTypes.campaign  , CampaignKey(SiteKey("siteguid2"), 23456L).toScopedKey, "siteguid2", ContentGroupStatus.inactive)
    ),
   List(
     ContentGroup(123456789L, "CgName3", ContentGroupSourceTypes.campaign, CampaignKey(SiteKey("siteguid2"), 23456L).toScopedKey, "siteguid2", ContentGroupStatus.inactive, true),
     ContentGroup(123456789L, "CgName4", ContentGroupSourceTypes.campaign, CampaignKey(SiteKey("siteguid2"), 23456L).toScopedKey, "siteguid2", ContentGroupStatus.inactive, false)
   ),
    List(
      ContentGroup(123456789L, "CgName5", ContentGroupSourceTypes.campaign, CampaignKey(SiteKey("siteguid2"), 23456L).toScopedKey, "siteguid2", ContentGroupStatus.inactive, false, false, "client1", "chan1", "feed1"),
      ContentGroup(123456789L, "CgName6", ContentGroupSourceTypes.campaign, CampaignKey(SiteKey("siteguid2"), 23456L).toScopedKey, "siteguid2", ContentGroupStatus.inactive, false, true, "client2", "chan2", "feed2")
    )
  )

  for (value <- values) {
    println(value.map(_.name).mkString(",") + ":")
    //  println("Lift JSON: " + grvjson.serialize(value))
    //  println("Play JSON: " + grvjson.jsonStr(value))
    println("ByteConverter bytes: " + Base64.encodeBase64String(ContentGroupSeqByteConverter.toBytes(value)))
    //  println("Java bytes: " + Base64.encodeBase64String(javaSerObjToBytes(value)))
    println()
  }
}
