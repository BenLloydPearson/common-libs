package com.gravity.logging

import scala.collection.Seq

/*
*     __         __
*  /"  "\     /"  "\
* (  (\  )___(  /)  )
*  \               /
*  /               \
* /    () ___ ()    \  erik 11/1/16
* |      (   )      |
*  \      \_/      /
*    \...__!__.../
*/

object Logstashable {

  val logstashPrefix = "Logstash:"

  // Key conventions
  val AbsolutePath = "absolutePath"
  val ActiveMQMsg = "activeMQMsg"
  val ArticleKey = "articleKey"
  val BeaconString = "beaconString"
  val BucketId = "bucketId"
  val CampaignKey = "campaignKey"
  val ClientContext = "clientContext"
  val Codec = "codec"
  val ConfigVersion = "configVersion"
  val ContentGroupId = "contentGroupId"
  val ContentGroupName = "contentGroupName"
  val Cookie = "cookie"
  val CountryCode = "countryCode"
  val Duration = "duration"
  val DatabaseUrl = "databaseUrl"
  val DeviceType = "deviceType"
  val ExchangeKey = "exchangeKey"
  val FallbackSrc = "fallbackSrc"
  val FallbacksVersion = "fallbacksVersion"
  val HasContent = "hasContent"
  val Headers = "headers"
  val HttpStatusCode = "httpStatusCode"
  val HttpStatusMessage = "httpStatusMessage"
  val Id = "grvId"
  val ImpressionViewedError = "iveError"
  val ImpressionViewedErrorExtra = "iveErrorExtraData"
  val IngestionSouce = "ingestionSouce"
  val IPAddress = "ipAddress"
  val Key = "key"
  val LastWriteCount = "lastWriteCount"
  val Message = "message"
  val Name = "name"
  val OriginalSrc = "originalSrc"
  val PageIndex = "pageIndex"
  val PlacementId = "placementId"
  val PartnerPlacementId = "partnerPlacementId"
  val RecoFailures = "recoFailures"
  val Failures = "failures"
  val RecommenderId = "recommenderId"
  val RecommendedScopeKey = "recommendedScopeKey"
  val RenderType = "renderType"
  val RequestUrl = "requestUrl"
  val ResponseStream = "responseStream"
  val Role = "role"
  val ScopedKey = "scopedKey"
  val SectionId = "sectionId"
  val Server = "server"
  val SiteGuid = "siteGuid"
  val SiteKey = "siteKey"
  val SiteName = "siteName"
  val SitePlacementId = "sitePlacementId"
  val SlotEnd = "slotEnd"
  val SlotIndex = "slotIndex"
  val SlotStart = "slotStart"
  val Status = "status"
  val SuperSiteGuid = "superSiteGuid"
  val SuperType = "superType"
  val TableName = "tableName"
  val Type = "grvType"
  val Url = "url"
  val UserAgent = "userAgent"
  val UserGuid = "userGuid"
  val UserHash = "userHash"
  val RequestedElasticSearchIndices = "requestedIndices"
  val MissingElasticSearchIndices = "missingIndices"
  val AvailableElasticSearchIndices = "availableIndices"
  val RemoteHost = "remoteHost"
  val RemoteRole = "remoteRole"

  def format(kvs: Seq[(String, String)]): String = {
    if (kvs.isEmpty) ""
    else {
      val sb = new StringBuilder
      val eq = "="
      var pastFirst = false

      for ((key, value) <- kvs) {
        if (pastFirst) {
          sb.append("^")
        } else {
          pastFirst = true
        }
        sb.append(key).append(eq).append(value)
      }

      sb.toString()
    }
  }

}

trait Logstashable {
  val typeKV: (String, String) = Logstashable.Type -> getClass.getSimpleName

  def getKVs: Seq[(String, String)]

  def getKVsWithType : Seq[(String, String)] = getKVs.+:(typeKV)

  def exceptionOption: Option[Throwable] = None

  def toKVString : String = Logstashable.format(getKVs)

  def toLogLine : String = {
    val kvString = Logstashable.format(getKVs)
    val prefix = if (kvString.isEmpty) "" else Logstashable.logstashPrefix

    val logString = prefix + " " + kvString
    exceptionOption match {
      case Some(exception) => logString + ": " + formatException(exception)
      case None => logString
    }
  }

  def formatException(ex: Throwable): String = {
    val sb = new StringBuilder
    sb.append("Exception of type ").append(ex.getClass.getCanonicalName).append(" was thrown.").append('\n')
    sb.append("Message: ").append(ex.getMessage).append('\n')
    sb.append("Stack Trace: ").append(ex.getStackTrace.mkString("", "\n", "\n")).toString()
  }
}