package com.gravity.interests.jobs.intelligence.operations

import com.gravity.algorithms.model.AlgoSettingNamespace
import com.gravity.domain.articles.{ContentGroupSourceTypes, ContentGroupStatus}
import com.gravity.domain.recommendations.ContentGroup
import com.gravity.interests.jobs.intelligence._
import com.gravity.interests.jobs.intelligence.hbase.ScopedKey
import com.gravity.interests.jobs.intelligence.operations.recommendations.CampaignRecommendationData
import com.gravity.interests.jobs.intelligence.operations.urlvalidation.{ImageUrlValidationRequestMessage, UrlValidationRequestMessage, UrlValidationResponse}
import com.gravity.interests.jobs.intelligence.schemas.DollarValue
import com.gravity.utilities.eventlogging.{FieldRegistry, FieldValueRegistry}
import com.gravity.utilities.grvfields.FieldConverter
import com.gravity.utilities.grvfunc._
import com.gravity.valueclasses.ValueClassesForDomain.ExchangeGuid
import org.joda.time.DateTime

import scalaz.{Failure, Success}

/*
*     __         __
*  /"  "\     /"  "\
* (  (\  )___(  /)  )
*  \               /
*  /               \
* /    () ___ ()    \  erik 2/25/15
* |      (   )      |
*  \      \_/      /
*    \...__!__.../
*/

object FieldConverters {
  /*
  case class AlgoSettingNamespace(variablesMap: Map[ScopedKey, Double] = Map.empty[ScopedKey, Double],
                                switchesMap: Map[ScopedKey, Boolean] = Map.empty[ScopedKey, Boolean],
                                settingsMap: Map[ScopedKey, String] = Map.empty[ScopedKey, String],
                                probabilitySettingsMap: Map[ScopedKey, ProbabilityDistribution] = Map.empty[ScopedKey, ProbabilityDistribution])
   */
  implicit object AlgoSettingNamespaceConverter extends FieldConverter[AlgoSettingNamespace] {
    override val compressBytes : Boolean = true

    import com.gravity.domain.FieldConverters._
    import com.gravity.interests.jobs.intelligence.hbase.ScopedKeyConverters.ScopedKeyConverter

    override def toValueRegistry(o: AlgoSettingNamespace): FieldValueRegistry = {
      import o._
      val sortedVariables = variablesMap.toSeq.sortBy(_._1.toString)
      val sortedSwitches = switchesMap.toSeq.sortBy(_._1.toString)
      val sortedSettings = settingsMap.toSeq.sortBy(_._1.toString)
      val sortedPSettings = probabilitySettingsMap.toSeq.sortBy(_._1.toString)

      new FieldValueRegistry(fields)
        .registerFieldValue(0, sortedVariables.map(_._1).map(ScopedKeyConverter.toBytes)).registerFieldValue(1, sortedVariables.map(_._2))
        .registerFieldValue(2, sortedSwitches.map(_._1).map(ScopedKeyConverter.toBytes)).registerFieldValue(3, sortedSwitches.map(_._2))
        .registerFieldValue(4, sortedSettings.map(_._1).map(ScopedKeyConverter.toBytes)).registerFieldValue(5, sortedSettings.map(_._2))
        .registerFieldValue(6, sortedPSettings.map(_._1).map(ScopedKeyConverter.toBytes)).registerFieldValue(7, sortedPSettings.map(_._2))

    }

    override def fromValueRegistry(reg: FieldValueRegistry): AlgoSettingNamespace = {

      val vKBytes = reg.getValue[Seq[Array[Byte]]](0)
      val vK = vKBytes.map(ScopedKeyConverter.fromBytes)
      val vV = reg.getValue[Seq[Double]](1)
      val variablesMap = vK.zip(vV).toMap
      val swKBytes = reg.getValue[Seq[Array[Byte]]](2)
      val swK = swKBytes.map(ScopedKeyConverter.fromBytes)
      val swV = reg.getValue[Seq[Boolean]](3)
      val switchesMap = swK.zip(swV).toMap
      val seKBytes = reg.getValue[Seq[Array[Byte]]](4)
      val seK = seKBytes.map(ScopedKeyConverter.fromBytes)
      val seV = reg.getValue[Seq[String]](5)
      val settingsMap = seK.zip(seV).toMap
      val psKBytes = reg.getValue[Seq[Array[Byte]]](6)
      val psK = psKBytes.map(ScopedKeyConverter.fromBytes)
      val psV = reg.getValue[Seq[ProbabilityDistribution]](7)
      val probabilitySettingsMap = psK.zip(psV).toMap
      AlgoSettingNamespace(variablesMap, switchesMap, settingsMap, probabilitySettingsMap)
    }

    override val fields: FieldRegistry[AlgoSettingNamespace] = new FieldRegistry[AlgoSettingNamespace]("AlgoSettingsNamespace", version = 0)
      .registerByteArraySeqField("variablesKeys", 0).registerDoubleSeqField("variablesValues", 1)
      .registerByteArraySeqField("switchesKeys", 2).registerBooleanSeqField("switchesValues", 3)
      .registerByteArraySeqField("settingsKeys", 4).registerStringSeqField("settingsValues", 5)
      .registerByteArraySeqField("probabilitySettingsKeys", 6).registerSeqField[ProbabilityDistribution]("probabilitySettingsValue", 7, Seq.empty[ProbabilityDistribution])
  }

  implicit object CampaignBudgetOverageConverter extends FieldConverter[CampaignBudgetOverage] {
    override def toValueRegistry(o: CampaignBudgetOverage): FieldValueRegistry =
      new FieldValueRegistry(fields).registerFieldValue(0, o.campaignKey.toString).registerFieldValue(1, o.foundBy).registerFieldValue(2, o.amount).registerFieldValue(3, o.maxSpendType.toString)

    override def fromValueRegistry(reg: FieldValueRegistry): CampaignBudgetOverage = {

      CampaignKey.parse(reg.getValue[String](0)) match {
        case Some(key) => CampaignBudgetOverage(key, reg.getValue[String](1), reg.getValue[Long](2), MaxSpendTypes.getOrDefault(reg.getValue[String](3)))
        case None => throw new Exception("Could not parse campaign key string " + reg.getValue[String](0))
      }

    }

    override val fields: FieldRegistry[CampaignBudgetOverage] = new FieldRegistry[CampaignBudgetOverage]("CampaignBudgetOverage", 0)
      .registerUnencodedStringField("campaignKey", 0, "").registerUnencodedStringField("foundBy", 1, "").registerLongField("amount", 2, 0l).registerUnencodedStringField("spendType", 3, "")
  }

  implicit object ArticleDataLiteRequestConverter extends FieldConverter[ArticleDataLiteRequest] {
    override def toValueRegistry(o: ArticleDataLiteRequest): FieldValueRegistry =
      new FieldValueRegistry(fields).registerFieldValue(0, o.keys.map(_.articleId).toSeq)

    override def fromValueRegistry(reg: FieldValueRegistry): ArticleDataLiteRequest = {
      ArticleDataLiteRequest(reg.getValue[Seq[Long]](0).map(ArticleKey(_)).toSet)
    }

    override val fields: FieldRegistry[ArticleDataLiteRequest] = new FieldRegistry[ArticleDataLiteRequest]("ArticleDataLiteRequest", 0)
      .registerLongSeqField("keys", 0)
  }

  implicit object ArticleDataLiteResponseConverter extends FieldConverter[ArticleDataLiteResponse] {
    override def toValueRegistry(o: ArticleDataLiteResponse): FieldValueRegistry = new FieldValueRegistry(fields).registerFieldValue(0, o.byteses)

    override def fromValueRegistry(reg: FieldValueRegistry): ArticleDataLiteResponse = ArticleDataLiteResponse(reg.getValue[Seq[Array[Byte]]](0))

    override val fields: FieldRegistry[ArticleDataLiteResponse] = new FieldRegistry[ArticleDataLiteResponse]("ArticleDataLiteResponse", 0).registerByteArraySeqField("byteses", 0)
  }

  implicit object UrlValidationResponseConverter extends FieldConverter[UrlValidationResponse] {
    def toValueRegistry(o: UrlValidationResponse): FieldValueRegistry = {
      val fieldReg = new FieldValueRegistry(fields)
      o.errorMessage match {
        case someMsg @ Some(_) =>
          fieldReg.registerFieldValue(0, someMsg).registerFieldValue(1, 0)
        case None =>
          fieldReg.registerFieldValue(0, None: Option[String])
          o.statusCode.foreach(code => fieldReg.registerFieldValue(1, code))
      }

      fieldReg.registerFieldValue(2, o.returnedFromCache)
    }

    def fromValueRegistry(reg: FieldValueRegistry): UrlValidationResponse = {

      reg.getValue[Option[String]](0) match {
        case someMsg @ Some(_) => UrlValidationResponse(None, someMsg, reg.getValue[Boolean](2))
        case None =>  UrlValidationResponse(Some(reg.getValue[Int](1)), None, reg.getValue[Boolean](2))
      }
    }

    val fields: FieldRegistry[UrlValidationResponse] = new FieldRegistry[UrlValidationResponse]("UrlValidationResponse", 0)
      .registerStringOptionField("errorMessage", 0, required = false)
      .registerIntField("statusCode", 1, required = false)
      .registerBooleanField("returnedFromCache", 2, required = false)
  }

  implicit object UrlValidationRequestMessageConverter extends FieldConverter[UrlValidationRequestMessage] {
    def toValueRegistry(o: UrlValidationRequestMessage): FieldValueRegistry = {
      new FieldValueRegistry(fields).registerFieldValue(0, o.url)
    }

    def fromValueRegistry(reg: FieldValueRegistry): UrlValidationRequestMessage = {
      UrlValidationRequestMessage(reg.getValue[String](0))
    }

    val fields: FieldRegistry[UrlValidationRequestMessage] = {
      new FieldRegistry[UrlValidationRequestMessage]("UrlValidationRequestMessage", 0)
        .registerStringField("url", 0, required = true)
    }
  }

  implicit object ImageUrlValidationRequestMessageConverter extends FieldConverter[ImageUrlValidationRequestMessage] {
    def toValueRegistry(o: ImageUrlValidationRequestMessage): FieldValueRegistry = {
      new FieldValueRegistry(fields).registerFieldValue(0, o.url)
    }

    def fromValueRegistry(reg: FieldValueRegistry): ImageUrlValidationRequestMessage = {
      ImageUrlValidationRequestMessage(reg.getValue[String](0))
    }

    val fields: FieldRegistry[ImageUrlValidationRequestMessage] = {
      new FieldRegistry[ImageUrlValidationRequestMessage]("ImageUrlValidationRequestMessage", 0)
        .registerStringField("url", 0, required = true)
    }
  }

  implicit object SiteMetaRequestConverter extends FieldConverter[SiteMetaRequest] {
    override def toValueRegistry(o: SiteMetaRequest): FieldValueRegistry = new FieldValueRegistry(fields).registerFieldValue(0, o.siteKeyFilter.map(_.siteId))

    override def fromValueRegistry(reg: FieldValueRegistry): SiteMetaRequest = SiteMetaRequest(reg.getValue[Seq[Long]](0).map(SiteKey(_)))

    override val fields: FieldRegistry[SiteMetaRequest] = new FieldRegistry[SiteMetaRequest]("SiteMetaRequest", 0).registerLongSeqField("keys", 0)
  }


  implicit object SiteMetaResponseConverter extends FieldConverter[SiteMetaResponse] {
 import com.gravity.logging.Logging._
    override val compressBytes = true
    val cacher = Schema.Sites.cache.asInstanceOf[IntelligenceSchemaCacher[SitesTable, SiteKey, SiteRow]]
    override def toValueRegistry(o: SiteMetaResponse): FieldValueRegistry = {
      val errorBuffer = scala.collection.mutable.ArrayBuffer[SiteKey]()
      val rowBytes: Seq[Array[Byte]] = o.rowMap.values.toSeq.sortBy(_.rowid.siteId).flatMap(row => {
        cacher.toBytesTransformer(row) match {
          case Success(bytes) =>
            Some(bytes)
          case Failure(fails) =>
            warn("Failed to serialized row: " + row.rowid.toString + ": " + fails.toString)
            errorBuffer += row.rowid//"Failed t+ ": " + fails.toString//throw new Exception("Failed to serialize campaign " + row.campaignKey.toString + ": " + fails.toString)
            None
        }
      })

      val errorSet = errorBuffer.toSet
      val keyIds = o.rowMap.keys.filter(key => !errorSet.contains(key)).map(_.siteId).toList.sorted.toSeq
      new FieldValueRegistry(fields)
        .registerFieldValue(0, keyIds)
        .registerFieldValue(1, rowBytes)
    }

    override def fromValueRegistry(reg: FieldValueRegistry): SiteMetaResponse = {
      val keys = reg.getValue[Seq[Long]](0).map(SiteKey(_))
      val values = reg.getValue[Seq[Array[Byte]]](1).map(bytes => {
        cacher.fromBytesTransformer(bytes) match {
          case Success(row) => row
          case Failure(fails) => throw new Exception("Exception deserializing site meta row:" + fails.toString)
        }})
      val map = keys.zip(values).toMap
      SiteMetaResponse(map)
    }

    override val fields: FieldRegistry[SiteMetaResponse] = new FieldRegistry[SiteMetaResponse]("SiteMetaResponse", 0)
      .registerLongSeqField("siteIds", 0)
      .registerByteArraySeqField("rowBytes", 1)
  }

  implicit object CampaignMetaRequestConverter extends FieldConverter[CampaignMetaRequest] {
    override def toValueRegistry(o: CampaignMetaRequest): FieldValueRegistry = new FieldValueRegistry(fields)

    override def fromValueRegistry(reg: FieldValueRegistry): CampaignMetaRequest = CampaignMetaRequest()

    override val fields: FieldRegistry[CampaignMetaRequest] = new FieldRegistry[CampaignMetaRequest]("CampaignMetaRequest", 0)
  }

  implicit object CampaignMetaResponseConverter extends FieldConverter[CampaignMetaResponse] {
 import com.gravity.logging.Logging._
    override val compressBytes = true
    val cacher = Schema.Campaigns.cache.asInstanceOf[IntelligenceSchemaCacher[CampaignsTable, CampaignKey, CampaignRow]]
    override def toValueRegistry(o: CampaignMetaResponse): FieldValueRegistry = {
      val errorBuffer = scala.collection.mutable.ArrayBuffer[CampaignKey]()
      val rowBytes: Seq[Array[Byte]] = o.rowMap.values.toList.sortBy(_.rowid.toString).flatMap(row => {
        cacher.toBytesTransformer(row) match {
          case Success(bytes) =>
            Some(bytes)
          case Failure(fails) =>
            warn("Failed to serialized row: " + row.campaignKey.toString + ": " + fails.toString)
            errorBuffer += row.campaignKey //"Failed t+ ": " + fails.toString//throw new Exception("Failed to serialize campaign " + row.campaignKey.toString + ": " + fails.toString)
            None
        }
      })

      val errorSet = errorBuffer.toSet
      val keyIds = o.rowMap.keys.filter(key => !errorSet.contains(key)).map(_.toString).toList.sorted.toSeq
      new FieldValueRegistry(fields)
        .registerFieldValue(0, keyIds)
        .registerFieldValue(1, rowBytes)
    }

    override def fromValueRegistry(reg: FieldValueRegistry): CampaignMetaResponse = {
      val keys = reg.getValue[Seq[String]](0).flatMap(CampaignKey.parse(_))
      val values = reg.getValue[Seq[Array[Byte]]](1).map(bytes => {
        cacher.fromBytesTransformer(bytes) match {
          case Success(row) => row
          case Failure(fails) => throw new Exception("Exception deserializing campaign row: " + fails.toString)
        }})
      val map = keys.zip(values).toMap
      CampaignMetaResponse(map)
    }

    override val fields: FieldRegistry[CampaignMetaResponse] = new FieldRegistry[CampaignMetaResponse]("CampaignMetaResponse", 0)
      .registerStringSeqField("campaignIds", 0)
      .registerByteArraySeqField("rowBytes", 1)
  }

  implicit object SitePlacementMetaRequestConverter extends FieldConverter[SitePlacementMetaRequest] {
    override def toValueRegistry(o: SitePlacementMetaRequest): FieldValueRegistry = new FieldValueRegistry(fields)

    override def fromValueRegistry(reg: FieldValueRegistry): SitePlacementMetaRequest = SitePlacementMetaRequest()

    override val fields: FieldRegistry[SitePlacementMetaRequest] = new FieldRegistry[SitePlacementMetaRequest]("SitePlacementMetaRequest", 0)
  }

   implicit object SitePlacementMetaResponseConverter extends FieldConverter[SitePlacementMetaResponse] {
 import com.gravity.logging.Logging._
    override val compressBytes = true
    val cacher = Schema.SitePlacements.cache.asInstanceOf[IntelligenceSchemaCacher[SitePlacementsTable, SitePlacementKey, SitePlacementRow]]
    override def toValueRegistry(o: SitePlacementMetaResponse): FieldValueRegistry = {
      val errorBuffer = scala.collection.mutable.ArrayBuffer[SitePlacementKey]()
      val rowBytes: List[Array[Byte]] = o.rowMap.values.toList.sortBy(_.rowid.toString).flatMap(row => {
        cacher.toBytesTransformer(row) match {
          case Success(bytes) =>
            Some(bytes)
          case Failure(fails) =>
            warn("Failed to serialize row: " + row.rowid.toString + ": " + fails.toString)
            errorBuffer += row.rowid//"Failed t+ ": " + fails.toString//throw new Exception("Failed to serialize campaign " + row.campaignKey.toString + ": " + fails.toString)
            None
        }
      })

      val errorSet = errorBuffer.toSet
      val keys = o.rowMap.keys.filter(key => !errorSet.contains(key)).toList.sortBy(_.toString)

      new FieldValueRegistry(fields)
        .registerFieldValue(0, keys.map(_.placementIdOrSitePlacementId))
        .registerFieldValue(1, keys.map(_.siteId))
        .registerFieldValue(2, rowBytes)
    }

    override def fromValueRegistry(reg: FieldValueRegistry): SitePlacementMetaResponse = {
      val placementIdOrSitePlacementIds = reg.getValue[Seq[Long]](0)
      val siteIds = reg.getValue[Seq[Long]](1)
      val keys = placementIdOrSitePlacementIds.zip(siteIds).map{case (pid, sid) => SitePlacementKey(pid, sid)}
      val values = reg.getValue[Seq[Array[Byte]]](2).map(bytes => {
        cacher.fromBytesTransformer(bytes) match {
          case Success(row) => row
          case Failure(fails) => throw new Exception("Exception deserializing site placement meta row: " + fails.toString)
        }})
      val map = keys.zip(values).toMap
      SitePlacementMetaResponse(map)
    }

    override val fields: FieldRegistry[SitePlacementMetaResponse] = new FieldRegistry[SitePlacementMetaResponse]("SitePlacementMetaResponse", 0)
      .registerLongSeqField("placementIdOrSitePlacementIds", 0)
      .registerLongSeqField("siteIds", 1)
      .registerByteArraySeqField("rowBytes", 2)
  }

  implicit object CampaignArticleSettingsFieldConverter extends FieldConverter[CampaignArticleSettings] {
    val version = 2

    val noneString: Option[String] = None
    val trueList: Seq[Int] = Seq(1)
    val falseList: Seq[Int] = Seq(0)

    val fields = new FieldRegistry[CampaignArticleSettings]("CampaignArticleSettings", version)
      .registerIntField("status", 0, RssFeedSettings.default.initialArticleStatus.i.toInt)
      .registerBooleanField("isBlacklisted", 1, defaultValue = false)
      .registerStringOptionField("clickUrl", 2, None)
      .registerStringOptionField("title", 3, None)
      .registerStringOptionField("image", 4, None)
      .registerStringOptionField("displayDomain", 5, None)
      .registerIntSeqField("isBlocked", 6, Seq.empty)
      .registerIntSeqField("blockedReasons", 7, Seq.empty)
      .registerStringOptionField("articleImpressionPixel", 8, None)
      .registerStringOptionField("articleClickPixel", 9, None)
      // ATTN: leaving this field in though it is removed from the CAS class. Removing fields is not allowed in FieldConverter.
      .registerStringOptionField("articleReviewStatus", 10, None, minVersion = 1)
      .registerStringSeqField("trackingParamsKEYS", 11, description = "Tracking Parameter Keys", minVersion = 2)
      .registerStringSeqField("trackingParamsVALUES", 12, description = "Tracking Parameter Values", minVersion = 2)

    override def toValueRegistry(o: CampaignArticleSettings): FieldValueRegistry = {
      new FieldValueRegistry(fields)
        .registerFieldValue(0, o.status.id.toInt)
        .registerFieldValue(1, o.isBlacklisted)
        .registerFieldValue(2, o.clickUrl)
        .registerFieldValue(3, o.title)
        .registerFieldValue(4, o.image)
        .registerFieldValue(5, o.displayDomain)
        .registerFieldValue(6, o.isBlocked match {
          case Some(true) => trueList
          case Some(false) => falseList
          case _ => List.empty[Int]
        })
        .registerFieldValue(7, o.blockedReasons.map(_.map(_.id.toInt).toList).getOrElse(List.empty))
        .registerFieldValue(8, o.articleImpressionPixel)
        .registerFieldValue(9, o.articleClickPixel)
        .registerFieldValue(10, noneString)
        .registerFieldValue(11, o.trackingParams.keys.toSeq)
        .registerFieldValue(12, o.trackingParams.values.toSeq)
    }

    override def fromValueRegistry(reg: FieldValueRegistry): CampaignArticleSettings = {
      def getTrackingParams: Map[String, String] = {
        val ignoreThisField = reg.getValue[Option[String]](10)
        reg.getValue[Seq[String]](11).zip(reg.getValue[Seq[String]](12)).toMap
      }
      val isBlockedSeq = reg.getValue[Seq[Int]](6)
      val isBlocked = if(isBlockedSeq.isEmpty) None else if(isBlockedSeq.head == 1) Some(true) else Some(false)
      CampaignArticleSettings(
        CampaignArticleStatus.getOrDefault(
          reg.getValue[Int](0).toByte)
        , reg.getValue[Boolean](1)
        , reg.getValue[Option[String]](2)
        , reg.getValue[Option[String]](3)
        , reg.getValue[Option[String]](4)
        , reg.getValue[Option[String]](5)
        , isBlocked
        , reg.getValue[Seq[Int]](7).ifThenElse(_.isEmpty)(_ => None: Option[Set[BlockedReason.Type]])(v => Some(v.map(i => BlockedReason.getOrDefault(i.toByte)).toSet))
        , reg.getValue[Option[String]](8)
        , reg.getValue[Option[String]](9)
        , getTrackingParams
      )
    }
  }

  implicit object CampaignRecommendationDataFieldConverter extends FieldConverter[CampaignRecommendationData] {
    val fields = new FieldRegistry[CampaignRecommendationData]("CampaignRecommendationData")
      .registerLongField("siteId", 0, 0)
      .registerLongField("campaignId", 1, 0)
      .registerLongField("cpc", 2, 0)
      .registerIntField("campaignType", 3, CampaignType.organic.id.toInt)
      .registerSeqField[CampaignArticleSettings]("settings", 4, Seq.empty)

    override def toValueRegistry(o: CampaignRecommendationData): FieldValueRegistry = {
      new FieldValueRegistry(fields)
        .registerFieldValue(0, o.key.siteKey.siteId)
        .registerFieldValue(1, o.key.campaignId)
        .registerFieldValue(2, o.cpc.pennies)
        .registerFieldValue(3, o.campaignType.id.toInt)
        .registerFieldValue(4, o.settings.toSeq)
    }

    override def fromValueRegistry(reg: FieldValueRegistry): CampaignRecommendationData = {
      CampaignRecommendationData(
        CampaignKey(SiteKey(reg.getValue[Long](0)), reg.getValue[Long](1)),
        DollarValue(reg.getValue[Long](2)),
        CampaignType.getOrDefault(reg.getValue[Int](3).toByte),
        reg.getValue[Seq[CampaignArticleSettings]](4).headOption
      )
    }
  }

  implicit object AlgoSettingsFieldConverter extends FieldConverter[AlgoSettingsData] {
    val fields = new FieldRegistry[AlgoSettingsData]("AlgoSettingsData")
      .registerStringField("settingName", 0, "")
      .registerIntField("settingType", 1, 0)
      .registerStringField("settingData", 2, "")

    override def toValueRegistry(o: AlgoSettingsData): FieldValueRegistry = {
      new FieldValueRegistry(fields)
        .registerFieldValue(0, o.settingName)
        .registerFieldValue(1, o.settingType)
        .registerFieldValue(2, o.settingData)
    }

    override def fromValueRegistry(reg: FieldValueRegistry): AlgoSettingsData = {
      AlgoSettingsData(
        reg.getValue[String](0),
        reg.getValue[Int](1),
        reg.getValue[String](2)
      )
    }
  }

  implicit object ContentGroupFieldConverter extends FieldConverter[ContentGroup] {
    val version = 2

    val fields = new FieldRegistry[ContentGroup]("ContentGroup", version)
      .registerLongField("id", 0, 0)
      .registerStringField("name", 1, "")
      .registerIntField("sourceType", 2, ContentGroupSourceTypes.defaultValue.id.toInt)
      .registerStringField("sourceKey", 3, ScopedKey.zero.keyString)
      .registerStringField("forSiteGuid", 4, "")
      .registerIntField("status", 5, ContentGroupStatus.defaultValue.id.toInt)
      .registerBooleanField("isGmsManaged", 6, defaultValue = false, minVersion = 1)
      .registerBooleanField("isAthena", 7, defaultValue = false, minVersion = 2)
      .registerStringField("chubClientId", 8, defaultValue = "", minVersion = 2)
      .registerStringField("chubChannelId", 9, defaultValue = "", minVersion = 2)
      .registerStringField("chubFeedId", 10, defaultValue = "", minVersion = 2)

    override def toValueRegistry(o: ContentGroup): FieldValueRegistry = {
      new FieldValueRegistry(fields)
        .registerFieldValue(0, o.id)
        .registerFieldValue(1, o.name)
        .registerFieldValue(2, o.sourceType.id.toInt)
        .registerFieldValue(3, o.sourceKey.keyString)
        .registerFieldValue(4, o.forSiteGuid)
        .registerFieldValue(5, o.status.id.toInt)
        .registerFieldValue(6, o.isGmsManaged)
        .registerFieldValue(7, o.isAthena)
        .registerFieldValue(8, o.chubClientId)
        .registerFieldValue(9, o.chubChannelId)
        .registerFieldValue(10, o.chubFeedId)
    }

    override def fromValueRegistry(reg: FieldValueRegistry): ContentGroup = {
      ContentGroup(
        reg.getValue[Long](0),
        reg.getValue[String](1),
        ContentGroupSourceTypes.getOrDefault(reg.getValue[Int](2).toByte),
        ScopedKey.validateKeyString(reg.getValue[String](3)).valueOr(fail => throw new RuntimeException("Invalid ScopedKey: " + reg.getValue[String](3) + ", failure(s): " + fail)),
        reg.getValue[String](4),
        ContentGroupStatus.getOrDefault(reg.getValue[Int](5).toByte),
        reg.getValue[Boolean](6),
        reg.getValue[Boolean](7),
        reg.getValue[String](8),
        reg.getValue[String](9),
        reg.getValue[String](10)
      )
    }
  }

  implicit object ArticleRecommendationFieldConverter extends FieldConverter[ArticleRecommendation] {
    val version = 1

    val fields = new FieldRegistry[ArticleRecommendation]("ArticleRecommendation", version = version)
      .registerLongField("articleKey", 0, 0)
      .registerDoubleField("score", 1, 0.0)
      .registerStringField("why", 2, "")
      .registerSeqField[CampaignRecommendationData]("campaign", 3, Seq.empty)
      .registerSeqField[AlgoSettingsData]("algoSettings", 4, Seq.empty)
      .registerSeqField[ContentGroup]("contentGroup", 5, Seq.empty)
      .registerStringOptionField("exchangeGuid", 6, None, minVersion = 1)

    override def toValueRegistry(o: ArticleRecommendation): FieldValueRegistry = {
      new FieldValueRegistry(fields, version = version)
        .registerFieldValue(0, o.article.articleId)
        .registerFieldValue(1, o.score)
        .registerFieldValue(2, o.why)
        .registerFieldValue(3, o.campaign.toSeq)
        .registerFieldValue(4, o.algoSettings)
        .registerFieldValue(5, o.contentGroup.toSeq)
        .registerFieldValue(6, o.exchangeGuid.map(_.raw))

    }

    override def fromValueRegistry(reg: FieldValueRegistry): ArticleRecommendation = {
      ArticleRecommendation(
        ArticleKey(reg.getValue[Long](0)),
        reg.getValue[Double](1),
        reg.getValue[String](2),
        reg.getValue[Seq[CampaignRecommendationData]](3).headOption,
        reg.getValue[Seq[AlgoSettingsData]](4).toSeq,
        reg.getValue[Seq[ContentGroup]](5).headOption,
        reg.getValue[Option[String]](6).map(exchangeGuid => ExchangeGuid(exchangeGuid))
      )
    }
  }

  implicit object ArticleRecommendationsSectionFieldConverter extends FieldConverter[ArticleRecommendationsSection] {
    val fields = new FieldRegistry[ArticleRecommendationsSection]("ArticleRecommendationsSection")
      .registerLongField("siteId", 0, 0)
      .registerLongField("sectionId", 1, 0)
      .registerDoubleField("score", 2, 0.0)
      .registerStringField("why", 3, "")
      .registerSeqField[ArticleRecommendation]("articles", 4, Seq.empty)
      .registerSeqField[AlgoSettingsData]("algoSettings", 5, Seq.empty)

    override def toValueRegistry(o: ArticleRecommendationsSection): FieldValueRegistry = {
      new FieldValueRegistry(fields)
        .registerFieldValue(0, o.key.siteId)
        .registerFieldValue(1, o.key.sectionId)
        .registerFieldValue(2, o.score)
        .registerFieldValue(3, o.why)
        .registerFieldValue(4, o.articles)
        .registerFieldValue(5, o.algoSettings)
    }

    override def fromValueRegistry(reg: FieldValueRegistry): ArticleRecommendationsSection = {
      ArticleRecommendationsSection(
        SectionKey(reg.getValue[Long](0), reg.getValue[Long](1)),
        reg.getValue[Double](2),
        reg.getValue[String](3),
        reg.getValue[Seq[ArticleRecommendation]](4).toSeq,
        reg.getValue[Seq[AlgoSettingsData]](5).toSeq
      )
    }
  }

  implicit object ArticleRecommendationsFieldConverter extends FieldConverter[ArticleRecommendations] {
    val fields = new FieldRegistry[ArticleRecommendations]("ArticleRecommendations")
      .registerIntField("algorithm", 0, -1)
      .registerLongField("dateTime", 1, new DateTime().getMillis)
      .registerSeqField[ArticleRecommendationsSection]("sections", 2, Seq.empty)

    override def toValueRegistry(o: ArticleRecommendations): FieldValueRegistry = {
      new FieldValueRegistry(fields)
        .registerFieldValue(0, o.algorithm)
        .registerFieldValue(1, o.dateTime.getMillis)
        .registerFieldValue(2, o.sections)
    }

    override def fromValueRegistry(reg: FieldValueRegistry): ArticleRecommendations = {
      ArticleRecommendations(
        reg.getValue[Int](0),
        new DateTime(reg.getValue[Long](1)),
        reg.getValue[Seq[ArticleRecommendationsSection]](2)
      )
    }
  }

}
