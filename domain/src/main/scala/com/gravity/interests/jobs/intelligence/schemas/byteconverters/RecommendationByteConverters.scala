package com.gravity.interests.jobs.intelligence.schemas.byteconverters

import com.gravity.hbase.schema.{ComplexByteConverter, MapConverter, PrimitiveInputStream, PrimitiveOutputStream}
import com.gravity.interests.jobs.intelligence._
import com.gravity.interests.jobs.intelligence.hbase.{ScopedKey, ScopedKeyTypes}
import com.gravity.interests.jobs.intelligence.operations.recommendations.CampaignRecommendationData
import com.gravity.interests.jobs.intelligence.schemas.DollarValue
import com.gravity.utilities.time.{DateHour, DateMonth, GrvDateMidnight}

trait RecommendationByteConverters {
  this: SchemaTypes.type =>

  implicit object RecommendationScopesConverter extends ComplexByteConverter[ScopedKeyTypes.ScopedKeyType] {
    def write(data: ScopedKeyTypes.ScopedKeyType, output: PrimitiveOutputStream) {
      output.writeShort(data.id)
    }

    def read(input: PrimitiveInputStream): ScopedKeyTypes.ScopedKeyType = ScopedKeyTypes.parseOrDefault(input.readShort())
  }

  trait RecommendationMetricsKeyConverter {
    self: ComplexByteConverter[_] =>

    def writeBase(data: RecommendationMetricsKey, output: PrimitiveOutputStream) {
      output.writeLong(-data.dateHour.getMillis)
      output.writeByte(data.countBy)
      output.writeInt(data.bucketId)
      output.writeObj(data.siteKey)
      output.writeShort(data.placementId.toShort)
      output.writeInt(data.geoLocationId)
      output.writeInt(data.algoId)
    }

    def readBase(input: PrimitiveInputStream): RecommendationMetricsKey = new RecommendationMetricsKey {
      val dateHour: DateHour = DateHour(-input.readLong())
      val countBy: Byte = input.readByte()
      val bucketId: Int = input.readInt()
      val siteKey: SiteKey = input.readObj[SiteKey](SiteKeyConverter)
      val placementId: Int = self.safeReadField(input)(_.readShort(), -1.toShort).toInt
      val geoLocationId: Int = self.safeReadField(input)(_.readInt(), ArticleRecommendationMetricKey.defaultGeoLocationId)
      val algoId: Int = self.safeReadField(input)(_.readInt(), ArticleRecommendationMetricKey.defaultAlgoId)

      val sitePlacementId = 0
    }
  }

  trait RecommendationDailyMetricsKeyConverter {
    self: ComplexByteConverter[_] =>

    def writeBase(data: RecommendationDailyMetricsKey, output: PrimitiveOutputStream) {
      output.writeLong(-data.dateMidnight.getMillis)
      output.writeByte(data.countBy)
      output.writeInt(data.bucketId)
      output.writeObj(data.siteKey)
      output.writeShort(data.placementId.toShort)
      output.writeInt(data.geoLocationId)
      output.writeInt(data.algoId)
    }

    def readBase(input: PrimitiveInputStream): RecommendationDailyMetricsKey = new RecommendationDailyMetricsKey {
      val dateMidnight: GrvDateMidnight = new GrvDateMidnight(-input.readLong())
      val countBy: Byte = input.readByte()
      val bucketId: Int = input.readInt()
      val siteKey: SiteKey = input.readObj[SiteKey](SiteKeyConverter)
      val placementId: Int = self.safeReadField(input)(_.readShort(), -1.toShort).toInt
      val geoLocationId: Int = self.safeReadField(input)(_.readInt(), ArticleRecommendationMetricKey.defaultGeoLocationId)
      val algoId: Int = self.safeReadField(input)(_.readInt(), ArticleRecommendationMetricKey.defaultAlgoId)

      val sitePlacementId = 0
    }
  }

  trait RecommendationMonthlyMetricsKeyConverter {
    self: ComplexByteConverter[_] =>

    def writeBase(data: RecommendationMonthlyMetricsKey, output: PrimitiveOutputStream) {
      output.writeLong(-data.dateMonth.getMillis)
      output.writeByte(data.countBy)
      output.writeInt(data.bucketId)
      output.writeObj(data.siteKey)
      output.writeShort(data.placementId.toShort)
      output.writeInt(data.geoLocationId)
      output.writeInt(data.algoId)
    }

    def readBase(input: PrimitiveInputStream): RecommendationMonthlyMetricsKey = new RecommendationMonthlyMetricsKey {
      val dateMonth: DateMonth = DateMonth(-input.readLong())
      val countBy: Byte = input.readByte()
      val bucketId: Int = input.readInt()
      val siteKey: SiteKey = input.readObj[SiteKey](SiteKeyConverter)
      val placementId: Int = self.safeReadField(input)(_.readShort(), -1.toShort).toInt
      val geoLocationId: Int = self.safeReadField(input)(_.readInt(), ArticleRecommendationMetricKey.defaultGeoLocationId)
      val algoId: Int = self.safeReadField(input)(_.readInt(), ArticleRecommendationMetricKey.defaultAlgoId)

      val sitePlacementId = 0
    }
  }

  implicit object CampaignRecommendationDataConverter extends ComplexByteConverter[CampaignRecommendationData] {
    def write(data: CampaignRecommendationData, output: PrimitiveOutputStream) {
      output.writeObj(data.key)
      output.writeObj(data.cpc)
      output.writeObj(data.campaignType)
      output.writeObj(data.settings)
    }

    def read(input: PrimitiveInputStream): CampaignRecommendationData = {
      CampaignRecommendationData(input.readObj[CampaignKey], input.readObj[DollarValue], input.readObj[CampaignType.Type], safeReadField(input)(_.readObj[Option[CampaignArticleSettings]], None))
    }
  }

  implicit object OptionCampaignRecommendationDataConverter extends ComplexByteConverter[Option[CampaignRecommendationData]] {
    def write(data: Option[CampaignRecommendationData], output: PrimitiveOutputStream) {
      data match {
        case Some(s) => {
          output.writeBoolean(true)
          output.writeObj(s)
        }
        case None => output.writeBoolean(false)
      }
    }

    def read(input: PrimitiveInputStream): Option[CampaignRecommendationData] = {
      if (input.readBoolean()) Some(input.readObj[CampaignRecommendationData]) else None
    }
  }

  implicit object RecommendationArticleLogConverter extends ComplexByteConverter[RecommendationArticleLog] {
    def write(data: RecommendationArticleLog, output: PrimitiveOutputStream) {
      output.writeInt(data.positionOne)
      output.writeInt(data.positionTwo)
      output.writeInt(data.positionThree)
      output.writeInt(data.positionFour)
      output.writeInt(data.positionFive)
      output.writeLong(data.lastGenDate)
    }

    def read(input: PrimitiveInputStream): RecommendationArticleLog = RecommendationArticleLog(input.readInt, input.readInt, input.readInt, input.readInt, input.readInt, input.readLong)
  }

  implicit object RecommendationArticleLogMapConverter extends MapConverter[ArticleKey, RecommendationArticleLog]

  implicit object RecommendationMetricsConverter extends ComplexByteConverter[RecommendationMetrics] {
    val currentVersion = 1
    override def write(data: RecommendationMetrics, output: PrimitiveOutputStream): Unit = {
      output.writeInt(currentVersion)
      output.writeLong(data.articleImpressions)
      output.writeLong(data.clicks)
      output.writeLong(data.unitImpressions)
      output.writeLong(data.organicClicks)
      output.writeLong(data.sponsoredClicks)
      output.writeLong(data.unitImpressionsViewed)
      output.writeLong(data.unitImpressionsControl)
      output.writeLong(data.unitImpressionsNonControl)
      output.writeLong(data.organicClicksControl)
      output.writeLong(data.organicClicksNonControl)
    }

    override def read(input: PrimitiveInputStream): RecommendationMetrics = {
      val version : Int = input.readInt()
      val articleImpressions = input.readLong
      val clicks = input.readLong
      val unitImpressions = input.readLong
      val organicClicks = input.readLong
      val sponsoredClicks = input.readLong
      val unitImpressionsViewed = input.readLong
      val unitImpressionsControl = input.readLong
      val unitImpressionsNonControl = input.readLong
      val organicClicksControl = input.readLong
      val organicClicksNonControl = input.readLong

      RecommendationMetrics(
        articleImpressions,
        clicks,
        unitImpressions,
        organicClicks,
        sponsoredClicks,
        unitImpressionsViewed,
        unitImpressionsControl,
        unitImpressionsNonControl,
        organicClicksControl,
        organicClicksNonControl)
    }
  }

  implicit object RecommenderIdKeyConverter extends ComplexByteConverter[RecommenderIdKey] {
    override def write(key: RecommenderIdKey, output: PrimitiveOutputStream) {
      output.writeLong(key.recommenderId)
      output.writeObj(key.scopedKey)
    }

    override def read(input: PrimitiveInputStream): RecommenderIdKey = {
      val recommenderId = input.readLong()
      val scopedKey = input.readObj[ScopedKey]
      RecommenderIdKey(recommenderId, scopedKey)
    }
  }
}
