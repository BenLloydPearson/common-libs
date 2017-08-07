package com.gravity.interests.jobs.intelligence.schemas.byteconverters

import com.gravity.hbase.schema._
import com.gravity.interests.jobs.intelligence
import com.gravity.interests.jobs.intelligence._
import com.gravity.interests.jobs.intelligence.schemas.DollarValue
import com.gravity.utilities.analytics.DateMidnightRange
import com.gravity.utilities.time.{DateHour, GrvDateMidnight}

import scala.collection.Seq

trait MetricsByteConverters {
  this: SchemaTypes.type =>

  implicit object StandardMetricsConverter extends ComplexByteConverter[StandardMetrics] {
    override def write(metrics: StandardMetrics, output: PrimitiveOutputStream) {
      output.writeLong(metrics.views)
      output.writeLong(metrics.socialReferrers)
      output.writeLong(metrics.searchReferrers)
      output.writeLong(metrics.keyPageReferrers)
      output.writeLong(metrics.publishes)
    }

    override def read(input: PrimitiveInputStream): StandardMetrics = {
      StandardMetrics(input.readLong(), input.readLong(), input.readLong(), input.readLong(), input.readLong())
    }
  }

  implicit object StandardMetricsYearDayMapConverter extends MapConverter[YearDay, StandardMetrics]

  implicit object StandardMetricsDateMidnightMapConverter extends MapConverter[GrvDateMidnight, StandardMetrics]

  implicit object StandardMetricsDailyKeyConverter extends ComplexByteConverter[StandardMetricsDailyKey] {
    override def write(data: StandardMetricsDailyKey, output: PrimitiveOutputStream) {
      output.writeLong(-data.dateMidnight.getMillis)
      output.writeShort(data.metricType.id)
    }

    override def read(input: PrimitiveInputStream): StandardMetricsDailyKey = {
      StandardMetricsDailyKey(new GrvDateMidnight(-input.readLong()), StandardMetricType(input.readShort()))
    }
  }

  implicit object StandardMetricsHourlyKeyConverter extends ComplexByteConverter[StandardMetricsHourlyKey] {
    override def write(data: StandardMetricsHourlyKey, output: PrimitiveOutputStream) {
      output.writeLong(-data.dateHour.getMillis)
      output.writeShort(data.metricType.id)
    }

    override def read(input: PrimitiveInputStream): StandardMetricsHourlyKey = {
      StandardMetricsHourlyKey(DateHour(-input.readLong()), StandardMetricType(input.readShort()))
    }
  }

  implicit object StandardMetricsDateHourMapConverter extends MapConverter[DateHour, StandardMetrics]

  implicit object StandardStatisticsConverter extends ComplexByteConverter[StandardStatistics] {
    override def write(s: StandardStatistics, output: PrimitiveOutputStream) {
      MeasurementsConverter.write(s.views, output)
      MeasurementsConverter.write(s.socialReferrers, output)
      MeasurementsConverter.write(s.searchReferrers, output)
      MeasurementsConverter.write(s.keyPageReferrers, output)
      MeasurementsConverter.write(s.publishes, output)
    }

    override def read(input: PrimitiveInputStream): StandardStatistics = {
      StandardStatistics(
        MeasurementsConverter.read(input),
        MeasurementsConverter.read(input),
        MeasurementsConverter.read(input),
        MeasurementsConverter.read(input),
        MeasurementsConverter.read(input)
      )
    }
  }

  implicit object ViralMetricsConverter extends ComplexByteConverter[intelligence.ViralMetrics] {
    override def write(metrics: ViralMetrics, output: PrimitiveOutputStream) {
      output.writeLong(metrics.tweets)
      output.writeLong(metrics.retweets)
      output.writeLong(metrics.facebookClicks)
      output.writeLong(metrics.facebookLikes)
      output.writeLong(metrics.facebookShares)
      output.writeLong(metrics.influencers)
      output.writeLong(metrics.stumbleUponShares)
      output.writeLong(metrics.facebookComments)
      output.writeLong(metrics.deliciousShares)
      output.writeLong(metrics.redditShares)
      output.writeLong(metrics.googlePlusOneShares)
      output.writeLong(metrics.diggs)
      output.writeLong(metrics.pinterestShares)
      output.writeLong(metrics.linkedInShares)
      output.writeLong(metrics.facebookTotals)
      output.writeLong(metrics.maleTotals)
      output.writeLong(metrics.femaleTotals)
      output.writeLong(metrics.mostlyMaleTotals)
      output.writeLong(metrics.mostlyFemaleTotals)
      output.writeLong(metrics.unisexTotals)
    }

    override def read(input: PrimitiveInputStream): ViralMetrics = {
      ViralMetrics(input.readLong(), input.readLong(), input.readLong(), input.readLong(), input.readLong(), input.readLong(), safeReadField(input)(_.readLong(), 0), safeReadField(input)(_.readLong(), 0), safeReadField(input)(_.readLong(), 0), safeReadField(input)(_.readLong(), 0), safeReadField(input)(_.readLong(), 0), safeReadField(input)(_.readLong(), 0), safeReadField(input)(_.readLong(), 0), safeReadField(input)(_.readLong(), 0), safeReadField(input)(_.readLong(), 0), safeReadField(input)(_.readLong(), 0), safeReadField(input)(_.readLong(), 0), safeReadField(input)(_.readLong(), 0), safeReadField(input)(_.readLong(), 0), safeReadField(input)(_.readLong(), 0))
    }
  }

  implicit object ViralMetricsDateMidnightMapConverter extends MapConverter[GrvDateMidnight, ViralMetrics]

  implicit object ViralMetricsDateHourMapConverter extends MapConverter[DateHour, ViralMetrics]

  implicit object SponsoredMetricsKeyConverter extends ComplexByteConverter[SponsoredMetricsKey] with RecommendationMetricsKeyConverter {

    override def write(data: SponsoredMetricsKey, output: PrimitiveOutputStream) {
      writeBase(data, output)
      output.writeObj(data.campaignKey)
      output.writeObj(data.cpc)
    }

    override def read(input: PrimitiveInputStream): SponsoredMetricsKey = {
      val recoMetrics = readBase(input)
      SponsoredMetricsKey(recoMetrics, input.readObj[CampaignKey], input.readObj[DollarValue])
    }
  }

  implicit object ArticleSponsoredMetricsKeyConverter extends ComplexByteConverter[ArticleSponsoredMetricsKey] with RecommendationMetricsKeyConverter {

    override def write(data: ArticleSponsoredMetricsKey, output: PrimitiveOutputStream) {
      writeBase(data, output)
      output.writeObj(data.campaignKey)
      output.writeObj(data.cpc)
      data.articleKeyOption match {
        case Some(articleKey) => {
          output.writeBoolean(true)
          output.writeObj(articleKey)
        }
        case None => {
          output.writeBoolean(false)
        }
      }
    }

    override def read(input: PrimitiveInputStream): ArticleSponsoredMetricsKey = {
      val recoMetrics = readBase(input)
      val campaignKey = input.readObj[CampaignKey]
      val cpc = input.readObj[DollarValue]
      if (input.readBoolean()) {
        ArticleSponsoredMetricsKey(recoMetrics, campaignKey, cpc, input.readObj[ArticleKey])
      }
      else {
        ArticleSponsoredMetricsKey(recoMetrics, campaignKey, cpc)
      }
    }
  }

  implicit object RollupRecommendationMetricsKeyConverter extends ComplexByteConverter[RollupRecommendationMetricKey] {
    self: ComplexByteConverter[_] =>

    override def write(data: RollupRecommendationMetricKey, output: PrimitiveOutputStream) {
      output.writeLong(-data.dateHour.getMillis)
      output.writeObj(data.siteKey)
      output.writeShort(data.placementId.toShort)
      output.writeInt(data.bucketId)
      output.writeInt(data.geoLocationId)
      output.writeByte(data.countBy)
    }

    override def read(input: PrimitiveInputStream): RollupRecommendationMetricKey = {
      val dateHour = DateHour(-input.readLong())
      val siteKey = input.readObj[SiteKey](SiteKeyConverter)
      val placementId = self.safeReadField(input)(_.readShort(), -1.toShort)
      val bucketId = input.readInt()
      val geoLocationId = self.safeReadField(input)(_.readInt(), ArticleRecommendationMetricKey.defaultGeoLocationId)
      val defaultByte: Byte = 2 //want to default to unit impression, because all of the value written before we added this field were those!
      val countByByte = self.safeReadField(input)(_.readByte(), defaultByte)
      val countBy = countByByte
      RollupRecommendationMetricKey(dateHour, siteKey, placementId, bucketId, geoLocationId, countBy)
    }
  }

  implicit object TopArticleRecommendationMetricKeyConverter extends ComplexByteConverter[TopArticleRecommendationMetricKey] with TopArticlesRecommendationMetricsKeyConverter {

    override def write(data: TopArticleRecommendationMetricKey, output: PrimitiveOutputStream) {
      writeBase(data, output)
    }

    override def read(input: PrimitiveInputStream): TopArticleRecommendationMetricKey = {
      val recoMetrics = readBase(input)
      TopArticleRecommendationMetricKey(recoMetrics)
    }
  }

  implicit object CampaignMetricsKeyConverter extends ComplexByteConverter[CampaignMetricsKey] with RecommendationMetricsKeyConverter {

    override def write(data: CampaignMetricsKey, output: PrimitiveOutputStream) {
      writeBase(data, output)
      output.writeObj(data.cpc)
    }

    override def read(input: PrimitiveInputStream): CampaignMetricsKey = {
      val recoMetrics = readBase(input)
      CampaignMetricsKey(recoMetrics, input.readObj[DollarValue])
    }
  }

  trait TopArticlesRecommendationMetricsKeyConverter {
    self: ComplexByteConverter[_] =>

    def writeBase(data: ArticleRecommendationMetricsKey, output: PrimitiveOutputStream) {
      output.writeLong(-data.dateHour.getMillis)
      output.writeByte(data.countBy)
      output.writeInt(data.bucketId)
      output.writeObj(data.siteKey)
      output.writeShort(data.placementId.toShort)
      output.writeInt(data.sitePlacementId)
      output.writeInt(data.geoLocationId)
      output.writeInt(data.algoId)
      output.writeLong(data.articleId)
    }

    def readBase(input: PrimitiveInputStream): ArticleRecommendationMetricsKey = new ArticleRecommendationMetricsKey {
      val dateHour: DateHour = DateHour(-input.readLong())
      val countBy: Byte = input.readByte()
      val bucketId: Int = input.readInt()
      val siteKey: SiteKey = input.readObj[SiteKey](SiteKeyConverter)
      val placementId: Int = self.safeReadField(input)(_.readShort(), -1.toShort).toInt
      val sitePlacementId: Int = self.safeReadField(input)(_.readInt(), ArticleRecommendationMetricKey.defaultGeoLocationId)
      val geoLocationId: Int = self.safeReadField(input)(_.readInt(), ArticleRecommendationMetricKey.defaultGeoLocationId)
      val algoId: Int = self.safeReadField(input)(_.readInt(), ArticleRecommendationMetricKey.defaultAlgoId)
      val articleId: Long = input.readLong()
    }
  }

  implicit object StatisticsIdConverter extends ComplexByteConverter[StatisticsId] {
    def write(data: StatisticsId, output: PrimitiveOutputStream) {
      output.writeShort(data.id)
    }

    def read(input: PrimitiveInputStream): StatisticsId = StatisticsId(input.readShort())
  }

  implicit object StatisticsRangedKeyConverter extends ComplexByteConverter[StatisticsRangedKey] {
    def write(data: StatisticsRangedKey, output: PrimitiveOutputStream) {
      output.writeObj(data.by)
      output.writeObj(data.range)
    }

    def read(input: PrimitiveInputStream): StatisticsRangedKey = StatisticsRangedKey(input.readObj[StatisticsId], input.readObj[DateMidnightRange])
  }

  implicit object ArticleRecommendationMetricKeyConverter extends ComplexByteConverter[ArticleRecommendationMetricKey] with RecommendationMetricsKeyConverter {

    override def write(data: ArticleRecommendationMetricKey, output: PrimitiveOutputStream) {
      writeBase(data, output)
    }

    override def read(input: PrimitiveInputStream): ArticleRecommendationMetricKey = {
      val recoMetrics = readBase(input)
      ArticleRecommendationMetricKey(recoMetrics)
    }
  }

  implicit object ArticleRecommendationMetricKeyLongMapConverter extends MapConverter[ArticleRecommendationMetricKey, Long]

  implicit object DateHourRecommendationMetricsMapConverter extends MapConverter[DateHour, RecommendationMetrics]

  implicit object DateHourRecommendationMetricsSeqTupleConverter extends ComplexByteConverter[Seq[(DateHour, RecommendationMetrics)]] {
    val currentVersion = 1
    override def write(data: Seq[(DateHour, RecommendationMetrics)], output: PrimitiveOutputStream) {
      output.writeInt(currentVersion)
      output.writeInt(data.size)
      data.foreach(i => {
        output.writeObj(i._1)
        output.writeObj(i._2)
      })
    }

    override def read(input: PrimitiveInputStream): Seq[(DateHour, RecommendationMetrics)] = {
      val version : Int = input.readInt()
      val size = input.readInt
      val fs = for (i <- 0 until size) yield {
        val dh = input.readObj[DateHour]
        val rm = input.readObj[RecommendationMetrics]
        (dh, rm)
      }
      fs
    }
  }
}
