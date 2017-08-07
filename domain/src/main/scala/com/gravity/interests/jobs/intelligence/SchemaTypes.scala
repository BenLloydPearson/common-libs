package com.gravity.interests.jobs.intelligence

import com.gravity.api.partnertagging.{AdvertiserTypeEnum, ContentCategoryEnum, ContentRatingEnum}
import com.gravity.domain.articles.{Author, AuthorData}
import com.gravity.hbase.schema._
import com.gravity.interests.graphs.graphing.ScoredTerm
import com.gravity.interests.jobs.intelligence.StoredGraph.NodeConverter
import com.gravity.interests.jobs.intelligence.hbase.ScopedKey
import com.gravity.interests.jobs.intelligence.hbase.ScopedKeyConverters.ScopedKeyConverter
import com.gravity.interests.jobs.intelligence.operations.analytics.{CountGroups, Counts, ReportNode}
import com.gravity.interests.jobs.intelligence.operations.graphing.SiteConceptMetrics
import com.gravity.interests.jobs.intelligence.operations.{ArtStoryInfo, FacebookLike, HbRgTagRef, ScrubberEnum}
import com.gravity.interests.jobs.intelligence.schemas._
import com.gravity.interests.jobs.intelligence.schemas.byteconverters._
import com.gravity.utilities.analytics.TimeSliceResolution
import com.gravity.utilities.time.{DateHour, GrvDateMidnight}
import com.gravity.utilities.{ArticleReviewStatus, Settings}
import com.gravity.valueclasses.PubId
import com.gravity.valueclasses.ValueClassesForDomain._
import com.gravity.valueclasses.ValueClassesForUtilities._
import org.apache.hadoop.hbase.util.Bytes
import org.joda.time._
import org.openrdf.model.impl.URIImpl

import scala.collection._
import scalaz.syntax.std.option._

/*             )\._.,--....,'``.
.b--.        /;   _.. \   _\  (`._ ,.
`=,-,-'~~~   `----(,_..'--(,_..'`-.;.'  */

object SchemaTypes extends LowPrioritySchemaTypes with AolByteConverters with ArticleKeyByteConverters
with CnnByteConverters with AuditLogByteConverters with GenericByteConverters with SitePlacementByteConverters
with LshByteConverters with MetricsByteConverters with RecommendationByteConverters with UserByteConverters
with CampaignByteConverters with ArtRgByteConverters with HbRgByteConverters with ExchangeByteConverters {
  type CounterFunc = (String, Long) => Unit

  val CounterNoOp: (String, Long) => Unit = (str: String, value: Long) => {}

  //We're doing byte rather than numeric comparison, so -1 is maxed long bytes
  val longUpperBounds: Long = SchemaTypeHelpers.longUpperBounds
  val longLowerBounds: Long = SchemaTypeHelpers.longLowerBounds

  implicit object ConversionTrackingParamConverter extends ComplexByteConverter[ConversionTrackingParam] {
    def write(data: ConversionTrackingParam, output: PrimitiveOutputStream) {
      output.writeUTF(data.id)
      output.writeUTF(data.displayName)
      output.writeObj(data.canonicalUrl)
    }

    def read(input: PrimitiveInputStream): ConversionTrackingParam = {
      ConversionTrackingParam(input.readUTF(), input.readUTF(), input.readObj[Option[String]])
    }
  }

  implicit object ConverstionTrackingParamSetConverter extends SetConverter[ConversionTrackingParam]

  implicit object AuthorConverter extends ComplexByteConverter[Author] {
    val version = 0

    def write(data: Author, output: PrimitiveOutputStream) {
      output.writeByte(version)
      output.writeUTF(data.name)
      output.writeObj(data.urlOption)
      output.writeObj(data.titleOption)
      output.writeObj(data.imageOption)
    }

    def read(input: PrimitiveInputStream): Author = {
      input.readByte() match {
        case currentOrNewer if currentOrNewer >= version =>
          Author(input.readUTF(), input.readObj[Option[String]], input.readObj[Option[String]], input.readObj[Option[String]])
        case unsupported => throw new RuntimeException(UnsupportedVersionExcepion.buildMessage("Author data", unsupported.toInt, version))
      }
    }
  }

  implicit object AuthorDataConverter extends ComplexByteConverter[AuthorData] {

    implicit object AuthorSeqConverter extends SeqConverter[Author]

    val version = 0

    def write(data: AuthorData, output: PrimitiveOutputStream) {
      output.writeByte(version)
      output.writeObj(data.authors)
    }

    def read(input: PrimitiveInputStream): AuthorData = {
      input.readByte() match {
        case currentOrNewer if currentOrNewer >= version =>
          AuthorData(input.readObj[Seq[Author]])
        case unsupported => throw new RuntimeException(UnsupportedVersionExcepion.buildMessage("AuthorData", unsupported.toInt, version))
      }
    }
  }

  implicit object SectionRecoKeyConverter extends ComplexByteConverter[SegmentRecoKey] {
    override def write(key: SegmentRecoKey, output: PrimitiveOutputStream) {
      output.writeByte(SegmentRecoKey.version)
      output.writeLong(key.segmentId.sitePlacementId)
      output.writeInt(key.segmentId.bucketId)
      output.writeLong(key.segmentId.sitePlacementConfigVersion)
      output.writeLong(key.recommenderId)
      output.writeInt(key.slotIndex)
    }

    override def read(input: PrimitiveInputStream): SegmentRecoKey = {
      val version = input.readByte() //to discard the unused byte

      version match {
        case 1 =>
          SegmentRecoKey(DatabaseSegmentCompositeKey(input.readLong(), input.readInt(), -1), input.readLong(), -1)
        case 2 =>
          SegmentRecoKey(DatabaseSegmentCompositeKey(input.readLong(), input.readInt(), -1), input.readLong(), input.readInt())
        case 3 =>
          SegmentRecoKey(DatabaseSegmentCompositeKey(input.readLong(), input.readInt(), input.readLong()), input.readLong(), input.readInt())
      }
    }
  }

  implicit object HbaseNullConverter extends ComplexByteConverter[HbaseNull] {
    override def write(key: HbaseNull, output: PrimitiveOutputStream): Unit = {
    }

    override def read(input: PrimitiveInputStream): HbaseNull = {
      HbaseNull()
    }
  }

  implicit object DemographicKeyConverter extends ComplexByteConverter[DemographicKey] {
    val version = 1

    override def read(input: PrimitiveInputStream): DemographicKey = {
      val version = input.readByte()
      version match {
        case 1 =>
          DemographicKey(
            DemoSource(input.readByte()),
            Gender(input.readByte()),
            AgeGroup(input.readByte())
          )
        case unsupported =>
          throw new RuntimeException(UnsupportedVersionExcepion.buildMessage("DemographicKeyData", unsupported, version))
      }
    }

    override def write(data: DemographicKey, output: PrimitiveOutputStream): Unit = {
      output.writeByte(version)
      output.writeByte(data.demoSource.id)
      output.writeByte(data.gender.id)
      output.writeByte(data.ageGroup.id)
    }
  }

  implicit object SitePartnerPlacementDomainKeyConverter extends ComplexByteConverter[SitePartnerPlacementDomainKey] {
    override def write(data: SitePartnerPlacementDomainKey, output: PrimitiveOutputStream): Unit = {
      output.writeLong(data.sitePartnerPlacementDomainId)
    }

    override def read(input: PrimitiveInputStream): SitePartnerPlacementDomainKey = {
      SitePartnerPlacementDomainKey(input.readLong())
    }
  }

  implicit object PubIdConverter extends ComplexByteConverter[PubId] {
    override def write(data: PubId, output: PrimitiveOutputStream): Unit = {
      output.writeUTF(data.raw)
    }

    override def read(input: PrimitiveInputStream): PubId = {
      PubId(input.readUTF())
    }
  }

  implicit object SiteGuidConverter extends ComplexByteConverter[SiteGuid] {
    override def write(data: SiteGuid, output: PrimitiveOutputStream): Unit = {
      output.writeUTF(data.raw)
    }

    override def read(input: PrimitiveInputStream): SiteGuid = {
      SiteGuid(input.readUTF())
    }
  }

  implicit object DomainConverter extends ComplexByteConverter[Domain] {
    override def write(data: Domain, output: PrimitiveOutputStream): Unit = {
      output.writeUTF(data.raw)
    }

    override def read(input: PrimitiveInputStream): Domain = {
      Domain(input.readUTF())
    }
  }

  implicit object PartnerPlacementIdConverter extends ComplexByteConverter[PartnerPlacementId] {
    override def write(data: PartnerPlacementId, output: PrimitiveOutputStream): Unit = {
      output.writeUTF(data.raw)
    }

    override def read(input: PrimitiveInputStream): PartnerPlacementId = {
      PartnerPlacementId(input.readUTF())
    }
  }

  implicit object OrdinalKeyConverter extends ComplexByteConverter[OrdinalKey] {
    override def write(data: OrdinalKey, output: PrimitiveOutputStream): Unit = {
      output.writeInt(data.ordinal)
    }

    override def read(input: PrimitiveInputStream): OrdinalKey = {
      OrdinalKey(input.readInt)
    }
  }

  implicit object NodeKeyConverter extends ComplexByteConverter[NodeKey] {
    override def write(key: NodeKey, output: PrimitiveOutputStream) {
      output.writeLong(key.nodeId)
    }

    override def read(input: PrimitiveInputStream): NodeKey = {
      NodeKey(input.readLong())
    }
  }

  implicit object CovisitationKeyConverter extends ComplexByteConverter[CovisitationKey] {
    override def write(key: CovisitationKey, output: PrimitiveOutputStream) {
      output.writeLong(-key.hour)
      output.writeObj(key.key)
      output.writeInt(key.distance)
    }

    override def read(input: PrimitiveInputStream): CovisitationKey = {
      CovisitationKey(-input.readLong(), input.readObj[ArticleKey], input.readInt)
    }
  }

  implicit object IngestionTypesConverter extends ComplexByteConverter[IngestionTypes.Type] {
    def write(data: IngestionTypes.Type, output: PrimitiveOutputStream) {
      output.writeByte(data.id)
    }

    def read(input: PrimitiveInputStream): IngestionTypes.Type = IngestionTypes.parseOrDefault(input.readByte())
  }

  implicit object ArticleIngestionKeyConverter extends ComplexByteConverter[ArticleIngestionKey] {
    val version = 0

    def write(data: ArticleIngestionKey, output: PrimitiveOutputStream) {
      output.writeByte(version)
      output.writeLong(-data.timestamp.getMillis)
      output.writeObj(data.ingestionType)
    }

    def read(input: PrimitiveInputStream): ArticleIngestionKey = {
      input.readByte() match {
        case currentOrNewer if currentOrNewer >= version => {
          val millis = -input.readLong()
          ArticleIngestionKey(new DateTime(millis), input.readObj[IngestionTypes.Type])
        }
        case unSupportedVersion => throw new RuntimeException("Version `" + unSupportedVersion + "` is not supported! Current version in this source: " + version)
      }
    }
  }

  implicit object ArticleTypesConverter extends ComplexByteConverter[ArticleTypes.Type] {
    def write(data: ArticleTypes.Type, output: PrimitiveOutputStream) {
      output.writeByte(data.id)
    }

    def read(input: PrimitiveInputStream): ArticleTypes.Type = ArticleTypes.parseOrDefault(input.readByte())
  }

  implicit object ArticleIngestionDataConverter extends ComplexByteConverter[ArticleIngestionData] {
 import com.gravity.logging.Logging._
    val maxNoteLen = 33000
    val version = 2

    def write(data: ArticleIngestionData, output: PrimitiveOutputStream) {
      output.writeByte(version)
      output.writeUTF(data.sourceSiteGuid)
      output.writeUTF(com.gravity.utilities.grvstrings.truncStringTo(data.ingestionNotes, maxNoteLen))
      output.writeObj(data.articleType)
      output.writeBoolean(data.isOrganicSource)
    }

    def read(input: PrimitiveInputStream): ArticleIngestionData = {
      input.readByte() match {
        case 0 => {
          ArticleIngestionData(input.readUTF(), input.readUTF(), ArticleTypes.content, isOrganicSource = true)
        }
        case 1 => {
          ArticleIngestionData(input.readUTF(), input.readUTF(), input.readObj[ArticleTypes.Type], isOrganicSource = true)
        }
        case currentOrGreater if currentOrGreater > 1 => {
          if (currentOrGreater != version) {
            warn("Current converter version is {0} but received version {1}. Will treat it as if it was serialized with version {0}.", version, currentOrGreater)
          }
          ArticleIngestionData(input.readUTF(), input.readUTF(), input.readObj[ArticleTypes.Type], input.readBoolean())
        }
        case unSupportedVersion => throw new RuntimeException("Version `" + unSupportedVersion + "` is not supported! Current version in this source: " + version)
      }
    }
  }

  case class TaggerWorkKey(pubDate: DateTime, articleKey: ArticleKey)

  implicit object TaggerWorkKeyConverter extends ComplexByteConverter[TaggerWorkKey] {
    def write(data: TaggerWorkKey, output: PrimitiveOutputStream) {
      output.writeLong(-data.pubDate.getMillis)
      output.writeObj(data.articleKey)
    }

    def read(input: PrimitiveInputStream): TaggerWorkKey = {
      TaggerWorkKey(new DateTime(-input.readLong), input.readObj[ArticleKey])
    }
  }

  implicit object ClickStreamKeyConverter extends ComplexByteConverter[ClickStreamKey] {
    override def write(data: ClickStreamKey, output: PrimitiveOutputStream) {
      output.writeLong(-data.hour.getMillis)
      output.writeShort(data.clickType.id)
      output.writeObj(data.articleKey)
      output.writeObj(ArticleKey.empty)
    }

    override def read(input: PrimitiveInputStream): ClickStreamKey = {
      val res = ClickStreamKey(
        DateHour(-input.readLong()),
        ClickType.getOrDefault(input.readShort().toInt),
        input.readObj[ArticleKey]
      )

      // must read this out even though we no longer need to have it, serialization needs to stay NOT corrupted ;)
      input.readObj[ArticleKey]

      res
    }
  }

  implicit object SponsoredArticleKeyConverter extends ComplexByteConverter[SponsoredArticleKey] {
    override def write(key: SponsoredArticleKey, output: PrimitiveOutputStream) {
      output.writeObj(key.key)
    }

    override def read(input: PrimitiveInputStream): SponsoredArticleKey = {
      SponsoredArticleKey(input.readObj[ArticleKey])
    }
  }

  implicit object SponsoredArticleDataConverter extends ComplexByteConverter[SponsoredArticleData] {
    override def write(key: SponsoredArticleData, output: PrimitiveOutputStream) {
      output.writeObj(key.site)(SiteKeyConverter)
    }

    override def read(input: PrimitiveInputStream): SponsoredArticleData = {
      SponsoredArticleData(input.readObj[SiteKey](SiteKeyConverter))
    }
  }

  implicit object PublishDateAndArticleKeyConverter extends ComplexByteConverter[PublishDateAndArticleKey] {
    override def write(key: PublishDateAndArticleKey, output: PrimitiveOutputStream) {
      output.writeLong(-key.publishDate.getMillis)
      output.writeLong(key.articleKey.articleId)
    }

    override def read(input: PrimitiveInputStream): PublishDateAndArticleKey = {
      PublishDateAndArticleKey(new DateTime(-input.readLong()), ArticleKey(input.readLong()))
    }
  }

  implicit object TimeSliceResolutionConverter extends ComplexByteConverter[TimeSliceResolution] {
    override def write(period: TimeSliceResolution, output: PrimitiveOutputStream) {
      output.writeUTF(period.resolution)
      output.writeInt(period.year)
      output.writeInt(period.point)
    }

    override def read(input: PrimitiveInputStream): TimeSliceResolution = {
      TimeSliceResolution(input.readUTF(), input.readInt(), input.readInt())
    }
  }

  implicit object GrvDateMidnightConverter extends ComplexByteConverter[GrvDateMidnight] {
    override def write(dm: GrvDateMidnight, output: PrimitiveOutputStream) {
      output.writeLong(dm.getMillis)
    }

    override def read(input: PrimitiveInputStream): GrvDateMidnight = new GrvDateMidnight(input.readLong())


    def apply(year: Int, day: Int): GrvDateMidnight = new GrvDateMidnight().withYear(year).withDayOfYear(day)
  }

  implicit object ArticleRangeSortedKeyConverter extends ComplexByteConverter[ArticleRangeSortedKey] {
    override def write(data: ArticleRangeSortedKey, output: PrimitiveOutputStream) {
      output.writeLong(data.metricsInterval.getStartMillis)
      output.writeLong(data.metricsInterval.getEndMillis)
      output.writeLong(data.publishInterval.getStartMillis)
      output.writeLong(data.publishInterval.getEndMillis)
      output.writeLong(data.sortId)
      output.writeBoolean(data.isDescending)
    }

    override def read(input: PrimitiveInputStream): ArticleRangeSortedKey = {
      ArticleRangeSortedKey(
        new Interval(
          input.readLong(),
          input.readLong()),
        new Interval(
          input.readLong(),
          input.readLong()),
        input.readLong(),
        input.readBoolean())
    }
  }

  implicit object ReportKeyConverter extends ComplexByteConverter[ReportKey] {
    override def write(data: ReportKey, output: PrimitiveOutputStream) {
      output.writeUTF(data.reportType)
      output.writeObj(data.timePeriod)
      output.writeLong(data.sortById)
      output.writeBoolean(data.sortDescending)
      output.writeBoolean(data.isGrouped)
    }

    override def read(input: PrimitiveInputStream): ReportKey = ReportKey(input.readUTF(), input.readObj[TimeSliceResolution], input.readLong(), input.readBoolean(), input.readBoolean())
  }


  implicit object DatedArticleViewsConverter extends ComplexByteConverter[DatedArticleViews] {
    override def write(views: DatedArticleViews, output: PrimitiveOutputStream) {
      output.writeLong(views.dateTime.getMillis)

      ArticleKeyConverter.write(views.articleId, output)
      output.writeBoolean(views.doNotTrack)
      ArticleKeyConverter.write(views.referrer, output)
    }

    override def read(input: PrimitiveInputStream): DatedArticleViews = {
      DatedArticleViews(new DateTime(input.readLong()), ArticleKeyConverter.read(input), input.readBoolean(), ArticleKeyConverter.read(input))
    }
  }

  /*
 Supports a URI that's interoperable with the Ontology stuff
  */
  implicit object RdfUriConverter extends ByteConverter[org.openrdf.model.URI] {
    override def toBytes(uri: org.openrdf.model.URI): Array[Byte] = Bytes.toBytes(uri.stringValue())

    override def fromBytes(bytes: Array[Byte], offset: Int, length: Int): URIImpl = new org.openrdf.model.impl.URIImpl(Bytes.toString(bytes, offset, length))
  }

  implicit object FacebookLikeConverter extends ComplexByteConverter[FacebookLike] {
    override def write(like: FacebookLike, output: PrimitiveOutputStream) {
      output.writeLong(like.id)
      output.writeUTF(like.name)
      output.writeUTF(like.category)
      output.writeUTF(like.image)
      output.writeLong(like.createdAtTimestamp)
    }

    override def read(input: PrimitiveInputStream): FacebookLike = FacebookLike(input.readLong(), input.readUTF(), input.readUTF(), input.readUTF(), input.readLong())
  }

  implicit object FacebookLikesSetConverter extends SetConverter[FacebookLike]

  implicit object SiteGuidWithYearDayConverter extends ComplexByteConverter[SiteGuidWithYearDay] {
    override def write(metrics: SiteGuidWithYearDay, output: PrimitiveOutputStream) {
      output.writeUTF(metrics.siteGuid)
      output.writeInt(metrics.year)
      output.writeInt(metrics.day)
    }

    override def read(input: PrimitiveInputStream): SiteGuidWithYearDay = {
      SiteGuidWithYearDay(input.readUTF(), input.readInt, input.readInt)
    }
  }

  implicit object SectionKeyConverter extends ComplexByteConverter[SectionKey] {
    override def write(key: SectionKey, output: PrimitiveOutputStream) {
      output.writeLong(key.siteId)
      output.writeLong(key.sectionId)
    }

    override def read(input: PrimitiveInputStream): SectionKey = {
      SectionKey(input.readLong(), input.readLong())
    }
  }

  implicit object SectionKeySeq extends SeqConverter[SectionKey]

  implicit object SectionKeySet extends SetConverter[SectionKey]

  implicit object EverythingKeyConverter extends ComplexByteConverter[EverythingKey] {
    val version = 1

    def write(data: EverythingKey, output: PrimitiveOutputStream) {
      output.writeByte(version)
    }

    def read(input: PrimitiveInputStream): EverythingKey.type = {
      val version = input.readByte()
      EverythingKey
    }
  }

  implicit object SiteKeyConverter extends ComplexByteConverter[SiteKey] {
    override def write(key: SiteKey, output: PrimitiveOutputStream) {
      output.writeLong(key.siteId)
    }

    override def read(input: PrimitiveInputStream): SiteKey = {
      SiteKey(input.readLong())
    }
  }

  implicit object RevenueModelKeyConverter extends ComplexByteConverter[RevenueModelKey] {
    override def write(key: RevenueModelKey, output: PrimitiveOutputStream) {
      output.writeLong(key.sitePlacementId)
    }

    override def read(input: PrimitiveInputStream): RevenueModelKey = {
      RevenueModelKey(input.readLong())
    }
  }

  implicit object LDAClusterKeyConverter extends ComplexByteConverter[LDAClusterKey] {
    def write(data: LDAClusterKey, output: PrimitiveOutputStream) {
      output.writeLong(data.topicModelTimestamp)
      output.writeInt(data.topicId)
      output.writeInt(data.profileId)
    }

    def read(input: PrimitiveInputStream): LDAClusterKey = LDAClusterKey(
      topicModelTimestamp = input.readLong(),
      topicId = input.readInt(),
      profileId = input.readInt()
    )
  }

  implicit object ContentGroupKeyConverter extends ComplexByteConverter[ContentGroupKey] {
    def write(data: ContentGroupKey, output: PrimitiveOutputStream): Unit = {
      output.writeLong(data.contentGroupId)
    }

    def read(input: PrimitiveInputStream): ContentGroupKey = {
      ContentGroupKey(input.readLong())
    }
  }

  implicit object SiteAlgoSettingKeyConverter extends ComplexByteConverter[SiteAlgoSettingKey] {
    override def write(data: SiteAlgoSettingKey, output: PrimitiveOutputStream): Unit = {
      output.writeUTF(data.algoSettingKey)
      output.writeUTF(data.siteGuid)
    }

    override def read(input: PrimitiveInputStream): SiteAlgoSettingKey = {
      val algoSettingKey = input.readUTF()
      val siteGuid = input.readUTF()
      SiteAlgoSettingKey(algoSettingKey, siteGuid)
    }
  }

  implicit object SiteAlgoKeyConverter extends ComplexByteConverter[SiteAlgoKey] {
    override def write(data: SiteAlgoKey, output: PrimitiveOutputStream): Unit = {
      output.writeInt(data.algoId)
      output.writeUTF(data.siteGuid)
    }

    override def read(input: PrimitiveInputStream): SiteAlgoKey = {
      val algoId = input.readInt()
      val siteGuid = input.readUTF()
      SiteAlgoKey(algoId, siteGuid)
    }
  }

  implicit object SiteRecoWorkDivisionKeyConverter extends ComplexByteConverter[SiteRecoWorkDivisionKey] {
    override def write(data: SiteRecoWorkDivisionKey, output: PrimitiveOutputStream): Unit = {
      output.writeUTF(data.hostname)
      output.writeUTF(data.siteGuid)
    }

    override def read(input: PrimitiveInputStream): SiteRecoWorkDivisionKey = {
      val hostname = input.readUTF()
      val siteGuid = input.readUTF()
      SiteRecoWorkDivisionKey(hostname, siteGuid)
    }
  }

  implicit object OntologyNodeAndTypeKeyConverter extends ComplexByteConverter[OntologyNodeAndTypeKey] {
    override def write(key: OntologyNodeAndTypeKey, output: PrimitiveOutputStream) {
      output.writeLong(key.nodeId)
      output.writeShort(key.nodeTypeId)
    }

    override def read(input: PrimitiveInputStream): OntologyNodeAndTypeKey = {
      val nodeId = input.readLong()
      val nodeTypeId = input.readShort()
      OntologyNodeAndTypeKey(nodeId, nodeTypeId)
    }
  }

  implicit object MultiKeyConverter extends ComplexByteConverter[MultiKey] {
    override def write(key: MultiKey, output: PrimitiveOutputStream) {
      output.writeInt(key.keys.size)
      key.keys.foreach(k => output.writeObj(k)(ScopedKeyConverter))
    }

    override def read(input: PrimitiveInputStream): MultiKey = {
      val size = input.readInt()
      val keys = for (i <- 0 until size) yield input.readObj[ScopedKey](ScopedKeyConverter)
      MultiKey(keys)
    }
  }

  implicit object EntityScopeKeyConverter extends ComplexByteConverter[EntityScopeKey] {
    override def write(key: EntityScopeKey, output: PrimitiveOutputStream) {
      output.writeUTF(key.entity)
      output.writeObj(key.scopedKey)
    }

    override def read(input: PrimitiveInputStream): EntityScopeKey = {
      val region = input.readUTF()
      val scopedKey = input.readObj[ScopedKey]
      EntityScopeKey(region, scopedKey)
    }
  }

  implicit object DimensionScopeKeyConverter extends ComplexByteConverter[DimensionScopeKey] {
    override def write(key: DimensionScopeKey, output: PrimitiveOutputStream) {
      output.writeUTF(key.dimension)
      output.writeObj(key.scopedKey)
    }

    override def read(input: PrimitiveInputStream): DimensionScopeKey = {
      val dimension = input.readUTF()
      val scopedKey = input.readObj[ScopedKey]
      DimensionScopeKey(dimension, scopedKey)
    }
  }

  implicit object SiteKeyPPIDKeyConverter extends ComplexByteConverter[SiteKeyPPIDKey] {

    override def write(data: SiteKeyPPIDKey, output: PrimitiveOutputStream) {
      output.writeLong(data.siteKey.siteId)
      output.writeUTF(data.PPID)
      output.writeByte(data.device.i)
      output.writeInt(data.geoLocationId)
    }

    override def read(input: PrimitiveInputStream): SiteKeyPPIDKey = {
      val siteId = input.readLong()
      val PPID = input.readUTF()
      val (device, geoLocationId) =
        try {
          (Device.getOrDefault(input.readByte()), input.readInt())
        }
        catch {
          case e: Exception => (Device.unknown, 0)
        }
      SiteKeyPPIDKey(SiteKey(siteId), PPID, device, geoLocationId)
    }
  }

  implicit object SiteKeyDomainKeyConverter extends ComplexByteConverter[SiteKeyDomainKey] {

    override def write(data: SiteKeyDomainKey, output: PrimitiveOutputStream) {
      output.writeLong(data.siteKey.siteId)
      output.writeUTF(data.domain)
      output.writeByte(data.device.i)
      output.writeInt(data.geoLocationId)
    }

    override def read(input: PrimitiveInputStream): SiteKeyDomainKey = {
      val siteId = input.readLong()
      val domain = input.readUTF()
      val (device, geoLocationId) =
        try {
          (Device.getOrDefault(input.readByte()), input.readInt())
        }
        catch {
          case e: Exception => (Device.unknown, 0)
        }
      SiteKeyDomainKey(SiteKey(siteId), domain, device, geoLocationId)
    }
  }

  implicit object SiteKeyPPIDDomainKeyConverter extends ComplexByteConverter[SiteKeyPPIDDomainKey] {

    override def write(data: SiteKeyPPIDDomainKey, output: PrimitiveOutputStream) {
      output.writeLong(data.siteKey.siteId)
      output.writeUTF(data.PPID)
      output.writeUTF(data.domain)
      output.writeByte(data.device.i)
      output.writeInt(data.geoLocationId)
    }

    override def read(input: PrimitiveInputStream): SiteKeyPPIDDomainKey = {
      val siteId = input.readLong()
      val PPID = input.readUTF()

      val domain = input.readUTF()
      val (device, geoLocationId) =
        try {
          (Device.getOrDefault(input.readByte()), input.readInt())
        }
        catch {
          case e: Exception => (Device.unknown, 0)
        }

      SiteKeyPPIDDomainKey(SiteKey(siteId), PPID, domain, device, geoLocationId)
    }
  }


  implicit object SiteKeySetConverter extends SetConverter[SiteKey]

  implicit object MaxSpendTypesConverter extends ComplexByteConverter[MaxSpendTypes.Type] {
    def write(data: MaxSpendTypes.Type, output: PrimitiveOutputStream) {
      output.writeByte(data.id)
    }

    def read(input: PrimitiveInputStream): MaxSpendTypes.Type = MaxSpendTypes.parseOrDefault(input.readByte())
  }

  implicit object BudgetSettingsConverter extends ComplexByteConverter[BudgetSettings] {
    def write(data: BudgetSettings, output: PrimitiveOutputStream) {
      output.writeInt(data.budgets.size)
      data.budgets.foreach(b => {
        output.writeLong(b.maxSpend.pennies)
        output.writeByte(b.maxSpendType.id)
      })
    }

    def read(input: PrimitiveInputStream): BudgetSettings = {
      try {
        val size = input.readInt
        val budgets = for (i <- 0 until size) yield {
          Budget(DollarValue(input.readLong), MaxSpendTypes.parseOrDefault(input.readByte()))
        }

        BudgetSettings(budgets.toSeq)
      } catch {
        case ex:Exception => BudgetSettings()
      }
    }
  }

  implicit object BudgetConverter extends ComplexByteConverter[Budget] {
    override def write(data: Budget, output: PrimitiveOutputStream) {
      output.writeObj(data.maxSpend)
      output.writeObj(data.maxSpendType)
    }

    override def read(input: PrimitiveInputStream): Budget = {
      val maxSpend = input.readObj[DollarValue]
      val maxSpendType = input.readObj[MaxSpendTypes.Type]
      Budget(maxSpend, maxSpendType)
    }
  }

  implicit object OptionBudgetConverter extends ComplexByteConverter[Option[Budget]] {
    override def write(data: Option[Budget], output: PrimitiveOutputStream) {
      if(data.isEmpty)
        output.writeBoolean(false)
      else {
        output.writeBoolean(true)
        output.writeObj(data.get)
      }
    }

    override def read(input: PrimitiveInputStream): Option[Budget] = {
      if(input.readBoolean) {
        Some(input.readObj[Budget])
      }
      else {
        None
      }
    }
  }

  implicit object RevenueModelTypesConverter extends ComplexByteConverter[RevenueModelTypes.Type] {
    def write(data: RevenueModelTypes.Type, output: PrimitiveOutputStream) {
      output.writeByte(data.id)
    }

    def read(input: PrimitiveInputStream): RevenueModelTypes.Type = RevenueModelTypes.parseOrDefault(input.readByte())
  }

  implicit object NewRevenueModelTypesConverter extends ComplexByteConverter[NewRevenueModelTypes.Type] {
    def write(data: NewRevenueModelTypes.Type, output: PrimitiveOutputStream) {
      output.writeByte(data.id)
    }

    def read(input: PrimitiveInputStream): NewRevenueModelTypes.Type = NewRevenueModelTypes.parseOrDefault(input.readByte())
  }

  //  implicit object CampaignBudgetTypesConverter extends ComplexByteConverter[CampaignBudgetTypes.Type] {
  //    def write(data: CampaignBudgetTypes.Type, output: PrimitiveOutputStream) {
  //      output.writeByte(data.id)
  //    }
  //
  //    def read(input: PrimitiveInputStream): CampaignBudgetTypes.Type =
  //      CampaignBudgetTypes.parseOrDefault(input.readByte())
  //  }
  //
  //  implicit object CampaignBudgetAmountConverter extends ComplexByteConverter[CampaignBudgetAmount] {
  //    override def write(data: CampaignBudgetAmount, output: PrimitiveOutputStream) {
  //      output.writeObj(data.kind)
  //      output.writeInt(data.dollars)
  //    }
  //
  //    override def read(input: PrimitiveInputStream): CampaignBudgetAmount = {
  //      val kind = input.readObj[CampaignBudgetTypes.Type]
  //      val dollars = input.readInt()
  //      CampaignBudgetAmount(dollars, kind)
  //    }
  //  }
  //
  //  implicit object OptionCampaignBudgetAmountConverter extends ComplexByteConverter[Option[CampaignBudgetAmount]] {
  //    override def write(data: Option[CampaignBudgetAmount], output: PrimitiveOutputStream) {
  //      if(data.isEmpty)
  //        output.writeBoolean(false)
  //      else {
  //        output.writeBoolean(true)
  //        output.writeObj(data.get)
  //      }
  //    }
  //
  //    override def read(input: PrimitiveInputStream): Option[CampaignBudgetAmount] = {
  //      if(input.readBoolean())
  //        Some(input.readObj[CampaignBudgetAmount])
  //      else
  //        None
  //    }
  //  }


  implicit object RevenueModelConverter extends ComplexByteConverter[RevenueModelData] {
    override def write(data: RevenueModelData, output: PrimitiveOutputStream) {
      output.writeObj(data.revenueTypeId)
      data match {
        case RevShareModelData(percentage, tech) =>
          output.writeDouble(percentage)
          output.writeDouble(tech)
        case GuaranteedImpressionRPMModelData(rpm) => output.writeObj[DollarValue](rpm)
        case RevShareWithGuaranteedImpressionRPMFloorModelData(percentage, rpm, tech) =>
          output.writeDouble(percentage)
          output.writeObj[DollarValue](rpm)
          output.writeDouble(tech)
      }
    }

    override def read(input: PrimitiveInputStream): RevenueModelData = {
      val revenueType = input.readObj[NewRevenueModelTypes.RevenueModelType]
      revenueType match {
        case NewRevenueModelTypes.REV => RevShareModelData(input.readDouble, input.readDouble)
        case NewRevenueModelTypes.RPM => GuaranteedImpressionRPMModelData(input.readObj[DollarValue])
        case NewRevenueModelTypes.REVRPM => RevShareWithGuaranteedImpressionRPMFloorModelData(input.readDouble, input.readObj[DollarValue], input.readDouble)
      }
    }
  }

  implicit object ArticleIngestingKeyConverter extends ComplexByteConverter[ArticleIngestingKey] {
    override def write(data: ArticleIngestingKey, output: PrimitiveOutputStream) {
      output.writeObj(data.articleKey)
      if (data.isEmpty) {
        output.writeBoolean(true)
      } else {
        output.writeBoolean(false)
        data.campaignKeyOption match {
          case Some(ck) => {
            output.writeBoolean(true)
            output.writeObj(ck)
          }
          case None => {
            output.writeBoolean(false)
            output.writeUTF(data.siteGuidOption.getOrElse(""))
          }
        }
      }
    }

    override def read(input: PrimitiveInputStream): ArticleIngestingKey = {
      val ak = input.readObj[ArticleKey]
      val isEmpty = input.readBoolean()

      if (isEmpty) {
        ArticleIngestingKey(ak, None, None)
      } else {
        val isCampaign = input.readBoolean()
        if (isCampaign) {
          ArticleIngestingKey(ak, input.readObj[CampaignKey])
        } else {
          ArticleIngestingKey(ak, input.readUTF())
        }
      }
    }
  }

  implicit object AdvertiserTypeEnumConverter extends ComplexByteConverter[AdvertiserTypeEnum.Type] {
    // These methods appropriate for a GrvEnum[Byte]...
    def write(data: AdvertiserTypeEnum.Type, output: PrimitiveOutputStream) {
      output.writeByte(data.id)
    }

    def read(input: PrimitiveInputStream): AdvertiserTypeEnum.Type = AdvertiserTypeEnum.parseOrDefault(input.readByte())
  }

  implicit object ContentCategoryEnumConverter extends ComplexByteConverter[ContentCategoryEnum.Type] {
    // These methods appropriate for a GrvEnum[Short]...
    def write(data: ContentCategoryEnum.Type, output: PrimitiveOutputStream) {
      output.writeShort(data.id)
    }

    def read(input: PrimitiveInputStream): ContentCategoryEnum.Type = ContentCategoryEnum.parseOrDefault(input.readShort())
  }

  implicit object ContentCategoryEnumSetConverter extends SetConverter[ContentCategoryEnum.Type]

  implicit object ContentRatingEnumConverter extends ComplexByteConverter[ContentRatingEnum.Type] {
    // These methods appropriate for a GrvEnum[Byte]...
    def write(data: ContentRatingEnum.Type, output: PrimitiveOutputStream) {
      output.writeByte(data.id)
    }

    def read(input: PrimitiveInputStream): ContentRatingEnum.Type = ContentRatingEnum.parseOrDefault(input.readByte())
  }

  implicit object ScrubberEnumConverter extends ComplexByteConverter[ScrubberEnum.Type] {
    def write(data: ScrubberEnum.Type, output: PrimitiveOutputStream) {
      output.writeByte(data.id)
    }

    def read(input: PrimitiveInputStream): ScrubberEnum.Type = ScrubberEnum.parseOrDefault(input.readByte())
  }

  implicit object RssFeedSettingsConverter extends ComplexByteConverter[RssFeedSettings] {
    override def write(data: RssFeedSettings, output: PrimitiveOutputStream) {
      output.writeBoolean(data.skipAlreadyIngested)
      output.writeBoolean(data.ignoreMissingImages)
      output.writeObj(data.initialArticleStatus)
      output.writeObj(data.feedStatus)
      output.writeObj(data.scrubberEnum)
      output.writeBoolean(data.initialArticleBlacklisted)
    }

    override def read(input: PrimitiveInputStream): RssFeedSettings = {
      RssFeedSettings(
        skipAlreadyIngested = input.readBoolean(),
        ignoreMissingImages = input.readBoolean(),
        initialArticleStatus = input.readObj[CampaignArticleStatus.Type],
        feedStatus = input.readObj[CampaignArticleStatus.Type],
        scrubberEnum = input.readObj[ScrubberEnum.Type],
        safeReadField(input)(_.readBoolean(), true)
      )
    }
  }

  implicit object SiteTopicKeyConverter extends ComplexByteConverter[SiteTopicKey] {
    override def write(key: SiteTopicKey, output: PrimitiveOutputStream) {
      // swap order to prevent hot-spotting
      output.writeLong(key.topicId)
      output.writeLong(key.siteId)
    }

    override def read(input: PrimitiveInputStream): SiteTopicKey = {
      val topicId = input.readLong()
      val siteId = input.readLong()
      SiteTopicKey(siteId, topicId)
    }
  }

  implicit object SiteTopicKeySeqConverter extends SeqConverter[SiteTopicKey]

  implicit object ArticleReviewStatusConverter extends ComplexByteConverter[ArticleReviewStatus.Type] {
    def write(data: ArticleReviewStatus.Type, output: PrimitiveOutputStream) {
      output.writeByte(data.id)
    }

    override def read(input: PrimitiveInputStream): ArticleReviewStatus.Type = {
      ArticleReviewStatus.parseOrDefault(input.readByte())
    }
  }

  implicit object ArticleReviewSettingsConverter extends ComplexByteConverter[ArticleReviewSettings] {
    def write(data: ArticleReviewSettings, output: PrimitiveOutputStream) {
      output.writeInt(data.status.i)
    }

    def read(input: PrimitiveInputStream): ArticleReviewSettings = {
      val status = ArticleReviewStatus.parseOrDefault(input.readInt())
      ArticleReviewSettings(status)
    }
  }

  implicit object ArticleCheckoutInfoConverter extends ComplexByteConverter[ArticleCheckoutInfo] {
    override def write(aci: ArticleCheckoutInfo, output: PrimitiveOutputStream) {
      output.writeLong(aci.date.getMillis)
      output.writeLong(aci.userId)
    }

    override def read(input: PrimitiveInputStream): ArticleCheckoutInfo = {
      ArticleCheckoutInfo(new DateTime(input.readLong()), input.readLong())
    }
  }

  implicit object MeasurementsConverter extends ComplexByteConverter[Measurements] {
    override def write(m: Measurements, output: PrimitiveOutputStream) {
      output.writeDouble(m.mean)
      output.writeDouble(m.variance)
    }

    override def read(input: PrimitiveInputStream): Measurements = {
      Measurements(input.readDouble(), input.readDouble())
    }
  }

  implicit object SiteReportKeyConverter extends ComplexByteConverter[SiteReportKey] {
    override def write(data: SiteReportKey, output: PrimitiveOutputStream) {
      output.writeObj(data.siteKey)
      output.writeObj(data.reportKey)
      output.write(data.sortData)
      output.writeLong(data.id)
    }

    def read(input: PrimitiveInputStream): SiteReportKey = {
      val sk = input.readObj[SiteKey]
      val rk = input.readObj[ReportKey]
      val sortData = new Array[Byte](8)
      input.read(sortData, 0, 8)
      val id = input.readLong()
      SiteReportKey(sk, rk, sortData, id)
    }
  }

  implicit object ReportOpportunityScoresConverter extends ComplexByteConverter[ReportOpportunityScores] {
    def write(data: ReportOpportunityScores, output: PrimitiveOutputStream) {
      output.writeDouble(data.oppScore)
      output.writeDouble(data.oppScoreFeature)
      output.writeDouble(data.oppScoreWrite)
    }

    def read(input: PrimitiveInputStream): ReportOpportunityScores = ReportOpportunityScores(input.readDouble(), input.readDouble(), input.readDouble())
  }

  implicit object ReportZScoresConverter extends ComplexByteConverter[ReportZScores] {
    def write(data: ReportZScores, output: PrimitiveOutputStream) {
      output.writeDouble(data.published)
      output.writeDouble(data.viewed)
      output.writeDouble(data.socialReferred)
      output.writeDouble(data.searchReferred)
      output.writeDouble(data.opportunity)
      output.writeDouble(data.adjustedOpportunity)
      output.writeDouble(data.viral)
      output.writeDouble(data.zViral)
    }

    def read(input: PrimitiveInputStream): ReportZScores = ReportZScores(
      input.readDouble(),
      input.readDouble(),
      input.readDouble(),
      input.readDouble(),
      input.readDouble(),
      input.readDouble(),
      input.readDouble(),
      input.readDouble())
  }

  implicit object ReportInterestScoresConverter extends ComplexByteConverter[ReportInterestScores] {
    def write(data: ReportInterestScores, output: PrimitiveOutputStream) {
      ReportZScoresConverter.write(data.keyPage, output)
      ReportZScoresConverter.write(data.nonKeyPage, output)
      ReportZScoresConverter.write(data.topicTotal, output)
    }

    def read(input: PrimitiveInputStream): ReportInterestScores = {
      ReportInterestScores(
        ReportZScoresConverter.read(input),
        ReportZScoresConverter.read(input),
        ReportZScoresConverter.read(input)
      )
    }
  }

  implicit object ReportTopicConverter extends ComplexByteConverter[ReportTopic] {
    def write(data: ReportTopic, output: PrimitiveOutputStream) {
      output.writeUTF(data.name)
      output.writeUTF(data.uri)
      output.writeLong(data.id)
      output.writeObj(data.opportunityScores)
      output.writeObj(data.metrics)
      output.writeObj(data.zscores)
      output.writeObj(data.viralMetrics)
      output.writeDouble(data.viralVelocity)
      output.writeLong(data.visitors)
      output.writeObj(data.measurements)
      output.writeObj(data.siteMeasurements)
    }

    def read(input: PrimitiveInputStream): ReportTopic = {
      ReportTopic(
        input.readUTF(),
        input.readUTF(),
        input.readLong(),
        input.readObj[ReportOpportunityScores],
        input.readObj[StandardMetrics],
        input.readObj[ReportInterestScores],
        input.readObj[ViralMetrics],
        input.readDouble(),
        input.readLong(),
        safeReadField(input)(_.readObj[StandardStatistics], StandardStatistics.empty),
        safeReadField(input)(_.readObj[StandardStatistics], StandardStatistics.empty)
      )
    }
  }

  implicit object ReportTopicSeqConverter extends SeqConverter[ReportTopic]

  implicit object ReportInterestConverter extends ComplexByteConverter[ReportInterest] {
    def write(data: ReportInterest, output: PrimitiveOutputStream) {
      output.writeUTF(data.name)
      output.writeUTF(data.uri)
      output.writeLong(data.id)
      output.writeInt(data.level)
      output.writeObj(data.scores)
      output.writeObj(data.metrics)
      output.writeObj(data.topics)
      output.writeObj(data.lowerInterests)(ReportInterestBufferConverter)
      output.writeObj(data.zscores)
      output.writeObj(data.viralMetrics)
      output.writeDouble(data.viralVelocity)
      output.writeLong(data.visitors)
      output.writeObj(data.measurements)
      output.writeObj(data.siteMeasurements)
    }

    def read(input: PrimitiveInputStream): ReportInterest = {
      ReportInterest(input.readUTF(), input.readUTF(), input.readLong(), input.readInt(),
        input.readObj[ReportOpportunityScores],
        input.readObj[StandardMetrics],
        input.readObj[Seq[ReportTopic]],
        input.readObj[mutable.Buffer[ReportInterest]],
        input.readObj[ReportInterestScores],
        input.readObj[ViralMetrics],
        input.readDouble(),
        input.readLong(),
        safeReadField(input)(_.readObj[StandardStatistics], StandardStatistics.empty),
        safeReadField(input)(_.readObj[StandardStatistics], StandardStatistics.empty)
      )
    }
  }

  implicit object ReportInterestBufferConverter extends BufferConverter[ReportInterest]

  implicit object ReportInterestSeqConverter extends SeqConverter[ReportInterest]

  implicit object InterestCountsConverter extends ComplexByteConverter[InterestCounts] {
    def write(data: InterestCounts, output: PrimitiveOutputStream) {
      output.writeUTF(data.uri)
      output.writeLong(data.viewCount)
      output.writeLong(data.publishedArticles)
      output.writeLong(data.socialReferrers)
      output.writeLong(data.searchReferrers)
      output.writeLong(data.keyPageReferrers)
      output.writeUTF(data.name)
      output.writeObj(data.viralMetrics)
      output.writeDouble(data.viralVelocity)
      output.writeLong(data.visitors)
    }

    def read(input: PrimitiveInputStream): InterestCounts = InterestCounts(
      input.readUTF(),
      input.readLong(),
      input.readLong(),
      input.readLong(),
      input.readLong(),
      input.readLong(),
      input.readUTF(),
      input.readObj[ViralMetrics],
      input.readDouble(),
      input.readLong()
    )
  }


  implicit object InterestCountsSeqConverter extends SeqConverter[InterestCounts]

  implicit object CountsConverter extends ComplexByteConverter[Counts] {
    def write(data: Counts, output: PrimitiveOutputStream) {
      output.writeLong(data.views)
      output.writeLong(data.publishedArticles)
      output.writeLong(data.socialReferrers)
      output.writeLong(data.searchReferrers)
    }

    def read(input: PrimitiveInputStream): Counts = Counts(input.readLong(), input.readLong(), input.readLong(), input.readLong())
  }

  implicit object CountGroupsConverter extends ComplexByteConverter[CountGroups] {
    def write(data: CountGroups, output: PrimitiveOutputStream) {
      output.writeObj(data.keyPage)
      output.writeObj(data.nonKeyPage)
      output.writeObj(data.topicTotal)
    }

    def read(input: PrimitiveInputStream): CountGroups = CountGroups(input.readObj[Counts], input.readObj[Counts], input.readObj[Counts])
  }

  implicit object ReportNodeConverter extends ComplexByteConverter[ReportNode] {
    def write(data: ReportNode, output: PrimitiveOutputStream) {
      output.writeObj(data.node)(NodeConverter)
      output.writeObj(data.metrics)
      output.writeObj(data.viralMetrics)
      output.writeDouble(data.velocity)
      output.writeLong(data.visitors)
      output.writeObj(data.graph)
      output.writeObj(data.counts)
      output.writeObj(data.zscores)
      output.writeObj(data.measurements)
      output.writeObj(data.siteMeasurements)
    }

    def read(input: PrimitiveInputStream): ReportNode = ReportNode(
      input.readObj[Node],
      input.readObj[StandardMetrics],
      input.readObj[ViralMetrics],
      input.readDouble(),
      input.readLong(),
      input.readObj[StoredGraph],
      input.readObj[CountGroups],
      input.readObj[ReportInterestScores],
      safeReadField(input)(_.readObj[StandardStatistics], StandardStatistics.empty),
      safeReadField(input)(_.readObj[StandardStatistics], StandardStatistics.empty)
    )
  }

  implicit object GraphKeyConverter extends ComplexByteConverter[GraphKey] {
    def write(data: GraphKey, output: PrimitiveOutputStream) {
      output.writeInt(data.algo)
    }

    def read(input: PrimitiveInputStream): GraphKey = GraphKey(input.readInt)
  }

  implicit object ArticleImageConverter extends ComplexByteConverter[ArticleImage] {
    def write(data: ArticleImage, output: PrimitiveOutputStream) {
      output.writeUTF(data.path)
      output.writeInt(data.height)
      output.writeInt(data.width)
      output.writeUTF(data.altText)
    }

    def read(input: PrimitiveInputStream): ArticleImage = ArticleImage(input.readUTF, input.readInt, input.readInt, input.readUTF)
  }

  implicit object GiltDealConverter extends ComplexByteConverter[GiltDeal] {
    def write(data: GiltDeal, output: PrimitiveOutputStream) {
      output.writeUTF(data.id)
      output.writeLong(data.expireTime.getMillis)
      output.writeDouble(data.price)
      output.writeDouble(data.value)
      output.writeUTF(data.soldOut)
    }

    def read(input: PrimitiveInputStream): GiltDeal = GiltDeal(input.readUTF, new DateTime(input.readLong), input.readDouble, input.readDouble, input.readUTF)
  }

  implicit object ArticleImageSeq extends SeqConverter[ArticleImage]

  implicit object GiltDealSeq extends SeqConverter[GiltDeal]

  implicit object ScoredTermConverter extends ComplexByteConverter[ScoredTerm] {
    def write(data: ScoredTerm, output: PrimitiveOutputStream) {
      output.writeUTF(data.term)
      output.writeDouble(data.score)
    }

    def read(input: PrimitiveInputStream): ScoredTerm = ScoredTerm(input.readUTF, input.readDouble)
  }

  implicit object ScoredTermSeqConverter extends SeqConverter[ScoredTerm]


  implicit object TaskResultKeyConverter extends ComplexByteConverter[TaskResultKey] {
    override def write(key: TaskResultKey, output: PrimitiveOutputStream) {
      output.writeLong(key.jobId)
      output.writeLong(key.taskId)
    }

    override def read(input: PrimitiveInputStream): TaskResultKey = {
      TaskResultKey(input.readLong(), input.readLong())
    }
  }

  implicit object SiteConceptMetricsConverter extends ComplexByteConverter[SiteConceptMetrics] {
    override def write(concept: SiteConceptMetrics, output: PrimitiveOutputStream) {
      output.writeUTF(concept.nodeUri)
      output.writeInt(concept.frequency)
      output.writeInt(concept.groupFrequency)
    }

    override def read(input: PrimitiveInputStream): SiteConceptMetrics = {
      SiteConceptMetrics(input.readUTF(), input.readInt(), input.readInt())
    }
  }

  implicit object SiteConceptMetricsSeqConverter extends SeqConverter[SiteConceptMetrics]

  implicit object SectionTopicKeyConverter extends ComplexByteConverter[SectionTopicKey] {

    override def write(k: SectionTopicKey, output: PrimitiveOutputStream) {
      output.writeObj(k.section)
      output.writeLong(k.topicId)
    }

    override def read(input: PrimitiveInputStream): SectionTopicKey = {
      val section = input.readObj[SectionKey]
      val topicId = input.readLong()
      SectionTopicKey(section, topicId)
    }

  }

  implicit object BlockedReasonSetConverter extends SetConverter[BlockedReason.Type]

  implicit object OptionScopedKeyConverter extends ComplexByteConverter[Option[ScopedKey]] {
    def write(data: Option[ScopedKey], output: PrimitiveOutputStream) {
      data match {
        case Some(s) => {
          output.writeBoolean(true)
          output.writeObj(s)
        }
        case None => output.writeBoolean(false)
      }
    }

    def read(input: PrimitiveInputStream): Option[ScopedKey] = {
      if (input.readBoolean()) Some(input.readObj[ScopedKey]) else None
    }
  }

  implicit object OptionIntConverter extends ComplexByteConverter[Option[Int]] {
    def write(data: Option[Int], output: PrimitiveOutputStream) {
      data match {
        case Some(s) => {
          output.writeBoolean(true)
          output.writeInt(s)
        }
        case None => output.writeBoolean(false)
      }
    }

    def read(input: PrimitiveInputStream): Option[Int] = {
      if (input.readBoolean()) Some(input.readInt()) else None
    }
  }

  implicit object OptionLongConverter extends ComplexByteConverter[Option[Long]] {
    def write(data: Option[Long], output: PrimitiveOutputStream) {
      data match {
        case Some(s) => {
          output.writeBoolean(true)
          output.writeLong(s)
        }
        case None => output.writeBoolean(false)
      }
    }

    def read(input: PrimitiveInputStream): Option[Long] = {
      if (input.readBoolean()) Some(input.readLong()) else None
    }
  }

  //
  // grv:map support
  //

  implicit object ArtGrvMapOneScopeKeyConverter extends ComplexByteConverter[ArtGrvMap.OneScopeKey] {
    val version = 1

    def write(data: ArtGrvMap.OneScopeKey, output: PrimitiveOutputStream) {
      output.writeByte(version)
      output.writeObj(data._1)
      output.writeUTF(data._2)
    }

    def read(input: PrimitiveInputStream): ArtGrvMap.OneScopeKey = input.readByte() match {
      case currentOrNewer if currentOrNewer >= version => {
        (input.readObj[Option[ScopedKey]], input.readUTF())
      }
      case unsupported => throw new RuntimeException(UnsupportedVersionExcepion.buildMessage("ArtGrvMapOneScopeKey", unsupported.toInt, version))
    }
  }

  implicit object ArtGrvMapMetaValConverter extends ComplexByteConverter[ArtGrvMapMetaVal] {
    val version = 1

    def write(data: ArtGrvMapMetaVal, output: PrimitiveOutputStream) {
      output.writeByte(version)
      output.writeBoolean(data.isPrivate)
      output.writeUTF(data.xsdTypeName)
      output.writeUTF(data.grvMapVal)
    }

    def read(input: PrimitiveInputStream): ArtGrvMapMetaVal = input.readByte() match {
      case currentOrNewer if currentOrNewer >= version => {
        ArtGrvMapMetaVal(input.readBoolean(), input.readUTF(), input.readUTF())
      }
      case unsupported => throw new RuntimeException(UnsupportedVersionExcepion.buildMessage("ArtGrvMapMetaVal", unsupported.toInt, version))
    }
  }

  // e.g. for artAllGrvMap.values() objects.
  implicit object          OneScopeMapByteConverter extends          MapConverter[String, ArtGrvMapMetaVal]
  implicit object ImmutableOneScopeMapByteConverter extends ImmutableMapConverter[String, ArtGrvMapMetaVal]

  implicit object AllScopesMapByteConverter extends MapConverter[ArtGrvMap.OneScopeKey, ArtGrvMap.OneScopeMap]

  // This one is for Bulk Import/Export.
  implicit object ArtCkGrvMapConverter extends MapConverter[String, scala.collection.Map[String, ArtGrvMapMetaVal]]

  implicit object RelegenceTagsMapConverter extends MapConverter[Long, HbRgTagRef]

  implicit object ArtStoryInfoConverter extends ComplexByteConverter[ArtStoryInfo] {
    val minReadableVersion = 1
    val maxReadableVersion = 1
    val writingVersion     = 1

    override def write(data: ArtStoryInfo, output: PrimitiveOutputStream): Unit = {
      // The following fields are all in version 1 or later.
      output.writeInt(writingVersion)

      output.writeLong(data.storyId)

      if (data.grvIngestionTime.isEmpty) {
        output.writeBoolean(false)
      } else {
        output.writeBoolean(true)
        output.writeLong(data.grvIngestionTime.get)
      }

      output.writeLong(data.creationTime)
      output.writeLong(data.lastUpdate)
      output.writeLong(data.avgAccessTime)
      output.writeUTF(data.title)
      output.writeDouble(data.magScore)
      output.writeInt(data.facebookShares)
      output.writeInt(data.facebookLikes)
      output.writeInt(data.facebookComments)
      output.writeInt(data.twitterRetweets)
      output.writeInt(data.numTotalDocs)
      output.writeInt(data.numOriginalDocs)
      output.writeDouble(data.socialDistPercentage)
      output.writeObj(data.entTags)
      output.writeObj(data.subTags)
    }

    override def read(input: PrimitiveInputStream): ArtStoryInfo = {
      val version: Int = input.readInt

      if (version < minReadableVersion || version > maxReadableVersion) {
        throw new RuntimeException(UnsupportedVersionExcepion.buildMessage("ArtStoryInfo", version, minReadableVersion, maxReadableVersion))
      } else {
        ArtStoryInfo(
          input.readLong,
          if (input.readBoolean == false) None else input.readLong.some,
          input.readLong,
          input.readLong,
          input.readLong,
          input.readUTF,
          input.readDouble,
          input.readInt,
          input.readInt,
          input.readInt,
          input.readInt,
          input.readInt,
          input.readInt,
          input.readDouble,
          input.readObj[Map[Long, HbRgTagRef]],
          input.readObj[Map[Long, HbRgTagRef]]
        )
      }
    }
  }

  implicit object OptionDateTimeConverter extends ComplexByteConverter[Option[DateTime]] {
    val currentVersion = 1
    def write(data: Option[DateTime], output: PrimitiveOutputStream) {
      output.writeInt(currentVersion)
      data match {
        case Some(s) => {
          output.writeBoolean(true)
          output.writeObj(s)
        }
        case None => output.writeBoolean(false)
      }
    }

    def read(input: PrimitiveInputStream): Option[DateTime] = {
      val version : Int = input.readInt()
      if (input.readBoolean()) Some(input.readObj[DateTime]) else None
    }
  }

  implicit object CovisitationEventLiteConverter extends ComplexByteConverter[CovisitationEventLite] {
    val currentVersion = 1
    override def write(data: CovisitationEventLite, output: PrimitiveOutputStream): Unit = {
      output.writeInt(currentVersion)
      output.writeObj(data.source)(ArticleKeyConverter)
      output.writeObj(data.destination)(ArticleKeyConverter)
      output.writeInt(data.distance)
      output.writeLong(data.hour)
      output.writeLong(data.views)
    }

    override def read(input: PrimitiveInputStream): CovisitationEventLite = {
      val version : Int = input.readInt()
      val source = input.readObj(ArticleKeyConverter)
      val destination = input.readObj(ArticleKeyConverter)
      val distance = input.readInt()
      val hour = input.readLong()
      val views = input.readLong()
      CovisitationEventLite(source, destination, hour, distance, views)
    }
  }

  implicit object CovisitationEventLiteSeqConverter extends SeqConverter[CovisitationEventLite]

  // end ArticleCandidate support

  // CampaignIngestion Converters

  // For CachedImagesTable:

  implicit object MD5HashKeyConverter extends ComplexByteConverter[MD5HashKey] {
    override def write(data: MD5HashKey, output: PrimitiveOutputStream): Unit = {
      output.writeUTF(data.hexstr)
    }

    override def read(input: PrimitiveInputStream): MD5HashKey = {
      MD5HashKey(input.readUTF())
    }
  }

  implicit object ImageUrlConverter extends ComplexByteConverter[ImageUrl] {
    override def write(data: ImageUrl, output: PrimitiveOutputStream): Unit = {
      output.writeUTF(data.raw)
    }

    override def read(input: PrimitiveInputStream): ImageUrl = {
      ImageUrl(input.readUTF())
    }
  }

  implicit object ImageShapeAndSizeKeyConverter extends ComplexByteConverter[ImageShapeAndSizeKey] {
    val version = 0

    def write(data: ImageShapeAndSizeKey, output: PrimitiveOutputStream) {
      output.writeByte(version)
      output.writeUTF(data.shape)
    }

    def read(input: PrimitiveInputStream): ImageShapeAndSizeKey = input.readByte() match {
      case currentOrNewer if currentOrNewer >= version => {
        ImageShapeAndSizeKey(
          shape = input.readUTF()
        )
      }
      case unsupported => throw new RuntimeException(UnsupportedVersionExcepion.buildMessage("ImageShapeAndSizeKey", unsupported.toInt, version))
    }
  }

  object ImageCacheDestScheme {
    val s3Ver1: ImageCacheDestScheme = ImageCacheDestScheme("s3", 1)

    lazy val s3Ver1CdnBaseUrl: String = Settings.getProperty("grvinterestimages.amazon.cdnbaseurl")  // "http://interestimages.grvcdn.com/"
  }

  case class ImageCacheDestScheme(dstId: String, verNum: Int) {
    def cdnUrl(protocol: String /* "http" or "https" */, dstObjKey: String): Option[String] = {
      if (dstObjKey == null || protocol == null)
        None
      else (dstId, verNum) match {
        case ("s3", 1) => {
          val httpUrl = s"${ImageCacheDestScheme.s3Ver1CdnBaseUrl}$dstObjKey"

          protocol.toLowerCase match {
            case "http"  => Option(httpUrl)
//            case "https" => Option(s"""$protocol${httpUrl.drop("http".length)}""")
            case _       => None
          }
        }

        case _ => None
      }
    }
  }

  implicit object ImageCacheDestSchemeConverter extends ComplexByteConverter[ImageCacheDestScheme] {
    val version = 0

    def write(data: ImageCacheDestScheme, output: PrimitiveOutputStream) {
      output.writeByte(version)
      output.writeUTF(data.dstId)
      output.writeInt(data.verNum)
    }

    def read(input: PrimitiveInputStream): ImageCacheDestScheme = input.readByte() match {
      case currentOrNewer if currentOrNewer >= version => {
        ImageCacheDestScheme(
          dstId  = input.readUTF(),
          verNum = input.readInt()
        )
      }
      case unsupported => throw new RuntimeException(UnsupportedVersionExcepion.buildMessage("ImageCacheDestScheme", unsupported.toInt, version))
    }
  }

  /**
   * Information about a single cached image (original or thumbnail).
   *
   * w/r/t dstScheme and dstObjKey, there will exist a method in the CachedImagesTable helper object e.g.:
   *   CachedImagesTable.cdnUrl(protocol: String /* "", "http", or "https" */, dstScheme, dstObjKey)
   * which will return the full CDN URL String to access the cached image using the given protocol.
   *
   * verWidthPix and verHeightPix are Option[Int] in case there are original image formats that we know or assume are valid,
   * but we can't extract (or haven't yet extracted) size info from them.
   */
  case class CachedImageInfo(dstScheme: ImageCacheDestScheme, // e.g. ImageCacheDestScheme.s3Ver1, meaning stored on Amazon S3, with S3 naming scheme 1
                             dstObjKey: String,               // e.g. "img/washingtonpost.com/9b292120470f8fac914a4c72a3130489-orig.jpg"
                             verContentType: String,          // e.g. "image/jpeg", "image/gif", etc.
                             verFileSize: Long,               // e.g. 57725L
                             verWidthPix: Option[Int],        // e.g. Some(400)
                             verHeightPix: Option[Int])       // e.g. Some(100)

  implicit object CachedImageInfoConverter extends ComplexByteConverter[CachedImageInfo] {
    val version = 0

    def write(data: CachedImageInfo, output: PrimitiveOutputStream) {
      output.writeByte(version)
      output.writeObj(data.dstScheme)
      output.writeUTF(data.dstObjKey)
      output.writeUTF(data.verContentType)
      output.writeLong(data.verFileSize)
      output.writeObj(data.verWidthPix)
      output.writeObj(data.verHeightPix)
    }

    def read(input: PrimitiveInputStream): CachedImageInfo = input.readByte() match {
      case currentOrNewer if currentOrNewer >= version =>
        CachedImageInfo(
          dstScheme      = input.readObj[ImageCacheDestScheme],
          dstObjKey      = input.readUTF(),
          verContentType = input.readUTF(),
          verFileSize    = input.readLong(),
          verWidthPix    = input.readObj[Option[Int]],
          verHeightPix   = input.readObj[Option[Int]])
      case unsupported => throw new RuntimeException(UnsupportedVersionExcepion.buildMessage("CachedImageInfo", unsupported.toInt, version))
    }
  }

  implicit object HostnameKeyConverter extends ComplexByteConverter[HostnameKey] {
    override def write(key: HostnameKey, output: PrimitiveOutputStream) {
      output.writeLong(key.hostname)
    }

    override def read(input: PrimitiveInputStream): HostnameKey = {
      HostnameKey(input.readLong())
    }
  }

  implicit object DimensionKeyConverter extends ComplexByteConverter[DimensionKey] {
    override def write(key: DimensionKey, output: PrimitiveOutputStream) {
      output.writeUTF(key.dimension)
    }

    override def read(input: PrimitiveInputStream) = {
      DimensionKey(input.readUTF())
    }
  }

  implicit object EntityKeyConverter extends ComplexByteConverter[EntityKey] {
    override def write(key: EntityKey, output: PrimitiveOutputStream) {
      output.writeUTF(key.entity)
    }

    override def read(input: PrimitiveInputStream) = {
      EntityKey(input.readUTF())
    }
  }

  implicit object RolenameKeyConverter extends ComplexByteConverter[RolenameKey] {
    override def write(key: RolenameKey, output: PrimitiveOutputStream) {
      output.writeUTF(key.role)
    }

    override def read(input: PrimitiveInputStream): RolenameKey = {
      val role = input.readUTF()
      RolenameKey(role)
    }
  }

  implicit object DashboardUserKeyConverter extends ComplexByteConverter[DashboardUserKey] {
    override def write(data: DashboardUserKey, output: PrimitiveOutputStream) {
      output.writeObj(data.siteKey)
      output.writeLong(data.userId)
    }

    override def read(input: PrimitiveInputStream): DashboardUserKey = DashboardUserKey(input.readObj[SiteKey], input.readLong())
  }

  implicit object UserFeedbackKeyConverter extends ComplexByteConverter[UserFeedbackKey] {
    val version = 0

    override def write(data: UserFeedbackKey, output: PrimitiveOutputStream): Unit = {
      output.writeByte(version)
      output.writeInt(data.feedbackVariation)
      output.writeInt(data.feedbackOption)
    }

    override def read(input: PrimitiveInputStream): UserFeedbackKey = input.readByte() match {
      case currentOrNewer if currentOrNewer >= version =>
        UserFeedbackKey(input.readInt(), input.readInt())
      case unsupported => throw new RuntimeException(UnsupportedVersionExcepion.buildMessage("UserFeedbackKey", unsupported.toInt, version))
    }
  }

  implicit object UserFeedbackByDayKeyConverter extends ComplexByteConverter[UserFeedbackByDayKey] {
    val version = 0

    override def write(data: UserFeedbackByDayKey, output: PrimitiveOutputStream): Unit = {
      output.writeByte(version)
      output.writeLong(data.day.getMillis)
      output.writeInt(data.feedbackVariation)
      output.writeInt(data.feedbackOption)
    }

    override def read(input: PrimitiveInputStream): UserFeedbackByDayKey = input.readByte() match {
      case currentOrNewer if currentOrNewer >= version =>
        UserFeedbackByDayKey(new GrvDateMidnight(input.readLong()), input.readInt(), input.readInt())
      case unsupported => throw new RuntimeException(UnsupportedVersionExcepion.buildMessage("UserFeedbackByDayKey", unsupported.toInt, version))
    }
  }

  implicit object ArticleAggregateByDayKeyConverter extends ComplexByteConverter[ArticleAggregateByDayKey] {
    val version = 0

    override def write(data: ArticleAggregateByDayKey, output: PrimitiveOutputStream): Unit = {
      output.writeByte(version)
      output.writeLong(data.day.getMillis)
      output.writeInt(data.aggregateType)
      output.writeByte(data.source)
    }

    override def read(input: PrimitiveInputStream): ArticleAggregateByDayKey = input.readByte() match {
      case currentOrNewer if currentOrNewer >= version =>
        ArticleAggregateByDayKey(new GrvDateMidnight(input.readLong()), input.readInt(), input.readByte())
      case unsupported => throw new RuntimeException(UnsupportedVersionExcepion.buildMessage("ArticleAggregateByDayKey", unsupported.toInt, version))
    }
  }

  implicit object ArticleTimeSpentKeyConverter extends ComplexByteConverter[ArticleTimeSpentKey] {
    override def write(data: ArticleTimeSpentKey, output: PrimitiveOutputStream): Unit = {
      output.writeShort(data.seconds)
    }
    override def read(input: PrimitiveInputStream): ArticleTimeSpentKey = ArticleTimeSpentKey(input.readShort())
  }

  implicit object ArticleTimeSpentByDayKeyConverter extends ComplexByteConverter[ArticleTimeSpentByDayKey] {
    override def write(data: ArticleTimeSpentByDayKey, output: PrimitiveOutputStream): Unit = {
      output.writeLong(data.day.getMillis)
      output.writeShort(data.timeSpent.seconds)
    }
    override def read(input: PrimitiveInputStream): ArticleTimeSpentByDayKey = {
      ArticleTimeSpentByDayKey(
        new GrvDateMidnight(input.readLong()),
        ArticleTimeSpentKey(input.readShort())
      )
    }
  }
}