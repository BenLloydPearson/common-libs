package com.gravity.interests.jobs.intelligence

import java.net.{InetAddress, URL}

import com.gravity.domain.articles.{ArticleAggregateSource, ArticleAggregateType}
import com.gravity.domain.grvstringconverters._
import com.gravity.interests.jobs.intelligence.hbase.ScopedKeyTypes.Type
import com.gravity.interests.jobs.intelligence.hbase._
import com.gravity.interests.jobs.intelligence.operations._
import com.gravity.interests.jobs.intelligence.operations.analytics.InterestSortBy
import com.gravity.interests.jobs.intelligence.operations.analytics.InterestSortBy.Keys
import com.gravity.interests.jobs.intelligence.schemas.DollarValue
import com.gravity.interests.jobs.intelligence.schemas.byteconverters.SchemaTypeHelpers
import com.gravity.service.grvroles
import com.gravity.utilities._
import com.gravity.utilities.analytics.URLUtils.NormalizedUrl
import com.gravity.utilities.analytics.articles._
import com.gravity.utilities.analytics.{DateMidnightRange, TimeSliceResolution}
import com.gravity.utilities.components.FailureResult
import com.gravity.utilities.grvenum.GrvEnum
import com.gravity.utilities.grvstrings._
import com.gravity.utilities.grvtime._
import com.gravity.utilities.grvz._
import com.gravity.utilities.time.{DateHour, GrvDateMidnight}
import com.gravity.valueclasses.PubId
import com.gravity.valueclasses.ValueClassesForDomain.{SitePlacementId, _}
import net.liftweb.json._
import org.apache.hadoop.hbase.util.Bytes
import org.joda.time.{DateTime, Interval, ReadableDateTime}
import org.openrdf.model.URI
import play.api.libs.functional.syntax._
import play.api.libs.json._

import scala.Predef._
import scala.collection.{Map, Seq, Set}
import scala.util.matching.Regex
import scalaz.syntax.std.option._
import scalaz.syntax.validation._
import scalaz.{NonEmptyList, ValidationNel}

object KeySchemas {}

case class StatisticsId(id: Short)

object StatisticsId {
  val byArticles: StatisticsId = StatisticsId(1)
  val bySiteNodes: StatisticsId = StatisticsId(2)
}

case class StatisticsRangedKey(by: StatisticsId, range: DateMidnightRange)

object StatisticsRangedKey {
  private def dmRanges = {
    val todayRange = TimeSliceResolution.today.range
    val lastSeven = TimeSliceResolution.lastSevenDays.range
    val lastThirty = TimeSliceResolution.lastThirtyDays.range
    val currentSeven = lastSeven.slideAheadDays(1)
    val currentThirty = lastThirty.slideAheadDays(1)

    Seq(todayRange, lastSeven, lastThirty, currentSeven, currentThirty) ++ lastThirty.singleDayRangesWithin
  }

  def allRanges(by: StatisticsId): scala.Seq[StatisticsRangedKey] = dmRanges.map(r => StatisticsRangedKey(by, r))
}

object GraphKey {
  val AnnoMatches: GraphKey = GraphKey(1)
  val AutoMatches: GraphKey = GraphKey(2)
  val IdentityGraph: GraphKey = GraphKey(3)
  val YesterdayGraph: GraphKey = GraphKey(4)
  val TodayGraph: GraphKey = GraphKey(4)
  val LastSevenDaysGraph: GraphKey = GraphKey(5)
  val LastThirtyDaysGraph: GraphKey = GraphKey(6)
  val ConceptGraph: GraphKey = GraphKey(7)
  val TfIdfGraph: GraphKey = GraphKey(8)
  val PhraseConceptGraph: GraphKey = GraphKey(8)
}

sealed trait EverythingKey extends CanBeScopedKey {
  def scope: Type = ScopedKeyTypes.ALL_SITES

  override def stringConverter: EverythingKeyStringConverter.type = EverythingKeyStringConverter
}

case object EverythingKey extends EverythingKey {
  override val toScopedKey: ScopedKey = super.toScopedKey // use a val as this is an identity object

  def fromEverythingScopedTo(toKey: CanBeScopedKey): ScopedFromToKey = {
    toKey.toScopedKey.fromEverythingToThisKey
  }
}

sealed trait NothingKey extends CanBeScopedKey {
  def scope: Type = ScopedKeyTypes.NOTHING_KEY

  override def stringConverter: NothingKeyStringConverter.type = NothingKeyStringConverter
}

case object NothingKey extends NothingKey {
  override val toScopedKey: ScopedKey = super.toScopedKey // use a val as this is an identity object
}

case class GraphKey(algo: Int)

object SiteKey {
  val empty: SiteKey = SiteKey(MurmurHash.hash64(emptyString))
  val zero: SiteKey = SiteKey(0l)

  def apply(siteGuid: String): SiteKey = SiteKey(MurmurHash.hash64(siteGuid))

  def everything: SiteKey = zero //This is a key that serves as a placeholder for ALL_SITES.

  val maxValue: SiteKey = SiteKey(SchemaTypeHelpers.longUpperBounds)
  val minValue: SiteKey = SiteKey(SchemaTypeHelpers.longLowerBounds)

  /** @deprecated Don't use this. This is only being used to aid transition to Play. */
  object TmpLiftSerializer extends Serializer[SiteKey] {
    override def deserialize(implicit format: Formats): PartialFunction[(TypeInfo, JValue), SiteKey] = {
      case (TypeInfo(cl, _), JString(keyStr)) if cl.toString.indexOf("SiteKey") != -1 => StringConverter.validateString(keyStr).fold(
        f => f.exceptionOption match {
          case Some(ex: Exception) => throw new MappingException(s"Couldn't parse $keyStr to SiteKey: ${f.message}", ex)
          case _ => throw new MappingException(s"Couldn't parse $keyStr to SiteKey: ${f.message}")
        },
        {
          case key: SiteKey => key
          case x => throw new MappingException(s"Parsed $keyStr to $x but needed SiteKey.")
        }
      )

      case (_, JObject(fields)) if fields.exists(_.name == "siteId") =>
        val siteIdOpt = fields.find(_.name == "siteId").flatMap(_.value match {
          case JInt(num) => Some(num.toLong)
          case _ => None
        })

        (for {
          siteId <- siteIdOpt
        } yield SiteKey(siteId)).getOrElse {
          throw new MappingException(s"Invalid fields $fields to create a SiteKey.")
        }
    }

    override def serialize(implicit format: Formats): PartialFunction[Any, JValue] = {
      case key: SiteKey => JString(StringConverter.writeString(key))
    }
  }
}

@SerialVersionUID(-4854032496293545704L)
case class SiteKey(siteId: Long) extends CanBeScopedKey {
  def scope: Type = ScopedKeyTypes.SITE

  /** Used to convert the child scoped key to a String that contains its type information (via StringConverter). */
  override def stringConverter: SiteKeyStringConverter.type = SiteKeyStringConverter
}

case class WidgetConfKey(widgetConfId: Long) extends CanBeScopedKey {
  def scope: Type = ScopedKeyTypes.WIDGET_CONF_KEY
  override def stringConverter: WidgetConfKeyStringConverter.type = WidgetConfKeyStringConverter
}

@SerialVersionUID(1l)
case class OntologyNodeAndTypeKey(nodeId: Long, nodeTypeId: Short) extends CanBeScopedKey {
  def scope: Type = ScopedKeyTypes.ONTOLOGY_NODE_AND_TYPE

  def nodeType: NodeType = NodeType.typesById(nodeTypeId)

  override def stringConverter: OntologyNodeAndTypeKeyStringConverter.type = OntologyNodeAndTypeKeyStringConverter
}

case class RevenueModelKey(sitePlacementId: Long)

object SiteKeys {
  val cnnMoney: SiteKey = SiteKey(ArticleWhitelist.siteGuid(_.CNN_MONEY))
  val wsj: SiteKey = SiteKey(ArticleWhitelist.siteGuid(_.WSJ))
}

case class LDAClusterKey(topicModelTimestamp: Long, topicId: Int, profileId: Int) extends CanBeScopedKey {
  def scope: Type = ScopedKeyTypes.LDA_CLUSTER

  override def stringConverter: LDAClusterKeyStringConverter.type = LDAClusterKeyStringConverter
}


case class SitePlacementWhyKey(why: String, placement: Long, siteId: Long) extends CanBeScopedKey {
  def scope: Type = ScopedKeyTypes.SITE_PLACEMENT_WHY

  //  def siteKey = SiteKey(siteId)

  override def toString: String = "{\"why\":" + why + ", \"sitePlacementId\":" + placement + ", \"siteId\":" + siteId + "}"

  override def stringConverter: SitePlacementWhyKeyStringConverter.type = SitePlacementWhyKeyStringConverter
}

case class ContentGroupKey(contentGroupId: Long) extends CanBeScopedKey {
  def scope: ScopedKeyTypes.Type = ScopedKeyTypes.CONTENT_GROUP
  
  override def stringConverter: ContentGroupKeyStringConverter.type = ContentGroupKeyStringConverter
}

object ContentGroupKey {
  implicit val jsonFormat: Format[ContentGroupKey] = Json.format[ContentGroupKey]
}

/**
 * @deprecated Use [[SitePlacementIdKey]].
  * @param placementIdOrSitePlacementId Whether placement ID (Int from now deprecated "Placements" concept) or site-placement
 *                                     ID (Long) is used depends entirely on context; please be sure to stay consistent.
 *                                     We are trying to get away from this bull shit but we can't fix it now. If you are
 *                                     working with SitePlacementsTable, then use site-placement ID (Long). If you are
 *                                     working with algo settings or sponsored metrics key, then use placement ID (the Int).
  * @see SitePlacementIdKey
 *
 */
@SerialVersionUID(5444054158279276802L)
case class SitePlacementKey(placementIdOrSitePlacementId: Long, siteId: Long) extends CanBeScopedKey {
  def scope: Type = ScopedKeyTypes.SITE_PLACEMENT

  def siteKey: SiteKey = SiteKey(siteId)

  override def toString: String = "{\"placementIdOrSitePlacementId\":" + placementIdOrSitePlacementId + ", \"siteId\":" + siteId + "}"
  
  override def stringConverter: SitePlacementKeyStringConverter.type = SitePlacementKeyStringConverter
}

case class RecommenderIdKey(recommenderId: Long, scopedKey: ScopedKey) extends CanBeScopedKey {
  def scope: Type = ScopedKeyTypes.RECOMMENDER_ID_KEY

  override def toString: String = "{\"recommenderId\":" + recommenderId + ", \"scopeKey\":" + scopedKey +  "}"

  override def stringConverter = RecommenderIdKeyStringConverter
}

case class DimensionScopeKey(dimension: String, scopedKey: ScopedKey) extends CanBeScopedKey {
  def scope = ScopedKeyTypes.DIMENSION_SCOPE_KEY

  override def toString: String = "{\"dimension\":" + dimension + ", \"scopeKey\":" + scopedKey +  "}"

  override def stringConverter = DimensionScopeKeyStringConverter
}

case class EntityScopeKey(entity: String, scopedKey: ScopedKey) extends CanBeScopedKey {
  def scope = ScopedKeyTypes.ENTITY_SCOPE_KEY

  override def toString: String = "{\"entity\":" + entity + ", \"scopeKey\":" + scopedKey +  "}"

  override def stringConverter = EntityScopeKeyStringConverter
}

case class MultiKey(keys: Seq[ScopedKey]) extends CanBeScopedKey {

  def scope: Type = ScopedKeyTypes.MULTI_KEY

  override def toString: String = "{ \"keys\": [" + keys.map(_.keyString).mkString(", ") + "] }"

  override def stringConverter: MultiKeyStringConverter.type = MultiKeyStringConverter

}

case class SiteRecoWorkDivisionKey(hostname: String, siteGuid: String) extends CanBeScopedKey {
  def scope: Type = ScopedKeyTypes.SITE_RECO_WORK_DIVISION_KEY

  def siteKey: SiteKey = SiteKey(siteGuid)

  override def toString: String = "{\"hostname\":" + hostname + ", \"siteGuid\":" + siteGuid + "}"

  override def stringConverter: SiteRecoWorkDivisionKeyStringConverter.type = SiteRecoWorkDivisionKeyStringConverter
}

case class SiteAlgoSettingKey(algoSettingKey: String, siteGuid: String) extends CanBeScopedKey {
  def scope: Type = ScopedKeyTypes.SITE_ALGO_SETTING_KEY

  def siteKey: SiteKey = SiteKey(siteGuid)

  override def toString: String = "{\"algoSettingKey\":" + algoSettingKey + ", \"siteGuid\":" + siteGuid + "}"

  override def stringConverter: SiteAlgoSettingKeyStringConverter.type = SiteAlgoSettingKeyStringConverter
}

case class SiteAlgoKey(algoId: Int, siteGuid: String) extends CanBeScopedKey {
  def scope: Type = ScopedKeyTypes.SITE_ALGO_KEY

  def siteKey: SiteKey = SiteKey(siteGuid)

  override def toString: String = "{\"algoId\":" + algoId + ", \"siteGuid\":" + siteGuid + "}"

  override def stringConverter: SiteAlgoKeyStringConverter.type = SiteAlgoKeyStringConverter
}

case class SitePlacementIdKey(id: SitePlacementId) extends CanBeScopedKey {
  override def scope: Type = ScopedKeyTypes.SITE_PLACEMENT_ID
  override def stringConverter: SitePlacementIdKeyStringConverter.type = SitePlacementIdKeyStringConverter
}

case class SitePlacementBucketKey(bucketId: Int, placement: Long, siteId: Long) extends CanBeScopedKey {
  def scope: Type = ScopedKeyTypes.SITE_PLACEMENT_BUCKET

  def siteKey: SiteKey = SiteKey(siteId)
  override def toString: String = "{\"bucketId\":" + bucketId + ", \"placementId\":" + placement + ", \"siteId\":" + siteId + "}"
  
  override def stringConverter: SitePlacementBucketKeyStringConverter.type = SitePlacementBucketKeyStringConverter
}

case class SiteKeyPPIDKey(siteKey: SiteKey, PPID: String, device: Device.Type, geoLocationId: Int) extends CanBeScopedKey {
  override def scope: Type = ScopedKeyTypes.SITEKEY_PPID

  /** Used to convert the child scoped key to a String that contains its type information (via StringConverter). */
  override def stringConverter: StringConverter[_ >: SiteKeyPPIDKey] = ???
}

case class SiteKeyPPIDDomainKey(siteKey: SiteKey, PPID: String, domain: String, device: Device.Type, geoLocationId: Int) extends CanBeScopedKey {
  override def scope: Type = ScopedKeyTypes.SITEKEY_PPID_DOMAIN

  /** Used to convert the child scoped key to a String that contains its type information (via StringConverter). */
  override def stringConverter: StringConverter[_ >: SiteKeyPPIDDomainKey] = ???
}

case class SiteKeyDomainKey(siteKey: SiteKey, domain: String, device: Device.Type, geoLocationId: Int) extends CanBeScopedKey {
  override def scope: Type = ScopedKeyTypes.SITEKEY_DOMAIN

  /** Used to convert the child scoped key to a String that contains its type information (via StringConverter). */
  override def stringConverter: StringConverter[_ >: SiteKeyDomainKey] = ???
}

/**
  * @see [[SitePlacementBucketKey]] It is not yet decided as to which is to be considered the de facto representation
  *                                 of this concept.
  */
case class SitePlacementBucketKeyAlt(bucketId: Int, sitePlacementId: Long) extends CanBeScopedKey {
  override def scope: Type = ScopedKeyTypes.SITE_PLACEMENT_BUCKET_ALT

  override def stringConverter: StringConverter[_ >: SitePlacementBucketKeyAlt.this.type] = ???
}

/**
  * @see [[SitePlacementBucketKeyAlt]] It is not yet decided as to which is to be considered the de facto representation
  *                                    of this concept.
  */
case class SitePlacementIdBucketKey(sitePlacementId: SitePlacementId, bucketId: BucketId) extends CanBeScopedKey {
  def scope: Type = ScopedKeyTypes.SITE_PLACEMENT_ID_BUCKET

  override def toString: String = "{\"bucketId\":" + bucketId.raw + ", \"sitePlacementId\":" + sitePlacementId + "}"

  override def stringConverter: StringConverter[_ >: SitePlacementIdBucketKey] = SitePlacementIdBucketKeyStringConverter
}


case class SitePlacementBucketSlotKey(slotIndex: Int, bucketId: Int, placement: Long, siteId: Long) extends CanBeScopedKey {
  def scope: Type = ScopedKeyTypes.SITE_PLACEMENT_BUCKET_SLOT

  def siteKey: SiteKey = SiteKey(siteId)
  override def toString: String = "{\"slotIndex:\":" + slotIndex + ", \"bucketId\":" + bucketId + ", \"placementId\":" + placement + ", \"siteId\":" + siteId + "}"
  
  override def stringConverter: SitePlacementBucketSlotKeyStringConverter.type = SitePlacementBucketSlotKeyStringConverter
}


/** Commonly used natural key for site-placement. */
case class SitePlacementNaturalKey(siteGuid: String, placementId: Int)

case class SiteReportKey(siteKey: SiteKey, reportKey: ReportKey, sortData: Array[Byte], id: Long) {
  require(sortData.length == 8, "Must be an 8 byte sort")
}

object ArticleKey {
  implicit val jsonFormat: Reads[ArticleKey] = CanBeScopedKey.jsonFormat.map {
    case ak: ArticleKey => ak
    case cbsk => throw new MappingException(s"Expected ArticleKey, got $cbsk.")
  }

  def apply(articleUrl: String): ArticleKey = ArticleKey(MurmurHash.hash64(articleUrl))

  def apply(articleUrl: URL): ArticleKey = ArticleKey(articleUrl.toString)

  /**
   * Almost all our articles are keyed by normalized URL.
   */
  def apply(normalizedUrl: NormalizedUrl): ArticleKey = ArticleKey(normalizedUrl.toString)

  val empty: ArticleKey = ArticleKey(0l)
  val emptyUrl: ArticleKey = ArticleKey(emptyString)
  val negativeOne: ArticleKey = ArticleKey(-1L)

  val min: ArticleKey = ArticleKey(SchemaTypeHelpers.longLowerBounds)
  val max: ArticleKey = ArticleKey(SchemaTypeHelpers.longUpperBounds)

  /** @deprecated Don't use this. This is only being used to aid transition to Play. */
  object TmpLiftSerializer extends Serializer[ArticleKey] {
    override def deserialize(implicit format: Formats): PartialFunction[(TypeInfo, JValue), ArticleKey] = {
      case (TypeInfo(cl, _), JString(keyStr)) if cl.toString.indexOf("ArticleKey") != -1 => StringConverter.validateString(keyStr).fold(
        f => f.exceptionOption match {
          case Some(ex: Exception) => throw new MappingException(s"Couldn't parse $keyStr to ArticleKey: ${f.message}", ex)
          case _ => throw new MappingException(s"Couldn't parse $keyStr to ArticleKey: ${f.message}")
        },
        {
          case key: ArticleKey => key
          case x => throw new MappingException(s"Parsed $keyStr to $x but needed ArticleKey.")
        }
      )

      case (_, JObject(fields)) if fields.exists(_.name == "articleId") =>
        val articleIdOpt = fields.find(_.name == "articleId").flatMap(_.value match {
          case JInt(num) => Some(num.toLong)
          case _ => None
        })
        
        (for {
          articleId <- articleIdOpt
        } yield ArticleKey(articleId)).getOrElse {
          throw new MappingException(s"Invalid fields $fields to create a ArticleKey.")
        }
    }

    override def serialize(implicit format: Formats): PartialFunction[Any, JValue] = {
      case key: ArticleKey => JString(StringConverter.writeString(key))
    }
  }
}

case class PublishDateAndArticleKey(publishDate: DateTime, articleKey: ArticleKey)

object PublishDateAndArticleKey {
  def partialByStartDate(dateTime: DateTime): PublishDateAndArticleKey = PublishDateAndArticleKey(dateTime, ArticleKey.min)

  def partialByEndDate(dateTime: DateTime): PublishDateAndArticleKey = PublishDateAndArticleKey(dateTime, ArticleKey.max)

}

object SitePartnerPlacementDomainKey {
  def apply(pubId: PubId): SitePartnerPlacementDomainKey = SitePartnerPlacementDomainKey(MurmurHash.hash64(pubId.raw))
}

case class SitePartnerPlacementDomainKey(sitePartnerPlacementDomainId: Long) extends CanBeScopedKey {
  def scope: Type = ScopedKeyTypes.PPID_KEY

  override def stringConverter: SitePartnerPlacementDomainKeyStringConverter.type = SitePartnerPlacementDomainKeyStringConverter
}

case class NodeKey(nodeId: Long) extends CanBeScopedKey {
  def scope: Type = ScopedKeyTypes.NODE

  override def stringConverter: NodeKeyStringConverter.type = NodeKeyStringConverter
}

case class ArticleOrdinalKey(articleKey:ArticleKey, ordinal:Int) extends CanBeScopedKey {
  def scope: Type = ScopedKeyTypes.ARTICLE_ORDINAL_KEY

  override def stringConverter: ArticleOrdinalKeyStringConverter.type = ArticleOrdinalKeyStringConverter
}

case class OrdinalKey(ordinal:Int) extends CanBeScopedKey {
  def scope: Type = ScopedKeyTypes.ORDINAL_KEY

  override def stringConverter: OrdinalKeyStringConverter.type = OrdinalKeyStringConverter
}


@SerialVersionUID(1l)
case class ArticleKey(articleId: Long) extends CanBeScopedKey {
  def <(other: ArticleKey): Boolean = articleId < other.articleId

  def >(other: ArticleKey): Boolean = articleId > other.articleId

  def scope: Type = ScopedKeyTypes.ARTICLE

  override def stringConverter: ArticleKeyStringConverter.type = ArticleKeyStringConverter

  /** Used for some partners, such as AOL, whose data layer only supports 32-bit ints. */
  def intId: Int = (articleId % 2e9.toLong).toInt
}

@SerialVersionUID(1l)
case class HbaseNull() {
}

object CovisitationKey {
  def partialByStartHour(hour: DateHour): CovisitationKey = {
    CovisitationKey(hour.getMillis, ArticleKey.empty, 0)
  }
}

case class CovisitationKey(hour: Long, key: ArticleKey, distance: Int) {
  def hourAsDateHour: DateHour = DateHour(hour)
}

@SerialVersionUID(4986475664867106687l)
object IngestionTypes extends GrvEnum[Byte] {
  case class Type(i: Byte, n: String) extends ValueTypeBase(i, n)
  override def mkValue(id: Byte, name: String): Type = Type(id, name)

  val unknown: Type = Value(0, "unknown")
  // not to be trusted for anything
  val fromBeacon: Type = Value(1, "fromBeacon")
  // old school bro
  val fromRss: Type = Value(2, "fromRss")
  // the most trusted source
  val fromCurrentUrl: Type = Value(3, "fromCurrentUrl")
  // just happened to be the URL someone got recos loaded on
  val fromBeaconOfAnRssEnabledSite: Type = Value(4, "fromBeaconOfAnRssEnabledSite")
  // from a beacon, but the siteGuid of the beacon belongs to a site with RSS ingestion enabled
  val fromFirehose: Type = Value(5, "fromFirehose")
  val fromCustomApi: Type = Value(6, "fromCustomApi")
  val fromCrawlingService: Type = Value(7, "fromCrawlingService")
  val fromHighlighter: Type = Value(8, "fromHighlighter")
  val fromShare: Type = Value(9, "fromShare")
  val fromAccountManager: Type = Value(10, "fromAccountManager")
  val fromAolPromotion: Type = Value(11, "fromAolPromotion")

  val defaultValue: IngestionTypes.Type = unknown

}

case class ArticleIngestionKey(timestamp: DateTime, ingestionType: IngestionTypes.Type) {
  override def toString: String = "Ingested " + ingestionType + " on " + timestamp.toString("MM/dd/yyyy 'at' hh:mm:ss a")
}

@SerialVersionUID(1l)
case class CampaignKey(siteKey: SiteKey, campaignId: Long) extends CanBeScopedKey {
  def scope: Type = ScopedKeyTypes.CAMPAIGN

  def toString(keyValDelim: String = ":", fieldDelim: String = "_"): String = {
    (new StringBuilder).append("siteId").append(keyValDelim).append(siteKey.siteId).append(fieldDelim)
      .append("campaignId").append(keyValDelim).append(campaignId).toString()
  }

  override lazy val toString: String = toString()

  override def stringConverter: CampaignKeyStringConverter.type = CampaignKeyStringConverter
}

@SerialVersionUID(1l)
case class CampaignArticleKey(campaignKey: CampaignKey, articleKey: ArticleKey) extends CanBeScopedKey {
  def scope: Type = ScopedKeyTypes.CAMPAIGN_ARTICLE

  override def toString(): String = {
    (new StringBuilder).append(campaignKey).append("_articleId:").append(articleKey.articleId).toString()
  }

  override def stringConverter: CampaignArticleKeyConverter.type = CampaignArticleKeyConverter
}

object CampaignKey {

  val empty: CampaignKey = CampaignKey(SiteKey(-1l), -1l)

  val maxValue: CampaignKey = CampaignKey(SiteKey.maxValue, SchemaTypeHelpers.longUpperBounds)
  val minValue: CampaignKey = CampaignKey(SiteKey.minValue, SchemaTypeHelpers.longLowerBounds)

  val deserializeRegex: Regex = new Regex( """^siteId:(-?\d+)_campaignId:(-?\d+)$""", "siteId", "campaignId")

  def apply(siteGuid: String, campaignId: Long): CampaignKey = CampaignKey(SiteKey(siteGuid), campaignId)

  def partialBySiteStartKey(siteKey: SiteKey): CampaignKey = CampaignKey(siteKey, SchemaTypeHelpers.longLowerBounds)

  def partialBySiteEndKey(siteKey: SiteKey): CampaignKey = CampaignKey(siteKey, SchemaTypeHelpers.longUpperBounds)

  implicit val jsonFormat: Reads[CampaignKey] = CanBeScopedKey.jsonFormat.map {
    case ck: CampaignKey => ck
    case cbsk => throw new MappingException(s"Expected CampaignKey, got $cbsk.")
  }

  def parse(paramValue: String, keyValDelim: String = ":", fieldDelim: String = "_"): Option[CampaignKey] = {
    if (paramValue == null) return None

    val cpcPrefixIndex = paramValue.indexOf("_cpc:")

    val parseMe = if (cpcPrefixIndex > -1) {
      paramValue.substring(0, cpcPrefixIndex)
    } else {
      paramValue
    }

    parseMe match {
      case deserializeRegex(siteIdStr, campaignIdStr) =>
        val siteIdOpt = siteIdStr.tryToLong
        val campaignIdOpt = campaignIdStr.tryToLong
        if(siteIdOpt.isDefined && campaignIdOpt.isDefined) {
          val siteKey = SiteKey(siteIdOpt.get)
          Some(CampaignKey(siteKey, campaignIdOpt.get))
        }
        else
          None
      case _ => None
    }
  }

  /**
   * Generates a new unique CampaignKey for the specified siteGuid. To maintain uniqueness,
   * we generate an MD5 array of bytes based on the:
   * 1) siteId from the specified siteGuid.
   * 2) current milliseconds.
   * 3) and a salt of 8 random bytes.
   *
   * We then run those MD5 bytes through MurmurHash's hash64 for the final campaignId.
    *
    * @param siteGuid specifies which site this key is generated for
   * @return
   */
  def generate(siteGuid: String): CampaignKey = {
    val sk = SiteKey(siteGuid)

    val skBytes = HashUtils.longBytes(sk.siteId)
    val md5bytes = HashUtils.generateSaltedMd5bytes(skBytes)
    val cid = HashUtils.hashedBytesToMurmurLong(md5bytes)

    CampaignKey(sk, cid)
  }

  /** @deprecated Don't use this. This is only being used to aid transition to Play. */
  object TmpLiftSerializer extends Serializer[CampaignKey] {
    override def deserialize(implicit format: Formats): PartialFunction[(TypeInfo, JValue), CampaignKey] = {
      case (TypeInfo(cl, _), JString(keyStr)) if cl.toString.indexOf("CampaignKey") != -1 => StringConverter.validateString(keyStr).fold(
        f => f.exceptionOption match {
          case Some(ex: Exception) => throw new MappingException(s"Couldn't parse $keyStr to CampaignKey: ${f.message}", ex)
          case _ => throw new MappingException(s"Couldn't parse $keyStr to CampaignKey: ${f.message}")
        },
        {
          case key: CampaignKey => key
          case x => throw new MappingException(s"Parsed $keyStr to $x but needed CampaignKey.")
        }
      )

      case (_, JObject(fields)) if fields.exists(_.name == "siteKey") && fields.exists(_.name == "campaignId") =>
        val siteKeyOpt = fields.find(_.name == "siteKey").flatMap {
          case JField(_, jValue) =>
            val withTmpSiteKeyFormat = DefaultFormats + SiteKey.TmpLiftSerializer
            def jesusChristThisIsStupid(implicit m: Manifest[SiteKey])
              = Extraction.extractOpt[SiteKey](jValue)(withTmpSiteKeyFormat, m)
            jesusChristThisIsStupid
        }

        val campaignIdOpt = fields.find(_.name == "campaignId").flatMap(_.value match {
          case JInt(num) => Some(num.toLong)
          case _ => None
        })

        (for {
          siteKey <- siteKeyOpt
          campaignId <- campaignIdOpt
        } yield CampaignKey(siteKey, campaignId)).getOrElse {
          throw new MappingException(s"Invalid fields $fields to create a CampaignKey.")
        }
    }

    override def serialize(implicit format: Formats): PartialFunction[Any, JValue] = {
      case key: CampaignKey => JString(StringConverter.writeString(key))
    }
  }
}

@SerialVersionUID(1l)
case class AuditKey2(scopedKey: ScopedKey, dateTime: DateTime, userId: Long, auditId: Long) {
  override def toString: String =
    s"""{"scopedKey":"${scopedKey.keyString}","dateTime":"${dateTime.toString("yyyy/MM/dd 'at' hh:mm:ss a")}","userId":$userId,"auditId":$auditId}"""
}

object AuditKey2 {
  def apply(scopedKey: ScopedKey, userId: Long, auditGuid: String): AuditKey2 = AuditKey2(scopedKey, grvtime.currentTime, userId, auditGuid.getMurmurHash)

  def partialByKeyFrom[T <: CanBeScopedKey](key: T, dateTime: Option[DateTime] = None): AuditKey2 = {
    AuditKey2(key.toScopedKey, dateTime.getOrElse(new DateTime(SchemaTypeHelpers.longLowerBounds)), SchemaTypeHelpers.longLowerBounds, SchemaTypeHelpers.longLowerBounds)
  }

  def partialByKeyTo[T <: CanBeScopedKey](key: T, dateTime: Option[DateTime] = None): AuditKey2 = {
    AuditKey2(key.toScopedKey, dateTime.getOrElse(new DateTime(1)), SchemaTypeHelpers.longUpperBounds, SchemaTypeHelpers.longUpperBounds)
  }

}

case class ArticleIngestingKey(articleKey: ArticleKey, siteGuidOption: Option[String], campaignKeyOption: Option[CampaignKey]) {
  def isEmpty: Boolean = siteGuidOption.isEmpty && campaignKeyOption.isEmpty

  def isForCampaign: Boolean = campaignKeyOption.isDefined

  lazy val siteKey: SiteKey = campaignKeyOption match {
    case Some(ck) => ck.siteKey
    case None => siteGuidOption match {
      case Some(sg) => SiteKey(sg)
      case None => SiteKey.empty
    }
  }
}

object ArticleIngestingKey {
  def apply(url: String, siteGuid: String): ArticleIngestingKey = ArticleIngestingKey(ArticleKey(url), Some(siteGuid), None)

  def apply(articleKey: ArticleKey, siteGuid: String): ArticleIngestingKey = ArticleIngestingKey(articleKey, Some(siteGuid), None)

  def apply(url: String, campaignKey: CampaignKey): ArticleIngestingKey = ArticleIngestingKey(ArticleKey(url), None, Some(campaignKey))

  def apply(articleKey: ArticleKey, campaignKey: CampaignKey): ArticleIngestingKey = ArticleIngestingKey(articleKey, None, Some(campaignKey))
}

trait RecommendationMetricsKey extends MetricsKey {
  implicit val isOrganicCampaignChecker: (CampaignKey) => Boolean = RecommendationMetricsKey.noChecking

  def geoLocationId: Int

  def sitePlacementId: Int

  def siteKey: SiteKey

  def appendAdditionalFieldsForToString(sb: StringBuilder) {
    // do nothing for trait, but do not force an implementation
  }

  def isClick: Boolean = countBy == RecommendationMetricCountBy.click

  def recoMetrics(value: Long, isControl: Boolean = false)(implicit isOrganicCampaignChecker: (CampaignKey) => Boolean): RecommendationMetrics = countBy match {
    case RecommendationMetricCountBy.click => RecommendationMetrics(0l, value, 0l)
    case RecommendationMetricCountBy.articleImpression => RecommendationMetrics(value, 0l, 0l)
    case RecommendationMetricCountBy.unitImpression => RecommendationMetrics(0l, 0l, value)
    case RecommendationMetricCountBy.unitImpressionViewed => RecommendationMetrics(0L, 0L, 0L, unitImpressionsViewed = value)
    case _ => RecommendationMetrics.empty
  }

  override def toString: String = {
    val sb = new StringBuilder
    sb.append(getClass.getSimpleName)
    sb.append(": { hour: '").append(dateHour.toString("yyyy/MM/dd-ha"))
    sb.append("'; bucketId: ").append(bucketId).append("; algoId: ").append(algoId).append("; siteKey: ")
    sb.append(siteKey.siteId).append("; placement: '").append(placementId).append("'; geoLocationId: ").append(geoLocationId)
    appendAdditionalFieldsForToString(sb)
    sb.append(" }")
    sb.toString()
  }
}

trait RecommendationDailyMetricsKey extends DailyMetricsKey {
  implicit val isOrganicCampaignChecker: (CampaignKey) => Boolean = RecommendationMetricsKey.noChecking

  def geoLocationId: Int

  def sitePlacementId: Int

  def siteKey: SiteKey

  def appendAdditionalFieldsForToString(sb: StringBuilder) {
    // do nothing for trait, but do not force an implementation
  }

  def metricType: String = countBy.toString

  def isClick: Boolean = countBy == RecommendationMetricCountBy.click

  def recoMetrics(value: Long, isControl: Boolean = false)(implicit isOrganicCampaignChecker: (CampaignKey) => Boolean): RecommendationMetrics = countBy match {
    case RecommendationMetricCountBy.click => RecommendationMetrics(0l, value, 0l)
    case RecommendationMetricCountBy.articleImpression => RecommendationMetrics(value, 0l, 0l)
    case RecommendationMetricCountBy.unitImpression => RecommendationMetrics(0l, 0l, value)
    case RecommendationMetricCountBy.unitImpressionViewed => RecommendationMetrics(0L, 0L, 0L, unitImpressionsViewed = value)
    case _ => RecommendationMetrics.empty
  }

  override def toString: String = {
    val sb = new StringBuilder
    sb.append(getClass.getSimpleName)
    sb.append(": { day: '").append(dateMidnight.toString("yyyy/MM/dd")).append("'; type: '")
    sb.append(metricType).append("'; bucketId: ").append(bucketId).append("; algoId: ").append(algoId).append("; siteKey: ")
    sb.append(siteKey.siteId).append("; placement: '").append(placementId).append("'; geoLocationId: ").append(geoLocationId)
    appendAdditionalFieldsForToString(sb)
    sb.append(" }")
    sb.toString()
  }
}

trait RecommendationMonthlyMetricsKey extends MonthlyMetricsKey {
  implicit val isOrganicCampaignChecker: (CampaignKey) => Boolean = RecommendationMetricsKey.noChecking

  def geoLocationId: Int

  def sitePlacementId: Int

  def siteKey: SiteKey

  def appendAdditionalFieldsForToString(sb: StringBuilder) {
    // do nothing for trait, but do not force an implementation
  }

  def metricType: String = countBy.toString

  def isClick: Boolean = countBy == RecommendationMetricCountBy.click

  def recoMetrics(value: Long, isControl: Boolean = false)(implicit isOrganicCampaignChecker: (CampaignKey) => Boolean): RecommendationMetrics = countBy match {
    case RecommendationMetricCountBy.click => RecommendationMetrics(0l, value, 0l)
    case RecommendationMetricCountBy.articleImpression => RecommendationMetrics(value, 0l, 0l)
    case RecommendationMetricCountBy.unitImpression => RecommendationMetrics(0l, 0l, value)
    case RecommendationMetricCountBy.unitImpressionViewed => RecommendationMetrics(0L, 0L, 0L, unitImpressionsViewed = value)
    case _ => RecommendationMetrics.empty
  }

  override def toString: String = {
    val sb = new StringBuilder
    sb.append(getClass.getSimpleName)
    sb.append(": { month: '").append(dateMonth.toString("yyyy/MM")).append("'; type: '")
    sb.append(metricType).append("'; bucketId: ").append(bucketId).append("; algoId: ").append(algoId).append("; siteKey: ")
    sb.append(siteKey.siteId).append("; placement: '").append(placementId).append("'; geoLocationId: ").append(geoLocationId)
    appendAdditionalFieldsForToString(sb)
    sb.append(" }")
    sb.toString()
  }
}

object RecommendationMetricsKey {
  implicit val noChecking: (CampaignKey) => Boolean = _ => false

  def groupMapgregate[K <: RecommendationMetricsKey, G](map: Map[K, Long], grouper: ((K, Long)) => G): Map[G, RecommendationMetrics] = groupMapgregate(map.toSeq, grouper)

  def groupMapgregate[K <: RecommendationMetricsKey, G](seq: Seq[(K, Long)], grouper: ((K, Long)) => G): Map[G, RecommendationMetrics] = {
    grvcoll.groupAndFold(RecommendationMetrics.empty)(seq)(grouper)({
      case (rm: RecommendationMetrics, kv: (K, Long)) =>
        rm + kv._1.recoMetrics(kv._2)
    }).toMap
  }

  /**
   * Preserves type inferencing for the grouper, thus allowing a terser syntax when calling, e.g. blah.groupMapgregate2(data)(_.dateHour)
    *
    * @param map
   * @param grouper
   * @tparam G
   * @return
   */
  def groupMapgregate2[G](map: scala.collection.Map[RecommendationMetricsKey, Long])(grouper: ((RecommendationMetricsKey, Long)) => G): Map[G, RecommendationMetrics] = groupMapgregate(map.toSeq, grouper)

  def groupSequence[G](seq: scala.collection.Map[RecommendationMetricsKey, Long])(grouper: ((RecommendationMetricsKey, Long)) => G): Seq[(G, RecommendationMetrics)] = {
    grvcoll.groupAndFold(RecommendationMetrics.empty)(seq)(grouper)({
      case (rm: RecommendationMetrics, kv: (RecommendationMetricsKey, Long)) =>
        rm + kv._1.recoMetrics(kv._2)
    }).toSeq
  }

}

trait ArticleRecommendationMetricsKey extends RecommendationMetricsKey {
  def articleId: Long

  override def toString: String = {
    val sb = new StringBuilder
    sb.append(getClass.getSimpleName)
    sb.append(": { hour: '").append(dateHour.toString("yyyy/MM/dd-ha"))
    sb.append("'; bucketId: ").append(bucketId).append("; algoId: ").append(algoId).append("; siteKey: ")
    sb.append(siteKey.siteId).append("; placement: '").append(placementId).append("'; geoLocationId: ").append(geoLocationId)
    sb.append("'; articleId: ").append(articleId)
    appendAdditionalFieldsForToString(sb)
    sb.append(" }")
    sb.toString()
  }
}

case class StandardMetricsDailyKey(dateMidnight: GrvDateMidnight, metricType: StandardMetricType.Type)

case class StandardMetricsHourlyKey(dateHour: DateHour, metricType: StandardMetricType.Type)

case class ClickStreamKey(hour: DateHour, clickType: ClickType.Type, articleKey: ArticleKey, referrerKey: ArticleKey = ArticleKey.empty)

object ClickStreamKey {
  def apply(dateTime: ReadableDateTime, url: String): ClickStreamKey = ClickStreamKey(DateHour(dateTime), ClickType.viewed, ArticleKey(url))
  
  def apply(dateTime: ReadableDateTime, articleKey: ArticleKey): ClickStreamKey = ClickStreamKey(DateHour(dateTime), ClickType.viewed, articleKey)

  def apply(dateTime: ReadableDateTime, articleKey: ArticleKey, clickType: ClickType.Type): ClickStreamKey = ClickStreamKey(DateHour(dateTime), clickType, articleKey)

  val keyValOrdering: scala.Ordering[(ClickStreamKey, Long)] with Object {def compare(x: (ClickStreamKey, Long), y: (ClickStreamKey, Long)): Int} = new scala.Ordering[(ClickStreamKey, Long)] {
    def compare(x: (ClickStreamKey, Long), y: (ClickStreamKey, Long)): Int = {
      if (x._1.hour.isAfter(y._1.hour)) return -1
      if (y._1.hour.isAfter(x._1.hour)) return 1
      x._2.compare(y._2)
    }
  }

  def minKeyByDateTime(dateTime: ReadableDateTime): ClickStreamKey = ClickStreamKey(dateTime, ArticleKey(SchemaTypeHelpers.longLowerBounds))

  def maxKeyByDateTime(dateTime: ReadableDateTime): ClickStreamKey = ClickStreamKey(dateTime, ArticleKey(SchemaTypeHelpers.longUpperBounds))

  def byArticleClick(click: ClickEvent): ClickStreamKey = ClickStreamKey(click.getClickDate,click.article.key,ClickType.clicked)

  def byImpressionEvent(event: ImpressionEvent, key: ArticleKey): ClickStreamKey = ClickStreamKey(event.getDate, key,ClickType.impressionserved)

  def byImpressionViewedEvent(event: ImpressionViewedEvent, key: ArticleKey): ClickStreamKey = ClickStreamKey(new DateTime(event.date), key, ClickType.impressionviewed)
}

case class SponsoredArticleKey(key: ArticleKey)

case class SponsoredArticleData(site: SiteKey)

case class ArticleKeyAndMetricsMap(key: ArticleKey, metrics: Map[GrvDateMidnight, StandardMetrics])

case class ArticleKeyAndMetrics(key: ArticleKey, metrics: StandardMetrics)

object ArticleKeyAndMetrics {
  def apply(url: String, metrics: StandardMetrics): ArticleKeyAndMetrics = ArticleKeyAndMetrics(ArticleKey(url), metrics)
}

object ReportKey {

  def apply(reportType: String, period: TimeSliceResolution, sortBy: InterestSortBy.Value, sortDescending: Boolean, isGrouped: Boolean): ReportKey = ReportKey(reportType, period, validateSortBy(sortBy), sortDescending, isGrouped)

  def validateSortBy(sortBy: InterestSortBy.Value): Long = if (InterestSortBy.isEnabled(sortBy)) {
    getSortId(sortBy)
  }
  else {
    throw new IllegalArgumentException("Unknown 'sortBy' specified: " + sortBy.toString)
  }

  def getSortId(sortBy: InterestSortBy.Value): Long = sortBy match {
    case InterestSortBy.PublishOpportunityScore => Sorts.publishOppId
    case InterestSortBy.FeatureOpportunityScore => Sorts.featureOppId
    case InterestSortBy.Views => Sorts.viewsId
    case InterestSortBy.Publishes => Sorts.publishesId
    case InterestSortBy.Social => Sorts.socialId
    case InterestSortBy.Search => Sorts.searchId
    case InterestSortBy.Tweets => Sorts.tweetsId
    case InterestSortBy.Retweets => Sorts.retweetsId
    case InterestSortBy.FacebookClicks => Sorts.fbClicksId
    case InterestSortBy.FacebookLikes => Sorts.fbLikesId
    case InterestSortBy.FacebookShares => Sorts.fbSharesId
    case InterestSortBy.Influencers => Sorts.influencersId
    case InterestSortBy.TotalViral => Sorts.totalViralId
    case InterestSortBy.ViralVelocity => Sorts.viralVelocityId
    case InterestSortBy.Visitors => Sorts.visitorsId
    case InterestSortBy.PublishDate => Sorts.publishDateId
    case InterestSortBy.CTR => Sorts.ctrId
    case InterestSortBy.Clicks => Sorts.clicksId
    case InterestSortBy.Impressions => Sorts.impressionsId
    case InterestSortBy.SiteUniques => Sorts.siteUniques
    case InterestSortBy.MostRecentlyPublished => Sorts.mostRecentlyPublished
    case InterestSortBy.Revenue => Sorts.Revenue
    case InterestSortBy.OrganicPublishDate => Sorts.OrganicPublishDate
    case InterestSortBy.OrganicViews => Sorts.OrganicViews
    case InterestSortBy.OrganicClicks => Sorts.OrganicClicks
    case InterestSortBy.SponsoredPublishDate => Sorts.SponsoredPublishDate
    case InterestSortBy.SponsoredViews => Sorts.SponsoredViews
    case InterestSortBy.SponsoredClicks => Sorts.SponsoredClicks
    case _ => MurmurHash.hash64(sortBy.toString)
  }

  def getSortBy(sortId: Long): Option[InterestSortBy.Value] = sortId match {
    case Sorts.publishOppId => Some(InterestSortBy.PublishOpportunityScore)
    case Sorts.featureOppId => Some(InterestSortBy.FeatureOpportunityScore)
    case Sorts.viewsId => Some(InterestSortBy.Views)
    case Sorts.publishesId => Some(InterestSortBy.Publishes)
    case Sorts.socialId => Some(InterestSortBy.Social)
    case Sorts.searchId => Some(InterestSortBy.Search)
    case Sorts.tweetsId => Some(InterestSortBy.Tweets)
    case Sorts.retweetsId => Some(InterestSortBy.Retweets)
    case Sorts.fbClicksId => Some(InterestSortBy.FacebookClicks)
    case Sorts.fbLikesId => Some(InterestSortBy.FacebookLikes)
    case Sorts.fbSharesId => Some(InterestSortBy.FacebookShares)
    case Sorts.influencersId => Some(InterestSortBy.Influencers)
    case Sorts.totalViralId => Some(InterestSortBy.TotalViral)
    case Sorts.viralVelocityId => Some(InterestSortBy.ViralVelocity)
    case Sorts.visitorsId => Some(InterestSortBy.Visitors)
    case Sorts.publishDateId => Some(InterestSortBy.PublishDate)
    case Sorts.ctrId => Some(InterestSortBy.CTR)
    case Sorts.clicksId => Some(InterestSortBy.Clicks)
    case Sorts.impressionsId => Some(InterestSortBy.Impressions)
    case Sorts.siteUniques => Some(InterestSortBy.SiteUniques)
    case Sorts.mostRecentlyPublished => Some(InterestSortBy.MostRecentlyPublished)
    case Sorts.Revenue => Some(InterestSortBy.Revenue)
    case Sorts.OrganicPublishDate => Some(InterestSortBy.OrganicPublishDate)
    case Sorts.OrganicViews => Some(InterestSortBy.OrganicViews)
    case Sorts.OrganicClicks => Some(InterestSortBy.OrganicClicks)
    case Sorts.SponsoredPublishDate => Some(InterestSortBy.SponsoredPublishDate)
    case Sorts.SponsoredViews => Some(InterestSortBy.SponsoredViews)
    case Sorts.SponsoredClicks => Some(InterestSortBy.SponsoredClicks)
    case _ => None
  }

  def lookupSortById(sortId: Long): Option[String] = sortId match {
    case Sorts.publishOppId => Some(Keys.PublishOpportunityScore)
    case Sorts.featureOppId => Some(Keys.FeatureOpportunityScore)
    case Sorts.viewsId => Some(Keys.Views)
    case Sorts.publishesId => Some(Keys.Publishes)
    case Sorts.socialId => Some(Keys.Social)
    case Sorts.searchId => Some(Keys.Search)
    case Sorts.tweetsId => Some(Keys.Tweets)
    case Sorts.retweetsId => Some(Keys.Retweets)
    case Sorts.fbClicksId => Some(Keys.FacebookClicks)
    case Sorts.fbLikesId => Some(Keys.FacebookLikes)
    case Sorts.fbSharesId => Some(Keys.FacebookShares)
    case Sorts.influencersId => Some(Keys.Influencers)
    case Sorts.totalViralId => Some(Keys.TotalViral)
    case Sorts.viralVelocityId => Some(Keys.ViralVelocity)
    case Sorts.visitorsId => Some(Keys.Visitors)
    case Sorts.publishDateId => Some(Keys.PublishDate)
    case Sorts.ctrId => Some(Keys.CTR)
    case Sorts.clicksId => Some(Keys.Clicks)
    case Sorts.impressionsId => Some(Keys.Impressions)
    case Sorts.siteUniques => Some(Keys.SiteUniques)
    case Sorts.mostRecentlyPublished => Some(Keys.MostRecentlyPublished)
    case Sorts.Revenue => Some(Keys.Revenue)
    case Sorts.OrganicPublishDate => Some(Keys.OrganicPublishDate)
    case Sorts.OrganicViews => Some(Keys.OrganicViews)
    case Sorts.OrganicClicks => Some(Keys.OrganicClicks)
    case Sorts.SponsoredPublishDate => Some(Keys.SponsoredPublishDate)
    case Sorts.SponsoredViews => Some(Keys.SponsoredViews)
    case Sorts.SponsoredClicks => Some(Keys.SponsoredClicks)
    case _ => None
  }

  object Sorts {
    val publishOppId: Long = MurmurHash.hash64(Keys.PublishOpportunityScore)
    val featureOppId: Long = MurmurHash.hash64(Keys.FeatureOpportunityScore)
    val viewsId: Long = MurmurHash.hash64(Keys.Views)
    val publishesId: Long = MurmurHash.hash64(Keys.Publishes)
    val socialId: Long = MurmurHash.hash64(Keys.Social)
    val searchId: Long = MurmurHash.hash64(Keys.Search)
    val tweetsId: Long = MurmurHash.hash64(Keys.Tweets)
    val retweetsId: Long = MurmurHash.hash64(Keys.Retweets)
    val fbClicksId: Long = MurmurHash.hash64(Keys.FacebookClicks)
    val fbLikesId: Long = MurmurHash.hash64(Keys.FacebookLikes)
    val fbSharesId: Long = MurmurHash.hash64(Keys.FacebookShares)
    val influencersId: Long = MurmurHash.hash64(Keys.Influencers)
    val totalViralId: Long = MurmurHash.hash64(Keys.TotalViral)
    val viralVelocityId: Long = MurmurHash.hash64(Keys.ViralVelocity)
    val visitorsId: Long = MurmurHash.hash64(Keys.Visitors)
    val publishDateId: Long = MurmurHash.hash64(Keys.PublishDate)
    val ctrId: Long = MurmurHash.hash64(Keys.CTR)
    val impressionsId: Long = MurmurHash.hash64(Keys.Impressions)
    val clicksId: Long = MurmurHash.hash64(Keys.Clicks)
    val siteUniques: Long = MurmurHash.hash64(Keys.SiteUniques)
    val mostRecentlyPublished: Long = MurmurHash.hash64(Keys.MostRecentlyPublished)
    val Revenue: Long = MurmurHash.hash64(Keys.Revenue)
    val OrganicPublishDate: Long = MurmurHash.hash64(Keys.OrganicPublishDate)
    val OrganicViews: Long = MurmurHash.hash64(Keys.OrganicViews)
    val OrganicClicks: Long = MurmurHash.hash64(Keys.OrganicClicks)
    val SponsoredPublishDate: Long = MurmurHash.hash64(Keys.SponsoredPublishDate)
    val SponsoredViews: Long = MurmurHash.hash64(Keys.SponsoredViews)
    val SponsoredClicks: Long = MurmurHash.hash64(Keys.SponsoredClicks)
  }

}

case class ReportKey(var reportType: String, timePeriod: TimeSliceResolution, sortById: Long, sortDescending: Boolean, isGrouped: Boolean) {

  def this(timePeriod: TimeSliceResolution, sortById: Long, sortDescending: Boolean, isGrouped: Boolean) = this("Topics", timePeriod, sortById, sortDescending, isGrouped)

  def getSortBy: InterestSortBy.Value = ReportKey.lookupSortById(sortById) match {
    case Some(key) => InterestSortBy.getByKey(key)
    case None => throw new IllegalArgumentException("The specified sortById: %d was not found!".format(sortById))
  }

  def prettyString: String = {
    reportType + " Report from " + timePeriod.range.fromInclusive.toString("MM-dd") + " to " + timePeriod.range.toInclusive.toString("MM-dd") + ", sorted by " + getSortBy.toString + ": descending: " + sortDescending
  }
}

@SerialVersionUID(-7164838947284521282l)
object UserRelationships extends GrvEnum[Short] {
  case class Type(i: Short, n: String) extends ValueTypeBase(i, n)
  override def mkValue(id: Short, name: String): Type = Type(id, name)

  val rejected: Type = Value(-1, "rejected")
  val noRelation: Type = Value(0, "noRelation")
  val stared: Type = Value(1, "stared")
  val read: Type = Value(2, "read")

  def defaultValue: Type = noRelation
}

case class UserRelationshipKey(relType: UserRelationships.Type, articleKey: ArticleKey)

object UserRelationshipKey {
  def minKeyByType(relType: UserRelationships.Type): UserRelationshipKey = UserRelationshipKey(relType, ArticleKey.min)

  def maxKeyByType(relType: UserRelationships.Type): UserRelationshipKey = UserRelationshipKey(relType, ArticleKey.max)
}

object RollupRecommendationMetricKey {
  implicit val isOrganicCampaignChecker: (CampaignKey) => Boolean = RecommendationMetricsKey.noChecking
  val missing: Int = -1

  def apply(impression: ImpressionEvent): RollupRecommendationMetricKey = {
    RollupRecommendationMetricKey(DateHour(impression.getDate), SiteKey(impression.getSiteGuid), impression.getPlacementId, impression.getBucketId, GrccableEvent.geoLocationIdForMetrics, RecommendationMetricCountBy.unitImpression)
  }

  def apply(click: ClickEvent): RollupRecommendationMetricKey = {
    RollupRecommendationMetricKey(DateHour(click.getClickDate), SiteKey(click.getSiteGuid), click.getPlacementId, click.getBucketId, GrccableEvent.geoLocationIdForMetrics, RecommendationMetricCountBy.click)
  }

  def apply(impression: ImpressionViewedEvent): RollupRecommendationMetricKey = {
    RollupRecommendationMetricKey(DateHour(impression.date), SiteKey(impression.siteGuid), missing, missing, GrccableEvent.geoLocationIdForMetrics, RecommendationMetricCountBy.unitImpressionViewed)
  }

  //def from(recoKey: RecommendationMetricsKey) = RollupRecommendationMetricKey(recoKey.dateHour, recoKey.siteKey, recoKey.placement, recoKey.bucketId, recoKey.geoLocationId)

  def partialByStartDate(date: DateHour): RollupRecommendationMetricKey = RollupRecommendationMetricKey(dateHour = date, siteKey = SiteKey.minValue, placementId = 0, bucketId = SchemaTypeHelpers.intLowerBounds, geoLocationId = SchemaTypeHelpers.intLowerBounds, countBy = RecommendationMetricCountBy.minValue)

  def partialByEndDate(date: DateHour): RollupRecommendationMetricKey = RollupRecommendationMetricKey(dateHour = date, siteKey = SiteKey.maxValue, placementId = 0, bucketId = SchemaTypeHelpers.intUpperBounds, geoLocationId = SchemaTypeHelpers.intUpperBounds, countBy = RecommendationMetricCountBy.maxValue)

  def groupMapgregate[K <: RollupRecommendationMetricKey, G](map: Map[K, Long], grouper: ((K, Long)) => G): Map[G, RecommendationMetrics] = groupMapgregate(map.toSeq, grouper)

  def groupMapgregate[K <: RollupRecommendationMetricKey, G](seq: Seq[(K, Long)], grouper: ((K, Long)) => G): Map[G, RecommendationMetrics] = {
    grvcoll.groupAndFold(RecommendationMetrics.empty)(seq)(grouper)({
      case (rm: RecommendationMetrics, kv: (K, Long)) =>
        rm + kv._1.recoMetrics(kv._2)
    }).toMap
  }

  def zroupMapgregate[RK <: RollupRecommendationMetricKey, K <: RecommendationMetricsKey, G](rollupMap: Map[RK, Long], map: Map[K, Long], grouper: (((RK, Long), (K, Long))) => G): Map[G, RecommendationMetrics] = zroupMapgregate(rollupMap.toSeq, map.toSeq, grouper)

  def zroupMapgregate[RK <: RollupRecommendationMetricKey, K <: RecommendationMetricsKey, G](rollupSeq: Seq[(RK, Long)], seq: Seq[(K, Long)], grouper: (((RK, Long), (K, Long))) => G): Map[G, RecommendationMetrics] = {
    val zipped = rollupSeq.zip(seq)
    grvcoll.groupAndFold(RecommendationMetrics.empty)(zipped)(grouper)({
      case (rm: RecommendationMetrics, kv: ((RK, Long), (K, Long))) =>
        val rollup = kv._1._1.recoMetrics(kv._1._2)
        val reg = kv._2._1.recoMetrics(kv._2._2)
        rm + RecommendationMetrics(reg.articleImpressions, reg.clicks, rollup.unitImpressions)
    }).toMap
  }
}



case class RollupRecommendationMetricKey(dateHour: DateHour, siteKey: SiteKey, placementId: Int, bucketId: Int, geoLocationId: Int = 0, countBy: Byte) extends ControllableMetricsKeyLite {
  def recoMetrics(value: Long, isControl: Boolean = false): RecommendationMetrics = countBy match {
    case RecommendationMetricCountBy.click =>
      val (control, nonControl) = if (isControl) (value, 0L) else (0L, value)
      RecommendationMetrics(0l, value, 0l, organicClicksControl = control, organicClicksNonControl = nonControl)
    case RecommendationMetricCountBy.articleImpression => RecommendationMetrics(value, 0l, 0l)
    case RecommendationMetricCountBy.unitImpression =>
      val (control, nonControl) = if (isControl) (value, 0L) else (0L, value)
      RecommendationMetrics(0l, 0l, value, unitImpressionsControl = control, unitImpressionsNonControl = nonControl)
    case RecommendationMetricCountBy.unitImpressionViewed => RecommendationMetrics(0L, 0L, 0L, unitImpressionsViewed = value)
    case _ => RecommendationMetrics.empty
  }

  def toDaily : RollupRecommendationMetricKey = RollupRecommendationMetricKey(dateHour.asMidnightHour, siteKey, placementId, bucketId, geoLocationId, countBy)

  def toMonthly : RollupRecommendationMetricKey = RollupRecommendationMetricKey(dateHour.asMonthHour, siteKey, placementId, bucketId, geoLocationId, countBy)
}

case class ArticleRecommendationMetricKey(
                                           override val dateHour: DateHour,
                                           override val countBy: Byte,
                                           override val bucketId: Int,
                                           override val siteKey: SiteKey,
                                           override val placementId: Int,
                                           override val geoLocationId: Int = 0,
                                           override val algoId: Int = -1,
                                           override val sitePlacementId: Int = 0 // should be set but so far never is
                                           ) extends RecommendationMetricsKey {
  def byClick: ArticleRecommendationMetricKey = if (countBy == RecommendationMetricCountBy.click) this else copy(countBy = RecommendationMetricCountBy.click)

  def byArticleImpression: ArticleRecommendationMetricKey = if (countBy == RecommendationMetricCountBy.articleImpression) this else copy(countBy = RecommendationMetricCountBy.articleImpression)

  def byUnitImpression: ArticleRecommendationMetricKey = if (countBy == RecommendationMetricCountBy.unitImpression) this else copy(countBy = RecommendationMetricCountBy.unitImpression)

  def toDaily : ArticleRecommendationMetricKey = ArticleRecommendationMetricKey(dateHour.asMidnightHour, countBy, bucketId, siteKey, placementId, geoLocationId, algoId, sitePlacementId)

  def toMonthly : ArticleRecommendationMetricKey = ArticleRecommendationMetricKey(dateHour.asMonthHour, countBy, bucketId, siteKey, placementId, geoLocationId, algoId, sitePlacementId)
  //  override val hashCode = scala.runtime.ScalaRunTime._hashCode(this)
}

object ArticleRecommendationMetricKey {
  implicit val isOrganicCampaignChecker: (CampaignKey) => Boolean = RecommendationMetricsKey.noChecking

  import com.gravity.utilities.grvcoll

  val defaultGeoLocationId: Int = 0
  val defaultAlgoId: Int = -1

  def partialByStartDate(date: DateHour): ArticleRecommendationMetricKey = ArticleRecommendationMetricKey(date, RecommendationMetricCountBy.minValue, 0, SiteKey(0l), 0, Integer.MIN_VALUE, Integer.MIN_VALUE)

  def partialByEndDate(date: DateHour): ArticleRecommendationMetricKey = ArticleRecommendationMetricKey(date, RecommendationMetricCountBy.maxValue, Integer.MAX_VALUE, SiteKey.maxValue, 0, Integer.MAX_VALUE, Integer.MAX_VALUE)

  def apply(recoMetricsKey: RecommendationMetricsKey): ArticleRecommendationMetricKey = {
    ArticleRecommendationMetricKey(recoMetricsKey.dateHour, recoMetricsKey.countBy, recoMetricsKey.bucketId, recoMetricsKey.siteKey, recoMetricsKey.placementId, recoMetricsKey.geoLocationId, recoMetricsKey.algoId)
  }

  def byArticleImpression(event: ImpressionEvent, article: ArticleRecoData): ArticleRecommendationMetricKey =
    ArticleRecommendationMetricKey(DateHour(event.getDate), RecommendationMetricCountBy.articleImpression, event.getBucketId, SiteKey(event.getSiteGuid), event.getPlacementId, GrccableEvent.geoLocationIdForMetrics, article.currentAlgo)

  def byUnitImpression(event: ImpressionEvent, article: ArticleRecoData): ArticleRecommendationMetricKey =
    ArticleRecommendationMetricKey(DateHour(event.getDate), RecommendationMetricCountBy.unitImpression, event.getBucketId, SiteKey(event.getSiteGuid), event.getPlacementId, GrccableEvent.geoLocationIdForMetrics, article.currentAlgo)

  def byClick(event: ClickEvent, contextualSiteGuid: String): ArticleRecommendationMetricKey =
    ArticleRecommendationMetricKey(DateHour(event.getClickDate), RecommendationMetricCountBy.click, event.getBucketId, SiteKey(event.getSiteGuid), event.getPlacementId, GrccableEvent.geoLocationIdForMetrics, event.getAlgoId)

  def groupMapgregate[G](map: Map[ArticleRecommendationMetricKey, Long], grouper: ((ArticleRecommendationMetricKey, Long)) => G): Map[G, RecommendationMetrics] = groupMapgregate(map.toSeq, grouper)

  def groupMapgregate[G](seq: Seq[(ArticleRecommendationMetricKey, Long)], grouper: ((ArticleRecommendationMetricKey, Long)) => G): Map[G, RecommendationMetrics] = {
    grvcoll.groupAndFold(RecommendationMetrics.empty)(seq)(grouper)({
      case (rm: RecommendationMetrics, kv: (ArticleRecommendationMetricKey, Long)) =>
        rm + kv._1.recoMetrics(kv._2)
    }).toMap
  }

  def groupMapgregate2[G](map: Map[ArticleRecommendationMetricKey, Long])(grouper: ((ArticleRecommendationMetricKey, Long)) => G): Map[G, RecommendationMetrics] = groupMapgregate(map.toSeq, grouper)


}

case class TopArticleRecommendationMetricKey(override val dateHour: DateHour,
                                             override val countBy: Byte,
                                             override val bucketId: Int,
                                             override val siteKey: SiteKey,
                                             override val placementId: Int,
                                             override val geoLocationId: Int = 0,
                                             override val algoId: Int = -1,
                                             override val sitePlacementId: Int = 0, // should be set but so far never is
                                             override val articleId: Long =  0 // need to set sensible (hopefully) default and preserve field ordering
                                             ) extends ArticleRecommendationMetricsKey

object TopArticleRecommendationMetricKey {

  def partialByStartDate(date: DateHour): TopArticleRecommendationMetricKey = TopArticleRecommendationMetricKey(date, RecommendationMetricCountBy.minValue, 0, SiteKey(0l), 0, Integer.MIN_VALUE, Integer.MIN_VALUE, 0, Long.MinValue)

  def partialByEndDate(date: DateHour): TopArticleRecommendationMetricKey = TopArticleRecommendationMetricKey(date, RecommendationMetricCountBy.maxValue, Integer.MAX_VALUE, SiteKey.maxValue, 0, Integer.MAX_VALUE, Integer.MAX_VALUE, 0, Long.MaxValue)

  def apply(articleRecoMetricsKey: ArticleRecommendationMetricsKey): TopArticleRecommendationMetricKey = {
    TopArticleRecommendationMetricKey(articleRecoMetricsKey.dateHour, articleRecoMetricsKey.countBy, articleRecoMetricsKey.bucketId, articleRecoMetricsKey.siteKey, articleRecoMetricsKey.placementId, articleRecoMetricsKey.geoLocationId, articleRecoMetricsKey.algoId, articleRecoMetricsKey.sitePlacementId, articleRecoMetricsKey.articleId)
  }

  def byArticleImpression(event: ImpressionEvent, article: ArticleRecoData): TopArticleRecommendationMetricKey =
    TopArticleRecommendationMetricKey(DateHour(event.getDate), RecommendationMetricCountBy.articleImpression, event.getBucketId, SiteKey(event.getSiteGuid), event.getPlacementId, GrccableEvent.geoLocationIdForMetrics, article.currentAlgo, 0, article.key.articleId)


  def byArticleClick(event: ClickEvent, article : ArticleRecoData): TopArticleRecommendationMetricKey =
    TopArticleRecommendationMetricKey(DateHour(event.getDate), RecommendationMetricCountBy.click, event.getBucketId, SiteKey(event.getSiteGuid), event.getPlacementId, GrccableEvent.geoLocationIdForMetrics, article.currentAlgo, 0, article.key.articleId)

}

case class CampaignMetricsKey(
                               dateHour: DateHour,
                               countBy: Byte,
                               bucketId: Int,
                               siteKey: SiteKey,
                               placementId: Int,
                               geoLocationId: Int,
                               algoId: Int,
                               cpc: DollarValue,
                               override val sitePlacementId: Int = 0
                               ) extends RecommendationMetricsKey {
  override def appendAdditionalFieldsForToString(sb: StringBuilder) {
    sb.append("; cpc: '").append(cpc).append("'")
  }
}

object CampaignMetricsKey {
  def apply(recoMetricsKey: RecommendationMetricsKey, cpc: DollarValue): CampaignMetricsKey = {
    CampaignMetricsKey(recoMetricsKey.dateHour, recoMetricsKey.countBy, recoMetricsKey.bucketId, recoMetricsKey.siteKey, recoMetricsKey.placementId, recoMetricsKey.geoLocationId, recoMetricsKey.algoId, cpc)
  }
}

case class SponsoredMetricsKey( dateHour: DateHour,
                                countBy: Byte,
                                bucketId: Int,
                                siteKey: SiteKey,
                                placementId: Int,
                                geoLocationId: Int,
                                algoId: Int,
                                campaignKey: CampaignKey,
                                cpc: DollarValue,
                                override val sitePlacementId: Int = 0
                                ) extends RecommendationMetricsKey with ControllableMetricsKeyLite {

  override def appendAdditionalFieldsForToString(sb: StringBuilder) {
    sb.append("; campaignKey: '").append(campaignKey).append("'")
    sb.append("; cpc: '").append(cpc).append("'")
  }

  override def recoMetrics(value: Long, isControl: Boolean = false)(implicit isOrganicCampaignChecker: (CampaignKey) => Boolean): RecommendationMetrics = countBy match {
    case RecommendationMetricCountBy.click =>
      val (orgClicks, sponsClicks, controlClicks, nonControlClicks) = if (isOrganicCampaignChecker(campaignKey)) {
        val (control, nonControl) = if (isControl) (value, 0L) else (0L, value)
        (value, 0L, control, nonControl)
      } else {
        (0L, value, 0L, 0L)
      }

      RecommendationMetrics(0l, value, 0l, orgClicks, sponsClicks, organicClicksControl = controlClicks, organicClicksNonControl = nonControlClicks)
    case RecommendationMetricCountBy.articleImpression => RecommendationMetrics(value, 0l, 0l)
    case RecommendationMetricCountBy.unitImpression =>
      val (control, nonControl) = if (isControl) (value, 0L) else (0L, value)
      RecommendationMetrics(0l, 0l, value, unitImpressionsControl = control, unitImpressionsNonControl = nonControl)
    case RecommendationMetricCountBy.unitImpressionViewed => RecommendationMetrics(0L, 0L, 0L, unitImpressionsViewed = value)
    case RecommendationMetricCountBy.conversion => RecommendationMetrics(0l, 0l, conversions = value)
    case _ => RecommendationMetrics.empty
  }

  def totalSpentPennies(value: Long): Long  = if (countBy == RecommendationMetricCountBy.click) value * cpc.pennies else 0L

  def totalSpent(value: Long): DollarValue = if (countBy == RecommendationMetricCountBy.click) DollarValue(value * cpc.pennies) else DollarValue.zero

  def sponsoredMetrics(value: Long, isControl: Boolean = false)(implicit isOrganicCampaignChecker: (CampaignKey) => Boolean): SponsoredRecommendationMetrics = {
    SponsoredRecommendationMetrics(recoMetrics(value, isControl), totalSpent(value))
  }

  def toDaily : SponsoredMetricsKey = SponsoredMetricsKey(dateHour.asMidnightHour, countBy, bucketId, siteKey, placementId, geoLocationId, algoId, campaignKey, cpc, sitePlacementId)

  def toMonthly : SponsoredMetricsKey = SponsoredMetricsKey(dateHour.asMonthHour, countBy, bucketId, siteKey, placementId, geoLocationId, algoId, campaignKey, cpc, sitePlacementId)
}

object SponsoredMetricsKey {
  def apply(recoMetricsKey: RecommendationMetricsKey, campaignKey: CampaignKey, cpc: DollarValue): SponsoredMetricsKey = {
    SponsoredMetricsKey(recoMetricsKey.dateHour, recoMetricsKey.countBy, recoMetricsKey.bucketId, recoMetricsKey.siteKey, recoMetricsKey.placementId, recoMetricsKey.geoLocationId, recoMetricsKey.algoId, campaignKey, cpc)
  }

  def partialByStartDate(date: DateHour): SponsoredMetricsKey = SponsoredMetricsKey(date, RecommendationMetricCountBy.minValue, SchemaTypeHelpers.intLowerBounds, SiteKey.minValue, 0, SchemaTypeHelpers.intLowerBounds, SchemaTypeHelpers.intLowerBounds, CampaignKey.minValue, DollarValue.minValue)

  def partialByEndDate(date: DateHour): SponsoredMetricsKey = SponsoredMetricsKey(date, RecommendationMetricCountBy.maxValue, SchemaTypeHelpers.intUpperBounds, SiteKey.maxValue, Int.MaxValue, SchemaTypeHelpers.intUpperBounds, SchemaTypeHelpers.intUpperBounds, CampaignKey.maxValue, DollarValue.maxValue)

  def groupMapgregate[G](map: Map[SponsoredMetricsKey, Long], grouper: ((SponsoredMetricsKey, Long)) => G)(implicit isOrganicCampaignChecker: (CampaignKey) => Boolean): Map[G, SponsoredRecommendationMetrics] = groupMapgregate(map.toSeq, grouper)

  def groupMapgregate[G](seq: Seq[(SponsoredMetricsKey, Long)], grouper: ((SponsoredMetricsKey, Long)) => G)(implicit isOrganicCampaignChecker: (CampaignKey) => Boolean): Map[G, SponsoredRecommendationMetrics] = {
    grvcoll.groupAndFold(SponsoredRecommendationMetrics.empty)(seq)(grouper)({
      case (rm: SponsoredRecommendationMetrics, kv: (SponsoredMetricsKey, Long)) =>
        rm + kv._1.sponsoredMetrics(kv._2)
    }).toMap
  }
}

case class ArticleSponsoredMetricsKey(
                                       dateHour: DateHour,
                                       countBy: Byte,
                                       bucketId: Int,
                                       siteKey: SiteKey,
                                       placementId: Int,
                                       geoLocationId: Int,
                                       algoId: Int,
                                       campaignKey: CampaignKey,
                                       cpc: DollarValue,
                                       //unit impressions won't have one
                                       articleKeyOption: Option[ArticleKey],
                                       override val sitePlacementId: Int = 0
                                       ) extends RecommendationMetricsKey {
  override def appendAdditionalFieldsForToString(sb: StringBuilder) {
    sb.append("; campaignKey: '").append(campaignKey).append("'")
    sb.append("; cpc: '").append(cpc).append("'")
  }

  def totalSpent(value: Long): DollarValue = if (countBy == RecommendationMetricCountBy.click) DollarValue(value * cpc.pennies) else DollarValue.zero

  def sponsoredMetrics(value: Long): SponsoredRecommendationMetrics = {
    SponsoredRecommendationMetrics(recoMetrics(value), totalSpent(value))
  }
}

object ArticleSponsoredMetricsKey {
  def apply(recoMetricsKey: RecommendationMetricsKey, campaignKey: CampaignKey, cpc: DollarValue): ArticleSponsoredMetricsKey = {
    ArticleSponsoredMetricsKey(recoMetricsKey.dateHour, recoMetricsKey.countBy, recoMetricsKey.bucketId, recoMetricsKey.siteKey, recoMetricsKey.placementId, recoMetricsKey.geoLocationId, recoMetricsKey.algoId, campaignKey, cpc, None)
  }

  def apply(recoMetricsKey: RecommendationMetricsKey, campaignKey: CampaignKey, cpc: DollarValue, articleKey: ArticleKey): ArticleSponsoredMetricsKey = {
    ArticleSponsoredMetricsKey(recoMetricsKey.dateHour, recoMetricsKey.countBy, recoMetricsKey.bucketId, recoMetricsKey.siteKey, recoMetricsKey.placementId, recoMetricsKey.geoLocationId, recoMetricsKey.algoId, campaignKey, cpc, Some(articleKey))
  }
}

case class ArticleRangeSortedKey(metricsInterval: Interval, publishInterval: Interval, sortId: Long, isDescending: Boolean) {
  override lazy val toString: String = {
    val sb = new StringBuilder
    sb.append("ArticleRangeSortedKey { metricsRange: ")
    if (isMetricsIntervalForAllTime) sb.append("ALL_TIME") else sb.append(metricsRange.toString)
    sb.append("; publishRange: ")
    if (isPublishIntervalForAllTime) sb.append("ALL_TIME") else sb.append(publishRange.toString)
    sb.append("; sortBy: ").append(sortByName)
    if (isDescending) sb.append(" (DESC) }") else sb.append(" (ASC) }")

    sb.toString()
  }

  lazy val toMinimalString: String = {
    val sb = new StringBuilder
    def range(isM: Boolean) = if ((isM && isMetricsIntervalForAllTime) || (!isM && isPublishIntervalForAllTime)) {
      sb.append("A__T")
    } else {
      val r = if (isM) metricsRange else publishRange
      sb.append(r.fromInclusive.toString("yyyy/M/d"))
      sb.append("-")
      sb.append(r.toInclusive.toString("yyyy/M/d"))
    }

    sb.append("S: ")
    if (isDescending) sb.append("-")
    sb.append(sortByName).append("; ")
    sb.append("M: ")
    range(isM = true)
    sb.append("; P: ")
    range(isM = false)
    sb.toString()
  }

  lazy val sortByName: String = ReportKey.lookupSortById(sortId).getOrElse("UNKNOWN")

  lazy val isPublishIntervalForAllTime: Boolean = ArticleRangeSortedKey.intervalForAllTime.equals(publishInterval)

  lazy val isMetricsIntervalForAllTime: Boolean = ArticleRangeSortedKey.intervalForAllTime.equals(metricsInterval)

  def getValidSortBy: Option[InterestSortBy.Value] = ReportKey.getSortBy(sortId) match {
    case Some(sb) => if (ArticleRangeSortedKey.enabledSorts.contains(sb)) Some(sb) else None
    case None => None
  }

  lazy val metricsRange: DateMidnightRange = DateMidnightRange(metricsInterval.getStart.toGrvDateMidnight, metricsInterval.getEnd.toGrvDateMidnight.minusDays(1))

  lazy val publishRange: DateMidnightRange = DateMidnightRange(publishInterval.getStart.toGrvDateMidnight, publishInterval.getEnd.toGrvDateMidnight.minusDays(1))
}

object ArticleRangeSortedKey {
  val intervalForAllTime: Interval = TimeSliceResolution.intervalForAllTime

  val enabledSorts: Set[InterestSortBy.Value] = Set(InterestSortBy.Revenue, InterestSortBy.MostRecentlyPublished, InterestSortBy.Views, InterestSortBy.TotalViral, InterestSortBy.PublishDate, InterestSortBy.Impressions, InterestSortBy.Clicks, InterestSortBy.CTR)

  val defaultSorts: List[InterestSortBy.Value] = List(InterestSortBy.Views) // add any needed oth here and in `enabledSorts' above

  val defaultSortIds: List[Long] = defaultSorts.map(s => ReportKey.getSortId(s))

  val igwSortId: Long = ReportKey.getSortId(InterestSortBy.TotalViral)

  val viewsSortId: Long = ReportKey.getSortId(InterestSortBy.Views)

  val socialSortId: Long = ReportKey.getSortId(InterestSortBy.Social)

  val publishDateSortId: Long = ReportKey.getSortId(InterestSortBy.PublishDate)

  def intervalUntilTodayFrom(numberOfDaysBack: Int): Interval = {
    val today = new GrvDateMidnight()
    new Interval(today.minusDays(numberOfDaysBack), today.plusDays(1))
  }

  val mostRecentArticlesKey: ArticleRangeSortedKey = ArticleRangeSortedKey(intervalForAllTime, intervalForAllTime, ReportKey.getSortId(InterestSortBy.MostRecentlyPublished), isDescending = true)

  def byClicksForRange(metricsRange: DateMidnightRange): ArticleRangeSortedKey = ArticleRangeSortedKey(metricsRange.interval, intervalForAllTime, ReportKey.getSortId(InterestSortBy.Clicks), isDescending = true)

  def apply(metricsInterval: Interval, publishInterval: Interval, sortBy: InterestSortBy.Value, isDescending: Boolean): ArticleRangeSortedKey = ArticleRangeSortedKey(metricsInterval, publishInterval, ReportKey.getSortId(sortBy), isDescending)

  def apply(metricsRange: DateMidnightRange, sortBy: InterestSortBy.Value, isDescending: Boolean): ArticleRangeSortedKey = ArticleRangeSortedKey(metricsRange.interval, intervalForAllTime, ReportKey.getSortId(sortBy), isDescending)

  def getDefaultKey(range: DateMidnightRange = TimeSliceResolution.lastThirtyDays.range): ArticleRangeSortedKey = apply(range.interval, intervalForAllTime, InterestSortBy.Views, isDescending = true)

  def igwKey: ArticleRangeSortedKey = {
    // IGW requires a single key: TotalViral with metrics for the past 3 days (inclusive of today) and a publish date for the past 8 days (inclusive of today)
    ArticleRangeSortedKey(
      TimeSliceResolution.pastThreeDays.interval,
      TimeSliceResolution.pastEightDays.interval,
      igwSortId, isDescending = true)
  }

  def mostViewedThatWerePublishedForTheLastNDays(numberOfDaysIncludingToday: Int): ArticleRangeSortedKey = {
    val today = new GrvDateMidnight()

    ArticleRangeSortedKey(
      intervalForAllTime,
      new Interval(today.minusDays(numberOfDaysIncludingToday - 1), today.plusDays(1)),
      viewsSortId,
      isDescending = true
    )
  }

  def mostSocialThatWerePublishedForTheLastNDays(numberOfDaysIncludingToday: Int): ArticleRangeSortedKey = {
    val today = new GrvDateMidnight()

    ArticleRangeSortedKey(
      intervalForAllTime,
      new Interval(today.minusDays(numberOfDaysIncludingToday - 1), today.plusDays(1)),
      socialSortId,
      isDescending = true
    )
  }

  def getRecentAndFewestViewsKeys: Set[ArticleRangeSortedKey] = {
    val intervals = Set(TimeSliceResolution.lastSevenDays.interval, TimeSliceResolution.yesterday.interval, TimeSliceResolution.today.interval)
    val sortId = ReportKey.getSortId(InterestSortBy.Views)
    val isDescending = false

    intervals.map(i => ArticleRangeSortedKey(intervalForAllTime, i, sortId, isDescending))
  }

  def getRecentAndMostViewsKeys: Set[ArticleRangeSortedKey] = {
    val intervals = Set(TimeSliceResolution.lastSevenDays.interval, TimeSliceResolution.yesterday.interval, TimeSliceResolution.today.interval)
    val sortId = ReportKey.getSortId(InterestSortBy.Views)
    val isDescending = true

    intervals.map(i => ArticleRangeSortedKey(intervalForAllTime, i, sortId, isDescending))
  }

  def getAllSiteTopicActiveKeyPermutations(incremental: Boolean = true): Set[ArticleRangeSortedKey] = {
    val dmrToday = TimeSliceResolution.today.range

    val intervalToday = dmrToday.interval

    val dmrLast7 = TimeSliceResolution.lastSevenDays.range
    val dmrLast30 = TimeSliceResolution.lastThirtyDays.range

    // Everything else needs the default sorts that include today for inclusive and last 7 + last 30 (and all days within) for non-inclusive
    val intervals = if (incremental) {
      Set(intervalToday, dmrLast7.slideAheadDays(1).interval, dmrLast30.slideAheadDays(1).interval)
    } else {
      Set(
        intervalToday,
        dmrLast7.interval,
        dmrLast7.slideAheadDays(1).interval, // handle midnight flip
        dmrLast30.interval,
        dmrLast30.slideAheadDays(1).interval // handle midnight flip
      ) ++ dmrLast30.singleDayIntervalsWithin
    }

    val others = for {
      i <- intervals
      sid <- defaultSortIds
    } yield {
      ArticleRangeSortedKey(i, intervalForAllTime, sid, isDescending = true)
    }

    Set(igwKey, mostRecentArticlesKey) ++ others
  }

  def getAllSiteActiveKeyPermutations(incremental: Boolean = true): Set[ArticleRangeSortedKey] = {
    getAllSiteTopicActiveKeyPermutations(incremental) ++ siteAdditionalKeys
  }

  def siteAdditionalKeys: Set[ArticleRangeSortedKey] = {
    val wsjRecos2 = mostViewedThatWerePublishedForTheLastNDays(2)
    val wsjRecos4 = mostViewedThatWerePublishedForTheLastNDays(4)
    val wsjRecos7 = mostViewedThatWerePublishedForTheLastNDays(7)
    val socialRecos2 = mostSocialThatWerePublishedForTheLastNDays(2)
    val socialRecos7 = mostSocialThatWerePublishedForTheLastNDays(7)

    Set(wsjRecos2, wsjRecos4, socialRecos2, wsjRecos7, socialRecos7) ++ getRecentAndFewestViewsKeys ++ getRecentAndMostViewsKeys
  }

  def getAllActiveKeyPermutations(incremental: Boolean = true): Set[ArticleRangeSortedKey] = {
    getAllSiteActiveKeyPermutations(incremental)
  }
}

object SectionKey {
  implicit val jsonFormat: Reads[SectionKey] = CanBeScopedKey.jsonFormat.map {
    case sk: SectionKey => sk
    case cbsk => throw new MappingException(s"Expected SectionKey, got $cbsk.")
  }

  def apply(siteGuid: String, sectionIdentifier: String): SectionKey = SectionKey(MurmurHash.hash64(siteGuid), MurmurHash.hash64(sectionIdentifier))

  def apply(siteGuid: String, sectionId: Long): SectionKey = SectionKey(MurmurHash.hash64(siteGuid), sectionId)

  def apply(siteKey: SiteKey, sectionIdentifier: String): SectionKey = SectionKey(siteKey.siteId, MurmurHash.hash64(sectionIdentifier))

  def apply(siteId: Long, sectionIdentifier: String): SectionKey = SectionKey(siteId, MurmurHash.hash64(sectionIdentifier))

  def partialBySiteStartKey(siteKey: SiteKey): SectionKey = SectionKey(siteKey.siteId, SchemaTypeHelpers.longLowerBounds)

  def partialBySiteEndKey(siteKey: SiteKey): SectionKey = SectionKey(siteKey.siteId, SchemaTypeHelpers.longUpperBounds)

  /** @deprecated Don't use this. This is only being used to aid transition to Play. */
  object TmpLiftSerializer extends Serializer[SectionKey] {
    override def deserialize(implicit format: Formats): PartialFunction[(TypeInfo, JValue), SectionKey] = {
      case (TypeInfo(cl, _), JString(keyStr)) if cl.toString.indexOf("SectionKey") != -1 => StringConverter.validateString(keyStr).fold(
        f => f.exceptionOption match {
          case Some(ex: Exception) => throw new MappingException(s"Couldn't parse $keyStr to SectionKey: ${f.message}", ex)
          case _ => throw new MappingException(s"Couldn't parse $keyStr to SectionKey: ${f.message}")
        },
        {
          case key: SectionKey => key
          case x => throw new MappingException(s"Parsed $keyStr to $x but needed SectionKey.")
        }
      )

      case (_, JObject(fields)) if fields.exists(_.name == "siteId") && fields.exists(_.name == "sectionId") =>
        val siteIdOpt = fields.find(_.name == "siteId").flatMap(_.value match {
          case JInt(num) => Some(num.toLong)
          case _ => None
        })

        val sectionIdOpt = fields.find(_.name == "sectionId").flatMap(_.value match {
          case JInt(num) => Some(num.toLong)
          case _ => None
        })

        (for {
          siteId <- siteIdOpt
          sectionId <- sectionIdOpt
        } yield SectionKey(siteId, sectionId)).getOrElse {
          throw new MappingException(s"Invalid fields $fields to create a SectionKey.")
        }
    }

    override def serialize(implicit format: Formats): PartialFunction[Any, JValue] = {
      case key: SectionKey => JString(StringConverter.writeString(key))
    }
  }
}

case class TaskResultKey(jobId: Long, taskId: Long)

object TaskResultKey {


  def apply(jobName: String, taskName: String): TaskResultKey = TaskResultKey(
    MurmurHash.hash64(jobName),
    MurmurHash.hash64(taskName)
  )
}

case class SectionKey(siteId: Long, sectionId: Long) extends CanBeScopedKey {
  def scope: Type = ScopedKeyTypes.SECTION

  override def stringConverter: SectionKeyStringConverter.type = SectionKeyStringConverter
}


case class UserSiteHourKey(userId: Long, siteId: Long, hourStamp: Long) {
  def comboHash: Long = {
    MurmurHash.hash64(Array.concat(Bytes.toBytes(userId), Bytes.toBytes(siteId), Bytes.toBytes(hourStamp)), 24)
  }
}

object UserSiteHourKey {
  def apply(userGuid: String, siteGuid: String, hour: DateHour): UserSiteHourKey = UserSiteHourKey(MurmurHash.hash64(userGuid), MurmurHash.hash64(siteGuid), hour.getMillis)
}

case class UserKey(userId: Long)

object UserKey {
  def apply(userGuid: String): UserKey = UserKey(MurmurHash.hash64(userGuid))
}

case class UserClusterKey(siteId: Long, clusterId: Long)

case class ExternalUserKey(partnerKeyId: Long, partnerUserId: Long)

object ExternalUserKey {
  def apply(partnerKey: ScopedKey, partnerUserGuid: String): ExternalUserKey = ExternalUserKey(MurmurHash.hash64(partnerKey.keyString), MurmurHash.hash64(partnerUserGuid))
}

@SerialVersionUID(1l)
case class ExchangeKey(exchangeId: Long) extends CanBeScopedKey {
  def scope: Type = ScopedKeyTypes.EXCHANGE_KEY

  def toString(keyValDelim: String = ":", fieldDelim: String = "_"): String = {
    (new StringBuilder).append("exchangeId").append(keyValDelim).append(exchangeId).toString()
  }

  override lazy val toString: String = toString()

  override def stringConverter: ExchangeKeyStringConverter.type = ExchangeKeyStringConverter
}

object ExchangeKey {
  def apply(exchangeGuid: String): ExchangeKey = ExchangeKey(MurmurHash.hash64(exchangeGuid))

  val empty: ExchangeKey = ExchangeKey(-1l)

  implicit val jsonFormat: Format[ExchangeKey] =  Format(Reads[ExchangeKey] {
    case JsNumber(num) => JsSuccess(ExchangeKey(num.toLong))
    case _ => JsError("expected number for ExchangeKey")
  }, Writes[ExchangeKey](x => JsNumber(x.exchangeId)))
}

@SerialVersionUID(1l)
case class ExchangeSiteKey(exchangeKey: ExchangeKey, siteKey: SiteKey) extends CanBeScopedKey {
  def scope: Type = ScopedKeyTypes.EXCHANGE_SITE_KEY

  def toString(keyValDelim: String = ":", fieldDelim: String = "_"): String = {
    (new StringBuilder).append(exchangeKey.toString(keyValDelim, fieldDelim)).append(fieldDelim)
        .append("siteId").append(keyValDelim).append(siteKey.siteId).toString()
  }

  override lazy val toString: String = toString()

  override def stringConverter: ExchangeSiteKeyStringConverter.type = ExchangeSiteKeyStringConverter
}

case class UserSiteKey(userId: Long, siteId: Long) {
  def comboHash: Long = {
    MurmurHash.hash64(Array.concat(Bytes.toBytes(userId), Bytes.toBytes(siteId)), 16)
  }


  def isBlankUser: Boolean = {
    UserSiteKey.emptyUserIds.contains(userId)
  }
}

object UserSiteKey {

  val emptyUserIds: Predef.Set[Long] = UserGuid.emptyUserGuids.map(guid=>UserSiteKey(guid,emptyString).userId) //List of user ids that are considered 'empty' regardless of iste
  val empty: UserSiteKey = UserSiteKey(emptyString, emptyString)

  def apply(userGuid: String, siteGuid: String): UserSiteKey = UserSiteKey(MurmurHash.hash64(userGuid), MurmurHash.hash64(siteGuid))

  def apply(userGuid: String, siteId: Long): UserSiteKey = UserSiteKey(MurmurHash.hash64(userGuid), siteId)

  def partialByUserStartKey(userGuid: String): UserSiteKey = partialByUserStartKey(MurmurHash.hash64(userGuid))

  def partialByUserEndKey(userGuid: String): UserSiteKey = partialByUserEndKey(MurmurHash.hash64(userGuid))

  def partialByUserStartKey(userId: Long): UserSiteKey = UserSiteKey(userId, SchemaTypeHelpers.longLowerBounds)

  //Byte rather than numeric, so 0 is the lowest value
  def partialByUserEndKey(userId: Long): UserSiteKey = UserSiteKey(userId, SchemaTypeHelpers.longUpperBounds) //We're doing byte rather than numeric comparison, so -1 is maxed long bytes

}

case class SiteTopicKey(siteId: Long, topicId: Long) {
  def siteKey: SiteKey = SiteKey(siteId)
}

object SiteTopicKey {
  def apply(siteGuid: String, topicUri: String): SiteTopicKey = SiteTopicKey(MurmurHash.hash64(siteGuid), MurmurHash.hash64(topicUri))

  def apply(siteGuid: String, topicUri: URI): SiteTopicKey = SiteTopicKey(MurmurHash.hash64(siteGuid), MurmurHash.hash64(topicUri.stringValue()))

  def apply(siteGuid: String, topicId: Long): SiteTopicKey = SiteTopicKey(MurmurHash.hash64(siteGuid), topicId)

  def apply(siteId: Long, topicUri: String): SiteTopicKey = SiteTopicKey(siteId, MurmurHash.hash64(topicUri))

  def apply(siteKey: SiteKey, topicId: Long): SiteTopicKey = SiteTopicKey(siteKey.siteId, topicId)

  val empty: SiteTopicKey = SiteTopicKey(0l, 0l)
}

case class ArticleArticleKey(article1: ArticleKey, article2: ArticleKey)

object ArticleArticleKey {

  def apply(articleId1: Long, articleId2: Long): ArticleArticleKey = ArticleArticleKey(ArticleKey(articleId1), ArticleKey(articleId2))

  def optimalOrdering(articleA: Long, articleB: Long): ArticleArticleKey = if (articleA < articleB) ArticleArticleKey(articleA, articleB) else ArticleArticleKey(articleB, articleA)

  def optimalOrdering(articleA: ArticleKey, articleB: ArticleKey): ArticleArticleKey = optimalOrdering(articleA.articleId, articleB.articleId)

}

case class ArticleTimeSpentKey(seconds: Short)
case class ArticleTimeSpentByDayKey(day: GrvDateMidnight, timeSpent: ArticleTimeSpentKey)
object ArticleTimeSpentByDayKey {
  def apply(dayMillis: Long, timeSpent: ArticleTimeSpentKey): ArticleTimeSpentByDayKey = ArticleTimeSpentByDayKey(new GrvDateMidnight(dayMillis), timeSpent)
  def apply(day: GrvDateMidnight, seconds: Int): ArticleTimeSpentByDayKey = ArticleTimeSpentByDayKey(day, ArticleTimeSpentKey(seconds.toShort))
  def apply(day: GrvDateMidnight, seconds: Short): ArticleTimeSpentByDayKey = ArticleTimeSpentByDayKey(day, ArticleTimeSpentKey(seconds))
  def apply(dayMillis: Long, seconds: Short): ArticleTimeSpentByDayKey = ArticleTimeSpentByDayKey(new GrvDateMidnight(dayMillis), ArticleTimeSpentKey(seconds))
}

case class ArticleAggregateByDayKey(day: GrvDateMidnight, aggregateType: Int, source: Byte)
object ArticleAggregateByDayKey {
  def apply(day: GrvDateMidnight, aggregateType: ArticleAggregateType.Type, source: ArticleAggregateSource.Type): ArticleAggregateByDayKey = ArticleAggregateByDayKey(day, aggregateType.i, source.id)
}

case class SectionTopicKey(section: SectionKey, topicId: Long)

case class UserFeedbackKey(feedbackVariation: Int, feedbackOption: Int)
case class UserFeedbackByDayKey(day: GrvDateMidnight, feedbackVariation: Int, feedbackOption: Int)

//
// grv:map support
//

object XsdType {
  val supportedTypes = List(XsdBooleanType, XsdIntType, XsdStringType)
  val fromName = Map(supportedTypes.map(x => x.typeString -> x):_*)
}

abstract class XsdType[T, J] {
  def typeString: String

  def asNative(strValue: String): ValidationNel[FailureResult, T]

  def asString(nativeValue: T): String

  def asValidJsValue(strValue: String): ValidationNel[FailureResult, J]

  def asTypedValue(stringValue: String): ValidationNel[FailureResult, XsdTypedValue]
}

object XsdStringType extends XsdType[String, JsString] {
  val typeString = "string"

  def asNative(strValue: String): ValidationNel[FailureResult, String] = strValue.successNel

  def asString(nativeValue: String): String = nativeValue

  def asValidJsValue(strValue: String): ValidationNel[FailureResult, JsString] = asNative(strValue).map(str => JsString(str))

  def asTypedValue(stringValue: String): ValidationNel[FailureResult, XsdTypedValue] = asNative(stringValue).map(v => XsdTypedValue(v))
}

object XsdIntType extends XsdType[Integer, JsNumber] {
  val typeString = "int"

  def asNative(strValue: String): ValidationNel[FailureResult, Integer] = {
    try {
      Integer.valueOf(strValue).successNel
    } catch {
      case ex: Exception => FailureResult(s"Failed to parse `$strValue` as type `$typeString`", ex).failureNel
    }
  }

  def asString(nativeValue: Integer): String = nativeValue.toString

  def asValidJsValue(strValue: String): ValidationNel[FailureResult, JsNumber] = asNative(strValue).map(i => JsNumber(BigDecimal(i)))

  def asTypedValue(stringValue: String): ValidationNel[FailureResult, XsdTypedValue] = asNative(stringValue).map(v => XsdTypedValue(v.intValue()))
}

object XsdBooleanType extends XsdType[java.lang.Boolean, JsBoolean] {
  val typeString = "boolean"

  def asNative(strValue: String): ValidationNel[FailureResult, java.lang.Boolean] = {
    try {
      strValue.toLowerCase match {
        case "true"  => java.lang.Boolean.TRUE.successNel
        case "false" => java.lang.Boolean.FALSE.successNel
        case _ => FailureResult(s"Failed to parse `$strValue` as type `$typeString`").failureNel
      }
    } catch {
      case ex: Exception => FailureResult(s"Failed to parse `$strValue` as type `$typeString`", ex).failureNel
    }
  }

  def asString(nativeValue: java.lang.Boolean): String = nativeValue.toString

  def asValidJsValue(strValue: String): ValidationNel[FailureResult, JsBoolean] = asNative(strValue).map(b => JsBoolean(b))

  def asTypedValue(stringValue: String): ValidationNel[FailureResult, XsdTypedValue] = asNative(stringValue).map(v => XsdTypedValue(v.booleanValue()))
}

case class XsdTypedValue(stringValue: Option[String], intValue: Option[Int], boolValue: Option[Boolean])

object XsdTypedValue {
  def apply(value: String): XsdTypedValue = XsdTypedValue(Some(value), None, None)
  def apply(value: Int): XsdTypedValue = XsdTypedValue(None, Some(value), None)
  def apply(value: Boolean): XsdTypedValue = XsdTypedValue(None, None, Some(value))
}

case class ArtGrvMapMetaVal(isPrivate: Boolean, xsdTypeName: String, grvMapVal: String) {
  def getTypedValue: ValidationNel[FailureResult, XsdTypedValue] = {
    for {
      xsd <- XsdType.fromName.get(xsdTypeName).toValidationNel(FailureResult("No XsdType found for xsdTypeName: " + xsdTypeName))
      typedValue <- xsd.asTypedValue(grvMapVal)
    } yield typedValue
  }

  def getString: ValidationNel[FailureResult, String] = {
    for {
      typedValue <- getTypedValue
      stringVal <- typedValue.stringValue.toValidationNel(FailureResult("Failed to get value `" + grvMapVal + "` as a String!"))
    } yield stringVal
  }

  def getInt: ValidationNel[FailureResult, Int] = {
    for {
      typedValue <- getTypedValue
      intVal <- typedValue.intValue.toValidationNel(FailureResult("Failed to get value `" + grvMapVal + "` as an Int!"))
    } yield intVal
  }

  def getBool: ValidationNel[FailureResult, Boolean] = {
    for {
      typedValue <- getTypedValue
      boolVal <- typedValue.boolValue.toValidationNel(FailureResult("Failed to get value `" + grvMapVal + "` as a Boolean!"))
    } yield boolVal
  }
}

object ArtGrvMapMetaVal {
  def apply(value: String, isPrivate: Boolean): ArtGrvMapMetaVal = ArtGrvMapMetaVal(isPrivate, XsdStringType.typeString, value)
  def apply(value: Int, isPrivate: Boolean): ArtGrvMapMetaVal = ArtGrvMapMetaVal(isPrivate, XsdIntType.typeString, value.toString)
  def apply(value: Boolean, isPrivate: Boolean): ArtGrvMapMetaVal = ArtGrvMapMetaVal(isPrivate, XsdBooleanType.typeString, value.toString)
  def apply(value: JsValue, isPrivate: Boolean): ArtGrvMapMetaVal = ArtGrvMapMetaVal(isPrivate, XsdStringType.typeString, Json.stringify(value))
}

object ArtGrvMapPublicMetaVal {
  def apply(value: String): ArtGrvMapMetaVal = ArtGrvMapMetaVal(isPrivate = false, XsdStringType.typeString, value)
  def apply(value: Int): ArtGrvMapMetaVal = ArtGrvMapMetaVal(isPrivate = false, XsdIntType.typeString, value.toString)
  def apply(value: Boolean): ArtGrvMapMetaVal = ArtGrvMapMetaVal(isPrivate = false, XsdBooleanType.typeString, value.toString)
  def apply(value: JsValue): ArtGrvMapMetaVal = ArtGrvMapMetaVal(isPrivate = false, XsdStringType.typeString, Json.stringify(value))
}

object ArtGrvMapPrivateMetaVal {
  def apply(value: String): ArtGrvMapMetaVal = ArtGrvMapMetaVal(isPrivate = true, XsdStringType.typeString, value)
  def apply(value: Int): ArtGrvMapMetaVal = ArtGrvMapMetaVal(isPrivate = true, XsdIntType.typeString, value.toString)
  def apply(value: Boolean): ArtGrvMapMetaVal = ArtGrvMapMetaVal(isPrivate = true, XsdBooleanType.typeString, value.toString)
  def apply(value: JsValue): ArtGrvMapMetaVal = ArtGrvMapMetaVal(isPrivate = true, XsdStringType.typeString, Json.stringify(value))
}

object ArtGrvMap {
  type OneScopeKey      = (Option[ScopedKey], String)
  type OneScopeMap      = scala.collection.Map[String, ArtGrvMapMetaVal]
  type AllScopesMap     = scala.collection.Map[OneScopeKey, OneScopeMap]
  type RelativeScopeMap = scala.collection.Map[String     , OneScopeMap]

  private val privateLabel    = "private:"
  private val privateLabelLen = privateLabel.size

  private implicit val jsonFormatOneScopeKey: OFormat[OneScopeKey] = (
    (__ \ "optScopedKey").formatNullable[ScopedKey] and
      (__ \ "namespace").format[String]
    )({ case (osk, str) => (osk, str) }, identity)

  // The way I did this, with the Exceptions, is wrong-headed; I should have used JsResult for-comprehensions.
  // Sorry, and please don't use this as an example for your own code.
  private implicit val jsonFormatOneScopeMap: Format[OneScopeMap] = Format(Reads[OneScopeMap] { jsOneScopeMapObject =>
    try {
      val oneScopeMap = jsOneScopeMapObject match {
        case JsObject(oneScopeMapFields) =>
          (for ((oneKey, oneVal) <- oneScopeMapFields) yield {
            val isPrivate = oneKey.startsWith(privateLabel)

            val newKey = if (isPrivate) oneKey.substring(privateLabelLen) else oneKey

            val newVal = oneVal match {
              case JsBoolean(bool)  => ArtGrvMapMetaVal(bool, isPrivate)
              case JsNumber(bigdec) => ArtGrvMapMetaVal(bigdec.toInt, isPrivate)
              case JsString(str)    => ArtGrvMapMetaVal(str, isPrivate)
              case other =>
                throw new Exception("Expected JsBoolean, JsNumber, or JsString for value in jsOneScopeMapObject")
            }

            newKey -> newVal
          }).toMap

        case _ =>
          throw new Exception("Expected JsObject for jsOneScopeMapObject")
      }

      JsSuccess(oneScopeMap)
    } catch {
      case ex: Exception =>
        JsError(s"Error parsing GrvMap.OneScopeMap: $ex")
    }
  }, Writes[OneScopeMap] { oneScopeMap =>
    grvMapAsJson(oneScopeMap.some, publicOnly = false).get
  })

  private implicit val jsonFormatOneScope: OFormat[(OneScopeKey, OneScopeMap)] = (
    (__ \ "oneScopeKey").format[OneScopeKey] and
      (__ \ "oneScopeMap").format[OneScopeMap]
    )({ case (oneScopeKey, oneScopeMap) => (oneScopeKey, oneScopeMap) }, identity)

  // OneScopeKeys are rich objects, so we serialize Map[OneScopeKey, OneScopeMap] as Seq[(OneScopeKey, OneScopeMap)]
  implicit val jsonFormatAllScopesMap: Format[AllScopesMap] = Format(Reads[AllScopesMap] { json =>
    json.validate[Seq[(OneScopeKey, OneScopeMap)]].map(_.toMap)
  }, Writes[AllScopesMap] { allScopesMap =>
    Json.toJson(allScopesMap.toSeq)
  })

  /**
   * Returns an Option[JSON String] representing ArticleRow's grv:map data for the article's site and current campaign, if any.
   *
   * @param optGrvMap The desired grv:map.
   * @param publicOnly If true, only includes the public fields in the grv:map (defaults to true).
   * @return An Option[JSON String]
   */
  def grvMapAsApiJsonString(optGrvMap: Option[ArtGrvMap.OneScopeMap], publicOnly: Boolean = true): Option[String] = {
    grvMapAsJson(optGrvMap, publicOnly).map(Json.stringify)
  }

  val noneGrvMapAsApiJsonString = grvMapAsApiJsonString(None)

  def grvMapAsJson(optGrvMap: Option[ArtGrvMap.OneScopeMap], publicOnly: Boolean = true): Option[JsValue] = {

    // Given an ArtGrvMapMetaVal, returns the equivalent JValue.
    def asJsValue(gmVal: ArtGrvMapMetaVal) = {
      XsdType.fromName(gmVal.xsdTypeName).asValidJsValue(gmVal.grvMapVal).valueOr {
        _ => JsString(gmVal.grvMapVal)
      }
    }

    // Given a Map[String, ArtGrvMapMetaVal], returns a JSON String representing the map's values.
    def asApiJson(richGrvMap: ArtGrvMap.OneScopeMap): JsValue = {
      val jFieldList = for {
        (key, value) <- richGrvMap.toList
        if !publicOnly || !value.isPrivate
      } yield {
        if (value.isPrivate)
          s"private:$key" -> asJsValue(value)
        else
          key -> asJsValue(value)
      }
      JsObject(jFieldList)
    }

    // Transmogrify campaign's grv:map appropriately for JSON.
    optGrvMap.map(grvMap => asApiJson(grvMap))
  }

  implicit class ArtGrvMapWrapper(val grvMap: OneScopeMap) {
    def getMetaVal(name: String): ValidationNel[FailureResult, ArtGrvMapMetaVal] = {
      grvMap.get(name).toValidationNel(FailureResult("Field name `" + name + "` was not found in article GrvMap!"))
    }

    def validateValue[T](name: String, default: T, isOptional: Boolean = false)(extractor: ArtGrvMapMetaVal => ValidationNel[FailureResult, T]): ValidationNel[FailureResult, T] = {
      if (isOptional && !grvMap.contains(name)) {
        return default.successNel
      }

      getMetaVal(name).flatMap(extractor)
    }

    def getValueFromGrvMap[T](name: String, default: T, isOptional: Boolean = false, failureHandler: NonEmptyList[FailureResult] => Unit = _ => {})(extractor: ArtGrvMapMetaVal => ValidationNel[FailureResult, T]): T = {
      validateValue(name, default, isOptional)(extractor).valueOr(fails => {
        failureHandler(fails)
        default
      })
    }

    def validateString(name: String, isOptional: Boolean = false, default: String = emptyString): ValidationNel[FailureResult, String] = {
      validateValue(name, default, isOptional)(_.getString)
    }

    def validateJson(name: String, isOptional: Boolean = false, default: JsValue = JsNull): ValidationNel[FailureResult, JsValue] = {
      val defaultPlaceholder = "ItbBIfXjlAasjZVuQ8iw"
      for {
        stringVal <- validateString(name, isOptional, defaultPlaceholder)
        jsValue <- {
          if(stringVal == defaultPlaceholder)
            default.successNel
          else
            tryToSuccessNEL(Json.parse(stringVal), ex => FailureResult(s"Couldn't parse $stringVal", ex))
        }
      } yield jsValue
    }

    def getStringFromGrvMap(name: String, isOptional: Boolean = false, default: String = emptyString, failureHandler: NonEmptyList[FailureResult] => Unit = _ => {}): String = {
      validateString(name, isOptional, default).valueOr(fails => {
        failureHandler(fails)
        default
      })
    }

    def validateInt(name: String, isOptional: Boolean = false, default: Int = 0): ValidationNel[FailureResult, Int] = {
      validateValue(name, default, isOptional)(_.getInt)
    }

    def getIntFromGrvMap(name: String, isOptional: Boolean = false, default: Int = 0, failureHandler: NonEmptyList[FailureResult] => Unit = _ => {}): Int = {
      validateInt(name, isOptional, default).valueOr(fails => {
        failureHandler(fails)
        default
      })
    }

    def validateBool(name: String, isOptional: Boolean = false, default: Boolean = false): ValidationNel[FailureResult, Boolean] = {
      validateValue(name, default, isOptional)(_.getBool)
    }

    def getBoolFromGrvMap(name: String, isOptional: Boolean = false, default: Boolean = false, failureHandler: NonEmptyList[FailureResult] => Unit = _ => {}): Boolean = {
      validateBool(name, isOptional, default).valueOr(fails => {
        failureHandler(fails)
        default
      })
    }
  }

  val emptyOneScopeMap: OneScopeMap = scala.collection.Map.empty[String, ArtGrvMapMetaVal]
  val emptyAllScopesMap: AllScopesMap = scala.collection.Map.empty[OneScopeKey, OneScopeMap]
  val emptyRelativeScopeMap: RelativeScopeMap = scala.collection.Map.empty[String, OneScopeMap]

  def toOneScopeKey(optScopeKey: Option[ScopedKey], namespace: String = emptyString): OneScopeKey = (optScopeKey, namespace)
  def toOneScopeKey(campaignKey: CampaignKey, namespace: String): OneScopeKey = toOneScopeKey(Option(campaignKey.toScopedKey), namespace)

  /**
   * @return Option[ArtGrvMap.OneScopeMap] -- Combines available/appropriate grv:map sources (Site-Scope, Campaign-Scope, etc.) into one merged map.
   */
  def combinedGrvMap(allArtGrvMap: AllScopesMap, scopes: Seq[ScopedKey] = Seq.empty): Option[ArtGrvMap.OneScopeMap] = {
    val siteScopedKeyOpt     = scopes.find(_.scope == ScopedKeyTypes.SITE)
    val campaignScopedKeyOpt = scopes.find(_.scope == ScopedKeyTypes.CAMPAIGN)

    val resultByScopeContext = siteScopedKeyOpt -> campaignScopedKeyOpt match {
      case (Some(_), Some(_)) =>
        // We have both site and campaign scopes. Merge their maps, overriding Site-based info with Campaign-based info.
        val merged = allArtGrvMap.getOrElse(ArtGrvMap.toOneScopeKey(siteScopedKeyOpt), ArtGrvMap.emptyOneScopeMap) ++
          allArtGrvMap.getOrElse(ArtGrvMap.toOneScopeKey(campaignScopedKeyOpt), ArtGrvMap.emptyOneScopeMap)

        // Only return `Some` if the merged result is non-empty
        if (merged.nonEmpty) Some(merged) else None

      case (Some(siteScopeKey), None) =>
        // We only have a site scope. Use its value
        allArtGrvMap.get(ArtGrvMap.toOneScopeKey(siteScopedKeyOpt))

      case (None, Some(campaignScopeKey)) =>
        // We only have campaign scope. Use its value
        allArtGrvMap.get(ArtGrvMap.toOneScopeKey(campaignScopedKeyOpt))

      case (None, None) =>
        // No scopes available...
        None
    }

    resultByScopeContext
  }
}


/**
 * Represents a 128-bit MD5 hash (as a 32-digit lowercase hex string).
 */
case class MD5HashKey(hexstr: String) {
  require(hexstr != null, s"Bad hexstr `$hexstr`, should be non-null")
  require(!hexstr.exists(_.isUpper), s"Bad hexstr `$hexstr`, should be lower-case")
}

/**
 * A Map key describing the image shape (and optionally, its size), which is currently always the original shape ("orig") and size (unspecified).
 */
case class ImageShapeAndSizeKey(shape: String)


@SerialVersionUID(-4854032496293545704L)
case class HostnameKey(hostname: Long) extends CanBeScopedKey {
  def scope: Type = ScopedKeyTypes.HOSTNAME

  /** Used to convert the child scoped key to a String that contains its type information (via StringConverter). */
  override def stringConverter: HostnameKeyStringConverter.type = HostnameKeyStringConverter
}

object HostnameKey {

  def apply(hostname: String): HostnameKey = HostnameKey(MurmurHash.hash64(hostname))

  val currentHostnameScopedKey = HostnameKey(InetAddress.getLocalHost.getHostName).toScopedKey
}

@SerialVersionUID(-4854032496293545704L)
case class DimensionKey(dimension: String) extends CanBeScopedKey {
  def scope = ScopedKeyTypes.DIMENSION_KEY

  /** Used to convert the child scoped key to a String that contains its type information (via StringConverter). */
  override def stringConverter = DimensionKeyStringConverter
}

@SerialVersionUID(-4854032496293545704L)
case class EntityKey(entity: String) extends CanBeScopedKey {
  def scope = ScopedKeyTypes.ENTITY_KEY

  /** Used to convert the child scoped key to a String that contains its type information (via StringConverter). */
  override def stringConverter = EntityKeyStringConverter
}

case class DemographicKey(demoSource: DemoSource.Type, gender: Gender.Type, ageGroup: AgeGroup.Type) {

}

@SerialVersionUID(1L)
case class RolenameKey(role: String) extends CanBeScopedKey {
  def scope: Type = ScopedKeyTypes.ROLE_KEY

  override def stringConverter: RolenameKeyStringConverter.type = RolenameKeyStringConverter
}

object RolenameKey {

  val currentRoleScopedKey = RolenameKey(grvroles.firstRole).toScopedKey
}

@SerialVersionUID(1L)
case class DashboardUserKey(siteKey: SiteKey, userId: Long) extends CanBeScopedKey {
  def scope: Type = ScopedKeyTypes.DASHBOARD_USER_KEY

  /** Used to convert the child scoped key to a String that contains its type information (via StringConverter). */
  override def stringConverter: DashboardUserKeyStringConverter.type = DashboardUserKeyStringConverter
}