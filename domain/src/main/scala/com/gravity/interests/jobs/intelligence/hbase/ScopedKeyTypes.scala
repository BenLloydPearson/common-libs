package com.gravity.interests.jobs.intelligence.hbase

import java.io.ByteArrayOutputStream

import com.gravity.domain.grvstringconverters.StringConverter
import com.gravity.hbase.schema.{StringConverter => _, _}
import com.gravity.interests.jobs.intelligence._
import com.gravity.interests.jobs.intelligence.hbase.CampaignNodeKeyConverters.CampaignNodeKeyConverter
import com.gravity.interests.jobs.intelligence.hbase.ScopedKeyConverters.ScopedKeyConverter
import com.gravity.utilities._
import com.gravity.utilities.components.FailureResult
import com.gravity.utilities.grvenum.GrvEnum
import com.gravity.utilities.grvjson._
import com.gravity.utilities.swagger.adapter.DefaultValueWriter
import com.gravity.valueclasses.ValueClassesForDomain._
import com.gravity.valueclasses.ValueClassesForUtilities._
import net.liftweb.json.JsonAST.{JString, JValue}
import net.liftweb.json.{Formats, MappingException, Serializer, TypeInfo}
import play.api.libs.json._

import scala.collection._
import scala.reflect._
import scalaz.syntax.std.option._
import scalaz.syntax.validation._
import scalaz.{Failure, Success, Validation, Index => _}



/**
 * Recommendation Scopes are a way to aggregate data at different levels: for example, cluster Articles at the Site or Sponsored Pool level.  Think of this as similar to the Super Sites
 * model, except allowing all key types to participate.
 *
 * See: http://confluence/pages/viewpage.action?pageId=14680986
 */
@SerialVersionUID(3523288479675267615l)
object ScopedKeyTypes extends GrvEnum[Short] {
  type ScopedKeyType = Type

  case class Type(i: Short, n: String) extends ValueTypeBase(i, n)

  override def mkValue(id: Short, name: String): Type = Type(id, name)

  def defaultValue: Type = SITE

  val ZERO: Type = Value(0, "zero_do_not_use")
  val SITE: Type = Value(1, "site")
  val SECTION: Type = Value(2, "section")
  val SPONSORED_POOL: Type = Value(3, "sponsored_pool")
  val SUPER_SITE: Type = Value(4, "super_site")
  val ARTICLE: Type = Value(5, "article")
  val CAMPAIGN: Type = Value(6, "campaign")
  val ALL_SITES: Type = Value(7, "all_sites")
  val CAMPAIGN_NODE: Type = Value(8, "campaign_node")
  val SITE_PLACEMENT: Type = Value(9, "site_placement")
  val NODE: Type = Value(10, "node")
  val LDA_CLUSTER: Type = Value(11, "behavioral_cluster")
  val SITE_PLACEMENT_WHY: Type = Value(12, "site_placement_why")
  val SITE_PLACEMENT_BUCKET: Type = Value(13, "site_placement_bucket")
  val SITE_PLACEMENT_BUCKET_SLOT: Type = Value(14, "site_placement_bucket_slot")
  val CAMPAIGN_ARTICLE: Type = Value(15, "campaign_article")
  val CONTENT_GROUP: Type = Value(16, "content_group")
  val CLUSTER_KEY: Type = Value(17, "cluster_key")
  val ARTICLE_ORDINAL_KEY: Type = Value(18, "article_ordinal_key")
  val ONTOLOGY_NODE_AND_TYPE: Type = Value(19, "ontology_node_and_type")
  val SITE_PLACEMENT_ID: Type = Value(20, "site_placement_id")
  val SITE_ALGO_SETTING_KEY: Type = Value(21, "site_algo_setting_key")
  val SITE_ALGO_KEY: Type = Value(22, "site_algo_key")
  val SITE_RECO_WORK_DIVISION_KEY: Type = Value(23, "site_reco_work_division_key")
  val RECOMMENDER_ID_KEY: Type = Value(24, "recommender_id_key")
  val SITEKEY_PPID: Type = Value(25, "siteguid_ppid")
  val HOSTNAME: Type = Value(26, "hostname")
  val PPID_KEY: Type = Value(27, "siteguid_ppid_domain")
  val SITE_PLACEMENT_ID_BUCKET: Type = Value(28, "site_placement_id_bucket")
  val ORDINAL_KEY: Type = Value(29, "ordinal_key")
  val SITEKEY_PPID_DOMAIN: Type = Value(30, "siteguid_ppid_domain2")
  val SITEKEY_DOMAIN: Type = Value(31, "siteguid_domain")
  val MULTI_KEY: Type = Value(32, "multi_key")
  val WIDGET_CONF_KEY: Type = Value(33, "widget_conf_key")
  val REGION_KEY: Type = Value(34, "region_key")
  val NOTHING_KEY: Type = Value(35, "nothing_key")
  val ROLE_KEY: Type = Value(36, "role")
  val DIMENSION_KEY = Value(37, "dimension_key")
  val ENTITY_KEY = Value(38, "entity_key")
  val ENTITY_SCOPE_KEY = Value(39, "entity_scope_key")
  val DIMENSION_SCOPE_KEY = Value(40, "dimension_scope_key")
  val DASHBOARD_USER_KEY = Value(41, "dashboard_user_key")
  val SITE_PLACEMENT_BUCKET_ALT = Value(42, "site_placement_bucket_alt")
  val EXCHANGE_KEY = Value(43, "exchange_key")
  val EXCHANGE_SITE_KEY = Value(44, "exchange_site_key")

  implicit val jsonFormat: Format[Type] = makeJsonFormat[Type]

  // used for filtering.  They will automatically map to the range of types defined above so that we can
  // restrict our query to only those types.  This avoids issues with other roles having new types that we are not aware of
  val filterMinType: Type = Type(values.map(_.id).min, "filter_min_do_not_use")
  val filterMaxType: Type = Type(values.map(_.id).max, "filter_max_do_not_use")

}


/**
 * The Context expresses what context a ScopedKey is used in.  Useful when you need to pivot things by implementation.
 * For example, you might want to store Site interest graphs, but you need several different approaches to how they are stored.
 */
object ScopedKeyContexts extends GrvEnum[Short] {
  type ScopedKeyContext = Type

  case class Type(i: Short, n: String) extends ValueTypeBase(i, n)

  override def mkValue(id: Short, name: String): Type = Type(id, name)

  def defaultValue: Type = DEFAULT

  val ZERO: Type = Value(0, "zero_do_not_use")
  val DEFAULT: Type = Value(1, "default_context")
  val SECTIONAL_BY_TEXT: Type = Value(2, "sectional_by_text")
  val SECTIONAL_BY_SUBDOMAIN: Type = Value(3, "sectional_by_subdomain")
  val NODE_CLICK_LINK: Type = Value(4, "node_click_link")

  // used for filtering.  They will automatically map to the range of types defined above so that we can
  // restrict our query to only those types.  This avoids issues with other roles having new types that we are not aware of
  val filterMinType: Type = Type(values.map(_.id).min, "filter_min_do_not_use")
  val filterMaxType: Type = Type(values.map(_.id).max, "filter_max_do_not_use")

  @deprecated("These metrics will no longer be populated, now request them via the default scope", "late 2014")
  val IMPRESSION_VIEWED_OVERLAY: Type = Value(5, "impression-viewed-overlay")

  implicit val jsonFormat: Format[Type] = makeJsonFormat[Type]
}

/**
 * An item that can participate polymorphically in a Recommendation Scope.  See above for documentation.
 *
 * DO NOT ADD MEMBERS TO THIS TRAIT.  IT PARTICIPATES IN SERIALIZATION.
 */
trait CanBeScopedKey {
  def scope: ScopedKeyTypes.Type

  def toScopedKey: ScopedKey = ScopedKey(this, scope)

  def toScopedKeyInContext(contextType: ScopedKeyContexts.ScopedKeyContext): ScopedKey = ScopedKey(this, scope, contextType)

  /** Used to convert the child scoped key to a String that contains its type information (via StringConverter). */
  def stringConverter: StringConverter[_ >: this.type]

  final def stringConverterSerialized: String = stringConverter.writeString(this)
}

object CanBeScopedKey {
  implicit val jsonFormat: Format[CanBeScopedKey] = Format(Reads[CanBeScopedKey] {
    case JsString(keyStr) =>
      StringConverter.validateString(keyStr).fold[JsResult[CanBeScopedKey]](
        fail => JsError(fail),
        {
          case key: CanBeScopedKey => JsSuccess(key)
          case x => JsError(s"$x (key string $keyStr) is not a CanBeScopedKey.")
        }
      )

    /**
     * These are here to support the legacy serialization of certain CanBeScopedKey to their JS object format. We intend
     * to use the StringConverter format going forward so please avoid this serialization.
     */
    case o: JsObject if o.iffFieldNames("articleId") => (o \ "articleId").tryToLong.map(ArticleKey.apply)
    case o: JsObject if o.iffFieldNames("siteId") => (o \ "siteId").tryToLong.map(SiteKey.apply)
    case o: JsObject if o.iffFieldNames("siteKey", "campaignId") => for {
        siteKey <- (o \ "siteKey").validate[CanBeScopedKey].flatMap {
            case key: SiteKey => key.jsSuccess
            case key => JsError(s"$key is not a SiteKey.")
          }
        campaignId <- (o \ "campaignId").tryToLong
      } yield CampaignKey(siteKey, campaignId)
    case o: JsObject if o.iffFieldNames("siteId", "sectionId") => for {
        siteId <- (o \ "siteId").tryToLong
        sectionId <- (o \ "sectionId").tryToLong
      } yield SectionKey(siteId, sectionId)

    case x => JsError(s"Can't convert $x to CanBeScopedKey.")
  }, Writes[CanBeScopedKey](cbsk => JsString(cbsk.stringConverterSerialized)))

  implicit val defaultValueWriter: DefaultValueWriter[CanBeScopedKey] with Object {def serialize(t: CanBeScopedKey): String} = new DefaultValueWriter[CanBeScopedKey] {
    override def serialize(t: CanBeScopedKey): String = t.stringConverterSerialized
  }
}

case class ScopedFromToKey(from: ScopedKey, to: ScopedKey)

object ScopedFromToKey {
  def partialByFrom(from: ScopedKey): ScopedFromToKey = ScopedFromToKey(from, ScopedKey.zero)

  def partialByFromEnd(from: ScopedKey): ScopedFromToKey = ScopedFromToKey(from, ScopedKey.max)

  def apply(tuple2: (ScopedKey, ScopedKey)): ScopedFromToKey = ScopedFromToKey(tuple2._1, tuple2._2)

  val empty: ScopedFromToKey = ScopedFromToKey(NothingKey.toScopedKey, NothingKey.toScopedKey)
}


object ScopedMetricsBucket extends Enumeration {
  type ScopedMetricsBucket = Value
  val minutely, hourly, daily, monthly = Value
}

/**
 * A way to use row keys polymorphically, and also a way to decorate row keys with the Recommendation Scope in which they're participating.
 *
 * http://confluence/pages/viewpage.action?pageId=14680986
 */
@SerialVersionUID(-6833401805150161840l)
case class ScopedKey(objectKey: CanBeScopedKey, scope: ScopedKeyTypes.Type, context: ScopedKeyContexts.Type = ScopedKeyContexts.DEFAULT) {

  import com.gravity.utilities.components.FailureResult

  def typedKey[T <: CanBeScopedKey]: T = objectKey.asInstanceOf[T]

  def tryToTypedKey[T <: CanBeScopedKey : Manifest]: Validation[FailureResult, T] = ScalaMagic.tryCast[T](objectKey)

  def transformContext(newContext: ScopedKeyContexts.Type): ScopedKey = copy(context = newContext)

  def fromToKey(otherKey: ScopedKey): ScopedFromToKey = ScopedFromToKey(this, otherKey)

  def fromEverythingToThisKey: ScopedFromToKey = ScopedFromToKey(EverythingKey.toScopedKey, this)

  override lazy val toString: String = {
    keyString
  }

  def toScopedKeyString: String = {
    "{\"scope\":\"" + scope + "\", \"context\":\"" + context + "\", \"key\":\"" + keyString + "\"}"
  }

  def keyType: String = scope match {
    case ScopedKeyTypes.SITE | ScopedKeyTypes.SUPER_SITE | ScopedKeyTypes.SPONSORED_POOL => classOf[SiteKey].getName
    case ScopedKeyTypes.ARTICLE | ScopedKeyTypes.ZERO => classOf[ArticleKey].getName
    case ScopedKeyTypes.CAMPAIGN => classOf[CampaignKey].getName
    case ScopedKeyTypes.SECTION => classOf[SectionKey].getName
    case ScopedKeyTypes.CAMPAIGN_NODE => classOf[CampaignNodeKey].getName
    case ScopedKeyTypes.NODE => classOf[NodeKey].getName
    case ScopedKeyTypes.SITE_PLACEMENT => classOf[SitePlacementKey].getName
    case ScopedKeyTypes.SITE_PLACEMENT_BUCKET => classOf[SitePlacementBucketKey].getName
    case ScopedKeyTypes.SITE_PLACEMENT_BUCKET_SLOT => classOf[SitePlacementBucketSlotKey].getName
    case ScopedKeyTypes.SITE_PLACEMENT_WHY => classOf[SitePlacementWhyKey].getName
    case ScopedKeyTypes.ALL_SITES => classOf[EverythingKey].getName
    case ScopedKeyTypes.CAMPAIGN_ARTICLE => classOf[CampaignArticleKey].getName
    case ScopedKeyTypes.CONTENT_GROUP => classOf[ContentGroupKey].getName
    case ScopedKeyTypes.CLUSTER_KEY => classOf[ClusterKey].getName
    case ScopedKeyTypes.ARTICLE_ORDINAL_KEY => classOf[ArticleOrdinalKey].getName
    case ScopedKeyTypes.ONTOLOGY_NODE_AND_TYPE => classOf[OntologyNodeAndTypeKey].getName
    case ScopedKeyTypes.SITE_PLACEMENT_ID => classOf[SitePlacementIdKey].getName
    case ScopedKeyTypes.SITE_PLACEMENT_ID_BUCKET => classOf[SitePlacementIdBucketKey].getName
    case ScopedKeyTypes.SITE_ALGO_SETTING_KEY => classOf[SiteAlgoSettingKey].getName
    case ScopedKeyTypes.SITE_ALGO_KEY => classOf[SiteAlgoKey].getName
    case ScopedKeyTypes.SITE_RECO_WORK_DIVISION_KEY => classOf[SiteRecoWorkDivisionKey].getName
    case ScopedKeyTypes.RECOMMENDER_ID_KEY => classOf[RecommenderIdKey].getName
    case ScopedKeyTypes.ORDINAL_KEY => classOf[OrdinalKey].getName
    case ScopedKeyTypes.MULTI_KEY => classOf[MultiKey].getName
    case ScopedKeyTypes.HOSTNAME => classOf[HostnameKey].getName
    case ScopedKeyTypes.ROLE_KEY => classOf[RolenameKey].getName
    case ScopedKeyTypes.DIMENSION_KEY => classOf[DimensionKey].getName
    case ScopedKeyTypes.ENTITY_KEY => classOf[EntityKey].getName
    case ScopedKeyTypes.ENTITY_SCOPE_KEY => classOf[EntityScopeKey].getName
    case ScopedKeyTypes.DIMENSION_SCOPE_KEY => classOf[DimensionScopeKey].getName
    case ScopedKeyTypes.DASHBOARD_USER_KEY => classOf[DashboardUserKey].getName
    case ScopedKeyTypes.EXCHANGE_KEY => classOf[ExchangeKey].getName
    case ScopedKeyTypes.EXCHANGE_SITE_KEY => classOf[ExchangeSiteKey].getName
    case wtf => "Scope not supported: " + wtf
  }

  def keyString: String = scope match {
    case ScopedKeyTypes.SITE | ScopedKeyTypes.SUPER_SITE | ScopedKeyTypes.SPONSORED_POOL => StringConverter.writeString[SiteKey](typedKey[SiteKey])
    case ScopedKeyTypes.ARTICLE | ScopedKeyTypes.ZERO => StringConverter.writeString[ArticleKey](typedKey[ArticleKey])
    case ScopedKeyTypes.CAMPAIGN => StringConverter.writeString[CampaignKey](typedKey[CampaignKey])
    case ScopedKeyTypes.SECTION => StringConverter.writeString[SectionKey](typedKey[SectionKey])
    case ScopedKeyTypes.CAMPAIGN_NODE => StringConverter.writeString[CampaignNodeKey](typedKey[CampaignNodeKey])
    case ScopedKeyTypes.NODE => StringConverter.writeString[NodeKey](typedKey[NodeKey])
    case ScopedKeyTypes.SITE_PLACEMENT => StringConverter.writeString[SitePlacementKey](typedKey[SitePlacementKey])
    case ScopedKeyTypes.SITE_PLACEMENT_BUCKET => StringConverter.writeString[SitePlacementBucketKey](typedKey[SitePlacementBucketKey])
    case ScopedKeyTypes.SITE_PLACEMENT_BUCKET_SLOT => StringConverter.writeString[SitePlacementBucketSlotKey](typedKey[SitePlacementBucketSlotKey])
    case ScopedKeyTypes.SITE_PLACEMENT_WHY => StringConverter.writeString[SitePlacementWhyKey](typedKey[SitePlacementWhyKey])
    case ScopedKeyTypes.ALL_SITES => StringConverter.writeString[EverythingKey](typedKey[EverythingKey])
    case ScopedKeyTypes.CAMPAIGN_ARTICLE => StringConverter.writeString[CampaignArticleKey](typedKey[CampaignArticleKey])
    case ScopedKeyTypes.CONTENT_GROUP => StringConverter.writeString[ContentGroupKey](typedKey[ContentGroupKey])
//    case ScopedKeyTypes.CLUSTER_KEY => StringConverter.writeString[ClusterKey](typedKey[ClusterKey])
    case ScopedKeyTypes.ARTICLE_ORDINAL_KEY => StringConverter.writeString[ArticleOrdinalKey](typedKey[ArticleOrdinalKey])
    case ScopedKeyTypes.ONTOLOGY_NODE_AND_TYPE => StringConverter.writeString[OntologyNodeAndTypeKey](typedKey[OntologyNodeAndTypeKey])
    case ScopedKeyTypes.SITE_PLACEMENT_ID => StringConverter.writeString[SitePlacementIdKey](typedKey[SitePlacementIdKey])
    case ScopedKeyTypes.SITE_PLACEMENT_ID_BUCKET => StringConverter.writeString[SitePlacementIdBucketKey](typedKey[SitePlacementIdBucketKey])
    case ScopedKeyTypes.SITE_ALGO_SETTING_KEY => StringConverter.writeString[SiteAlgoSettingKey](typedKey[SiteAlgoSettingKey])
    case ScopedKeyTypes.SITE_ALGO_KEY => StringConverter.writeString[SiteAlgoKey](typedKey[SiteAlgoKey])
    case ScopedKeyTypes.SITE_RECO_WORK_DIVISION_KEY => StringConverter.writeString[SiteRecoWorkDivisionKey](typedKey[SiteRecoWorkDivisionKey])
    case ScopedKeyTypes.RECOMMENDER_ID_KEY => StringConverter.writeString[RecommenderIdKey](typedKey[RecommenderIdKey])
    case ScopedKeyTypes.ORDINAL_KEY => StringConverter.writeString[OrdinalKey](typedKey[OrdinalKey])
    case ScopedKeyTypes.MULTI_KEY => StringConverter.writeString[MultiKey](typedKey[MultiKey])
    case ScopedKeyTypes.HOSTNAME => StringConverter.writeString[HostnameKey](typedKey[HostnameKey])
    case ScopedKeyTypes.DIMENSION_KEY => StringConverter.writeString[DimensionKey](typedKey[DimensionKey])
    case ScopedKeyTypes.ENTITY_KEY => StringConverter.writeString[EntityKey](typedKey[EntityKey])
    case ScopedKeyTypes.ENTITY_SCOPE_KEY => StringConverter.writeString[EntityScopeKey](typedKey[EntityScopeKey])
    case ScopedKeyTypes.DIMENSION_SCOPE_KEY => StringConverter.writeString[DimensionScopeKey](typedKey[DimensionScopeKey])
    case ScopedKeyTypes.DASHBOARD_USER_KEY => StringConverter.writeString[DashboardUserKey](typedKey[DashboardUserKey])
    case ScopedKeyTypes.EXCHANGE_KEY => StringConverter.writeString[ExchangeKey](typedKey[ExchangeKey])
    case ScopedKeyTypes.EXCHANGE_SITE_KEY => StringConverter.writeString[ExchangeSiteKey](typedKey[ExchangeSiteKey])
    case _ => objectKey.scope.toString
  }
}


object ScopedKey {

  val hostname: String = Settings.CANONICAL_HOST_NAME

  val zero: ScopedKey = ScopedKey(ArticleKey.empty, ScopedKeyTypes.ZERO)
  val max: ScopedKey = ScopedKey(ArticleKey.max, ScopedKeyTypes.ZERO)

  def partialByTypeFrom(t: ScopedKeyTypes.Type): ScopedKey = ScopedKey(CampaignKey.minValue, t, ScopedKeyContexts.filterMinType)
  def partialByTypeTo(t: ScopedKeyTypes.Type): ScopedKey = ScopedKey(CampaignKey.maxValue, t, ScopedKeyContexts.filterMaxType)

  def extractScopes(bId: Option[BucketId], plId: Option[PlacementId], ix: Option[Index], siteId: Option[SiteId], spId: Option[SitePlacementId]): Seq[ScopedKey] = {
    val scopes = Seq(

      for(b <- bId; pl <- plId; i <- ix; sId <- siteId) yield {
        // site placement bucket slot level
        SitePlacementBucketSlotKey(i.raw, b.raw, pl.raw, sId.raw).toScopedKey
      },

      for(b <- bId; sp <- spId) yield {
        // site placement ID bucket level
        SitePlacementIdBucketKey(sp, b).toScopedKey
      },

      for(b <- bId; pl <- plId; sId <- siteId) yield {
        // site placement bucket level
        SitePlacementBucketKey(b.raw, pl.raw, sId.raw).toScopedKey
      },

      for(pl <- plId; sId <- siteId) yield {
        // site placement level
        SitePlacementKey(pl.raw, sId.raw).toScopedKey
      },

      for(sp <- spId) yield {
        //Site placement level by SitePlacementId
        SitePlacementIdKey(sp).toScopedKey
      },

      for(sId <- siteId) yield {
        //Site level by siteId
        sId.siteKey.toScopedKey
      },

      HostnameKey.currentHostnameScopedKey.some,

      RolenameKey.currentRoleScopedKey.some,

      EverythingKey.toScopedKey.some

    ).flatten

    // prepend host to all scopes and prioritize above the rest
    scopes.map(sk => MultiKey(Seq(HostnameKey(hostname).toScopedKey, sk)).toScopedKey) ++ scopes
  }

  def validateKeyString(keyString: String): Validation[FailureResult, ScopedKey] = StringConverter.validateString(keyString) match {
    case Success(keyValue) => keyValue match {
      case scopedKey: ScopedKey => scopedKey.success
      case canBe: CanBeScopedKey => canBe.toScopedKey.success
      case wtf => FailureResult("keyString `" + keyString + "` was parsed successfully, but did not return a ScopedKey or CanBeScopedKey instance of: " + wtf.getClass.getName).failure
    }
    case Failure(failed) => failed.failure
  }

  implicit val defaultScopedKeyConverter: ScopedKeyConverter.type = ScopedKeyConverters.ScopedKeyConverter

  object ScopedKeyJsonSerializer extends Serializer[ScopedKey] {
    val ScopedKeyClass: Class[ScopedKey] = classOf[ScopedKey]

    def deserialize(implicit format: Formats): PartialFunction[(TypeInfo, JValue), ScopedKey] = {
      case (TypeInfo(ScopedKeyClass, _), json) => {
        json match {
          case JString(value) =>
            val conv = ScopedKey.validateKeyString(value)
            conv.getOrElse(throw new MappingException("Couldn't deser scoped key: " + value))
          case value => {
            throw new MappingException(s"ScopedKeySerializer can't convert $value to ScopedKeySerializer")
          }
        }
      }
    }

    def serialize(implicit format: Formats): PartialFunction[Any, JValue] = {
      case i: ScopedKey => {
        val res = i.keyString
        JString(res)
      }
    }
  }

  import play.api.libs.functional.syntax._

  /** When the above format is removed, this one can be marked implicit and used ad infinitum. */
  val scopedKeyJsonFormat: Format[ScopedKey] = (
    (__ \ "objectKey").format[CanBeScopedKey] and
    (__ \ "scope").format[ScopedKeyTypes.Type] and
    (__ \ "context").formatNullable[ScopedKeyContexts.Type]
  )(
    { case (objectKey, scope, contextOpt) => ScopedKey(objectKey, scope, contextOpt.getOrElse(ScopedKeyContexts.DEFAULT)) },
    sk => (sk.objectKey, sk.scope, sk.context.some)
  )

  /**
   * @deprecated Temporarily used while transitioning from Liftweb to Play. When the above format is removed, this one
   *             can be marked implicit and used ad infinitum.
   */
  implicit val tmpScopedKeyJsonFormat: Format[ScopedKey] = Format(
    Reads[ScopedKey] {
      case JsString(keyStr) => StringConverter.validateString(keyStr).fold(
        f => JsError(failureResultToValidationError(f)),
        {
          case key: CanBeScopedKey => key.toScopedKey.jsSuccess
          case x => JsError(s"$keyStr expected to deserialize to CanBeScopedKey; instead got $x.")
        }
      )

      case obj: JsObject if obj.hasFieldNames("objectKey", "scope") => scopedKeyJsonFormat.reads(obj)

      case x => JsError(s"Couldn't read $x into ScopedKey.")
    },
    scopedKeyJsonFormat
  )

  implicit val defaultValueWriter: DefaultValueWriter[ScopedKey] with Object {def serialize(t: ScopedKey): String} = new DefaultValueWriter[ScopedKey] {
    override def serialize(t: ScopedKey): String = t.objectKey.stringConverterSerialized
  }
}


object ScopedKeyConverters {

  implicit object ScopedKeyConverter extends ComplexByteConverter[ScopedKey] {
    val version: Byte = 2

    def write(data: ScopedKey, output: PrimitiveOutputStream) {
      output.writeByte(version) //For the future

      // used for filtering
      if (data.scope == ScopedKeyTypes.filterMinType || data.scope == ScopedKeyTypes.filterMaxType) {
        output.writeShort(data.scope.id)
        output.writeShort(data.context.id)
        output.writeObj(data.typedKey[CampaignKey])(SchemaTypes.CampaignKeyConverter)
        return
      }

      output.writeShort(data.scope.id)
      output.writeShort(data.context.id)
      data.scope match {
        case ScopedKeyTypes.SITE => output.writeObj(data.typedKey[SiteKey])(SchemaTypes.SiteKeyConverter)
        case ScopedKeyTypes.SUPER_SITE => output.writeObj(data.typedKey[SiteKey])(SchemaTypes.SiteKeyConverter)
        case ScopedKeyTypes.SPONSORED_POOL => output.writeObj(data.typedKey[SiteKey])(SchemaTypes.SiteKeyConverter)
        case ScopedKeyTypes.ARTICLE => output.writeObj(data.typedKey[ArticleKey])(SchemaTypes.ArticleKeyConverter)
        case ScopedKeyTypes.CAMPAIGN => output.writeObj(data.typedKey[CampaignKey])(SchemaTypes.CampaignKeyConverter)
        case ScopedKeyTypes.SECTION => output.writeObj(data.typedKey[SectionKey])(SchemaTypes.SectionKeyConverter)
        case ScopedKeyTypes.ZERO => output.writeObj(data.typedKey[ArticleKey])(SchemaTypes.ArticleKeyConverter)
        case ScopedKeyTypes.CAMPAIGN_NODE => output.writeObj(data.typedKey[CampaignNodeKey])(CampaignNodeKeyConverter)
        case ScopedKeyTypes.SITE_PLACEMENT => output.writeObj(data.typedKey[SitePlacementKey])(SchemaTypes.SitePlacementKeyConverter)
        case ScopedKeyTypes.SITE_PLACEMENT_ID => output.writeObj(data.typedKey[SitePlacementIdKey])(SchemaTypes.SitePlacementIdKeyConverter)
        case ScopedKeyTypes.SITE_PLACEMENT_ID_BUCKET => output.writeObj(data.typedKey[SitePlacementIdBucketKey])(SchemaTypes.SitePlacementIdBucketKeyConverter)
        case ScopedKeyTypes.SITE_PLACEMENT_BUCKET => output.writeObj(data.typedKey[SitePlacementBucketKey])(SchemaTypes.SitePlacementBucketKeyConverter)
        case ScopedKeyTypes.SITE_PLACEMENT_BUCKET_SLOT => output.writeObj(data.typedKey[SitePlacementBucketSlotKey])(SchemaTypes.SitePlacementBucketSlotKeyConverter)
        case ScopedKeyTypes.SITE_PLACEMENT_WHY => output.writeObj(data.typedKey[SitePlacementWhyKey])(SchemaTypes.SitePlacementWhyKeyConverter)
        case ScopedKeyTypes.ALL_SITES => output.writeObj(data.typedKey[EverythingKey])(SchemaTypes.EverythingKeyConverter)
        case ScopedKeyTypes.NODE => output.writeObj(data.typedKey[NodeKey])(SchemaTypes.NodeKeyConverter)
        case ScopedKeyTypes.LDA_CLUSTER => output.writeObj(data.typedKey[LDAClusterKey])(SchemaTypes.LDAClusterKeyConverter)
        case ScopedKeyTypes.CAMPAIGN_ARTICLE => output.writeObj(data.typedKey[CampaignArticleKey])(SchemaTypes.CampaignArticleKeyConverter)
        case ScopedKeyTypes.CONTENT_GROUP => output.writeObj(data.typedKey[ContentGroupKey])(SchemaTypes.ContentGroupKeyConverter)
        case ScopedKeyTypes.CLUSTER_KEY => output.writeObj(data.typedKey[ClusterKey])(ClusterTableConverters.ClusterKeyConverter)
        case ScopedKeyTypes.ONTOLOGY_NODE_AND_TYPE => output.writeObj(data.typedKey[OntologyNodeAndTypeKey])(SchemaTypes.OntologyNodeAndTypeKeyConverter)
        case ScopedKeyTypes.ARTICLE_ORDINAL_KEY => output.writeObj(data.typedKey[ArticleOrdinalKey])(SchemaTypes.ArticleOrdinalKeyConverter)
        case ScopedKeyTypes.SITE_ALGO_SETTING_KEY => output.writeObj(data.typedKey[SiteAlgoSettingKey])(SchemaTypes.SiteAlgoSettingKeyConverter)
        case ScopedKeyTypes.SITE_ALGO_KEY => output.writeObj(data.typedKey[SiteAlgoKey])(SchemaTypes.SiteAlgoKeyConverter)
        case ScopedKeyTypes.SITE_RECO_WORK_DIVISION_KEY => output.writeObj(data.typedKey[SiteRecoWorkDivisionKey])(SchemaTypes.SiteRecoWorkDivisionKeyConverter)
        case ScopedKeyTypes.RECOMMENDER_ID_KEY => output.writeObj(data.typedKey[RecommenderIdKey])(SchemaTypes.RecommenderIdKeyConverter)
        case ScopedKeyTypes.SITEKEY_PPID => output.writeObj(data.typedKey[SiteKeyPPIDKey])(SchemaTypes.SiteKeyPPIDKeyConverter)
        case ScopedKeyTypes.HOSTNAME => output.writeObj(data.typedKey[HostnameKey])(SchemaTypes.HostnameKeyConverter)
        case ScopedKeyTypes.ORDINAL_KEY => output.writeObj(data.typedKey[OrdinalKey])(SchemaTypes.OrdinalKeyConverter)
        case ScopedKeyTypes.SITEKEY_PPID_DOMAIN => output.writeObj(data.typedKey[SiteKeyPPIDDomainKey])(SchemaTypes.SiteKeyPPIDDomainKeyConverter)
        case ScopedKeyTypes.SITEKEY_DOMAIN => output.writeObj(data.typedKey[SiteKeyDomainKey])(SchemaTypes.SiteKeyDomainKeyConverter)
        case ScopedKeyTypes.MULTI_KEY => output.writeObj(data.typedKey[MultiKey])(SchemaTypes.MultiKeyConverter)
        case ScopedKeyTypes.ROLE_KEY => output.writeObj(data.typedKey[RolenameKey])(SchemaTypes.RolenameKeyConverter)
        case ScopedKeyTypes.DIMENSION_KEY => output.writeObj(data.typedKey[DimensionKey])(SchemaTypes.DimensionKeyConverter)
        case ScopedKeyTypes.ENTITY_KEY => output.writeObj(data.typedKey[EntityKey])(SchemaTypes.EntityKeyConverter)
        case ScopedKeyTypes.ENTITY_SCOPE_KEY => output.writeObj(data.typedKey[EntityScopeKey])(SchemaTypes.EntityScopeKeyConverter)
        case ScopedKeyTypes.DIMENSION_SCOPE_KEY => output.writeObj(data.typedKey[DimensionScopeKey])(SchemaTypes.DimensionScopeKeyConverter)
        case ScopedKeyTypes.DASHBOARD_USER_KEY => output.writeObj(data.typedKey[DashboardUserKey])(SchemaTypes.DashboardUserKeyConverter)
        case ScopedKeyTypes.EXCHANGE_KEY => output.writeObj(data.typedKey[ExchangeKey])(SchemaTypes.ExchangeKeyConverter)
        case ScopedKeyTypes.EXCHANGE_SITE_KEY => output.writeObj(data.typedKey[ExchangeSiteKey])(SchemaTypes.ExchangeSiteKeyConverter)
        case unknown => throw new RuntimeException(s"Scope $unknown not supported in byte converter")
      }
    }

    def read(input: PrimitiveInputStream): ScopedKey = {
      val version = input.readByte() //For the future
      val scope = input.readShort()
      val context = if (version == 1) ScopedKeyContexts.DEFAULT else ScopedKeyContexts.get(input.readShort()).get
      val (scopeObj, key): (ScopedKeyTypes.Type, CanBeScopedKey) = ScopedKeyTypes.get(scope) match {
        case Some(ScopedKeyTypes.SITE) => (ScopedKeyTypes.SITE, input.readObj[SiteKey](SchemaTypes.SiteKeyConverter))
        case Some(ScopedKeyTypes.SPONSORED_POOL) => (ScopedKeyTypes.SPONSORED_POOL, input.readObj[SiteKey](SchemaTypes.SiteKeyConverter))
        case Some(ScopedKeyTypes.SUPER_SITE) => (ScopedKeyTypes.SUPER_SITE, input.readObj[SiteKey](SchemaTypes.SiteKeyConverter))
        case Some(ScopedKeyTypes.ARTICLE) => (ScopedKeyTypes.ARTICLE, input.readObj[ArticleKey](SchemaTypes.ArticleKeyConverter))
        case Some(ScopedKeyTypes.SECTION) => (ScopedKeyTypes.SECTION, input.readObj[SectionKey](SchemaTypes.SectionKeyConverter))
        case Some(ScopedKeyTypes.CAMPAIGN) => (ScopedKeyTypes.CAMPAIGN, input.readObj[CampaignKey](SchemaTypes.CampaignKeyConverter))
        case Some(ScopedKeyTypes.ZERO) => (ScopedKeyTypes.ZERO, input.readObj[ArticleKey](SchemaTypes.ArticleKeyConverter))
        case Some(ScopedKeyTypes.CAMPAIGN_NODE) => (ScopedKeyTypes.CAMPAIGN_NODE, input.readObj[CampaignNodeKey](CampaignNodeKeyConverter))
        case Some(ScopedKeyTypes.SITE_PLACEMENT) => (ScopedKeyTypes.SITE_PLACEMENT, input.readObj[SitePlacementKey](SchemaTypes.SitePlacementKeyConverter))
        case Some(ScopedKeyTypes.SITE_PLACEMENT_ID) => (ScopedKeyTypes.SITE_PLACEMENT_ID, input.readObj[SitePlacementIdKey](SchemaTypes.SitePlacementIdKeyConverter))
        case Some(ScopedKeyTypes.SITE_PLACEMENT_ID_BUCKET) => (ScopedKeyTypes.SITE_PLACEMENT_ID_BUCKET, input.readObj[SitePlacementIdBucketKey](SchemaTypes.SitePlacementIdBucketKeyConverter))
        case Some(ScopedKeyTypes.SITE_PLACEMENT_BUCKET) => (ScopedKeyTypes.SITE_PLACEMENT_BUCKET, input.readObj[SitePlacementBucketKey](SchemaTypes.SitePlacementBucketKeyConverter))
        case Some(ScopedKeyTypes.SITE_PLACEMENT_BUCKET_SLOT) => (ScopedKeyTypes.SITE_PLACEMENT_BUCKET_SLOT, input.readObj[SitePlacementBucketSlotKey](SchemaTypes.SitePlacementBucketSlotKeyConverter))
        case Some(ScopedKeyTypes.SITE_PLACEMENT_WHY) => (ScopedKeyTypes.SITE_PLACEMENT_WHY, input.readObj[SitePlacementWhyKey](SchemaTypes.SitePlacementWhyKeyConverter))
        case Some(ScopedKeyTypes.ALL_SITES) => (ScopedKeyTypes.ALL_SITES, input.readObj[EverythingKey](SchemaTypes.EverythingKeyConverter))
        case Some(ScopedKeyTypes.NODE) => (ScopedKeyTypes.NODE, input.readObj[NodeKey](SchemaTypes.NodeKeyConverter))
        case Some(ScopedKeyTypes.LDA_CLUSTER) => (ScopedKeyTypes.LDA_CLUSTER, input.readObj[LDAClusterKey](SchemaTypes.LDAClusterKeyConverter))
        case Some(ScopedKeyTypes.CAMPAIGN_ARTICLE) => (ScopedKeyTypes.CAMPAIGN_ARTICLE, input.readObj[CampaignArticleKey](SchemaTypes.CampaignArticleKeyConverter))
        case Some(ScopedKeyTypes.CONTENT_GROUP) => (ScopedKeyTypes.CONTENT_GROUP, input.readObj[ContentGroupKey](SchemaTypes.ContentGroupKeyConverter))
        case Some(ScopedKeyTypes.CLUSTER_KEY) => (ScopedKeyTypes.CLUSTER_KEY, input.readObj[ClusterKey](ClusterTableConverters.ClusterKeyConverter))
        case Some(ScopedKeyTypes.ONTOLOGY_NODE_AND_TYPE) => (ScopedKeyTypes.ONTOLOGY_NODE_AND_TYPE, input.readObj[OntologyNodeAndTypeKey](SchemaTypes.OntologyNodeAndTypeKeyConverter))
        case Some(ScopedKeyTypes.ARTICLE_ORDINAL_KEY) => (ScopedKeyTypes.ARTICLE_ORDINAL_KEY, input.readObj[ArticleOrdinalKey](SchemaTypes.ArticleOrdinalKeyConverter))
        case Some(ScopedKeyTypes.SITE_ALGO_SETTING_KEY) => (ScopedKeyTypes.SITE_ALGO_SETTING_KEY, input.readObj[SiteAlgoSettingKey](SchemaTypes.SiteAlgoSettingKeyConverter))
        case Some(ScopedKeyTypes.SITE_ALGO_KEY) => (ScopedKeyTypes.SITE_ALGO_KEY, input.readObj[SiteAlgoKey](SchemaTypes.SiteAlgoKeyConverter))
        case Some(ScopedKeyTypes.SITE_RECO_WORK_DIVISION_KEY) => (ScopedKeyTypes.SITE_RECO_WORK_DIVISION_KEY, input.readObj[SiteRecoWorkDivisionKey](SchemaTypes.SiteRecoWorkDivisionKeyConverter))
        case Some(ScopedKeyTypes.RECOMMENDER_ID_KEY) => (ScopedKeyTypes.RECOMMENDER_ID_KEY, input.readObj[RecommenderIdKey](SchemaTypes.RecommenderIdKeyConverter))
        case Some(ScopedKeyTypes.SITEKEY_PPID) => (ScopedKeyTypes.SITEKEY_PPID, input.readObj[SiteKeyPPIDKey](SchemaTypes.SiteKeyPPIDKeyConverter))
        case Some(ScopedKeyTypes.HOSTNAME) => (ScopedKeyTypes.HOSTNAME, input.readObj[HostnameKey](SchemaTypes.HostnameKeyConverter))
        case Some(ScopedKeyTypes.ORDINAL_KEY) => (ScopedKeyTypes.ORDINAL_KEY, input.readObj[OrdinalKey](SchemaTypes.OrdinalKeyConverter))
        case Some(ScopedKeyTypes.SITEKEY_PPID_DOMAIN) => (ScopedKeyTypes.SITEKEY_PPID_DOMAIN, input.readObj[SiteKeyPPIDDomainKey](SchemaTypes.SiteKeyPPIDDomainKeyConverter))
        case Some(ScopedKeyTypes.SITEKEY_DOMAIN) => (ScopedKeyTypes.SITEKEY_DOMAIN, input.readObj[SiteKeyDomainKey](SchemaTypes.SiteKeyDomainKeyConverter))
        case Some(ScopedKeyTypes.MULTI_KEY) => (ScopedKeyTypes.MULTI_KEY, input.readObj[MultiKey](SchemaTypes.MultiKeyConverter))
        case Some(ScopedKeyTypes.ROLE_KEY) => (ScopedKeyTypes.ROLE_KEY, input.readObj[RolenameKey](SchemaTypes.RolenameKeyConverter))
        case Some(ScopedKeyTypes.DIMENSION_KEY) => (ScopedKeyTypes.DIMENSION_KEY, input.readObj[DimensionKey](SchemaTypes.DimensionKeyConverter))
        case Some(ScopedKeyTypes.ENTITY_KEY) => (ScopedKeyTypes.ENTITY_KEY, input.readObj[EntityKey](SchemaTypes.EntityKeyConverter))
        case Some(ScopedKeyTypes.ENTITY_SCOPE_KEY) => (ScopedKeyTypes.ENTITY_SCOPE_KEY, input.readObj[EntityScopeKey](SchemaTypes.EntityScopeKeyConverter))
        case Some(ScopedKeyTypes.DIMENSION_SCOPE_KEY) => (ScopedKeyTypes.DIMENSION_SCOPE_KEY, input.readObj[DimensionScopeKey](SchemaTypes.DimensionScopeKeyConverter))
        case Some(ScopedKeyTypes.DASHBOARD_USER_KEY) => (ScopedKeyTypes.DASHBOARD_USER_KEY, input.readObj[DashboardUserKey](SchemaTypes.DashboardUserKeyConverter))
        case Some(ScopedKeyTypes.EXCHANGE_KEY) => (ScopedKeyTypes.EXCHANGE_KEY, input.readObj[ExchangeKey](SchemaTypes.ExchangeKeyConverter))
        case Some(ScopedKeyTypes.EXCHANGE_SITE_KEY) => (ScopedKeyTypes.EXCHANGE_SITE_KEY, input.readObj[ExchangeSiteKey](SchemaTypes.ExchangeSiteKeyConverter))
        case unknown => throw new RuntimeException("Unable to parse scope " + unknown)
      }
      ScopedKey(
        key,
        scopeObj,
        context
      )

    }
  }

  implicit object ScopedFromToKeyConverter extends ComplexByteConverter[ScopedFromToKey] {
    val version = 1

    def write(data: ScopedFromToKey, output: PrimitiveOutputStream) {
      val box = new ByteArrayOutputStream(64) //An article to site scoped key is currently 29 bytes.  Let's assume denser keys are somewhat larger.
      val tempOutputStream = new PrimitiveOutputStream(box)
      tempOutputStream.writeObj(data.from)(ScopedKeyConverter)
      tempOutputStream.writeObj(data.to)(ScopedKeyConverter)

      val bytesOfKey = box.toByteArray
      val length = bytesOfKey.length
      val randomizer = MurmurHash.hash64(bytesOfKey, length)

      output.writeShort(randomizer.toInt) //Truncating the randomizer to short, we're just trying to redistribute over a few hundred regions
      output.writeByte(version)
      output.write(bytesOfKey)
    }

    def read(input: PrimitiveInputStream): ScopedFromToKey = {
      val randomizerToDiscard = input.readShort()
      val version = input.readByte()
      if(version != version) {
        return ScopedFromToKey.empty
      }
      ScopedFromToKey(input.readObj[ScopedKey], input.readObj[ScopedKey])
    }
  }
}

