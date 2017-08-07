package com.gravity.valueclasses

import com.gravity.domain.StrictUserGuid
import com.gravity.interests.jobs.intelligence.BlockedReason.Type
import com.gravity.interests.jobs.intelligence._
import com.gravity.utilities._
import com.gravity.utilities.analytics.URLUtils
import com.gravity.utilities.analytics.articles.ArticleWhitelist
import com.gravity.utilities.eventlogging.{FieldRegistry, FieldValueRegistry}
import com.gravity.utilities.grvfields.FieldConverter
import com.gravity.utilities.grvstrings._
import com.gravity.utilities.swagger.adapter.DefaultValueWriter
import org.joda.time._
import play.api.libs.json.{JsString, Writes, _}

import scala.util.Random

/**
 * Created by runger on 8/11/14.
 */

object ValueClassesForDomain extends TypeClassesForValueClassesForDomainWrapper.TypeClassesForValueClassesForDomain {

  case class MaxRecoResults(raw: Int) extends AnyVal
  case class MaxCandidateSize(raw: Int) extends AnyVal
  case class MaxDaysOld(raw: Int) extends AnyVal

  case class SiteGuid(raw: String) extends AnyVal {
    def siteKey: SiteKey = SiteKey(raw)
    def isEmpty: Boolean = raw.isEmpty
    def nonEmpty: Boolean = raw.nonEmpty

    override def toString: String = raw
  }
  object SiteGuid{
    val empty: SiteGuid = SiteGuid(emptyString)
  }

  case class SiteName(raw: String) extends AnyVal {
    def isEmpty: Boolean = raw.isEmpty
    def nonEmpty: Boolean = raw.nonEmpty
  }

  case class SiteId(raw: Long) extends AnyVal {
    def siteKey: SiteKey = SiteKey(raw)
  }

  case class PartnerPlacementId(raw: String) extends AnyVal
  object PartnerPlacementId {
    val empty: PartnerPlacementId = emptyString.asPartnerPlacementId

    implicit val defaultValueWriter:DefaultValueWriter[PartnerPlacementId] = new DefaultValueWriter[PartnerPlacementId] {
      override def serialize(t: PartnerPlacementId): String = t.raw
    }
  }

  case class SitePlacementId(raw: Long) extends AnyVal {
    def sitePlacementIdKey: SitePlacementIdKey = SitePlacementIdKey(this)
    def sitePlacementKey(siteKey: SiteKey): SitePlacementKey = SitePlacementKey(raw, siteKey.siteId)
  }

  case class BucketId(raw: Int) extends AnyVal

  case class PlacementId(raw: Int) extends AnyVal

  case class UserGuid(raw: String) extends AnyVal {
    def isEmpty: Boolean = raw.isEmpty || UserGuid.emptyUserGuids.contains(raw)
    def nonEmpty: Boolean = raw.nonEmpty
    def hash: Long = MurmurHash.hash64(raw)

    def toOption: Option[UserGuid] = if (raw.nonEmpty) Some(this) else None
  }

  object UserGuid {
    def empty: UserGuid = UserGuid(grvstrings.emptyString)
    val emptyUserGuids: Set[String] = Set("null", emptyString, "unknown", StrictUserGuid.emptyUserGuidHash) //List of user guids that are considered 'empty' regardless of site

    implicit val defaultValueWriter:DefaultValueWriter[UserGuid] = new DefaultValueWriter[UserGuid] {
      override def serialize(t: UserGuid): String = t.raw
    }
  }

  case class ExchangeGuid(raw: String) extends AnyVal {
    def exchangeKey: ExchangeKey = ExchangeKey(raw)
    def isEmpty: Boolean = raw.isEmpty
    def nonEmpty: Boolean = raw.nonEmpty

    override def toString: String = raw
  }
  object ExchangeGuid{
    val empty: ExchangeGuid = emptyString.asExchangeGuid
    def generateGuid: ExchangeGuid = HashUtils.md5((System.currentTimeMillis() + Random.nextLong).toString).asExchangeGuid
  }


  case class Title(raw: String) extends AnyVal {
    def isEmpty: Boolean = raw.isEmpty
    def nonEmpty: Boolean = raw.nonEmpty
  }


  case class ImageUrl(raw: String) extends AnyVal {
    def isEmpty: Boolean = raw.isEmpty
    def nonEmpty: Boolean = raw.nonEmpty
  }

  case class Attribution(raw: String) extends AnyVal

  case class ClickUrl(raw: String) extends AnyVal {
    def isEmpty: Boolean = raw.isEmpty

    def nonEmpty: Boolean = raw.nonEmpty

    def appendParams(params: Map[String, String]): ClickUrl = {
      if (params.isEmpty) return this

      ClickUrl(URLUtils.appendParameters(raw, params))
    }

    def mergeParams(params: Map[String, String]): ClickUrl = {
      if (params.isEmpty) return this

      ClickUrl(URLUtils.mergeParameters(raw, params))
    }

    def replaceParams(params: Map[String, String]): ClickUrl = {
      if (params.isEmpty) return this

      ClickUrl(URLUtils.replaceParameters(raw, params))
    }
  }

  case class DisplayUrl(raw: String) extends AnyVal {
    def isEmpty: Boolean = raw.isEmpty
    def nonEmpty: Boolean = raw.nonEmpty
    def asArticleKey: ArticleKey = ArticleKey(raw)
    def articleId: Long = asArticleKey.articleId
  }

  case class RenderType(raw: String) extends AnyVal
  implicit val rtFmt: Format[RenderType] = Format(Reads[RenderType] {
    case JsString(str) => JsSuccess(RenderType.apply(str))
    case _ => JsError("expected string for RenderType")
  }, Writes[RenderType](rt => JsString(rt.raw)))

  case class HostName(raw: String) extends AnyVal

  case class ImpressionHash(raw: String) extends AnyVal

  case class ImpressionViewedHashString(raw: String) extends AnyVal

  //This is for helping convert a string encoded with BlockedWhy to a BlockedReasonSet
  case class BlockedReasonHelper(raw: String) extends AnyVal {
    def isEmpty: Boolean = raw.isEmpty
    def nonEmpty: Boolean = raw.nonEmpty


    override def toString: String = raw
    def +(that: BlockedReasonHelper): BlockedReasonHelper = BlockedReasonHelper(raw + "; " + that.raw)
    def toBlockedReasonSet: Set[Type] = "; ".r.split(raw).withFilter(_ != emptyString).map { blockedWhyStr =>
      BlockedReason.valuesMap.collectFirst {
        case (blockedReason, enum) if blockedWhyStr.startsWith(blockedReason) =>
          enum
      }.getOrElse(BlockedReason.UNKNOWN)
    }.toSet
  }

  case class Etag(raw: String) extends AnyVal

  case class VideoId(raw: String) extends AnyVal


    implicit val ivhs: Writes[ImpressionViewedHashString] = Writes[ImpressionViewedHashString](ivhs => JsString(ivhs.raw))

  implicit class StringToDomainValueClasses(val underlying: String) extends AnyVal {
    def asSiteGuid: SiteGuid = SiteGuid(underlying)
    def asSiteName: SiteName = SiteName(underlying)
    def asUserGuid: UserGuid = UserGuid(underlying)
    def asExchangeGuid: ExchangeGuid = ExchangeGuid(underlying)
    def asTitle: Title = Title(underlying)
    def asImageUrl: ImageUrl = ImageUrl(underlying)
    def asClickUrl: ClickUrl = ClickUrl(underlying)
    def asDisplayUrl: DisplayUrl = DisplayUrl(underlying)
    def asRenderType: RenderType = RenderType(underlying)
    def asPartnerPlacementId: PartnerPlacementId = PartnerPlacementId(underlying)
    def asAttribution: Attribution = Attribution(underlying)
    def asHostName: HostName = HostName(underlying)
    def asPubId: PubId = PubId(underlying)
    def asImpressionViewedHashString: ImpressionViewedHashString = ImpressionViewedHashString(underlying)
    def asImpressionHash: ImpressionHash = ImpressionHash(underlying)
  }

  implicit class IntToDomainValueClasses(val underlying: Int) extends AnyVal {
    def asPlacementId: PlacementId = PlacementId(underlying)
    def asBucketId: BucketId = BucketId(underlying)
    def asSitePlacementId: SitePlacementId = SitePlacementId(underlying.toLong)
    def asMaxCandidateSize: MaxCandidateSize = MaxCandidateSize(underlying)
    def asMaxRecoResults: MaxRecoResults = MaxRecoResults(underlying)
    def asMaxDaysOld: MaxDaysOld = MaxDaysOld(underlying)
  }

  implicit class ShortToDomainValueClasses(val underlying: Short) {
    def asPlacementId: PlacementId = PlacementId(underlying.toInt)
  }

  implicit class LongToDomainValueClasses(val underlying: Long) extends AnyVal {
    def asSitePlacementId: SitePlacementId = SitePlacementId(underlying)
    def asSiteId: SiteId = SiteId(underlying)
  }

  implicit class StringToDomainValueClassOption(val underlying: String) extends AnyVal {
    def asSitePlacementId: Option[SitePlacementId] = ScalaMagic.tryOption(underlying.toLong).map(_.asSitePlacementId)
    def asBucketId: Option[BucketId] = ScalaMagic.tryOption(underlying.toInt).map(_.asBucketId)
    def asEtag: Etag = Etag(underlying)
    def asVideoId: VideoId = VideoId(underlying)
  }

}

object RenderTypes {
  import ValueClassesForDomain._

  def widget(sg: SiteGuid): RenderType = siteGuidWidgetRenderTypeOverrides.getOrElse(sg, widget)
  val widget: RenderType = "wdgt".asRenderType
  val api: RenderType = "api".asRenderType
  val synd: RenderType = "synd".asRenderType
  val unknown: RenderType = "unknown".asRenderType

  private val siteGuidWidgetRenderTypeOverrides: Map[SiteGuid, RenderType] = Map(
    SiteGuid(ArticleWhitelist.siteGuid(_.DISQUS)) -> synd
    ,SiteGuid(ArticleWhitelist.siteGuid(_.ADVERTISINGDOTCOM)) -> synd
    ,SiteGuid(ArticleWhitelist.siteGuid(_.DISQUS_PREMIUM)) -> synd
    ,SiteGuid(ArticleWhitelist.siteGuid(_.IMEDIA)) -> synd
    ,SiteGuid(ArticleWhitelist.siteGuid(_.TL_MEDIA)) -> synd
  )
}

//This wrapper hides the TypeClasses from IntelliJ autocomplete
object TypeClassesForValueClassesForDomainWrapper {

  trait TypeClassesForValueClassesForDomain {

    import ValueClassesForDomain._

    trait HasSiteGuid[T] {
      def getSiteGuid(t: T): SiteGuid
    }

    trait HasSitePlacementId[T] {
      def getSitePlacementId(t: T): SitePlacementId
    }

    trait HasStartDate[T] {
      def getStartDate(t: T): DateTime
    }

    //String formats
    implicit val sgFmt: Format[SiteGuid] = Format(Reads[SiteGuid] {
      case JsString(str) => JsSuccess(SiteGuid.apply(str))
      case _ => JsError("expected string for SiteGuid")
    }, Writes[SiteGuid](sg => JsString(sg.raw)))

    implicit val ugFmt: Format[UserGuid] = Format(Reads[UserGuid] {
      case JsString(str) => JsSuccess(UserGuid.apply(str))
      case _ => JsError("expected string for UserGuid")
    }, Writes[UserGuid](ug => JsString(ug.raw)))

    implicit val exgFmt: Format[ExchangeGuid] = Format(Reads[ExchangeGuid] {
      case JsString(str) => JsSuccess(ExchangeGuid.apply(str))
      case _ => JsError("expected string for ExchangeGuid")
    }, Writes[ExchangeGuid](exg => JsString(exg.raw)))

    implicit val hnFmt: Format[HostName] = Format(Reads[HostName] {
      case JsString(str) => JsSuccess(HostName.apply(str))
      case _ => JsError("expected string for HostName")
    }, Writes[HostName](x => JsString(x.raw)))

    implicit val ppidFmt: Format[PartnerPlacementId] = Format(Reads[PartnerPlacementId] {
      case JsString(str) => JsSuccess(PartnerPlacementId.apply(str))
      case _ => JsError("expected string for PartnerPlacementId")
    }, Writes[PartnerPlacementId](x => JsString(x.raw)))

    implicit val du: Writes[DisplayUrl] = Writes[DisplayUrl](du => JsString(du.raw))
    implicit val cu: Writes[ClickUrl] = Writes[ClickUrl](cu => JsString(cu.raw))
    implicit val atW: Writes[Attribution] = Writes[Attribution](cu => JsString(cu.raw))
    implicit val tW: Writes[Title] = Writes[Title](t => JsString(t.raw))
    implicit val iuW: Writes[ImageUrl] = Writes[ImageUrl](iu => JsString(iu.raw))

    //Numeric formats
    implicit val spidFmt: Format[SitePlacementId] = Format(Reads[SitePlacementId] {
      case JsNumber(num) => JsSuccess(SitePlacementId.apply(num.toLong))
      case _ => JsError("expected number for SitePlacementId")
    }, Writes[SitePlacementId](spid => JsNumber(spid.raw)))

    implicit val bIdWrites: Writes[BucketId] = Writes[BucketId](bId => JsNumber(bId.raw))

    implicit val fieldConverterForSiteGuid : FieldConverter[SiteGuid]= new FieldConverter[SiteGuid] {
      override val fields: FieldRegistry[SiteGuid] = new FieldRegistry[SiteGuid]("SiteGuid").registerStringField("SiteGuid", 0)

      override def toValueRegistry(o: SiteGuid): FieldValueRegistry = new FieldValueRegistry(fields).registerFieldValue(0, o.raw)

      override def fromValueRegistry(reg: FieldValueRegistry): SiteGuid = SiteGuid(reg.getValue[String](0))
    }

    implicit val fieldConverterForUserGuid : FieldConverter[UserGuid]= new FieldConverter[UserGuid] {
      override val fields: FieldRegistry[UserGuid] = new FieldRegistry[UserGuid]("UserGuid").registerStringField("UserGuid", 0)

      override def toValueRegistry(o: UserGuid): FieldValueRegistry = new FieldValueRegistry(fields).registerFieldValue(0, o.raw)

      override def fromValueRegistry(reg: FieldValueRegistry): UserGuid = UserGuid(reg.getValue[String](0))
    }

    implicit val fieldConverterForSitePlacementId:FieldConverter[SitePlacementId] = new FieldConverter[SitePlacementId] {
      override val fields: FieldRegistry[SitePlacementId] = new FieldRegistry[SitePlacementId]("SitePlacementId").registerLongField("SitePlacementId", 0)

      override def toValueRegistry(o: SitePlacementId): FieldValueRegistry = new FieldValueRegistry(fields).registerFieldValue(0, o.raw)

      override def fromValueRegistry(reg: FieldValueRegistry): SitePlacementId = SitePlacementId(reg.getValue[Long](0))
    }

    implicit val fieldConverterForRenderType:FieldConverter[RenderType] = new FieldConverter[RenderType] {
      override val fields: FieldRegistry[RenderType] = new FieldRegistry[RenderType]("RenderType").registerStringField("RenderType", 0)

      override def toValueRegistry(o: RenderType): FieldValueRegistry = new FieldValueRegistry(fields).registerFieldValue(0, o.raw)

      override def fromValueRegistry(reg: FieldValueRegistry): RenderType = RenderType(reg.getValue[String](0))
    }

    implicit val fieldConverterForHostName :FieldConverter[HostName]= new FieldConverter[HostName] {
      override val fields: FieldRegistry[HostName] = new FieldRegistry[HostName]("HostName").registerStringField("HostName", 0)

      override def toValueRegistry(o: HostName): FieldValueRegistry = new FieldValueRegistry(fields).registerFieldValue(0, o.raw)

      override def fromValueRegistry(reg: FieldValueRegistry): HostName = HostName(reg.getValue[String](0))
    }

    implicit val fieldConverterForPartnerPlacementId:FieldConverter[PartnerPlacementId] = new FieldConverter[PartnerPlacementId] {
      override val fields: FieldRegistry[PartnerPlacementId] = new FieldRegistry[PartnerPlacementId]("PartnerPlacementId").registerStringField("PartnerPlacementId", 0)

      override def toValueRegistry(o: PartnerPlacementId): FieldValueRegistry = new FieldValueRegistry(fields).registerFieldValue(0, o.raw)

      override def fromValueRegistry(reg: FieldValueRegistry): PartnerPlacementId = PartnerPlacementId(reg.getValue[String](0))
    }
  }
}

