package com.gravity.valueclasses

import com.gravity.utilities.swagger.adapter.DefaultValueWriter
import com.gravity.utilities.{ScalaMagic, SplitHost}
import com.gravity.utilities.eventlogging.{FieldValueRegistry, FieldRegistry}
import com.gravity.utilities.grvfields.FieldConverter
import com.gravity.utilities.web.http
import com.gravity.utilities.grvjson._
import eu.bitwalker.useragentutils.{DeviceType, OperatingSystem}
import play.api.libs.json._

/**
 * Created by runger on 8/11/14.
 */
object ValueClassesForUtilities {


  case class LongId(raw: Long) extends AnyVal

  case class IntId(raw: Int) extends AnyVal

  case class VersionInt(raw: Int) extends AnyVal

  case class OutputLimit(raw: Int) extends AnyVal

  case class InputLimit(raw: Int) extends AnyVal

  case class Size(raw: Int) extends AnyVal

  case class Hour(raw: Int) extends AnyVal
  implicit val hourW : Writes[Hour]= Writes[Hour](u => JsNumber(u.raw))

  case class Count(raw: Int) extends AnyVal

  case class Rank(raw: Int) extends AnyVal

  case class Height(raw: Int) extends AnyVal

  case class Width(raw: Int) extends AnyVal

  case class Year(raw: Int) extends AnyVal

  case class CountLong(raw: Long) extends AnyVal
  implicit val countlW: Writes[CountLong] = Writes[CountLong](u => JsNumber(u.raw))

  case class FeatureToggle(raw: Boolean) extends AnyVal
  object FeatureToggle {
    val enabled: FeatureToggle = FeatureToggle(true)
    val disabled: FeatureToggle = FeatureToggle(false)
  }

  case class BinaryState(raw: Boolean) extends AnyVal
  object BinaryState {
    val yes: BinaryState = BinaryState(true)
    val no: BinaryState = BinaryState(false)
  }

  case class RoleName(raw: String) extends AnyVal

  case class StringId(raw: String) extends AnyVal

  case class VersionString(raw: String) extends AnyVal

  case class SearchString(raw: String) extends AnyVal

  case class KeyWord(raw: String) extends AnyVal

  case class DataString(raw: String) extends AnyVal

  case class FullyQualifiedName(raw: String) extends AnyVal

  case class Make(raw: String) extends AnyVal

  case class Model(raw: String) extends AnyVal

  case class OS(raw: String) extends AnyVal

  case class Language(raw: String) extends AnyVal

  case class Hash(raw: String) extends AnyVal

  case class Gender(raw: String) extends AnyVal

  case class Path(raw: String) extends AnyVal

  case class Text(raw: String) extends AnyVal

  case class XmlString(raw: String) extends AnyVal

  case class Domain(raw: String) extends AnyVal
  object Domain {
    val empty: Domain = "".asDomain

    implicit val defaultValueWriter : DefaultValueWriter[Domain] = new DefaultValueWriter[Domain] {
      override def serialize(t: Domain): String = t.raw
    }
  }

  case class Label(raw: String) extends AnyVal

  case class ContinentCode(raw: String) extends AnyVal
  implicit val contW: Writes[ContinentCode] = Writes[ContinentCode](u => JsString(u.raw))

  case class CountryCode(raw: String) extends AnyVal
  implicit val ccW: Writes[CountryCode] = Writes[CountryCode](u => JsString(u.raw))

  case class RegionCode(raw: String) extends AnyVal
  implicit val regW: Writes[RegionCode] = Writes[RegionCode](u => JsString(u.raw))

  case class DMACode(raw: String) extends AnyVal
  implicit val dmaW: Writes[DMACode] = Writes[DMACode](u => JsString(u.raw))

  case class CountryCode3(raw: String) extends AnyVal

  case class Name(raw: String) extends AnyVal

  case class Url(raw: String) extends AnyVal {
    def domain: Option[Domain] = SplitHost.registeredDomainFromUrl(raw).map(_.asDomain)
    def domainOrEmpty: Domain = domain.getOrElse(Domain.empty)
    def toAHref(linkText: String): String = "<a href='" + xml.Utility.escape(raw) + "'>" + xml.Utility.escape(linkText) + "</a>"
    override def toString: String = raw
  }

  case class Html(raw: String) extends AnyVal

  case class DashboardUserId(raw: Long) extends AnyVal

  case class EmailAddress(raw: String) extends AnyVal

  case class QueryString(raw: String) extends AnyVal {
    def isEmpty: Boolean = raw.isEmpty
  }

  case class Action(raw: String) extends AnyVal
  implicit val aW: Writes[Action] = Writes[Action](a => JsString(a.raw))

  case class UserAgent(raw: String) extends AnyVal {
    def os: Option[OperatingSystem] = http.os(this)
    def device: Option[DeviceType] = http.device(this)
  }

  object UserAgent {
    implicit val defaultValueWriter:DefaultValueWriter[UserAgent] = new DefaultValueWriter[UserAgent] {
      override def serialize(t: UserAgent): String = t.raw
    }
  }

  case class Millis(raw: Long) extends AnyVal

  case class Seconds(raw: Long) extends AnyVal

  case class Index(raw: Int) extends AnyVal {
    def +(n: Int): Index = Index(raw + n)
  }

  case class IPAddressString(raw: String) extends AnyVal

  case class JsonStr(raw: String) extends AnyVal {
    def toPlayJson: JsValue = Json.parse(raw)
  }

  case class Percent(raw: Double) extends AnyVal {
    def display(decimalPlaces: Int = 0): String = s"%.${decimalPlaces}f%%".format(raw * 100)
    def /(that: Percent): Percent = (raw / that.raw).asPercent
    def >(that: Double): Boolean = (raw > that)
    def invert: Percent = (1 - raw).asPercent
  }

  case class RatioFloat(raw: Float) extends AnyVal

  case class Latitude(raw: Float) extends AnyVal

  case class Longitude(raw: Float) extends AnyVal

  /*
  Implicit Conversions
  */
  implicit class StringToUtilitiesValueClasses(val underlying: String) extends AnyVal {
    def asName: Name = Name(underlying)
    def asUrl: Url = Url(underlying)
    def asEmailAddress: EmailAddress = EmailAddress(underlying)
    def asIPAddressString: IPAddressString = IPAddressString(underlying)
    def asJsonStr: JsonStr = JsonStr(underlying)
    def asUserAgent: UserAgent = UserAgent(underlying)
    def asQueryString: QueryString = QueryString(underlying)
    def asLabel: Label = Label(underlying)
    def asRoleName: RoleName = RoleName(underlying)
    def asDomain: Domain = Domain(underlying)
    def asAction: Action = Action(underlying)
    def asStringId: StringId = StringId(underlying)
    def asText: Text = Text(underlying)
  }

  implicit class LongToUtilitiesValueClasses(val underlying: Long) extends AnyVal {
    def asMillis: Millis = Millis(underlying)
    def asLongId: LongId = LongId(underlying)
    def asCountLong: CountLong = CountLong(underlying)
  }

  implicit class DoubleToUtilitiesValueClasses(val underlying: Double) extends AnyVal {
    def asPercent: Percent = Percent(underlying)
  }

  implicit class BooleanToUtilitiesValueClasses(val underlying: Boolean) extends AnyVal {
    def asFeatureToggle: FeatureToggle = FeatureToggle(underlying)
    def asBinaryState: BinaryState = BinaryState(underlying)
  }

  implicit class IntToUtilitiesValueClasses(val underlying: Int) extends AnyVal {
    def asIndex: Index = Index(underlying)
    def asIntId: IntId = IntId(underlying)
    def asSize: Size = Size(underlying)
    def asOutputLimit: OutputLimit = OutputLimit(underlying)
    def asInputLimit: InputLimit = InputLimit(underlying)
    def asHour: Hour = Hour(underlying)
    def asCount: Count = Count(underlying)
    def asHeight: Height = Height(underlying)
    def asWidth: Width = Width(underlying)
    def asRank: Rank = Rank(underlying)
  }

  implicit class StringToUtilitiesValueClassOption(val underlying: String) extends AnyVal {
    def asCount: Option[Count] = ScalaMagic.tryOption(underlying.toInt).map(_.asCount)
  }

  //String formats
  implicit val htmlFmt: Format[Html] = Format(Reads.StringReads.map(Html), Writes(_.raw))

  /** @note See [[htmlFmt]] for a better way to write these. */
  implicit val strIdFmt: Format[StringId] = Format(Reads[StringId] {
    case JsString(str) => JsSuccess(StringId.apply(str))
    case _ => JsError("expected string for StringId")
  }, Writes[StringId](x => JsString(x.raw)))

  /** @note See [[htmlFmt]] for a better way to write these. */
  implicit val nameFmt: Format[Name] = Format(Reads[Name] {
    case JsString(str) => JsSuccess(Name.apply(str))
    case _ => JsError("expected string for Name")
  }, Writes[Name](x => JsString(x.raw)))

  /** @note See [[htmlFmt]] for a better way to write these. */
  implicit val makeFmt: Format[Make] = Format(Reads[Make] {
    case JsString(str) => JsSuccess(Make.apply(str))
    case _ => JsError("expected string for Make")
  }, Writes[Make](x => JsString(x.raw)))

  /** @note See [[htmlFmt]] for a better way to write these. */
  implicit val modelFmt: Format[Model] = Format(Reads[Model] {
    case JsString(str) => JsSuccess(Model.apply(str))
    case _ => JsError("expected string for Model")
  }, Writes[Model](x => JsString(x.raw)))

  /** @note See [[htmlFmt]] for a better way to write these. */
  implicit val osFmt: Format[OS] = Format(Reads[OS] {
    case JsString(str) => JsSuccess(OS.apply(str))
    case _ => JsError("expected string for OS")
  }, Writes[OS](x => JsString(x.raw)))

  /** @note See [[htmlFmt]] for a better way to write these. */
  implicit val languageFmt: Format[Language] = Format(Reads[Language] {
    case JsString(str) => JsSuccess(Language.apply(str))
    case _ => JsError("expected string for Language")
  }, Writes[Language](x => JsString(x.raw)))

  /** @note See [[htmlFmt]] for a better way to write these. */
  implicit val versionStrFmt: Format[VersionString] = Format(Reads[VersionString] {
    case JsString(str) => JsSuccess(VersionString.apply(str))
    case _ => JsError("expected string for VersionString")
  }, Writes[VersionString](x => JsString(x.raw)))

  /** @note See [[htmlFmt]] for a better way to write these. */
  implicit val domainFmt: Format[Domain] = Format(Reads[Domain] {
    case JsString(str) => JsSuccess(Domain.apply(str))
    case _ => JsError("expected string for Domain")
  }, Writes[Domain](x => JsString(x.raw)))

  /** @note See [[htmlFmt]] for a better way to write these. */
  implicit val cc3Fmt: Format[CountryCode3] = Format(Reads[CountryCode3] {
    case JsString(str) => JsSuccess(CountryCode3.apply(str))
    case _ => JsError("expected string for CountryCode3")
  }, Writes[CountryCode3](x => JsString(x.raw)))

  /** @note See [[htmlFmt]] for a better way to write these. */
  implicit val dataStrFmt: Format[DataString] = Format(Reads[DataString] {
    case JsString(str) => JsSuccess(DataString.apply(str))
    case _ => JsError("expected string for DataString")
  }, Writes[DataString](x => JsString(x.raw)))

  /** @note See [[htmlFmt]] for a better way to write these. */
  implicit val uaFmt: Format[UserAgent] = Format(Reads[UserAgent] {
    case JsString(str) => JsSuccess(UserAgent.apply(str))
    case _ => JsError("expected string for UserAgent")
  }, Writes[UserAgent](ua => JsString(ua.raw)))

  /** @note See [[htmlFmt]] for a better way to write these. */
  implicit val textFmt: Format[Text] = Format(Reads[Text] {
    case JsString(str) => JsSuccess(Text.apply(str))
    case _ => JsError("expected string for Text")
  }, Writes[Text](txt => JsString(txt.raw)))

  /** @note See [[htmlFmt]] for a better way to write these. */
  implicit val ipFmt: Format[IPAddressString] = Format(Reads[IPAddressString] {
    case JsString(str) => JsSuccess(IPAddressString.apply(str))
    case _ => JsError("expected string for IPAddressString")
  }, Writes[IPAddressString](ip => JsString(ip.raw)))

  /** @note See [[htmlFmt]] for a better way to write these. */
  implicit val urlFmt: Format[Url] = Format(Reads[Url] {
    case JsString(str) => JsSuccess(Url.apply(str))
    case _ => JsError("expected string for Url")
  }, Writes[Url](x => JsString(x.raw)))

  /** @note See [[htmlFmt]] for a better way to write these. */
  implicit val labelFmt: Format[Label] = Format(Reads[Label] {
    case JsString(str) => JsSuccess(Label.apply(str))
    case _ => JsError("expected string for Label")
  }, Writes[Label](l => JsString(l.raw)))

  /** @note See [[htmlFmt]] for a better way to write these. */
  implicit val qsFmt: Format[QueryString] = Format(Reads[QueryString] {
    case JsString(str) => JsSuccess(QueryString.apply(str))
    case _ => JsError("expected string for QueryString")
  }, Writes[QueryString](x => JsString(x.raw)))

  /** @note See [[htmlFmt]] for a better way to write these. */
  implicit val xmlFmt: Format[XmlString] = Format(Reads[XmlString] {
    case JsString(str) => JsSuccess(XmlString.apply(str))
    case _ => JsError("expected string for XmlString")
  }, Writes[XmlString](x => JsString(x.raw)))

  /** @note See [[htmlFmt]] for a better way to write these. */
  implicit val searchStringFmt: Format[SearchString] = Format(Reads[SearchString] {
    case JsString(str) => JsSuccess(SearchString.apply(str))
    case _ => JsError("expected string for SearchString")
  }, Writes[SearchString](x => JsString(x.raw)))

  /** @note See [[htmlFmt]] for a better way to write these. */
  implicit val keywordFmt: Format[KeyWord] = Format(Reads[KeyWord] {
    case JsString(str) => JsSuccess(KeyWord.apply(str))
    case _ => JsError("expected string for KeyWord")
  }, Writes[KeyWord](x => JsString(x.raw)))

  /** @note See [[htmlFmt]] for a better way to write these. */
  implicit val fqnFmt: Format[FullyQualifiedName] = Format(Reads[FullyQualifiedName] {
    case JsString(str) => JsSuccess(FullyQualifiedName.apply(str))
    case _ => JsError("expected string for FullyQualifiedName")
  }, Writes[FullyQualifiedName](x => JsString(x.raw)))

  //Numeric Formats
  implicit val countFmt: Format[Count] = Format(Reads[Count] {
    case JsNumber(str) => JsSuccess(Count.apply(str.toInt))
    case _ => JsError("expected number for Count")
  }, Writes[Count](x => JsNumber(x.raw)))

  implicit val yearFmt: Format[Year] = Format(Reads[Year] {
    case JsNumber(str) => JsSuccess(Year.apply(str.toInt))
    case _ => JsError("expected number for Year")
  }, Writes[Year](x => JsNumber(x.raw)))

  implicit val versionIntFmt: Format[VersionInt] = Format(Reads[VersionInt] {
    case JsNumber(str) => JsSuccess(VersionInt.apply(str.toInt))
    case _ => JsError("expected number for VersionInt")
  }, Writes[VersionInt](x => JsNumber(x.raw)))

  implicit val widthFmt: Format[Width] = Format(Reads[Width] {
    case JsNumber(str) => JsSuccess(Width.apply(str.toInt))
    case _ => JsError("expected number for Width")
  }, Writes[Width](x => JsNumber(x.raw)))

  implicit val heightFmt: Format[Height] = Format(Reads[Height] {
    case JsNumber(str) => JsSuccess(Height.apply(str.toInt))
    case _ => JsError("expected number for Height")
  }, Writes[Height](x => JsNumber(x.raw)))

  implicit val intIdFmt: Format[IntId] = Format(Reads[IntId] {
    case JsNumber(str) => JsSuccess(IntId.apply(str.toInt))
    case _ => JsError("expected number for Index")
  }, Writes[IntId](x => JsNumber(x.raw)))

  implicit val ixFmt: Format[Index] = Format(Reads[Index] {
    case JsNumber(str) => JsSuccess(Index.apply(str.toInt))
    case _ => JsError("expected number for Index")
  }, Writes[Index](x => JsNumber(x.raw)))

  implicit val outFmt: Format[OutputLimit] = Format(Reads[OutputLimit] {
    case JsNumber(num) => JsSuccess(OutputLimit.apply(num.toInt))
    case _ => JsError("expected number for OutputLimit")
  }, Writes[OutputLimit](x => JsNumber(x.raw)))

  implicit val inFmt: Format[InputLimit] = Format(Reads[InputLimit] {
    case JsNumber(num) => JsSuccess(InputLimit.apply(num.toInt))
    case _ => JsError("expected number for InputLimit")
  }, Writes[InputLimit](x => JsNumber(x.raw)))

  implicit val szFmt: Format[Size] = Format(Reads[Size] {
    case JsNumber(num) => JsSuccess(Size.apply(num.toInt))
    case _ => JsError("expected number for Size")
  }, Writes[Size](x => JsNumber(x.raw)))

  //Long
  implicit val dashboardUserIdFmt: Format[DashboardUserId] = Format(Reads.LongReads.map(DashboardUserId), Writes(_.raw))

  /** @note See [[dashboardUserIdFmt]] for a better way to write these. */
  implicit val millisFmt: Format[Millis] = Format(Reads[Millis] {
    case JsNumber(num) => JsSuccess(Millis.apply(num.toLong))
    case _ => JsError("expected number for Millis")
  }, Writes[Millis](x => JsNumber(x.raw)))

  /** @note See [[dashboardUserIdFmt]] for a better way to write these. */
  implicit val secondssFmt: Format[Seconds] = Format(Reads[Seconds] {
    case JsNumber(num) => JsSuccess(Seconds.apply(num.toLong))
    case _ => JsError("expected number for Seconds")
  }, Writes[Seconds](x => JsNumber(x.raw)))

  //Float
  implicit val latitudeFmt: Format[Latitude] = Format(Reads[Latitude] {
    case JsNumber(num) => JsSuccess(Latitude.apply(num.toFloat))
    case _ => JsError("expected number for Latitude")
  }, Writes[Latitude](x => JsNumber(x.raw)))

  implicit val longitudeFmt: Format[Longitude] = Format(Reads[Longitude] {
    case JsNumber(num) => JsSuccess(Longitude.apply(num.toFloat))
    case _ => JsError("expected number for Longitude")
  }, Writes[Longitude](x => JsNumber(x.raw)))

  //Boolean format
  implicit val ftFmt: Format[FeatureToggle] = Format(Reads[FeatureToggle] {
    case JsBoolean(b) => JsSuccess(FeatureToggle.apply(b))
    case _ => JsError("expected boolean for FeatureToggle")
  }, Writes[FeatureToggle](x => JsBoolean(x.raw)))

  implicit val bsFmt: Format[BinaryState] = Format(Reads[BinaryState] {
    case JsBoolean(b) => JsSuccess(BinaryState.apply(b))
    case _ => JsError("expected boolean for BinaryState")
  }, Writes[BinaryState](x => JsBoolean(x.raw)))

  implicit val fieldConverterForLabel:FieldConverter[Label] = new FieldConverter[Label] {
    override val fields: FieldRegistry[Label] = new FieldRegistry[Label]("Label").registerStringField("Label", 0)
    override def toValueRegistry(o: Label): FieldValueRegistry = new FieldValueRegistry(fields).registerFieldValue(0, o.raw)
    override def fromValueRegistry(reg: FieldValueRegistry): Label = Label(reg.getValue[String](0))
  }

  implicit val fieldConverterForMillis:FieldConverter[Millis] = new FieldConverter[Millis] {
    override val fields: FieldRegistry[Millis] = new FieldRegistry[Millis]("Millis").registerLongField("Millis", 0)
    override def toValueRegistry(o: Millis): FieldValueRegistry = new FieldValueRegistry(fields).registerFieldValue(0, o.raw)
    override def fromValueRegistry(reg: FieldValueRegistry): Millis = Millis(reg.getValue[Long](0))
  }

  implicit val fieldConverterForQueryString:FieldConverter[QueryString] = new FieldConverter[QueryString] {
    override val fields: FieldRegistry[QueryString] = new FieldRegistry[QueryString]("QueryString").registerStringField("QueryString", 0)
    override def toValueRegistry(o: QueryString): FieldValueRegistry = new FieldValueRegistry(fields).registerFieldValue(0, o.raw)
    override def fromValueRegistry(reg: FieldValueRegistry): QueryString = QueryString(reg.getValue[String](0))
  }

  implicit val fieldConverterForUserAgent:FieldConverter[UserAgent] = new FieldConverter[UserAgent] {
    override val fields: FieldRegistry[UserAgent] = new FieldRegistry[UserAgent]("UserAgent").registerStringField("UserAgent", 0)
    override def toValueRegistry(o: UserAgent): FieldValueRegistry = new FieldValueRegistry(fields).registerFieldValue(0, o.raw)
    override def fromValueRegistry(reg: FieldValueRegistry): UserAgent = UserAgent(reg.getValue[String](0))
  }

  implicit val fieldConverterForOutputLimit:FieldConverter[OutputLimit] = new FieldConverter[OutputLimit] {
    override val fields: FieldRegistry[OutputLimit] = new FieldRegistry[OutputLimit]("OutputLimit").registerIntField("OutputLimit", 0)
    override def toValueRegistry(o: OutputLimit): FieldValueRegistry = new FieldValueRegistry(fields).registerFieldValue(0, o.raw)
    override def fromValueRegistry(reg: FieldValueRegistry): OutputLimit = OutputLimit(reg.getValue[Int](0))
  }

  implicit val fieldConverterForIndex:FieldConverter[Index] = new FieldConverter[Index] {
    override val fields: FieldRegistry[Index] = new FieldRegistry[Index]("Index").registerIntField("Index", 0)
    override def toValueRegistry(o: Index): FieldValueRegistry = new FieldValueRegistry(fields).registerFieldValue(0, o.raw)
    override def fromValueRegistry(reg: FieldValueRegistry): Index = Index(reg.getValue[Int](0))
  }

  implicit val fieldConverterForBinaryState:FieldConverter[BinaryState] = new FieldConverter[BinaryState] {
    override val fields: FieldRegistry[BinaryState] = new FieldRegistry[BinaryState]("BinaryState").registerBooleanField("BinaryState", 0)
    override def toValueRegistry(o: BinaryState): FieldValueRegistry = new FieldValueRegistry(fields).registerFieldValue(0, o.raw)
    override def fromValueRegistry(reg: FieldValueRegistry): BinaryState = BinaryState(reg.getValue[Boolean](0))
  }

  implicit val fieldConverterForFeatureToggle:FieldConverter[FeatureToggle] = new FieldConverter[FeatureToggle] {
    override val fields: FieldRegistry[FeatureToggle] = new FieldRegistry[FeatureToggle]("FeatureToggle").registerBooleanField("FeatureToggle", 0)
    override def toValueRegistry(o: FeatureToggle): FieldValueRegistry = new FieldValueRegistry(fields).registerFieldValue(0, o.raw)
    override def fromValueRegistry(reg: FieldValueRegistry): FeatureToggle = FeatureToggle(reg.getValue[Boolean](0))
  }

  implicit val fieldConverterForUrl:FieldConverter[Url] = new FieldConverter[Url] {
    override val fields: FieldRegistry[Url] = new FieldRegistry[Url]("Url").registerStringField("Url", 0)
    override def toValueRegistry(o: Url): FieldValueRegistry = new FieldValueRegistry(fields).registerFieldValue(0, o.raw)
    override def fromValueRegistry(reg: FieldValueRegistry): Url = Url(reg.getValue[String](0))
  }

  implicit val fieldConverterForName:FieldConverter[Name] = new FieldConverter[Name] {
    override val fields: FieldRegistry[Name] = new FieldRegistry[Name]("Name").registerStringField("Name", 0)
    override def toValueRegistry(o: Name): FieldValueRegistry = new FieldValueRegistry(fields).registerFieldValue(0, o.raw)
    override def fromValueRegistry(reg: FieldValueRegistry): Name = {
      Name(reg.getValue[String](0))
    }
  }

  implicit val fieldConverterForSize:FieldConverter[Size] = new FieldConverter[Size] {
    override val fields: FieldRegistry[Size] = new FieldRegistry[Size]("Size").registerIntField("Size", 0)
    override def toValueRegistry(o: Size): FieldValueRegistry = new FieldValueRegistry(fields).registerFieldValue(0, o.raw)
    override def fromValueRegistry(reg: FieldValueRegistry): Size = Size(reg.getValue[Int](0))
  }

}

