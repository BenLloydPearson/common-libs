package com.gravity.api.partnertagging

import com.gravity.utilities.grvenum.GrvEnum
import com.gravity.utilities.swagger.adapter.DefaultValueWriter
import net.liftweb.json.JsonAST.JValue
import net.liftweb.json.{Formats, Serializer, TypeInfo}
import play.api.libs.json.{Format, Json, Writes}

object PartnerTagging {
  type AdvertiserTypeSetting    = Option[AdvertiserTypeEnum.Type]
  type ContentCategoriesSetting = Option[Seq[ContentCategoryEnum.Type]]
  type ContentRatingSetting     = Option[ContentRatingEnum.Type]
}

@SerialVersionUID(1l)
object AdvertiserTypeEnum extends GrvEnum[Byte] {
  case class Type(i: Byte, n: String) extends ValueTypeBase(i, n)
  override def mkValue(id: Byte, name: String): Type = Type(id, name)

  val brand: Type = Value(1, "Brand")
  val directResponse: Type = Value(2, "Direct Response")
  val publisher: Type = Value(3, "Publisher")

  override val defaultValue: Type = publisher   // GrvEnum should really define defaultValue as Option[T]

  // For Lift JSON Serialization used by e.g. ApiResponse.
  object LiftJsonSerializer extends Serializer[AdvertiserTypeEnum.Type] {
    import net.liftweb.json.JsonDSL._

    private val classOfEnumType = classOf[AdvertiserTypeEnum.Type]

    def serialize(implicit format: Formats): PartialFunction[Any, JValue] = {
      case obj: AdvertiserTypeEnum.Type => {
        ("id", obj.i.asInstanceOf[Int]) ~ ("name", obj.n)
      }
    }

    def deserialize(implicit format: Formats): PartialFunction[(TypeInfo, JValue), AdvertiserTypeEnum.Type] = {
      case (TypeInfo(`classOfEnumType`, _), json) => {
        val id = (json \\ ("id")).extract[Byte]

        AdvertiserTypeEnum(id)
      }
    }
  }

  implicit val jsonFormat: Format[Type] = makeJsonFormat[Type]
  implicit val defaultValueWriter: DefaultValueWriter[Type] = makeDefaultValueWriter[Type]
}

@SerialVersionUID(1l)
object ContentCategoryEnum extends GrvEnum[Short] {
  case class Type(i: Short, n: String) extends ValueTypeBase(i, n)
  override def mkValue(id: Short, name: String): Type = Type(id, name)

  val artsAndEntertainment: Type = Value( 1, "Arts & Entertainment")
  val automotive: Type = Value( 2, "Automotive")
  val business: Type = Value( 3, "Business")
  val careers: Type = Value( 4, "Careers")
  val education: Type = Value( 5, "Education")
  val familyAndParenting: Type = Value( 6, "Family & Parenting")
  val healthAndFitness: Type = Value( 7, "Health & Fitness")
  val foodAndDrink: Type = Value( 8, "Food & Drink")
  val hobbiesAndInterests: Type = Value( 9, "Hobbies & Interests")
  val homeAndGarden: Type = Value(10, "Home & Garden")
  val lawGovtAndPolitices: Type = Value(11, "Law, Gov't & Politics")
  val news: Type = Value(12, "News")
  val personalFinance: Type = Value(13, "Personal Finance")
  val society: Type = Value(14, "Society")
  val science: Type = Value(15, "Science")
  val pets: Type = Value(16, "Pets")
  val sports: Type = Value(17, "Sports")
  val styleAndFashion: Type = Value(18, "Style & Fashion")
  val technologyAndComputing: Type = Value(19, "Technology & Computing")
  val travel: Type = Value(20, "Travel")
  val realEstate: Type = Value(21, "Real Estate")
  val shopping: Type = Value(22, "Shopping")
  val religionAndSpirituality: Type = Value(23, "Religion and Spirituality")
  val uncategorized: Type = Value(24, "Uncategorized")
  val nonstandardContent: Type = Value(25, "Non-Standard Content")
  val illegalContent: Type = Value(26, "Illegal Content")

  override val defaultValue: Type = uncategorized    // GrvEnum should really define defaultValue as Option[T]

  // For Lift JSON Serialization used by e.g. ApiResponse.
  object LiftJsonSerializer extends Serializer[ContentCategoryEnum.Type] {
    import net.liftweb.json.JsonDSL._

    private val classOfEnumType = classOf[ContentCategoryEnum.Type]

    def serialize(implicit format: Formats): PartialFunction[Any, JValue] = {
      case obj: ContentCategoryEnum.Type => {
        ("id", obj.i.asInstanceOf[Int]) ~ ("name", obj.n)
      }
    }

    def deserialize(implicit format: Formats): PartialFunction[(TypeInfo, JValue), ContentCategoryEnum.Type] = {
      case (TypeInfo(`classOfEnumType`, _), json) => {
        val id = (json \\ ("id")).extract[Short]

        ContentCategoryEnum(id)
      }
    }
  }

  implicit val standardEnumJsonFormat = makeJsonFormat[Type]

  val jsonWritesWithId: Writes[Type] = Writes[ContentCategoryEnum.Type] { enum =>
    Json.obj(
      "id"   -> enum.i.asInstanceOf[Int],
      "name" -> enum.n
    )
  }

  implicit val defaultValueWriter: DefaultValueWriter[Type] = makeDefaultValueWriter[Type]
}

@SerialVersionUID(1l)
object ContentRatingEnum extends GrvEnum[Byte] {
  case class Type(i: Byte, n: String) extends ValueTypeBase(i, n)
  override def mkValue(id: Byte, name: String): Type = Type(id, name)

  // QUESTION: Is there any value in leaving "holes" between the value id's?
  val g: Type = Value(1, "G")
  val pg: Type = Value(2, "PG")
  val pg13: Type = Value(3, "PG-13")
  val mature: Type = Value(4, "Mature")
  val nsfw: Type = Value(5, "NSFW")

  // There's no good choice here: G would be a "safe" default for publisher, but NSFW would be the "safe" default for advertiser.
  override val defaultValue: Type = pg

  // For Lift JSON Serialization used by e.g. ApiResponse.
  object LiftJsonSerializer extends Serializer[ContentRatingEnum.Type] {
    import net.liftweb.json.JsonDSL._

    private val classOfEnumType = classOf[ContentRatingEnum.Type]

    def serialize(implicit format: Formats): PartialFunction[Any, JValue] = {
      case obj: ContentRatingEnum.Type => {
        ("id", obj.i.asInstanceOf[Int]) ~("name", obj.n)
      }
    }

    def deserialize(implicit format: Formats): PartialFunction[(TypeInfo, JValue), ContentRatingEnum.Type] = {
      case (TypeInfo(`classOfEnumType`, _), json) => {
        val id = (json \\ ("id")).extract[Byte]

        ContentRatingEnum(id)
      }
    }
  }

  // Play-JSON Serialization, needed for CampaignView.
  implicit val jsonWritesContentRatingEnum: Writes[Type] = Writes[ContentRatingEnum.Type] { enum =>
    Json.obj(
      "id"   -> enum.i.asInstanceOf[Int],
      "name" -> enum.n
    )
  }

  implicit val jsonFormat: Format[Type] = makeJsonFormat[Type]
  implicit val defaultValueWriter: DefaultValueWriter[Type] = makeDefaultValueWriter[Type]
}

