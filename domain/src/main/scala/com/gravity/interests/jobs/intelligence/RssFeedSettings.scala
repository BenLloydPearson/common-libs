package com.gravity.interests.jobs.intelligence

import com.gravity.interests.jobs.intelligence.operations.ScrubberEnum
import net.liftweb.json._
import play.api.libs.json.{Format, Json}

@SerialVersionUID(1L)
case class RssFeedSettings(skipAlreadyIngested: Boolean, ignoreMissingImages: Boolean, initialArticleStatus: CampaignArticleStatus.Type,
                           feedStatus: CampaignArticleStatus.Type, scrubberEnum: ScrubberEnum.Type, initialArticleBlacklisted: Boolean = true) {
  override lazy val toString: String = {
    val sb = new StringBuilder
    sb.append("{\"skipAlreadyIngested\":").append(skipAlreadyIngested)
    sb.append(", \"ignoreMissingImages\":").append(ignoreMissingImages)
    sb.append(", \"initialArticleStatus\":\"").append(initialArticleStatus.toString).append("\"")
    sb.append(", \"feedStatus\":\"").append(feedStatus.toString).append("\"")
    sb.append(", \"scrubberEnum\":\"").append(scrubberEnum.toString).append("\"")
    sb.append(", \"initialArticleBlacklisted\":").append(initialArticleBlacklisted).append("}")
    sb.toString()
  }
}

object RssFeedSettings {
  implicit val jsonFormat: Format[RssFeedSettings] = Json.format[RssFeedSettings]

  val default: RssFeedSettings = RssFeedSettings(skipAlreadyIngested = true, ignoreMissingImages = false, initialArticleStatus = CampaignArticleStatus.inactive, feedStatus = CampaignArticleStatus.active, ScrubberEnum.defaultValue, initialArticleBlacklisted = true)

  object JsonSerializer extends Serializer[RssFeedSettings] {
    import JsonDSL._

    override def deserialize(implicit format: Formats): PartialFunction[(TypeInfo, JValue), RssFeedSettings] = {
      case (TypeInfo(clazz, _), _) if clazz.toString.indexOf("RssFeedSettings") != -1 => throw new MappingException("Not implemented")
    }

    override def serialize(implicit format: Formats): PartialFunction[Any, JValue] = {
      case RssFeedSettings(skipAlreadyIngested, ignoreMissingImages, initialArticleStatus, feedStatus, scrubberEnum, initialArticleBlacklisted) =>
        ("skipAlreadyIngested" -> skipAlreadyIngested) ~
        ("ignoreMissingImages" -> ignoreMissingImages) ~
        ("initialArticleStatus" -> initialArticleStatus) ~
        ("feedStatus" -> feedStatus) ~
        ("scrubberEnum" -> scrubberEnum) ~
        ("initialArticleBlacklisted" -> initialArticleBlacklisted)
    }
  }
}
