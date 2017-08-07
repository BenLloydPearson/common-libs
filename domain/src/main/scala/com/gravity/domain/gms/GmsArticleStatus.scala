package com.gravity.domain.gms

import com.gravity.interests.jobs.intelligence.{CampaignArticleSettings, CampaignArticleStatus}
import com.gravity.utilities.grvenum.GrvEnum
import com.gravity.utilities.swagger.adapter.DefaultValueWriter
import play.api.libs.json.Format

/**
 * Created by robbie on 08/05/2015.
  *            _ 
  *          /  \
  *         / ..|\
  *        (_\  |_)
  *        /  \@'
  *       /     \
  *   _  /  \   |
  * \\/  \  | _\
  *   \   /_ || \\_
  *    \____)|_) \_)
  *
  */
object GmsArticleStatus extends GrvEnum[Short] {
 import com.gravity.logging.Logging._
  case class Type(i: Short, n: String) extends ValueTypeBase(i, n)
  override def mkValue(id: Short, name: String): Type = Type(id, name)

  val Pending: Type = Value(0, "pending")

  val Rejected: Type = Value(1, "rejected")

  val Live: Type = Value(2, "live")

  val Expired: Type = Value(3, "expired")

  val Deleted: Type = Value(4, "deleted")

  val Approved: Type = Value(5, "approved")

  val Invalid: Type = Value(6, "invalid")

  override def defaultValue: Type = Pending

  implicit val format: Format[Type] = makeJsonFormat[Type]

  implicit class WrappedEnumInstance(val enumInstance: Type) extends AnyVal {
    def campaignArticleSettings: Option[CampaignArticleSettings] = {
      val (campaignArticleStatus, isBlacklisted) = enumInstance match {
        case GmsArticleStatus.Pending | GmsArticleStatus.Expired | GmsArticleStatus.Approved => (CampaignArticleStatus.inactive, false)
        case GmsArticleStatus.Rejected | GmsArticleStatus.Deleted | GmsArticleStatus.Invalid => (CampaignArticleStatus.inactive, true)
        case GmsArticleStatus.Live => (CampaignArticleStatus.active, false)
        case x =>
          warn("Unknown GmsArticleStatus enum val {0}", x)
          (CampaignArticleStatus.inactive, false)
      }

      Some(CampaignArticleSettings(campaignArticleStatus, isBlacklisted))
    }

    def isRecommendable: Boolean = enumInstance == GmsArticleStatus.Live

    def isToBeKept: Boolean = enumInstance match {
      case GmsArticleStatus.Pending | GmsArticleStatus.Live | GmsArticleStatus.Approved | GmsArticleStatus.Invalid => true
      case _ => false
    }
  }

  implicit val defaultValueWriter: DefaultValueWriter[Type] = makeDefaultValueWriter[Type]
}
