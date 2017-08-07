package com.gravity.utilities

import com.gravity.utilities.grvenum.GrvEnum
import com.gravity.utilities.swagger.adapter.DefaultValueWriter
import play.api.libs.json.Format

/**
 * Created by tdecamp on 12/1/15.
 * {{insert neat ascii diagram here}}
 */
@SerialVersionUID(1l)
object ArticleReviewStatus extends GrvEnum[Int] {
  case class Type(i: Int, n: String) extends ValueTypeBase(i, n)
  override def mkValue(id: Int, name: String): Type = Type(id, name)

  // This is the default until the external review tool is hooked up.
  val notReviewed: Type = Value(0, "notReviewed")

  // These 2 statuses mean that the article has gone through the TMT or AOL review process.
  val externallyApproved: Type = Value(1, "externallyApproved")
  val externallyRejected: Type = Value(2, "externallyRejected")

  // These 4 statuses come from the Dashboard when using the review tool.
  val internallyApproved: Type = Value(3, "approved")
  val internallyConditionallyApproved: Type = Value(4, "conditionallyApproved")
  val internallyRejected: Type = Value(5, "rejected")
  val internallyLeaveNotReviewed: Type = Value(6, "leaveNotReviewed")

  // These statuses will never be stored but will propagate to similar articles
  val internallyRejectAllWithTitle: Type = Value(7, "rejectAllWithTitle")
  val internallyRejectAllWithImage: Type = Value(8, "rejectAllWithImage")
  val internallyRejectAllWithUrl: Type = Value(9, "rejectAllWithUrl")

  val propagatingStatuses: List[Type] = List(internallyRejectAllWithTitle, internallyRejectAllWithImage,
    internallyRejectAllWithUrl)

  val updatableStatuses: List[Type] = List(internallyApproved, internallyConditionallyApproved, internallyRejected,
    internallyLeaveNotReviewed)

  val campaignArticleStatusInactive: List[Type] = List(notReviewed, externallyRejected, internallyRejected,
    internallyLeaveNotReviewed, internallyRejectAllWithTitle, internallyRejectAllWithImage, internallyRejectAllWithUrl)

  val campaignArticleStatusActive: List[Type] = List(externallyApproved, internallyApproved,
    internallyConditionallyApproved)

  def defaultValue: Type = notReviewed

  val dashboardStatuses: List[Type] = List(internallyApproved, internallyConditionallyApproved, internallyRejected,
    internallyLeaveNotReviewed, internallyRejectAllWithTitle, internallyRejectAllWithImage, internallyRejectAllWithUrl)

  //  val JsonSerializer: EnumNameSerializer[CampaignArticleStatus.type] = new EnumNameSerializer(this)
  //  val SetJsonSerializer: EnumSetNameSerializer[CampaignArticleStatus.type] = new EnumSetNameSerializer(this)

  implicit val jsonFormat: Format[Type] = makeJsonFormat[Type]

  implicit val listDefaultValueWriter: DefaultValueWriter[List[Type]] = DefaultValueWriter.listDefaultValueWriter[Type]()
}
