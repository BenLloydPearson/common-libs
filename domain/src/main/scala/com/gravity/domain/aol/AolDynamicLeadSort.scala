package com.gravity.domain.aol

import com.gravity.utilities.grvenum.GrvEnum

/**
 * Created by IntelliJ IDEA.
 * Author: Robbie Coleman
 * Date: 5/5/14
 * Time: 4:26 PM
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
object AolDynamicLeadSort extends GrvEnum[Short] {
  case class Type(i: Short, n: String) extends ValueTypeBase(i, n)
  override def mkValue(id: Short, name: String): Type = Type(id, name)

  val UpdatedTime: Type = Value(0, "updatedTime")
//  val CreatedTime = Value(1, "createdTime")
  val UpdatedTimeDesc: Type = Value(2, "-updatedTime")
//  val CreatedTimeDesc = Value(3, "-createdTime")
  val PlanName: Type = Value(4, "planName")
  val PlanNameDesc: Type = Value(5, "-planName")
  val ProductId: Type = Value(6, "productId")
  val ProductIdDesc: Type = Value(7, "-productId")
  val ClicksTotal: Type = Value(8, "clicksTotal")
  val ClicksTotalDesc: Type = Value(9, "-clicksTotal")
  val ImpressionsTotal: Type = Value(10, "impressionsTotal")
  val ImpressionsTotalDesc: Type = Value(11, "-impressionsTotal")
  val CtrTotal: Type = Value(12, "ctrTotal")
  val CtrTotalDesc: Type = Value(13, "-ctrTotal")
  val Title: Type = Value(14, "title")
  val TitleDesc: Type = Value(15, "-title")
  val Category: Type = Value(16, "aolCategory")
  val CategoryDesc: Type = Value(17, "-aolCategory")
  val Source: Type = Value(18, "aolSource")
  val SourceDesc: Type = Value(19, "-aolSource")
  val PinnedSlot: Type = Value(20, "pinnedSlotForCurrentChannelFilter")
  val PinnedSlotDesc: Type = Value(21, "-pinnedSlotForCurrentChannelFilter")
  val LastGoLiveTime: Type = Value(22, "dlArticleLastGoLiveTime")
  val LastGoLiveTimeDesc: Type = Value(23, "-dlArticleLastGoLiveTime")
  val Status: Type = Value(24, "dlArticleStatus")
  val StatusDesc: Type = Value(25, "-dlArticleStatus")
  val SubmittedTime: Type = Value(26, "dlArticleSubmittedTime")
  val SubmittedTimeDesc: Type = Value(27, "-dlArticleSubmittedTime")
  val SubmittedByUserId: Type = Value(28, "dlArticleSubmittedUserId")
  val SubmittedByUserIdDesc: Type = Value(29, "-dlArticleSubmittedUserId")
  val ApprovedRejectedByUserId: Type = Value(30, "dlArticleApprovedRejectedByUserId")
  val ApprovedRejectedByUserIdDesc: Type = Value(31, "-dlArticleApprovedRejectedByUserId")
  val PublishedOn: Type = Value(32, "publishOnDate")
  val PublishedOnDesc: Type = Value(33, "-publishOnDate")
  val EndDate: Type = Value(34, "endDate")
  val EndDateDesc: Type = Value(35, "-endDate")
  val ThirtyMinuteCtr: Type = Value(36, "thirtyMinuteCtr")
  val ThirtyMinuteCtrDesc: Type = Value(37, "-thirtyMinuteCtr")

  override def defaultValue: Type = UpdatedTime

}
