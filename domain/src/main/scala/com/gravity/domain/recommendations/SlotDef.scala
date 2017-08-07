package com.gravity.domain.recommendations

import com.gravity.domain.Ranged
import com.gravity.domain.recommendations.FailureStrategies.Type
import com.gravity.utilities.grvz._

import scalaz.NonEmptyList

/** Created by IntelliJ IDEA.
  * Author: Robbie Coleman
  * Date: 8/23/13
  * Time: 10:53 AM
  */

trait HasSlotDef {
  def slot: SlotDef
}

trait SlotDef extends Ranged {
  def start: Int
  def end: Int
  def recommenderId: Long
  def maxDaysOld: Option[Int]
  def failureStrategy: FailureStrategies.Type
  def contentGroups: NonEmptyList[ContentGroup]
}

@SerialVersionUID(-9156974743843740652l)
case class SerializableSlotDef(start: Int, end: Int, recommenderId: Long, maxDaysOld: Option[Int], failureStrategyId: Int, contentGroupList: List[ContentGroup]) extends SlotDef with Serializable {
  override def failureStrategy: Type = FailureStrategies(failureStrategyId)
  override def contentGroups: NonEmptyList[ContentGroup] = nel(contentGroupList.head, contentGroupList.tail: _*)
}