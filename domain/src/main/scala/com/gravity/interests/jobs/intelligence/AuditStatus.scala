package com.gravity.interests.jobs.intelligence

import com.gravity.utilities.grvenum.GrvEnum
import play.api.libs.json.Format

/**
 * Created by IntelliJ IDEA.
 * Author: Robbie Coleman
 * Date: 1/15/14
 * Time: 11:32 AM
 *           __
 *          /  \
 *         / ..|\
 *        (_\  |_)
 *        /  \@'
 *       /     \
 *   _  /  `   |
 * \\/  \  | _\
 *   \   /_ || \\_
 *    \____)|_) \_)
 *
 */
object AuditStatus extends GrvEnum[Byte] {
  case class Type(i: Byte, n: String) extends ValueTypeBase(i, n)
  override def mkValue(id: Byte, name: String): Type = Type(id, name)

  val notSet: Type = Value(0, "notSet")
  val succeeded: Type = Value(1, "succeeded")
  val failed: Type = Value(2, "failed")

  def defaultValue: AuditStatus.Type = notSet

  implicit val jsonFormat: Format[Type] = makeJsonFormat[Type]
}
