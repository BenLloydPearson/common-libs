package com.gravity.domain.aol

import com.gravity.utilities.grvenum.GrvEnum
import com.gravity.utilities.swagger.adapter.DefaultValueWriter
import play.api.libs.json.Format

/**
 * Created with IntelliJ IDEA.
 * Author: robbie
 * Date: 4/14/15
 * Time: 8:01 PM
 *            _ 
 *          /  \
 *         / ..|\
 *        (_\  |_)
 *        /  \@'
 *       /     \
 *   _  /  \   |
 *  \\/  \  | _\
 *   \   /_ || \\_
 *    \____)|_) \_)
 *
 */
object AolDeliveryMethod extends GrvEnum[Byte] {
  case class Type(i: Byte, n: String) extends ValueTypeBase(i, n)
  override def mkValue(id: Byte, name: String): Type = Type(id, name)

  val any: Type = Value(0, "any")
  val pinned: Type = Value(1, "pinned")
  val personalized: Type = Value(2, "personalized")

  def defaultValue: Type = any

  implicit val jsonFormat: Format[Type] = makeJsonFormat[Type]
  implicit val defaultValueWriter: DefaultValueWriter[Type] = makeDefaultValueWriter[Type]
}
