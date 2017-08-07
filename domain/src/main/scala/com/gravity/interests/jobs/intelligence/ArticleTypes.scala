package com.gravity.interests.jobs.intelligence

import com.gravity.utilities.grvenum.GrvEnum

/**
 * Created by IntelliJ IDEA.
 * Author: Robbie Coleman
 * Date: 6/26/13
 * Time: 2:42 PM
 */
@SerialVersionUID(148429009671799679l)
object ArticleTypes extends GrvEnum[Byte] {
  case class Type(i: Byte, n: String) extends ValueTypeBase(i, n)
  override def mkValue(id: Byte, name: String): Type = Type(id, name)

  val unknown: Type = Value(0, "unknown")
  val content: Type = Value(1, "content")
  val video: Type = Value(2, "video")
  val image: Type = Value(3, "image")

  def defaultValue: ArticleTypes.Type = unknown
}
