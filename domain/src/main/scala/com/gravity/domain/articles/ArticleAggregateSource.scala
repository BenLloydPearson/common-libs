package com.gravity.domain.articles

import com.gravity.utilities.grvenum.GrvEnum

/**
  * Created by jengelman14 on 2/9/17.
  */
object ArticleAggregateSource extends GrvEnum[Byte] {
  case class Type(i: Byte, n: String) extends ValueTypeBase(i, n)
  override def mkValue(id: Byte, name: String): Type = Type(id, name)

  val Gravity: Type = Value(0, "Gravity")
  val AOL: Type = Value(1, "AOL")

  val defaultValue: Type = Gravity
}
