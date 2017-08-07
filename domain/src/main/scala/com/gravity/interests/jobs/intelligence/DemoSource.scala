package com.gravity.interests.jobs.intelligence

import com.gravity.utilities.grvenum.GrvEnum

/**
 * Created by jengelman14 on 8/13/15.
 */
@SerialVersionUID(1l)
object DemoSource extends GrvEnum[Byte] {
  case class Type(i: Byte, n: String) extends ValueTypeBase(i, n)
  override def mkValue(id: Byte, name: String): Type = Type(id, name)

  val Gravity: Type = Value(0, "Gravity")
  val AOL: Type = Value(1, "AOL")
  val BlueKai: Type = Value(2, "BlueKai")

  val defaultValue: Type = Gravity
}
