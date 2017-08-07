package com.gravity.utilities

import com.gravity.utilities.grvenum.GrvEnum

/**
 * Created by tdecamp on 11/16/15.
 * {{insert neat ascii diagram here}}
 */
object DatabasePivot extends GrvEnum[Int] {
  case class Type(i: Int, n: String) extends ValueTypeBase(i, n)

  override def mkValue(id: Int, name: String): DatabasePivot.Type = Type(id, name)

  override def defaultValue: DatabasePivot.Type = date

  val date: Type = Value(0, "date")
  val sitePlacementId: Type = Value(1, "placement")
  val countryCodeId: Type = Value(2, "geo")
  val device: Type = Value(3, "device")
  val site: Type = Value(4, "site")
}
