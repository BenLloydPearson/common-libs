package com.gravity.domain.recommendations

import com.gravity.utilities.grvenum.GrvEnum


/*
 *    __   _         __         
 *   / /  (_)__  ___/ /__  ____
 *  / _ \/ / _ \/ _  / _ \/ _  /
 * /_//_/_/_//_/\_,_/\___/\_, / 
 *                       /___/
 */

@SerialVersionUID(-7571062693321189992l)
object FailureStrategies extends GrvEnum[Int] {
  type FailureStrategy = Type

  case class Type(i: Int, n: String) extends ValueTypeBase(i, n)

  override def mkValue(id: Int, name: String): Type = Type(id, name)

  //  @deprecated("FailureStrategies.none has no useful use cases", "2-18-2014")
  //  def none = _none
  //  val _none = Value(0, "none") // i.e. this slot is required
  val stealPrevious: Type = Value(1, "stealPrevious") // steal articles from the previous slot

  override def defaultValue: Type = stealPrevious

}
