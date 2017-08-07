package com.gravity.interests.jobs.intelligence

import com.gravity.utilities.grvenum.GrvEnum
import play.api.libs.json.Format


/*
 *    __   _         __         
 *   / /  (_)__  ___/ /__  ____
 *  / _ \/ / _ \/ _  / _ \/ _  /
 * /_//_/_/_//_/\_,_/\___/\_, / 
 *                       /___/
 */
object SegmentStatus extends GrvEnum[Byte] {

  case class Type(i: Byte, n: String) extends ValueTypeBase(i, n)

  override def mkValue(id: Byte, name: String): Type = Type(id, name)

  val pending: Type = Value(0, "pending")
  val active: Type = Value(1, "active")
  val inactive: Type = Value(2, "inactive")

  override def defaultValue: Type = inactive

  implicit val jsonFormat: Format[Type] = makeJsonFormat[Type]

}