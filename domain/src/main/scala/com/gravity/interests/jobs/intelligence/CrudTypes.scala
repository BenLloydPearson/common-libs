package com.gravity.interests.jobs.intelligence

import com.gravity.utilities.grvenum.GrvEnum

/**
 * Created with IntelliJ IDEA.
 * User: Unger
 * Date: 6/7/13
 * Time: 2:37 PM
 * To change this template use File | Settings | File Templates.
 */

@SerialVersionUID(6010528714348252625l)
object CrudTypes extends GrvEnum[Short] {
  case class Type(i: Short, n: String) extends ValueTypeBase(i, n)
  override def mkValue(id: Short, name: String): Type = Type(id, name)

  val unknown: Type = Value(0, "unknown")
  val create: Type = Value(1, "create")
  val read: Type = Value(2, "read")
  val update: Type = Value(3, "update")
  val delete: Type = Value(4, "delete")

  def defaultValue: Type = unknown
}

