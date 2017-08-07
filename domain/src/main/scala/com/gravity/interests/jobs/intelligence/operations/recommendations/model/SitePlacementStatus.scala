package com.gravity.interests.jobs.intelligence.operations.recommendations.model

import com.gravity.utilities.grvenum.GrvEnum
import com.gravity.utilities.swagger.adapter.DefaultValueWriter

/*
              ___...---''
  ___...---'\'___
''       _.-''  _`'.______\\.
        /_.)  )..-  __..--'\\
       (    __..--''
        '-''\@


 Ⓐ Ⓐ Ⓐ Ⓐ Ⓐ Ⓐ Ⓐ Ⓐ Ⓐ Ⓐ Ⓐ
*/

object SitePlacementStatus extends GrvEnum[Int] {
  type SitePlacementStatus = Type
  case class Type(i: Int, n: String) extends ValueTypeBase(i, n)
  override def mkValue(id: Int, name: String) = Type(id, name)

  /** @deprecated */
  val prospecting = Value(0, "prospecting")
  val testing = Value(1, "testing")
  /** @deprecated */
  val evaluating = Value(3, "evaluating")
  val live = Value(4, "live")
  val paused = Value(5, "paused")
  val disabled = Value(6, "disabled")

  val activeValues = Set(testing, live, paused, disabled)
  val canServe = Set(testing, live)
  val canServeIds = canServe.map(_.id)
  val cannotServe = Set(paused, disabled)

  override def defaultValue = testing

  implicit val jsonFormat = makeJsonFormat[Type]

  implicit val defaultValueWriter: DefaultValueWriter[Type] = makeDefaultValueWriter[Type]
}
