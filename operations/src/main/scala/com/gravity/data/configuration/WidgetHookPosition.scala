package com.gravity.data.configuration

import com.gravity.utilities.grvenum.GrvEnum

/*
              ___...---''
  ___...---'\'___
''       _.-''  _`'.______\\.
        /_.)  )..-  __..--'\\
       (    __..--''
        '-''\@


 Ⓐ Ⓐ Ⓐ Ⓐ Ⓐ Ⓐ Ⓐ Ⓐ Ⓐ Ⓐ Ⓐ
*/

object WidgetHookPosition extends GrvEnum[Short] {
  case class Type(i: Short, n: String) extends ValueTypeBase(i, n)
  override def mkValue(id: Short, name: String) = Type(id, name)

  /**
   * This one can actually appear in practice in cases where a new widget hook position is introduced, but an API server
   * with an older build reads from the DB and fails to take the new position into account.
   */
  val unknown = Value(-1, "unknown")

  val footer = Value(0, "footer")
  val afterWidgetVarsCreated = Value(1, "afterWidgetVarsCreated")

  /**
   * The very first event fired after a widget 200 response indicating that an impression was served with an appropriate
   * number of articles. If you hook into this event, you should be careful not to block or destroy widget rendering.
   */
  val impressionServed = Value(2, "impressionServed")

  val domReady = Value(3, "domReady")

  val defaultValue = unknown

  implicit val jsonFormat = makeJsonFormat[Type]
  implicit val defaultValueWriter = makeDefaultValueWriter[Type]
}