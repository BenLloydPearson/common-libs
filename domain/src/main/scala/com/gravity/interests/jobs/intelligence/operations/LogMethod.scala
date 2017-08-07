package com.gravity.interests.jobs.intelligence.operations

import com.gravity.utilities.grvenum.GrvEnum
import play.api.libs.json.Format

/*
              ___...---''
  ___...---'\'___
''       _.-''  _`'.______\\.
        /_.)  )..-  __..--'\\
       (    __..--''
        '-''\@


 Ⓐ Ⓐ Ⓐ Ⓐ Ⓐ Ⓐ Ⓐ Ⓐ Ⓐ Ⓐ Ⓐ
*/

/** Represents when/how a log event was generated. */
object LogMethod extends GrvEnum[Int] {
  case class Type(i: Int, n: String) extends ValueTypeBase(i, n)
  override def mkValue(id: Int, name: String): Type = Type(id, name)

  val unknown: Type = Value(0, "unknown")

  /** Logged during widget reco generation (/w2, etc.). */
  val widgetRecos: Type = Value(1, "widgetRecos")

  /** Logged during API reco generation. */
  val apiRecos: Type = Value(2, "apiRecos")

  /** Logged asynchronously via the Log Impression API. */
  val async: Type = Value(3, "async")

  val defaultValue: Type = unknown

  implicit val jsonFormat: Format[Type] = makeJsonFormat[Type]
}
