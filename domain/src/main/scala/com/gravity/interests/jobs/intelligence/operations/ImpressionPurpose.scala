package com.gravity.interests.jobs.intelligence.operations

import com.gravity.utilities.grvenum.GrvEnum
import com.gravity.utilities.grvjson
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

object ImpressionPurpose extends GrvEnum[Int] {
  case class Type(i: Int, n: String) extends ValueTypeBase(i, n)
  override def mkValue(id: Int, name: String): Type = Type(id, name)

  /** An impression tied to normal placement article recommendations. */
  val recirc = Value(0, "recirc")

  /** An impression tied to user feedback. See [[com.gravity.interests.interfaces.userfeedback.UserFeedbackVariation]]. */
  val userFeedback = Value(1, "userFeedback")

  /** An impression tied to a pageview for /me pages. */
  val slashMePageView = Value(2, "slashMePageView")

  override def defaultValue: ImpressionPurpose.Type = recirc

  implicit val standardEnumJsonFormat = makeJsonFormat[Type]
  implicit val nelJsonWrites = grvjson.nelWrites[Type]

  implicit val defaultValueWriter: DefaultValueWriter[Type] = makeDefaultValueWriter[Type]
}
