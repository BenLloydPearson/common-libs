package com.gravity.interests.interfaces.userfeedback

import com.gravity.utilities.grvenum.GrvEnum
import com.gravity.utilities.grvstrings
import com.gravity.utilities.swagger.adapter.DefaultValueWriter
import play.api.libs.json.{Json, Writes}

/*
              ___...---''
  ___...---'\'___
''       _.-''  _`'.______\\.
        /_.)  )..-  __..--'\\
       (    __..--''
        '-''\@


 Ⓐ Ⓐ Ⓐ Ⓐ Ⓐ Ⓐ Ⓐ Ⓐ Ⓐ Ⓐ Ⓐ
*/

object UserFeedbackPresentation extends GrvEnum[Int] {
  type Name = String

  case class Type(i: Int, n: Name) extends ValueTypeBase(i, n)
  override def mkValue(id: Int, name: Name): Type = Type(id, name)

  val none = Value(-1, "none")

  /** User feedback options presented "inline" with the page flow just as an ordinary placement would be. */
  val inline = Value(0, "inline")

  /**
    * A user feedback placement that pops up from the bottom of the viewport when the user scrolls up. The placement
    * will have a "more" button that allows the user to expand it and see ordinary article recommendations. If the
    * site is configured with /me pages, the user may click through to those via "view more" buttons.
    */
  val toaster = Value(1, "toaster")

  private val havingRecos = Set(none, toaster)

  override def defaultValue: Type = inline

  def hasRecos(presentation: Type): Boolean = havingRecos.contains(presentation)
  def hasRecos(presentationName: Name): Boolean = havingRecos.exists(_.name == presentationName)

  implicit class UserFeedbackPresentationType(val t: Type) {
    lazy val displayName = grvstrings.camelCaseToTitleCase(t.name)
  }

  object UserFeedbackPresentationType {
    implicit val richTypeJsonWrites = Writes[UserFeedbackPresentationType](richType => Json.obj(
      "id" -> richType.t.id,
      "name" -> richType.t.name,
      "displayName" -> richType.displayName
    ))
  }

  implicit val standardEnumJsonFormat = makeJsonFormat[Type]

  implicit val defaultValueWriter: DefaultValueWriter[Type] = makeDefaultValueWriter[Type]
}
