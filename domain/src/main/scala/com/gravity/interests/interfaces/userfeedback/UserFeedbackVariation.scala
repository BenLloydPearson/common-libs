package com.gravity.interests.interfaces.userfeedback

import com.gravity.interests.interfaces.userfeedback.{UserFeedbackOption => o}
import com.gravity.utilities.grvenum.GrvEnum
import com.gravity.utilities.grvstrings
import com.gravity.utilities.swagger.adapter.DefaultValueWriter
import play.api.libs.json.{Json, Writes}

import scalaz.Scalaz._
import scalaz._

/*
              ___...---''
  ___...---'\'___
''       _.-''  _`'.______\\.
        /_.)  )..-  __..--'\\
       (    __..--''
        '-''\@


 Ⓐ Ⓐ Ⓐ Ⓐ Ⓐ Ⓐ Ⓐ Ⓐ Ⓐ Ⓐ Ⓐ
*/

object UserFeedbackVariation extends GrvEnum[Int] {
  type Name = String
  case class Type(i: Int, n: Name) extends ValueTypeBase(i, n)
  override def mkValue(id: Int, name: Name): Type = Type(id, name)

  val none = Value(0, "none")

  // First run variations
  val thumbsUpDown = Value(1, "thumbsUpDown")
  val upvoteDownvote = Value(2, "upvoteDownvote")
  val stars1to5 = Value(3, "stars1to5")
  val numbers1to5 = Value(4, "numbers1to5")
  val happyIndifferentAmusedExcitedAngry = Value(5, "happyIndifferentAmusedExcitedAngry")
  val loveItMehNotForMe = Value(6, "loveItMehNotForMe")
  val satisfactionScale = Value(13, "satisfactionScale")
  val interestScale = Value(15, "interestScale")

  override def defaultValue: Type = none

  /** Options (the List[UserFeedbackOption.Type] are arranged in display order. */
  val variationToOptions: Map[UserFeedbackVariation.Type, List[UserFeedbackOption.Type]] = Map(
    thumbsUpDown -> List(o.thumbUp, o.thumbDown),
    upvoteDownvote -> List(o.upvote, o.downvote),
    stars1to5 -> List(o.star1, o.star2, o.star3, o.star4, o.star5),
    numbers1to5 -> List(o.number1, o.number2, o.number3, o.number4, o.number5),
    happyIndifferentAmusedExcitedAngry -> List(o.happy, o.excited, o.amused, o.indifferent, o.angry),
    loveItMehNotForMe -> List(o.loveIt, o.meh, o.notForMe),
    satisfactionScale -> List(o.completelyUnsatisfied, o.unsatisfied, o.neutral, o.satisfied, o.completelySatisfied),
    interestScale -> List(o.veryUninteresting, o.uninteresting, o.neutral, o.interesting, o.veryInteresting)
  )

  implicit class UserFeedbackVariationType(val t: Type) {
    val displayName = t match {
      case `numbers1to5` => "Numbers 1 to 5"
      case `stars1to5` => "Stars 1 to 5"
      case _ => grvstrings.camelCaseToTitleCase(t.name)
    }
  }

  object UserFeedbackVariationType {
    implicit val richTypeJsonWrites = Writes[UserFeedbackVariationType](richType => Json.obj(
      "id" -> richType.t.id,
      "name" -> richType.t.name,
      "displayName" -> richType.displayName
    ))
  }

  implicit val standardEnumJsonFormat = makeJsonFormat[Type]

  implicit val defaultValueWriter: DefaultValueWriter[Type] = makeDefaultValueWriter[Type]

  implicit val UserFeedbackVariationTypeEqual: Equal[Type] = Equal.equalBy(t => t.id)
}
