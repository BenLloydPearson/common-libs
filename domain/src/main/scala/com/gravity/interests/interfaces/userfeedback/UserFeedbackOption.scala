package com.gravity.interests.interfaces.userfeedback

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

object UserFeedbackOption extends GrvEnum[Int] {
  case class Type(i: Int, n: String) extends ValueTypeBase(i, n)
  override def mkValue(id: Int, name: String): Type = Type(id, name)

  val none = Value(0, "none")

  val thumbUp = Value(1, "thumbUp")
  val thumbDown = Value(2, "thumbDown")
  val upvote = Value(3, "upvote")
  val downvote = Value(4, "downvote")
  val star1 = Value(5, "star1")
  val star2 = Value(6, "star2")
  val star3 = Value(7, "star3")
  val star4 = Value(8, "star4")
  val star5 = Value(9, "star5")
  val number1 = Value(10, "number1")
  val number2 = Value(11, "number2")
  val number3 = Value(12, "number3")
  val number4 = Value(13, "number4")
  val number5 = Value(14, "number5")
  val happy = Value(15, "happy")
  val indifferent = Value(16, "indifferent")
  val amused = Value(17, "amused")
  val excited = Value(18, "excited")
  val angry = Value(19, "angry")
  val loveIt = Value(20, "loveIt")
  val meh = Value(21, "meh")
  val notForMe = Value(22, "notForMe")
  val completelyUnsatisfied = Value(23, "completelyUnsatisfied")
  val unsatisfied = Value(24, "unsatisfied")
  val neutral = Value(25, "neutral")
  val satisfied = Value(26, "satisfied")
  val completelySatisfied = Value(27, "completelySatisfied")
  val veryUninteresting = Value(28, "veryUninteresting")
  val uninteresting = Value(29, "uninteresting")
  val interesting = Value(31, "interesting")
  val veryInteresting = Value(32, "veryInteresting")

  override def defaultValue: Type = none

  implicit val standardEnumJsonFormat = makeJsonFormat[Type]

  implicit val defaultValueWriter: DefaultValueWriter[Type] = makeDefaultValueWriter[Type]
}
