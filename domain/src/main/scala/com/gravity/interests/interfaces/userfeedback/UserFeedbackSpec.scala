package com.gravity.interests.interfaces.userfeedback

/*
              ___...---''
  ___...---'\'___
''       _.-''  _`'.______\\.
        /_.)  )..-  __..--'\\
       (    __..--''
        '-''\@


 Ⓐ Ⓐ Ⓐ Ⓐ Ⓐ Ⓐ Ⓐ Ⓐ Ⓐ Ⓐ Ⓐ
*/

/**
  * Enum name vals maintained as Strings and not UserFeedbackVariation.Type to allow downstream systems to pass
  * through new user feedback variations without needing the most up-to-date code.
  */
case class UserFeedbackSpec(v: UserFeedbackVariation.Name, p: UserFeedbackPresentation.Name)

object UserFeedbackSpec {
  val none = UserFeedbackSpec(UserFeedbackVariation.none.name, UserFeedbackPresentation.none.name)

  def apply(vp: (UserFeedbackVariation.Type, UserFeedbackPresentation.Type)): UserFeedbackSpec =
    new UserFeedbackSpec(vp._1.name, vp._2.name)
}