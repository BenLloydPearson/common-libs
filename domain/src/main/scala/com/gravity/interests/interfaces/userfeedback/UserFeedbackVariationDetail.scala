package com.gravity.interests.interfaces.userfeedback

import play.api.libs.json.{JsArray, Json, Writes}

/*
              ___...---''
  ___...---'\'___
''       _.-''  _`'.______\\.
        /_.)  )..-  __..--'\\
       (    __..--''
        '-''\@


 Ⓐ Ⓐ Ⓐ Ⓐ Ⓐ Ⓐ Ⓐ Ⓐ Ⓐ Ⓐ Ⓐ
*/

case class UserFeedbackVariationDetail(variation: UserFeedbackVariation.Type, options: List[UserFeedbackOption.Type])

object UserFeedbackVariationDetailCompanion {
  val variationDetails = UserFeedbackVariation.variationToOptions.map(UserFeedbackVariationDetail.tupled).toSeq

  implicit val jsonWrites = Writes[UserFeedbackVariationDetail](ufvd => Json.obj(
    "variation" -> Json.obj("id" -> ufvd.variation.id, "name" -> ufvd.variation.name),
    "options" -> JsArray(
      ufvd.options.map(option =>
        Json.obj("id" -> option.id, "name" -> option.name)
      )
    )
  ))
}