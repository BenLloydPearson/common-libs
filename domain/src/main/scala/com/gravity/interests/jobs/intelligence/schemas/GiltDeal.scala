package com.gravity.interests.jobs.intelligence.schemas

import com.gravity.utilities.grvjson
import org.joda.time.DateTime
import play.api.libs.json._

case class GiltDeal(id: String, expireTime: DateTime, price: Double, value: Double, soldOut: String)

object GiltDeal {
  import play.api.libs.functional.syntax._
  implicit val jsonFormat: OFormat[GiltDeal] = (
    (__ \ "id").format[String] and
    (__ \ "expireTime").format[DateTime](grvjson.shortDateTimeFormat) and
    (__ \ "price").format[Double](grvjson.liftwebFriendlyPlayJsonDoubleFormat) and
    (__ \ "value").format[Double](grvjson.liftwebFriendlyPlayJsonDoubleFormat) and
    (__ \ "soldOut").format[String]
  )(GiltDeal.apply, unlift(GiltDeal.unapply))
}
