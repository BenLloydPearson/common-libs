package com.gravity.interests.jobs.intelligence.operations.recommendations

import com.gravity.utilities.grvenum.GrvEnum

/**
 * Created by apatel on 6/15/15.
 */
object RecommendationReasons extends GrvEnum[Int] {
  case class Type(reasonId: Int, reasonStr: String) extends ValueTypeBase(reasonId, reasonStr)
  override def mkValue(id: Int, name: String): Type = Type(id, name)

  val None: Type = Value(0, "None")
  val Popular: Type = Value(1000, "Popular")
  val Contextual: Type = Value(2000, "Contextual")
  val Semantic: Type = Value(2500, "Semantic")
  val Personalized: Type = Value(3000, "Personalized")

  override def defaultValue: Type = None

  def greaterThan(reason1: String, reason2: String): Boolean = {
    val r1 = RecommendationReasons.getOrDefault(reason1)
    val r2 = RecommendationReasons.getOrDefault(reason2)
    r1.reasonId > r2.reasonId
  }

  implicit val jsonFormat = makeJsonFormat[Type]

  implicit class RichType(t: Type) {
    def cssClass: String = t match {
      case Popular => "grv_popular"
      case Contextual => "grv_contextual"
      case Semantic => "grv_semantic"
      case Personalized => "grv_personalized"
      case _ => "grv_none"
    }
  }
}