package com.gravity.interests.jobs.intelligence

/** Created by IntelliJ IDEA.
  * Author: Robbie Coleman
  * Date: 10/4/13
  * Time: 10:12 AM
  */
object RecommendationMetricCountBy {

  val articleImpression: Byte = 0 // CountByArticleImpression
  val click: Byte = 1 // CountByClick
  val unitImpression: Byte = 2 // CountByUnitImpression
  val articleView: Byte = 3 // = CountByArticleView
  val unitImpressionViewed: Byte = 4 // CountByUnitImpressionViewed
  val conversion: Byte = 5 // CountByConversion
  val costPerClick: Byte = 6 // costPerClick
  val unitImpressionFailed: Byte = 7 // unitImpressionFailed

  val defaultValue: Byte = articleImpression

  val minValue: Byte = defaultValue
  val maxValue: Byte = unitImpressionViewed

}
