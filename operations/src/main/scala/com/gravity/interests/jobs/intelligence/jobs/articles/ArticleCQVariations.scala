package com.gravity.interests.jobs.intelligence.jobs.articles

import com.gravity.interests.jobs.intelligence.UserFeedbackKey
import com.gravity.utilities.IndexingMathOperations.CQIOps
import com.gravity.utilities.cache.user.UserFeedbackContentQualityCache
import com.gravity.utilities.grvmath

/**
  * Created by akash on 8/24/16.
  */
case class ArticleCQVariations(lifetimeClicks: scala.collection.Map[UserFeedbackKey, Double]){

  val validVariationOptions = UserFeedbackContentQualityCache.validKeys

  val validLifetimeClicks = lifetimeClicks.filter{case (key, score) =>  validVariationOptions.contains(UserFeedbackContentQualityCache.getKey(key.feedbackVariation, key.feedbackOption) ) }

  lazy val variationGroups = validLifetimeClicks.toSeq.groupBy(_._1.feedbackVariation)

  lazy val variantToNMap = variationGroups.map{
    case (variant: Int, clicks: Seq[(UserFeedbackKey, Double)] ) => {
//      val n = clicks.filter{case (key, _) => key.feedbackOption != 0 && key.feedbackVariation !=0}.map(_._2).sum
      val n = clicks.map(_._2).sum
      (variant -> n)
    }}

  lazy val totalN = variantToNMap.map(_._2).sum

  lazy val cqiParams: Seq[CQIParams] = {

    val cqiParamsVector =
      validLifetimeClicks.map{
        case (key: UserFeedbackKey, k: Double) =>
          new CQIParams(key.feedbackVariation, key.feedbackOption, k, variantToNMap(key.feedbackVariation))
      }

    cqiParamsVector.toSeq

  }

  lazy val zVByVariation = {
    val cqiParamsByVariation = cqiParams.groupBy(_.variant)

    val zVByVariation =
      cqiParamsByVariation.map{
        case (variant: Int, cqiParamVector: Seq[CQIParams]) => {
          val quantileV = cqiParamVector.map(_.quantile)
          val normalizedQuantileV = CQIOps.userFeedbackOps.reweightScore(quantileV)

          val calibrationNumericV = cqiParamVector.map(_.calibrationNumeric)
          val calibrationPositivityV = cqiParamVector.map(_.calibrationPositivity)
          val calibrationNegativityV = cqiParamVector.map(_.calibrationNegativity)

          val numericScore = grvmath.dotProduct(normalizedQuantileV, calibrationNumericV)
          val positivityScore = grvmath.dotProduct(normalizedQuantileV, calibrationPositivityV)
          val negativityScore = grvmath.dotProduct(normalizedQuantileV, calibrationNegativityV)

          val weight = variantToNMap(variant) / totalN

          (variant -> ZVector(numericScore, positivityScore, negativityScore, weight))
        }
      }

    zVByVariation

  }

  lazy val zByArticle = {

    val weightV = zVByVariation.map(_._2.weight).toSeq

    val numericV = zVByVariation.map(_._2.numeric).toSeq
    val positiveV = zVByVariation.map(_._2.positive).toSeq
    val negativeV = zVByVariation.map(_._2.negative).toSeq

    val zNum = grvmath.dotProduct(weightV, numericV)
    val zPos = grvmath.dotProduct(weightV, positiveV)
    val zNeg = grvmath.dotProduct(weightV, negativeV)

    ZVector(zNum, zPos, zNeg, 0.0d)
  }


  def fixInRange(score: Double, min: Double = 0.0d, max : Double = 1.0d) = {
    score.max(min).min(max)
  }

  // final scores!
  lazy val numericScore = fixInRange(zByArticle.numeric)
  lazy val positiveScore = fixInRange(zByArticle.positive)
  lazy val negativeScore = fixInRange(zByArticle.negative)

}
