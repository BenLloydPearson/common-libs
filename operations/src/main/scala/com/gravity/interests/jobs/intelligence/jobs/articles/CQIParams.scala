package com.gravity.interests.jobs.intelligence.jobs.articles

import com.gravity.utilities.IndexingMathOperations.CQIOps
import com.gravity.utilities.cache.user.UserFeedbackContentQualityCache

/**
  * Created by akash on 8/24/16.
  */
case class CQIParams(variant: Int, opt: Int, k: Double, n: Double){
  private lazy val pAndTheta =  UserFeedbackContentQualityCache.getNormalizationParams(variant, opt)
  private lazy val p = pAndTheta.map(_.p).getOrElse(0.0d)
  private lazy val theta = pAndTheta.map(_.theta).getOrElse(0.0d)

  lazy val quantile =  CQIOps.userFeedbackOps.quantileOfBetaBinomial(k, n, p, theta)


  private lazy val calibrationParams = UserFeedbackContentQualityCache.getCalibrationParams(variant, opt)
  lazy val calibrationNumeric = calibrationParams.map(_.numeric).getOrElse(0.0d)
  lazy val calibrationPositivity = calibrationParams.map(_.positivity).getOrElse(0.0d)
  lazy val calibrationNegativity = calibrationParams.map(_.negativity).getOrElse(0.0d)

  override def toString() = {

    val data = Seq("variant/opt", variant, opt, "k/n/p", k, n, p, "theta", theta, "qunatile", quantile,  "num/pos/neg",  calibrationNumeric, calibrationPositivity, calibrationNegativity)

    "CQIParams - " + data
  }

}


case class ZVector(numeric: Double, positive: Double, negative: Double, weight: Double)
