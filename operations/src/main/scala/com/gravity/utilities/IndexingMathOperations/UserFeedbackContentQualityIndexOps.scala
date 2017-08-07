package com.gravity.utilities.IndexingMathOperations

import org.apache.commons.math3.analysis.UnivariateFunction
import org.apache.commons.math3.analysis.integration.IterativeLegendreGaussIntegrator
import org.apache.commons.math3.exception.NumberIsTooLargeException
import org.apache.commons.math3.special.Beta.logBeta
import org.apache.commons.math3.special.Gamma.logGamma

/**
  * Created by akash on 8/15/16.
  */


class UserFeedbackContentQualityIndexOps(maxEval: Int = 8000000, accuracySettings: AbsoluteRelativeAccuracySetting = AbsoluteRelativeAccuracySetting.BALANCED_SETTINGS) {
  import com.gravity.utilities.Counters._

  def reweightScore(vectorQuantile: Seq[Double]) = {
    val max = vectorQuantile.max
    val sum = vectorQuantile.sum
    vectorQuantile.map(score => max * score / sum)
  }

  // qbetabinom
  // from congifuration
  //    p: per trial probability
  //    theta: rate of over dispersion
  // from metrics
  //    k: number of successes (clicks for a given option)
  //    n: number of trials (total clicks)
  //    k <= n

  def quantileOfBetaBinomial(k: Double, n: Double, p: Double, theta: Double): Double ={
    integrate(k, n, p, theta)

  }

  private def integrate(k: Double, n: Double, p: Double, theta: Double): Double = {
    val gauss = new IterativeLegendreGaussIntegrator(1, accuracySettings.relative,  accuracySettings.absolute)
    val g = new dbetabinom(n, p, theta)
    val d =
      try {
        gauss.integrate(maxEval, g.asInstanceOf[UnivariateFunction], 0, k)
      }
      catch {
        case ntlEx: NumberIsTooLargeException =>
          countPerSecond("CQI Index Ops", "NumberIsTooLargeException")
          0.0
//        case ex1: TooManyEvaluationsException =>
//          error("error in solving quantileOfBetaBinomial of k,m,p,theta: " + Seq(k,n,p,theta), ex1)
//          0.0
//        case ex2: MaxCountExceededException  =>
//          error("error in solving quantileOfBetaBinomial of k,m,p,theta: " + Seq(k,n,p,theta), ex2)
//          0.0
      }
    d
  }

}


class dbetabinom(n: Double, p: Double, theta: Double) extends UnivariateFunction{
  override def value(k: Double): Double = {
    distributionBetaBinomial(k, n, p, theta)
  }

  // dbetabinom
  def distributionBetaBinomial(k: Double, n: Double, p: Double, theta: Double): Double = {
    math.exp(logDistibutionFunctForBetaBinomial(k, n, p, theta))
  }

  // dbetabinom.log
  def logDistibutionFunctForBetaBinomial(k: Double, n: Double, p: Double, theta: Double) : Double = {

//    println(".")

    val x1 = k + p * theta
    val y1 = n - k + ( 1.0d - p ) * theta

    val x2 = p * theta
    val y2 = ( 1.0d - p ) * theta

    val t_log = logBeta( x1, y1 ) - logBeta( x2, y2 )
    val cf_log = logGamma( n + 1) - logGamma( k + 1 ) - logGamma( n - k + 1 )

    val d_log = t_log + cf_log
    d_log
  }

}
