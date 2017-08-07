package com.gravity.algorithms.model

import com.gravity.utilities.web.CategorizedFailureResult

import scalaz.Scalaz._
import scalaz._

trait AlgoSettingsValidations {
  def validateMaxDaysOld(maxDaysOld: Option[Int]): ValidationNel[CategorizedFailureResult, Option[Int]] = maxDaysOld match {
    case Some(days) =>
      if(days < maxDaysOldMin)
        new CategorizedFailureResult(_.BeyondMin, s"Max days old must be at least $maxDaysOldMin.").failureNel
      else if(days > maxDaysOldMax)
        new CategorizedFailureResult(_.BeyondMax, s"Max days old must be no greater than $maxDaysOldMax.").failureNel
      else
        days.some.successNel

    case _ => None.successNel
  }

  private val maxDaysOldMin = 1
  private val maxDaysOldMax = 1000
}
