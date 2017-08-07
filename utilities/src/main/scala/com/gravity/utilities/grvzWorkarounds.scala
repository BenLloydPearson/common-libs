package com.gravity.utilities

import com.gravity.utilities.components.FailureResult

import scalaz.ValidationNel
import scalaz.syntax.apply._

/*             )\._.,--....,'``.
.b--.        /;   _.. \   _\  (`._ ,.
`=,-,-'~~~   `----(,_..'--(,_..'`-.;.'  */

/**
 * Obsolete: THIS FILE CONTAINS METHODS THAT CURRENTLY CAUSE INTELLIJ'S SCALA PLUGIN TO CRASH, DON'T EXTEND
 *
 * Just checked this after the scala release of May 18, and it works fine!
 */
object grvzWorkarounds {

  def validateAndReturnFailuresOrTrue3(nt: ValidationNel[FailureResult, Boolean], wf: ValidationNel[FailureResult, Boolean], nr: ValidationNel[FailureResult, Boolean]): ValidationNel[FailureResult, Boolean] = {
    (nt |@| wf |@| nr) {
      (_, _, _) => true
    }
  }

  def validateAndReturnFailuresOrTrue4(nt: ValidationNel[FailureResult, Boolean], wf: ValidationNel[FailureResult, Boolean], nr: ValidationNel[FailureResult, Boolean], np: ValidationNel[FailureResult, Boolean]): ValidationNel[FailureResult, Boolean] = {
    (nt |@| wf |@| nr |@| np) {
      (_, _, _, _) => true
    }
  }

  def validateAndReturnFailuresOrTrue5(nt: ValidationNel[FailureResult, Boolean], wf: ValidationNel[FailureResult, Boolean], nr: ValidationNel[FailureResult, Boolean], np: ValidationNel[FailureResult, Boolean], vi: ValidationNel[FailureResult, Boolean]): ValidationNel[FailureResult, Boolean] = {
    (nt |@| wf |@| nr |@| np |@| vi) {
      (_, _, _, _, _) => true
    }
  }
}
