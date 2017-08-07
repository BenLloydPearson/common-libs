package com.gravity.utilities

import com.gravity.utilities.components.FailureResult

import scalaz.ValidationNel
import scalaz.syntax.validation._

object JsUtils {
  /** @return Success value is the input string. */
  def validateJavaScript(js: String): ValidationNel[FailureResult, String] = try {
    org.mozilla.javascript.Context.enter().compileString(js, "JsUtils_validateJavaScript.js", 0, null)
    js.successNel
  }
  catch {
    case ex: Exception =>
      FailureResult("validateJavaScript failure", ex).failureNel
  }

  def validateJavaScriptOrDie(js: String): Unit = {
    org.mozilla.javascript.Context.enter().compileString(js, "JsUtils_validateJavaScriptOrDie.js", 0, null)
  }
}