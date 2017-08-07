package com.gravity.domain.grvstringconverters

import com.gravity.utilities.components.FailureResult

/** Created by IntelliJ IDEA.
  * Author: Robbie Coleman
  * Date: 8/14/13
  * Time: 2:40 PM
  */
class StringConverterParsingException(val keyString: String, val failed: FailureResult) extends Exception("Failed to parse keyString: " + keyString + ". " + failed.toString)
