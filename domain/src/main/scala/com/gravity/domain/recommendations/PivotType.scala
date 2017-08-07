package com.gravity.domain.recommendations


/**
 * Created by apatel on 8/6/15.
 */
object PivotType extends Enumeration {
  type PivotType = Value
  val default, contextual, behavioral, semantic = Value
}
