package com.gravity.valueclasses

import com.gravity.valueclasses.ValueClassesForDomain.ImpressionViewedHashString

/**
 * Created by runger on 8/14/14.
 */
object ValueClassesForOperations {

  case class Scope(raw: String)

  implicit class StringToOperationsValueClasses(val underlying: String) extends AnyVal {
    def asScope = Scope(underlying)
  }

}
