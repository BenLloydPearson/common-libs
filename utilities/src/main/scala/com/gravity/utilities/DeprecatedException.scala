package com.gravity.utilities

/*
 *    __   _         __         
 *   / /  (_)__  ___/ /__  ____
 *  / _ \/ / _ \/ _  / _ \/ _  /
 * /_//_/_/_//_/\_,_/\___/\_, / 
 *                       /___/
 */

class DeprecatedException(name: String, reason: String) extends Exception("'" + name + "' has been deprecated due to: " + reason) {
  import com.gravity.utilities.Counters._
  val counterCategory: String = "Deprecations"
  countPerSecond(counterCategory, name)
}

class NotContentGroupAwareException(name: String) extends DeprecatedException(name, "Not content-group aware")

