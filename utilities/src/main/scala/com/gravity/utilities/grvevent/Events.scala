package com.gravity.utilities.grvevent

import scala.concurrent.duration.Duration

/*
 *    __   _         __         
 *   / /  (_)__  ___/ /__  ____
 *  / _ \/ / _ \/ _  / _ \/ _  /
 * /_//_/_/_//_/\_,_/\___/\_, / 
 *                       /___/
 */

case class Timing(start: Long, end: Long) {
  def duration: Duration = Duration(end - start, "ms")
}
