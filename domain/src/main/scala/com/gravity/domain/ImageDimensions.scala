package com.gravity.domain

import com.gravity.utilities.grvstrings._

/**
 * Created by tdecamp on 10/27/15.
 * {{insert neat ascii diagram here}}
 */
case class ImageDimensions(width: Int, height: Int) {
  def toFieldString: String = width + "|" + height
}

object ImageDimensions {
  def fromFieldString(s: String): Option[ImageDimensions] = {
    val Array(width, height) = s.split("\\|")
    for {
      w <- width.tryToInt
      h <- height.tryToInt
    } yield ImageDimensions(w, h)
  }
}
