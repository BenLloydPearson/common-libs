package com.gravity.domain

import scala.util.matching.Regex
import scalaz.syntax.std.string._
import scalaz.syntax.validation._
import scalaz.{Failure, Success, Validation}

case class CssDimension private(value: Either[Int, Double]) {
  override def toString: String = value match {
    case Left(pixels) => pixels + "px"
    case Right(percent) => percent + "%"
  }
}

object CssDimension {

  private val CssDimensionRegex = new Regex("""^([\d.-]+)(px|%)?$""", "number", "optionalUnit")

  def apply(pixels: Int): Option[CssDimension] = if (pixels >= 0) Some(new CssDimension(Left(pixels))) else None

  def apply(percent: Double): Option[CssDimension] = if (percent >= 0) Some(new CssDimension(Right(percent))) else None

  def apply(cssDimensionString: String): Validation[IllegalArgumentException, Option[CssDimension]] = {
    val parsedV = cssDimensionString match {
      case mt if mt.isEmpty => None.success
      case CssDimensionRegex(unitlessPixels, null) => unitlessPixels.parseInt map (CssDimension(_))
      case CssDimensionRegex(pixels, "px") => pixels.parseInt map (CssDimension(_))
      case CssDimensionRegex(percent, "%") => percent.parseDouble map (CssDimension(_))
      case _ => new MatchError(cssDimensionString).failure
    }

    parsedV match {
      case Success(cssDimension) => Success(cssDimension)
      case Failure(err) =>
        val ex = new IllegalArgumentException(s"Invalid CSS dimension string: '$cssDimensionString'", err)
        ex.failure
    }
  }

  def parseOption(cssDimensionString: String): Option[CssDimension] = apply(cssDimensionString).toOption.flatten

}