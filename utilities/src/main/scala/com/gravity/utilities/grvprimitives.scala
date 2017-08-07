package com.gravity.utilities

import play.api.libs.json.{JsError, JsResult, JsSuccess}

import scalaz.syntax.validation._
import scalaz.{Validation, ValidationNel}

object grvprimitives {
  implicit class GrvBoolean(b: Boolean) {
    def asOpt[T](ifTrue: => T): Option[T] = if(b) Some(ifTrue) else None

    def asValidation[F](ifFail: => F): Validation[F, Boolean] = if(b) b.success else ifFail.failure

    def asValidationNel[F](ifFail: => F): ValidationNel[F, Boolean] = if(b) b.successNel else ifFail.failureNel

    def asJsResult[T](successVal: T, ifFail: => String): JsResult[T] = if(b) JsSuccess(successVal) else JsError(ifFail)
  }

  implicit class GrvInt(i: Int) {
    def isEven: Boolean = i % 2 == 0
    def isOdd: Boolean = i % 2 != 0
  }
}