package com.gravity.interests.jobs.intelligence.querybuilder

import play.api.libs.json._

import scala.collection._
import Operator._

/*
 *    __   _         __         
 *   / /  (_)__  ___/ /__  ____
 *  / _ \/ / _ \/ _  / _ \/ _  /
 * /_//_/_/_//_/\_,_/\___/\_, / 
 *                       /___/
 */

abstract class InputType[T0](val name: String) {
  type T = T0

  def configure(json: JsObject): JsObject = json
}

case class SelectInput[T0 : Format](values: Seq[T0], multiple: Boolean = false) extends InputType[T0]("select") {

  override def configure(json: JsObject): JsObject = json ++ Json.obj(
    "values" -> values,
    "multiple" -> true
  )
}

case class CheckboxInput[T0 : Format](values: Seq[T0]) extends InputType[T0]("checkbox") {
  override def configure(json: JsObject): JsObject = json ++ Json.obj(
    "values" -> values
  )

}

case class RadioInput[T0 : Format](values: Seq[T0]) extends InputType[T0]("radio") {
  override def configure(json: JsObject): JsObject = json ++ Json.obj(
    "values" -> values
  )
}

case class TextInput[T0]() extends InputType[T0]("text")

