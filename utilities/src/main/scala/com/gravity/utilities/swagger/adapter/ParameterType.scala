package com.gravity.utilities.swagger.adapter

import play.api.libs.json._

/*
              ___...---''
  ___...---'\'___
''       _.-''  _`'.______\\.
        /_.)  )..-  __..--'\\
       (    __..--''
        '-''\@


 Ⓐ Ⓐ Ⓐ Ⓐ Ⓐ Ⓐ Ⓐ Ⓐ Ⓐ Ⓐ Ⓐ
*/

/** @see http://swagger.io/specification/#parameterType */
case class ParameterType(value: String) extends AnyVal
object ParameterType {
  val string: ParameterType = ParameterType("string")
  val number: ParameterType = ParameterType("number")
  val integer: ParameterType = ParameterType("integer")
  val boolean: ParameterType = ParameterType("boolean")
  val array: ParameterType = ParameterType("array")
  val file: ParameterType = ParameterType("file")

  val values: Set[ParameterType] = Set(string, number, integer, boolean, array, file)

  implicit val jsonFormat: Format[ParameterType] = Format[ParameterType](
    Reads[ParameterType] {
      case JsString(strVal) =>
        values.find(_.value == strVal) match {
          case Some(parameterType) => JsSuccess(parameterType)
          case None => JsError("Invalid value for ParameterType")
        }

      case _ => JsError()
    },
    Writes[ParameterType](p => JsString(p.value))
  )
}