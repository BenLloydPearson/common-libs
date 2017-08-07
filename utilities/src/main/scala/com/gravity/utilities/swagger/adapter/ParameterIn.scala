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

/** @see http://swagger.io/specification/#parameterIn */
case class ParameterIn(value: String) extends AnyVal
object ParameterIn {
  val query: ParameterIn = ParameterIn("query")
  val header: ParameterIn = ParameterIn("header")
  val path: ParameterIn = ParameterIn("path")
  val formData: ParameterIn = ParameterIn("formData")
  val body: ParameterIn = ParameterIn("body")

  val values: Set[ParameterIn] = Set(query, header, path, formData, body)

  implicit val jsonFormat: Format[ParameterIn] = Format[ParameterIn](
    Reads[ParameterIn] {
      case JsString(strVal) =>
        values.find(_.value == strVal) match {
          case Some(parameterIn) => JsSuccess(parameterIn)
          case None => JsError("Invalid value for ParameterIn")
        }

      case _ => JsError()
    },
    Writes[ParameterIn](p => JsString(p.value))
  )
}