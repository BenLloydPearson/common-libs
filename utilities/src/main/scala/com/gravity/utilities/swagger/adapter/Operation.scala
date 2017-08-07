package com.gravity.utilities.swagger.adapter

import com.gravity.utilities.swagger.adapter.Responses.ResponseStatusOrDefault
import play.api.libs.json._
import play.api.libs.functional.syntax._

/*
              ___...---''
  ___...---'\'___
''       _.-''  _`'.______\\.
        /_.)  )..-  __..--'\\
       (    __..--''
        '-''\@


 Ⓐ Ⓐ Ⓐ Ⓐ Ⓐ Ⓐ Ⓐ Ⓐ Ⓐ Ⓐ Ⓐ
*/

/** @see http://swagger.io/specification/#operationObject */
case class Operation(
  tags: Seq[String] = Seq.empty,
  summary: Option[String] = None,
  description: Option[String] = None,
  externalDocs: Option[ExternalDocumentation] = None,
  operationId: Option[String] = None,
  consumes: Seq[String] = Seq.empty,
  produces: Seq[String] = Seq.empty,
  parameters: Seq[Parameter] = Seq.empty,
  responses: Map[ResponseStatusOrDefault, Response] = Map.empty,
  schemes: Seq[String] = Seq.empty,
  deprecated: Boolean = false
)

object Operation {
  implicit val jsonFormat: Format[Operation] = (
    (__ \ "tags").format[Seq[String]] and
    (__ \ "summary").formatNullable[String] and
    (__ \ "description").formatNullable[String] and
    (__ \ "externalDocs").formatNullable[ExternalDocumentation] and
    (__ \ "operationId").formatNullable[String] and
    (__ \ "consumes").format[Seq[String]] and
    (__ \ "produces").format[Seq[String]] and
    (__ \ "parameters").format[Seq[Parameter]] and
    (__ \ "responses").format[Map[String, Response]] and
    (__ \ "schemes").format[Seq[String]] and
    (__ \ "deprecated").format[Boolean]
  )(Operation.apply, unlift(Operation.unapply))
}