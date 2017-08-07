package com.gravity.utilities.swagger.adapter

import com.gravity.utilities.swagger.adapter.ParametersDefinitions.ParameterName
import com.gravity.utilities.swagger.adapter.Paths.PathName
import com.gravity.utilities.swagger.adapter.Response.{MimeType, ResponseName}
import com.gravity.utilities.swagger.adapter.Schema.SchemaName
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

/** @see http://swagger.io/specification/#swaggerObject */
case class Swagger(
  swagger: String = "2.0",
  info: Info,
  host: Option[String] = None,
  basePath: Option[String] = None,
  schemes: Seq[String] = Seq.empty,
  consumes: Seq[MimeType] = Seq.empty,
  produces: Seq[MimeType] = Seq.empty,
  paths: Map[PathName, PathItem],
  definitions: Map[SchemaName, Schema] = Map.empty,
  parameters: Map[ParameterName, Parameter] = Map.empty,
  responses: Map[ResponseName, Response] = Map.empty,
  tags: Seq[Tag] = Seq.empty,
  externalDocs: Option[ExternalDocumentation] = None) {
}

object Swagger {
  private def orderPreservingMapWriter[T: Writes]: Writes[Map[String, T]] = Writes[Map[String, T]](map => {
    JsObject(map.mapValues(Json.toJson(_)).toSeq)
  })

  implicit val jsonFormat: Format[Swagger] = (
    (__ \ "swagger").format[String] and
    (__ \ "info").format[Info] and
    (__ \ "host").formatNullable[String] and
    (__ \ "basePath").formatNullable[String] and
    (__ \ "schemes").format[Seq[String]] and
    (__ \ "consumes").format[Seq[String]] and
    (__ \ "produces").format[Seq[String]] and
    (__ \ "paths").format[Map[String, PathItem]](orderPreservingMapWriter[PathItem]) and
    (__ \ "definitions").format[Map[String, Schema]] and
    (__ \ "parameters").format[Map[String, Parameter]] and
    (__ \ "responses").format[Map[String, Response]] and
    (__ \ "tags").format[Seq[Tag]] and
    (__ \ "externalDocs").formatNullable[ExternalDocumentation]
  )(Swagger.apply, unlift(Swagger.unapply))
}