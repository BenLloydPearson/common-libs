package com.gravity.utilities.swagger.adapter

import play.api.libs.json.{Format, Json}

/*
              ___...---''
  ___...---'\'___
''       _.-''  _`'.______\\.
        /_.)  )..-  __..--'\\
       (    __..--''
        '-''\@


 Ⓐ Ⓐ Ⓐ Ⓐ Ⓐ Ⓐ Ⓐ Ⓐ Ⓐ Ⓐ Ⓐ
*/

/** @see http://swagger.io/specification/#pathItemObject */
case class PathItem(
  $ref: Option[String] = None,
  get: Option[Operation] = None,
  put: Option[Operation] = None,
  post: Option[Operation] = None,
  delete: Option[Operation] = None,
  options: Option[Operation] = None,
  head: Option[Operation] = None,
  patch: Option[Operation] = None,
  parameters: Option[Parameter] = None
) {
  /** Combines path items in a non-associative way. Operations in the righthand path item take precedence. */
  def ++(otherPathItem: PathItem): PathItem = otherPathItem.copy(
    $ref = otherPathItem.$ref.orElse($ref),
    get = otherPathItem.get.orElse(get),
    put = otherPathItem.put.orElse(put),
    post = otherPathItem.post.orElse(post),
    delete = otherPathItem.delete.orElse(delete),
    options = otherPathItem.options.orElse(options),
    head = otherPathItem.head.orElse(head),
    patch = otherPathItem.patch.orElse(patch),
    parameters = otherPathItem.parameters.orElse(parameters)
  )
}

object PathItem {
  val empty: PathItem = PathItem()

  implicit val jsonFormat: Format[PathItem] = Json.format[PathItem]
}