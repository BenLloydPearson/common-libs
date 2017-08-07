package com.gravity.data.configuration

import com.gravity.interests.jobs.intelligence.SitePlacementIdKey
import com.gravity.interests.jobs.intelligence.hbase.ScopedKey
import com.gravity.utilities.grvstrings.emptyString
import com.gravity.valueclasses.ValueClassesForDomain.SitePlacementId
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
case class WidgetHookRow(
  id: Long,
  scopedKey: ScopedKey,
  position: WidgetHookPosition.Type,
  html: String = emptyString,
  js: String = emptyString,
  htmlWrapperClass: String = emptyString
) {
  def withScopedEntity[E: Writes](scopedEntity: E): WidgetHookRowWithScopedEntity =
    new WidgetHookRowWithScopedEntity(id, scopedKey, Json.toJson(scopedEntity), position, html, js, htmlWrapperClass)
}

/** Enables inclusion in JSON of the entity identified by a widget hook's scopedKey. */
class WidgetHookRowWithScopedEntity(
  _id: Long,
  _scopedKey: ScopedKey,
  val scopedEntity: JsValue,
  _position: WidgetHookPosition.Type,
  _html: String = emptyString,
  _js: String = emptyString,
  _htmlWrapperClass: String = emptyString
)
extends WidgetHookRow(_id, _scopedKey, _position, _html, _js, _htmlWrapperClass)

object WidgetHookRow extends ((Long, ScopedKey, WidgetHookPosition.Type, String, String, String) => WidgetHookRow) {
  private val jsonWritesBase = Json.writes[WidgetHookRow]

  private val jsonWritesWithEntity = Writes[WidgetHookRowWithScopedEntity](rowWithEntity => {
    // IntelliJ identifies result of this particular writer as JsValue, but Scala compiler knows and reports the result
    // as being subtype JsObject; if you try to test against non-JsObject JsValue, you will get "unreachable code" warnings
    val obj = jsonWritesBase.writes(rowWithEntity).asInstanceOf[JsObject]
    obj + ("scopedEntity" -> Json.toJson(rowWithEntity.scopedEntity))
  })

  /** Capable of writing [[WidgetHookRowWithScopedEntity]], but only reads into [[WidgetHookRow]]. */
  implicit val jsonFormat: Format[WidgetHookRow] = Format[WidgetHookRow](
    Json.reads[WidgetHookRow],
    Writes[WidgetHookRow] {
      case row: WidgetHookRowWithScopedEntity => jsonWritesWithEntity.writes(row)
      case row: WidgetHookRow => jsonWritesBase.writes(row)
    }
  )
}

/** @param id Optional where None means "insert" and Some(id: Long) means "update". */
case class WidgetHookUpsert(
  id: Option[Long],
  scopedKey: ScopedKey,
  position: WidgetHookPosition.Type,
  html: String = emptyString,
  js: String = emptyString,
  htmlWrapperClass: String = emptyString
) {
  /** @return If TRUE, this upsert can be considered a "delete hook" request since an empty hook is equivalent to no hook. */
  def isEmpty: Boolean = html.isEmpty && js.isEmpty && htmlWrapperClass.isEmpty
  def nonEmpty: Boolean = !isEmpty

  lazy val dbOp: WidgetHookDbOp = id match {
    case None if isEmpty => WidgetHookNoOp
    case None if nonEmpty => WidgetHookInsert(scopedKey, position, html, js, htmlWrapperClass)
    case Some(_id) if isEmpty => WidgetHookDelete(_id)
    case Some(_id) if nonEmpty => WidgetHookUpdate(_id, html, js, htmlWrapperClass)
    case _ => throw new RuntimeException(s"Unable to determine DB operation intended for $this")
  }
}

object WidgetHookUpsert {
  implicit val jsonFormat = Json.format[WidgetHookUpsert]

  val example = WidgetHookUpsert(Some(1L), SitePlacementIdKey(SitePlacementId(2L)).toScopedKey,
    WidgetHookPosition.defaultValue, "<html></html>")
}

case class WidgetHookUpsertList(widgetHooks: List[WidgetHookUpsert])

object WidgetHookUpsertList {
  implicit val jsonFormat = Json.format[WidgetHookUpsertList]

  val example = WidgetHookUpsertList(List(WidgetHookUpsert.example,
    WidgetHookUpsert(Some(3L), SitePlacementIdKey(SitePlacementId(4L)).toScopedKey,
      WidgetHookPosition.footer, "<html></html>")))
}

trait WidgetHookDbOp {
  def actionDescriptor: String
  def baseJsObject: JsObject
  final def jsObject: JsObject = Json.obj("actionDescriptor" -> actionDescriptor) ++ baseJsObject
}
object WidgetHookDbOp {
  implicit val jsonWrites = Writes[WidgetHookDbOp](_.jsObject)
}

object WidgetHookNoOp extends WidgetHookDbOp {
  override def actionDescriptor: String = "noop"
  override def baseJsObject: JsObject = Json.obj()
}

case class WidgetHookDelete(id: Long) extends WidgetHookDbOp {
  override def actionDescriptor: String = "delete"
  override def baseJsObject: JsObject = Json.obj("id" -> id)
}

/** @param id Would be populated post-insert. */
case class WidgetHookInsert(
  scopedKey: ScopedKey,
  position: WidgetHookPosition.Type,
  html: String = emptyString,
  js: String = emptyString,
  htmlWrapperClass: String = emptyString,
  id: Option[Long] = None
) extends WidgetHookDbOp {
  def asRow: WidgetHookRow = WidgetHookRow(-1L, scopedKey, position, html, js, htmlWrapperClass)

  override def actionDescriptor: String = "insert"
  override def baseJsObject: JsObject = Json.obj(
    "id" -> id,
    "scopedKey" -> scopedKey,
    "position" -> position,
    "html" -> html,
    "js" -> js,
    "htmlWrapperClass" -> htmlWrapperClass
  )
}

case class WidgetHookUpdate(
  id: Long,
  html: String = emptyString,
  js: String = emptyString,
  htmlWrapperClass: String = emptyString
) extends WidgetHookDbOp {
  override def actionDescriptor: String = "update"
  override def baseJsObject: JsObject = Json.obj(
    "id" -> id,
    "html" -> html,
    "js" -> js,
    "htmlWrapperClass" -> htmlWrapperClass
  )
}