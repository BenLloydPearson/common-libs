package com.gravity.interests.jobs.intelligence

import com.gravity.hbase.schema.{DeserializedResult, _}
import com.gravity.interests.jobs.intelligence.SchemaContext._
import com.gravity.interests.jobs.intelligence.SchemaTypes._
import com.gravity.interests.jobs.intelligence.hbase._
import com.gravity.utilities.grvstrings.emptyString
import net.liftweb.json
import net.liftweb.json.JsonAST.JValue
import org.joda.time.DateTime
import play.api.libs.json._

import scala.collection._

/**
 * Created by IntelliJ IDEA.
 * Author: Robbie Coleman
 * Date: 3/20/13
 * Time: 2:28 PM
 */
class AuditTable2 extends HbaseTable[AuditTable2, AuditKey2, AuditRow2](tableName = "audit2", rowKeyClass = classOf[AuditKey2], tableConfig = defaultConf)
with ConnectionPoolingTableManager
{
  def rowBuilder(result: DeserializedResult): AuditRow2 = new AuditRow2(result, this)

  val meta = family[String, Any]("meta", compressed = true)
  val guid = column(meta, "g", classOf[String])
  val dateTime = column(meta, "dt", classOf[DateTime])
  val userId = column(meta, "uid", classOf[Long])
  val eventType = column(meta, "et", classOf[AuditEvents.Type])
  val fieldName = column(meta, "fn", classOf[String])
  val fromValue = column(meta, "fv", classOf[String])
  val toValue = column(meta, "tv", classOf[String])
  val status = column(meta, "s", classOf[AuditStatus.Type])
  val notes = column(meta, "nts", classOf[Seq[String]])

  // Some events, notably the DLUG/GMS events, 300-349, e.g. AuditEvents.{dlugUnitCreated, dlugUnitApproved, ...}
  // track actions that are limited to a single SiteGuid -- the same ArticleRow can have a different DLUG/GMS summary, etc.
  // for Site A than it does for Site B. So we track those events per-SiteGuid, so that later retrievals can be filtered.
  // DLUG events where this field is missing will be for Aol.com, and GMS events where this is missing will be for Aol.ca or TheOrbit.com,
  // because those sites were already using DLUG/GMS before we added this field to the column family.
  val forSiteGuid = column(meta, "sg", classOf[String])
}

class AuditRow2(result: DeserializedResult, table: AuditTable2) extends HRow[AuditTable2, AuditKey2](result, table) {
  val auditKey = rowid

  val guid = column(_.guid)
  val dateTime = column(_.dateTime)
  val userId = column(_.userId)
  val eventType = column(_.eventType)
  val fieldName = column(_.fieldName)
  val fromValue = column(_.fromValue)
  val toValue = column(_.toValue)
  val status = column(_.status)
  val notes = column(_.notes).getOrElse(Seq.empty)

  // See note above for AuditTable2.forSiteGuid.
  val forSiteGuid = column(_.forSiteGuid)

  def isOfEventType(eventTypeGetter: AuditEvents.type => AuditEvents.Type): Boolean = eventType.exists(_ == eventTypeGetter(AuditEvents))

  override def toString: String = {
    val sb = new StringBuilder
    val dq = '"'
    val ca = ','
    val cn = ':'
    sb.append('{')
    sb.append(dq).append("auditKey").append(dq).append(cn).append(auditKey)
    guid.foreach(g => sb.append(ca).append(dq).append("guid").append(dq).append(cn).append(dq).append(g).append(dq))
    dateTime.foreach(dt => sb.append(ca).append(dq).append("dateTime").append(dq).append(cn).append(dq).append(dt.toString("yyyy/MM/dd 'at' hh:mm:ss a")).append(dq))
    userId.foreach(id => sb.append(ca).append(dq).append("userId").append(dq).append(cn).append(id))
    eventType.foreach(et => sb.append(ca).append(dq).append("eventType").append(dq).append(cn).append(dq).append(et).append(dq))
    fieldName.foreach(fn => sb.append(ca).append(dq).append("fieldName").append(dq).append(cn).append(dq).append(fn).append(dq))
    fromValue.foreach(f => sb.append(ca).append(dq).append("fromValue").append(dq).append(cn).append(f))
    toValue.foreach(t => sb.append(ca).append(dq).append("toValue").append(dq).append(cn).append(t))
    forSiteGuid.foreach(fsg => sb.append(ca).append(dq).append("forSiteGuid").append(dq).append(cn).append(fsg))
    status.foreach(st => sb.append(ca).append(dq).append("status").append(dq).append(cn).append(dq).append(st).append(dq))
    sb.append(ca).append(dq).append("notes").append(dq).append(cn).append(notes.mkString("[\"", "\",\"", "\"]"))
    sb.append('}')

    sb.toString()
  }
}

/**
 * @param eventType    One of [[AuditEvents.Type]].
 * @param articleField One of [[DlArticleAuditEventFields.Type]]. In the future perhaps there will be mappings for
 *                     non-DL article fields as well.
 */
case class ArticleAuditRowPayload(auditRowGuid: String, articleId: String, dateTime: Long, userId: Option[Long],
                                  eventType: String, articleField: Option[String], fromValue: Option[JValue], toValue: Option[JValue],
                                  forSiteGuid: Option[String])
object ArticleAuditRowPayload {
  def apply(row: AuditRow2): ArticleAuditRowPayload = ArticleAuditRowPayload(
    row.guid.getOrElse(emptyString),
    row.auditKey.scopedKey.typedKey[ArticleKey].articleId.toString,
    row.dateTime.fold(0l)(_.getMillis),
    row.userId,
    row.eventType.getOrElse(AuditEvents.unknown).toString,
    row.eventType.flatMap(AuditEvents.dlugEventToDlArticleField.get).map(_.toString),
    row.fromValue.flatMap(json.parseOpt),
    row.toValue.flatMap(json.parseOpt),
    row.forSiteGuid
  )
}

case class AuditEventPayload(
  entityType: String,
  key: ScopedKey,
  guid: String,
  dateTime: DateTime,
  userId: Long,
  eventType: AuditEvents.Type,
  fieldName: Option[String],
  fromValue: Option[String],
  toValue: Option[String],
  status: AuditStatus.Type,
  notes: Seq[String]
)

object AuditEventPayload {
  implicit val jsonFormat = Json.format[AuditEventPayload]

  def fromRow(row: AuditRow2): AuditEventPayload = {
    AuditEventPayload(
      row.rowid.scopedKey.scope.name,
      row.rowid.scopedKey,
      row.guid.getOrElse(""),
      row.rowid.dateTime,
      row.rowid.userId,
      row.eventType.getOrElse(AuditEvents.unknown),
      row.fieldName,
      row.fromValue,
      row.toValue,
      row.status.getOrElse(AuditStatus.notSet),
      row.notes
    )
  }
}