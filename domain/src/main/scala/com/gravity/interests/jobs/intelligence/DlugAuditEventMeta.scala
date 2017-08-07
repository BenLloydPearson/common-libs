package com.gravity.interests.jobs.intelligence

import com.gravity.utilities.grvenum.GrvEnum
import com.gravity.utilities.grvjson.EnumNameSerializer

trait DlugAuditEventMeta {
  this: AuditEvents.type =>

  /** Helps deliver pivot to the front end indicating what DLUG field a given audit log event type corresponds to. */
  lazy val dlugEventToDlArticleField: Map[Type, DlArticleAuditEventFields.Type] = Map(
    dlugUnitApproved -> DlArticleAuditEventFields.dlStatus,
    dlugUnitRejected -> DlArticleAuditEventFields.dlStatus,
    dlugUnitRemoved -> DlArticleAuditEventFields.dlStatus,
    dlugImageRecropped -> DlArticleAuditEventFields.dlImage,
    dlugPlanNameChanged -> DlArticleAuditEventFields.dlPlanName,
    dlugProductIdChanged -> DlArticleAuditEventFields.dlProductId,
    dlugPublishDateChanged -> DlArticleAuditEventFields.dlPublishDate,
    dlugTtlChanged -> DlArticleAuditEventFields.dlTtl,
    dlugRibbonChanged -> DlArticleAuditEventFields.dlRibbonImage,
    dlugCategoryNameChanged -> DlArticleAuditEventFields.dlCategoryName,
    dlugCategoryUrlChanged -> DlArticleAuditEventFields.dlCategoryUrl,
    dlugSourceNameChanged -> DlArticleAuditEventFields.dlSourceName,
    dlugSourceUrlChanged -> DlArticleAuditEventFields.dlSourceUrl,
    dlugIsVideoChanged -> DlArticleAuditEventFields.dlIsVideo,
    dlugTitleChanged -> DlArticleAuditEventFields.dlTitle,
    dlugArticleUrlChanged -> DlArticleAuditEventFields.dlArticleUrl,
    dlugSummaryChanged -> DlArticleAuditEventFields.dlSummary,
    dlugSecondaryTitleChanged -> DlArticleAuditEventFields.dlSecondaryTitle,
    dlugMoreLinksHeaderChanged -> DlArticleAuditEventFields.dlMoreLinksHeader,
    dlugMoreLinksAdded -> DlArticleAuditEventFields.dlMoreLinksHeader,
    dlugMoreLinksRemoved -> DlArticleAuditEventFields.dlMoreLinksHeader,
    dlugChannelImageRecropped -> DlArticleAuditEventFields.dlChannelImage,
    dlugChannelAdded -> DlArticleAuditEventFields.dlChannels,
    dlugChannelRemoved -> DlArticleAuditEventFields.dlChannels,
    dlugContentGroupAdded -> DlArticleAuditEventFields.contentGroupIds,
    dlugContentGroupRemoved -> DlArticleAuditEventFields.contentGroupIds,
    dlugAltTitleChanged -> DlArticleAuditEventFields.altTitle
  )
}

/** DL article fields whose changes are tracked in audit events. These are used for pivoting GUIs on the front end. */
object DlArticleAuditEventFields extends GrvEnum[Short] {
  case class Type(i: Short, n: String) extends ValueTypeBase(i, n)
  override def mkValue(id: Short, name: String): Type = Type(id, name)

  val unknown: Type = Value(0, "unknown")
  val dlStatus: Type = Value(1, "dlStatus")
  val dlImage: Type = Value(2, "dlImage")
  val dlPlanName: Type = Value(3, "dlPlanName")
  val dlProductId: Type = Value(4, "dlProductId")
  val dlPublishDate: Type = Value(5, "dlPublishDate")
  val dlTtl: Type = Value(6, "dlTtl")
  val dlRibbonImage: Type = Value(7, "dlRibbonImage")
  val dlCategoryName: Type = Value(8, "dlCategoryName")
  val dlCategoryUrl: Type = Value(9, "dlCategoryUrl")
  val dlSourceName: Type = Value(10, "dlSourceName")
  val dlSourceUrl: Type = Value(11, "dlSourceUrl")
  val dlIsVideo: Type = Value(12, "dlIsVideo")
  val dlTitle: Type = Value(13, "dlTitle")
  val dlArticleUrl: Type = Value(14, "dlArticleUrl")
  val dlSummary: Type = Value(15, "dlSummary")
  val dlSecondaryTitle: Type = Value(16, "dlSecondaryTitle")
  val dlMoreLinksHeader: Type = Value(17, "dlMoreLinksHeader")
  val dlChannelImage: Type = Value(18, "dlChannelImage")
  val dlChannels: Type = Value(19, "dlChannels")
  val contentGroupIds: Type = Value(20, "contentGroupIds")
  val altTitle: Type = Value(21, "altTitle")
  val altImage: Type = Value(22, "altImage")
  val altImageSource: Type = Value(23, "altImageSource")

  def defaultValue: Type = unknown

  val jsonSerializer: EnumNameSerializer[DlArticleAuditEventFields.type] = new EnumNameSerializer(this)
}