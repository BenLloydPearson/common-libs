package com.gravity.interests.jobs.intelligence

import com.gravity.hbase.schema._
import com.gravity.interests.jobs.intelligence.SchemaContext._
import com.gravity.interests.jobs.intelligence.SchemaTypes._
import com.gravity.interests.jobs.intelligence.hbase.ConnectionPoolingTableManager
import com.gravity.interests.jobs.intelligence.operations.HbRgTagRef
import org.joda.time.DateTime
import scalaz.syntax.std.option._

// The RowKey is a Relegence Story ID.
class StoryWorkTable extends HbaseTable[StoryWorkTable, Long, StoryWorkRow](
  tableName = "story-work", rowKeyClass=classOf[Long], logSchemaInconsistencies=false, tableConfig=defaultConf)
  with ConnectionPoolingTableManager
{
  override def rowBuilder(result: DeserializedResult) = new StoryWorkRow(result, this)

  val meta: Fam[String, Any] = family[String, Any]("meta", compressed = true)

  val isNewStory       = column(meta, "in", classOf[Boolean])

  val grvIngestionTime = column(meta, "gt", classOf[Long])
  val failStatusCode   = column(meta, "fsc", classOf[Int])
  val failErrMessage   = column(meta, "fem", classOf[String])
  val creationTime     = column(meta, "ct", classOf[Long])
  val lastUpdate       = column(meta, "lu", classOf[Long])
}

class StoryWorkRow(result: DeserializedResult, table: StoryWorkTable) extends HRow[StoryWorkTable, Long](result, table)
  with StoryAgeAndStalenessRow[StoryWorkTable, Long, StoryWorkRow]
{
  lazy val storyId: Long = rowid

  lazy val isNewStory: Boolean = column(_.isNewStory).getOrElse(false)

  //  creationTime: Long,                   // e.g. 1467319018548
  lazy val creationTimeOpt: Option[Long] = column(_.creationTime)

  //  lastUpdate: Long,                     // e.g. 1467319991370
  lazy val lastUpdate: Option[Long] = column(_.lastUpdate)

  //  grvIngestionTime: Long,               // e.g. 1467319991370
  lazy val grvIngestionTime: Option[Long] = column(_.grvIngestionTime)

  // e.g. None, Some(204)
  lazy val failStatusCode: Option[Int]    = column(_.failStatusCode)

  lazy val failErrMessage: Option[String] = column(_.failErrMessage)

  // For dumping values to CSV.
  lazy val fieldSeq = Seq(
    "storyId" -> storyId.some,

    "effStoryAgeHours" -> effStoryAgeHours.some,
    "infoStaleMinutes" -> infoStaleMinutes.some,
    "tooStaleMinutes" -> tooStaleMinutes.some,
    "isStoryActive" -> isStoryActive.some,
    "shouldRefreshStoryInfo" -> shouldRefreshStoryInfo.some,

    "isNewStory" -> isNewStory.some,
    "creationTimeOpt" -> creationTimeOpt,
    "lastUpdate" -> lastUpdate,
    "grvIngestionTime" -> grvIngestionTime,
    "failStatusCode" -> failStatusCode,
    "failErrMessage" -> failErrMessage
  )

  def fieldHeaders = fieldSeq.map(_._1)

  def fieldValues  = fieldSeq.map { case (name, value) => name + "=" + value.map(_.toString).getOrElse("") }
}
