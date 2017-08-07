package com.gravity.interests.jobs.intelligence

import com.gravity.hbase.schema._
import com.gravity.interests.jobs.intelligence.SchemaContext._
import com.gravity.interests.jobs.intelligence.SchemaTypes._
import com.gravity.interests.jobs.intelligence.hbase.ConnectionPoolingTableManager
import com.gravity.interests.jobs.intelligence.operations.HbRgTagRef
import org.joda.time.DateTime

class TaggerWorkTable extends HbaseTable[TaggerWorkTable, TaggerWorkKey, TaggerWorkRow](
  tableName = "tagger-work", rowKeyClass=classOf[TaggerWorkKey], logSchemaInconsistencies=false, tableConfig=defaultConf)
  with ConnectionPoolingTableManager
{
  override def rowBuilder(result: DeserializedResult) = new TaggerWorkRow(result, this)

  val meta: Fam[String, Any] = family[String, Any]("meta", compressed = true)

  val reqTimeStamp     = column(meta, "rqt", classOf[DateTime])       // The DateTime that the TaggerWorkRow was created.
  val casTitle         = column(meta, "cti", classOf[Option[String]]) // The CampaignArticleSettings title override for this request.

  val failStatusCode   = column(meta, "fsc", classOf[Option[Int]])    // Any HTTP Status Code associated with previous failure to execute.
  val failErrMessage   = column(meta, "fsm", classOf[Option[String]]) // Any message associated with previous failure to execute.
}

class TaggerWorkRow (result: DeserializedResult, table: TaggerWorkTable) extends HRow[TaggerWorkTable, TaggerWorkKey](result, table)
{
  def pubDate: DateTime      = rowid.pubDate
  def articleKey: ArticleKey = rowid.articleKey

  lazy val reqTimeStamp: Option[DateTime] = column(_.reqTimeStamp)                // The DateTime that the TaggerWorkRow was created.
  lazy val casTitle: Option[String] = column(_.casTitle).getOrElse(None)          // The CampaignArticleSettings title override for this request.

  lazy val failStatusCode: Option[Int] = column(_.failStatusCode).getOrElse(None) // e.g. None, Some(503)
  lazy val failErrMessage: Option[String] = column(_.failErrMessage).getOrElse(None)              // Any message associated with the failure.

  override def toString(): String = {
    s"($pubDate, $articleKey): reqTimeStamp=$reqTimeStamp, casTitle=$casTitle, failStatusCode=$failStatusCode, failErrMessage=$failErrMessage"
  }
}
