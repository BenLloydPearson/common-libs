package com.gravity.interests.jobs.intelligence

import com.gravity.hbase.schema._
import com.gravity.interests.jobs.intelligence.SchemaContext._
import com.gravity.interests.jobs.intelligence.SchemaTypes._
import com.gravity.interests.jobs.intelligence.hbase.ConnectionPoolingTableManager
import com.gravity.utilities.grvstrings._

import scalaz._

/**
 * For each Article, contains a backup of the various article images that might be changed by the ImageCachingService
 * (for peace of mind while rolling out S3 image caching).
 */
class ImageBackupsTable extends HbaseTable[ImageBackupsTable, ArticleKey, ImageBackupRow](
  tableName = "image-backups", rowKeyClass=classOf[ArticleKey], logSchemaInconsistencies=false, tableConfig=defaultConf)
    with ConnectionPoolingTableManager
{
  override def rowBuilder(result: DeserializedResult) = new ImageBackupRow(result, this)

  val meta      = family[String, Any]("meta", compressed=true, versions=15)
  val artImage  = column(meta, "ar", classOf[String])
  
  val casImages = family[CampaignKey, Option[String]]("ca", compressed=true, versions=15)
}

class ImageBackupRow(result: DeserializedResult, table: ImageBackupsTable) extends HRow[ImageBackupsTable, ArticleKey](result, table)
  {
 import com.gravity.logging.Logging._
//  override def toString = ???

  lazy val articleKey = rowid

  lazy val artImage = column(_.artImage).getOrElse(emptyString)

  lazy val casImages: collection.Map[CampaignKey, Option[String]] = family(_.casImages)
}
