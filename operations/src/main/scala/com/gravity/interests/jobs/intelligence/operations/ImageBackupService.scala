package com.gravity.interests.jobs.intelligence.operations

import com.gravity.hbase.schema.OpsResult
import com.gravity.interests.jobs.intelligence.{ImageBackupRow, ImageBackupsTable}
import com.gravity.interests.jobs.intelligence.{CampaignKey, ArticleRow, Schema, ArticleKey}
import com.gravity.utilities.components.FailureResult
import com.gravity.utilities.grvz._

import scalaz._, Scalaz._

object ImageBackupService extends TableOperations[ImageBackupsTable, ArticleKey, ImageBackupRow] {
 import com.gravity.logging.Logging._
  val table = Schema.ImageBackups

  def putOpFromArticleRow(artRow: ArticleRow) =
    putOp(artRow.articleKey, artRow.image, artRow.campaignSettings.mapValues(_.image).toMap)

  def putOp(arKey: ArticleKey,
            artImage: String,
            casImages: Map[CampaignKey, Option[String]]) = {
    Schema.ImageBackups.put(arKey, writeToWAL = false)
      .value(_.artImage    , artImage)
      .valueMap(_.casImages, casImages)
      .successNel
  }

  def saveFromArticleRow(artRow: ArticleRow) =
    saveRow(artRow.articleKey, artRow.image, artRow.campaignSettings.mapValues(_.image).toMap)

  def saveRow(arKey: ArticleKey,
              artImage: String,
              casImages: Map[CampaignKey, Option[String]]
               ): ValidationNel[FailureResult, OpsResult] = {
    try {
      for {
        putOp <- putOp(arKey, artImage, casImages)

        opsResult <- put(putOp)
      } yield opsResult
    } catch {
      case ex: Exception => FailureResult("Exception while saving CachedImage info.", ex).failureNel
    }
  }

  def readRow(arKey: ArticleKey, skipCache: Boolean): ValidationNel[FailureResult, ImageBackupRow] = {
    try {
      fetch(arKey, skipCache)(_.withAllColumns)
    } catch {
      case ex: Exception => FailureResult("Exception while reading ImageBackupRow info.", ex).failureNel
    }
  }
}
