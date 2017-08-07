package com.gravity.interests.jobs.intelligence.operations

import com.gravity.interests.jobs.intelligence._
import com.gravity.interests.jobs.intelligence.helpers.grvcascading

import scalaz.{Failure, Success}

/**
 * Takes a snapshot of existing ArticleRow.image and ArticleRow.campaignSettings(*).image.
 */
class ImageBackupJob extends Serializable {
  import grvcascading._

  val counterGroup = "Custom - Image Backup Job"

  val source       = grvcascading.fromTable(Schema.Articles)(_.withColumn(_.image).withFamilies(_.campaignSettings))
  val destination  = grvcascading.toTables

  val assembly = grvcascading.pipe("Map Articles to ImageBackups").each((flowProcess, call) => {
    flowProcess.increment(counterGroup, "ArticleRows Read", 1)

    val article = call.getRow(Schema.Articles)

    ImageBackupService.putOpFromArticleRow(article) match {
      case Success(putOp) =>
        flowProcess.increment(counterGroup, "Writing ImageBackupRows", 1)
        call.writeOp(putOp)

      case Failure(boo) =>
        flowProcess.increment(counterGroup, "Failures - PutOp Generation", 1)
    }
  })("t","p")   // The names "t" and "p" are magic -- Don't ask why, and don't change them.

  grvcascading.flow.connect("ImageBackupJob Main Flow", source, destination, assembly).complete()
}

