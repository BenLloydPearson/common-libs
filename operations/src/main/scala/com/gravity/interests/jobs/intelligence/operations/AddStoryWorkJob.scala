package com.gravity.interests.jobs.intelligence.operations

import com.gravity.interests.jobs.intelligence._
import com.gravity.interests.jobs.intelligence.helpers.grvcascading

import scalaz.{Failure, Success}

/**
  * Writes all known StoryIds in ArticlesTable to StoryWork.
  */
class AddStoryWorkJob extends Serializable {
  import grvcascading._

  val counterGroup = "Custom - AddStoryWork"

  val source       = grvcascading.fromTable(Schema.Articles)(_.withColumn(_.relegenceStoryId))
  val destination  = grvcascading.toTables

  val assembly = grvcascading.pipe("Map Articles to ImageBackups").each((flowProcess, call) => {
    flowProcess.increment(counterGroup, "ArticleRows Read", 1)

    val article = call.getRow(Schema.Articles)

    article.relegenceStoryId.foreach { storyId =>
      StoryWorkService.updateStoryWork(storyId, addIfInactive = false, deleteIfInactive = true)
      flowProcess.increment(counterGroup, "StoryWork Written", 1)
    }
  })("t","p")   // The names "t" and "p" are magic -- Don't ask why, and don't change them.

  grvcascading.flow.connect("AddStoryWork Main Flow", source, destination, assembly).complete()
}

