package com.gravity.interests.jobs.intelligence.operations

import com.gravity.interests.jobs.intelligence.Schema
import com.gravity.interests.jobs.intelligence.helpers.grvcascading

import scalaz.{Index => _}

class CheckRowReadabilityJob extends Serializable {
  import grvcascading._

  val counterGroup = "Custom - CheckRowReadabilityJob"

  val wantGuids    = Set("db40c164f67fa85c902ee561a35d6c5b", "8bdbc018a1ab8e228301624e599952e6", "cd48baf422d82c721c1f5673c69d7895")
  val source       = grvcascading.fromTable(Schema.Articles)(_.withFamilies(_.meta).filter(_.and(_.columnValueMustBeIn(_.siteGuid, wantGuids))))
  val destination  = grvcascading.toTables

  var failCount = 0

  val assembly = grvcascading.pipe("CheckRowReadabilityJob Assembly").each((flowProcess, call) => {
    flowProcess.increment(counterGroup, "ArticleRows Read", 1)

    val row = call.getRow(Schema.Articles)

    try {
      val isOk: Boolean = row.doRecommendations

      flowProcess.increment(counterGroup, s"Readable Ok (dr=${row.doRecommendations}) - ${row.siteGuid}", 1)
    } catch {
      case th: Throwable =>
        failCount += 1

        if (failCount <= 10)
          flowProcess.increment(counterGroup, s"Readable Failed - ${row.articleKey}", 1)
    }
  })("t","p")   // The names "t" and "p" are magic -- Don't ask why, and don't change them.

  grvcascading.flow.connect("CheckRowReadabilityJob Main Flow", source, destination, assembly).complete()
}

