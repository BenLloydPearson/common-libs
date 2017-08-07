package com.gravity.test

import com.gravity.interests.jobs.intelligence._
import com.gravity.interests.jobs.intelligence.operations.{HbRgTagRef, StoryService}
import com.gravity.utilities.BaseScalaTest

import scalaz._, Scalaz._

class StoryServiceTest extends BaseScalaTest with operationsTesting with SerializationTesting {
  //
  // Test the database I/O round-trip of StoriesTable via StoriesService.
  //

  case class StoryRowVals(id: Long,
                          grvIngestionVersion: Int,
                          grvIngestionTime: Option[Long],
                          creationTime: Long,
                          lastUpdate: Long,
                          avgAccessTime: Long,
                          title: String,
                          magScore: Double,
                          facebookShares: Int,
                          facebookLikes: Int,
                          facebookComments: Int,
                          twitterRetweets: Int,
                          numTotalDocs: Int,
                          numOriginalDocs: Int,
                          socialDistPercentage: Option[Double],
                          entTags: Map[Long, HbRgTagRef],
                          subTags: collection.Map[Long, HbRgTagRef]
                         )

  object StoryRowVals {
    def nextTagRefs = {
      (for (i <- 0 until srnd.nextInt(5)) yield nextLong -> HbRgTagRef(nextString, nextDouble)).toMap
    }

    def fromStoryRow(row: StoryRow) =
      StoryRowVals(id = row.id,
        grvIngestionVersion = row.grvIngestionVersion,
        grvIngestionTime = row.grvIngestionTime,
        creationTime = row.creationTime,
        lastUpdate = row.lastUpdate,
        avgAccessTime = row.avgAccessTime,
        title = row.title,
        magScore = row.magScore,
        facebookShares = row.facebookShares,
        facebookLikes = row.facebookLikes,
        facebookComments = row.facebookComments,
        twitterRetweets = row.twitterRetweets,
        numTotalDocs = row.numTotalDocs,
        numOriginalDocs = row.numOriginalDocs,
        socialDistPercentage = row.column(_.socialDistPercentage),
        entTags = row.entTags.toMap,
        subTags = row.subTags.toMap
      )

    val seed = System.currentTimeMillis
    val srnd = new scala.util.Random(seed)

    def nextBoolean = srnd.nextInt(2) == 0

    def nextInt = srnd.nextInt

    def nextLong = srnd.nextLong

    def nextDouble = srnd.nextDouble

    def nextString = srnd.nextInt(2) match {
      case 0 => ""
      case 1 => srnd.nextString(100)
    }

    def nextOptString = srnd.nextInt(2) match {
      case 0 => None
      case 1 => nextString.some
    }

    def nextStoryRowVals: StoryRowVals = {
      StoryRowVals(
        id = nextLong,
        grvIngestionVersion = nextInt,
        grvIngestionTime = nextLong.some,
        creationTime = nextLong,
        lastUpdate = nextLong,
        avgAccessTime = nextLong,
        title = nextString,
        magScore = nextDouble,
        facebookShares = nextInt,
        facebookLikes = nextInt,
        facebookComments = nextInt,
        twitterRetweets = nextInt,
        numTotalDocs = nextInt,
        numOriginalDocs = nextInt,
        socialDistPercentage = if (nextBoolean) None else nextDouble.some,
        nextTagRefs,
        nextTagRefs
      )
    }
  }

  test("Test StoryService.saveRow vs. StoryService.readRow") {
    // Generate a bunch of fake StoryRow values.
    val origStoryVals = (for {
      count <- 1 to 100
    } yield StoryRowVals.nextStoryRowVals).toSet

    println(s"Attempting to round-trip ${origStoryVals.size} StoryRowVals...")

    // Save the fake StoryRow values.
    origStoryVals.foreach { sv =>
      val result = StoryService.saveRow(
        id = sv.id,
        sv.grvIngestionVersion,
        sv.grvIngestionTime.get,
        creationTime = sv.creationTime,
        lastUpdate = sv.lastUpdate,
        avgAccessTime = sv.avgAccessTime,
        title = sv.title,
        magScore = sv.magScore,
        facebookShares = sv.facebookShares,
        facebookComments = sv.facebookComments,
        facebookLikes = sv.facebookLikes,
        twitterRetweets = sv.twitterRetweets,
        numTotalDocs = sv.numTotalDocs,
        numOriginalDocs = sv.numOriginalDocs,
        socialDistPercentage = sv.socialDistPercentage,
        entTags = sv.entTags,
        subTags = sv.subTags
      )

      assertResult(true, "Expected successful write to HBase") {
        result.isSuccess
      }
    }

    // This is what we expect to read back from the database.
    val origOptSet : Set[Option[StoryRowVals]] = origStoryVals.map(_.some)

    // See if we get the desired results when reading back using the origImgUrl as the read key.
    assertResult(true, "StoryService saveRow/readRow round-trip should return same values as written.") {
      val readOptSet = origStoryVals map { origImgValue =>
        StoryService.readRow(origImgValue.id, true).toOption map StoryRowVals.fromStoryRow
      }

      readOptSet == origOptSet
    }
  }
}


