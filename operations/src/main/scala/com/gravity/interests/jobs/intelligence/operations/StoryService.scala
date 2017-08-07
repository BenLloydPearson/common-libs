package com.gravity.interests.jobs.intelligence.operations

import com.gravity.hbase.schema.{Query2, OpsResult, PutOp}
import com.gravity.interests.jobs.hbase.HBaseConfProvider
import com.gravity.interests.jobs.intelligence.operations.ServiceFailures.RowNotFound
import com.gravity.interests.jobs.intelligence.{ArticleKey, Schema, StoriesTable, StoryRow}
import com.gravity.utilities.Counters._
import com.gravity.utilities.components.FailureResult
import com.gravity.utilities.grvz._

import scalaz.Scalaz._
import scalaz._

object StoryService extends TableOperations[StoriesTable, Long, StoryRow] {
 import com.gravity.logging.Logging._
  val table = Schema.Stories

  // The current grvIngestion version.
  val curVersion: Int = 1

  def putOp(id: Long,
            grvIngestionVersion: Int,
            grvIngestionTime: Long,
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
           ): ValidationNel[FailureResult, PutOp[StoriesTable, Long]] = {
    val putOp = Schema.Stories.delete(id)
      .values(_.meta, Set(Schema.Stories.failStatusCode.getQualifier))
      .values(_.meta, Set(Schema.Stories.failErrMessage.getQualifier))
      .put(id, writeToWAL = true)
      .value(_.grvIngestionVersion, grvIngestionVersion)
      .value(_.grvIngestionTime, grvIngestionTime)
      .value(_.creationTime, creationTime)
      .value(_.lastUpdate, lastUpdate)
      .value(_.avgAccessTime, avgAccessTime)
      .value(_.title, title)
      .value(_.magScore, magScore)
      .value(_.facebookShares, facebookShares)
      .value(_.facebookLikes, facebookLikes)
      .value(_.facebookComments, facebookComments)
      .value(_.twitterRetweets, twitterRetweets)
      .value(_.numTotalDocs, numTotalDocs)
      .value(_.numOriginalDocs, numOriginalDocs)
      .valueMap(_.entTags, entTags)
      .valueMap(_.subTags, subTags)

    socialDistPercentage.foreach(value => putOp.value(_.socialDistPercentage, value))

    putOp.successNel
  }

  def saveFail(id: Long,
               failStatusCode: Option[Int],
               failErrMessage: Option[String]
              ): ValidationNel[FailureResult, OpsResult] = {
    try {
      for {
        putOp <- {
          val po = Schema.Stories.put(id, writeToWAL = true)
          failStatusCode.foreach(v => po.value(_.failStatusCode, v))
          failErrMessage.foreach(v => po.value(_.failErrMessage, v))
          po
        }.successNel

        opsResult <- put(putOp)
      } yield opsResult
    } catch {
      case ex: Exception => FailureResult("Exception while saving Relegence Story info.", ex).failureNel
    }
  }

  def saveRow(id: Long,
              grvIngestionVersion: Int,
              grvIngestionTime: Long,
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
             ): ValidationNel[FailureResult, OpsResult] = {
    try {
      for {
        putOp <- putOp(id, grvIngestionVersion, grvIngestionTime, creationTime, lastUpdate, avgAccessTime, title, magScore, facebookShares, facebookLikes, facebookComments, twitterRetweets, numTotalDocs, numOriginalDocs, socialDistPercentage, entTags, subTags)

        opsResult <- put(putOp)
      } yield opsResult
    } catch {
      case ex: Exception => FailureResult("Exception while saving Relegence Story info.", ex).failureNel
    }
  }

  def readRow(id: Long, skipCache: Boolean): ValidationNel[FailureResult, StoryRow] = {
    try {
      fetch(id, skipCache)(_.withFamilies(_.meta, _.entTags, _.subTags)) // ...does not include _.articles family.
    } catch {
      case ex: Exception => FailureResult("Exception while reading Relegence Story info for storyId: " + id, ex).failureNel
    }
  }
}

object StoryManagementService {
  import StoryService._

  val counterCategory = "Relegence"

  // This updates only the ArticleRow with the new story information (mgtStoryId, optArtStoryInfo)
  def updateArtRowStoryInfo(mgtStoryId: Long, mgtArtKey: ArticleKey, optArtStoryInfo: Option[ArtStoryInfo]): ValidationNel[FailureResult, Unit] = {
    countPerSecond(counterCategory, "In updateArtRowStoryInfo")

    // Update the ArticleRow with the new StoryId and optArtStoryInfo:
    val articleUpdateOp = {
      // We always want to put the StoryId...
      def baseOp = Schema.Articles.put(mgtArtKey, writeToWAL = true).value(_.relegenceStoryId, mgtStoryId)

      // ...and then we either want to delete or to add the ArtStoryInfo.
      optArtStoryInfo match {
        case None =>
          baseOp.delete(mgtArtKey).values(_.meta, Set(Schema.Articles.relegenceStoryInfo.getQualifier))

        case Some(artStoryInfo) =>
          baseOp.put(mgtArtKey, writeToWAL = true).value(_.relegenceStoryInfo, artStoryInfo)
      }
    }

    for {
      // Update the ArticleRow.
      _ <- try {
        articleUpdateOp.execute()(HBaseConfProvider.getConf.defaultConf).successNel
      } catch {
        case ex: Exception => FailureResult("Failed to update Article with StoryId/StoryInfo!", ex).failureNel
      }
    } yield {
      ()
    }
  }

  // This updates both the StoryRow and (potentially) all of the ArticleRows specified by StoryRow.articles to be in sync.
  def updateAllStoryArticleInfo(mgtStoryId: Long): ValidationNel[FailureResult, Unit] = {
    countPerSecond(counterCategory, "In updateAllStoryArticleInfo")

    for {
      // optStoryRow will only be a failure if there was an HBase failure; it'll be Some().success for StoryRow exists, and None.success for none such.
      optStoryRow <- {
        // We want the StoryRow, if it exists.
        val storyRowV = try {
          fetch(mgtStoryId, skipCache = false)(_.withAllColumns)
        } catch {
          case ex: Exception => FailureResult("Exception while reading Relegence Story info for storyId: " + id, ex).failureNel
        }

        storyRowV match {
          case Failure(NonEmptyList(RowNotFound(_, _))) =>
            None.successNel

          case other =>
            other.map(_.some)
        }
      }

//      _ = println(optStoryRow.map(st => st.prettyFormat()).toString)

      fullResult <- optStoryRow match {
        case None =>
          ().successNel

        case Some(storyRow) =>
          // Create the ArtStoryInfo object and compute its hash.
          val optArtStoryInfo = optStoryRow.flatMap(_.tryToArtStoryInfo)
          val storyInfoHash64 = ArtStoryInfo.murmurHash64(optArtStoryInfo)

          // Only operate on the articles whose hash doesn't match the desired hash.
          val results = for (mgtArtKey <- storyRow.articles.filter(_._2 != storyInfoHash64).map(_._1)) yield {
            for {
              // We're going to need the ArticleRow.
              mgtArtRow <- ArticleService.fetchOrEmptyRow(mgtArtKey)(_.withColumns(_.url, _.relegenceStoryId))

//              _ = mgtArtRow.prettyPrint()

              result <- if (!mgtArtRow.relegenceStoryId.map(_ == mgtStoryId).getOrElse(false)) {
                // If the ArticleRow does not embrace the Story Association, then remove the association from the StoryRow.
                StoryService.delete(Schema.Stories.delete(mgtStoryId).values(_.articles, Set(mgtArtKey))).map(_ => ())
              } else {
                // Update the Article's Story Id and Info
                updateArtRowStoryInfo(mgtStoryId, mgtArtKey, optArtStoryInfo)
              }
            } yield {
              ()
            }
          }

          results.extrude
      }
    } yield {
      ()
    }
  }

  // This associates mgtStoryId with the ArticleRow mgtArtKey, potentially updating both StoryRow and ArticleRow,
  // and ensures that there is a StoryWork task pending to potentially update both the StoryRow and (consequently) its ArticleRows.
  //
  // Improvement: We could make this more efficient by making a decision about whether or not the StoryWork needs to be created:
  // if we already have a good StoryInfo record (not failures-only) and if the Story is over 50 hours old,
  // then no reason to update it.
  def connectStoryToArticleAndAddStoryWork(mgtStoryId: Long, mgtArtKey: ArticleKey, forceArticleUpdate: Boolean = false): ValidationNel[FailureResult, Unit] = {
    countPerSecond(counterCategory, "In connectStoryToArticleAndAddStoryWork")

    for {
      // Ensure that the story is connected to the article.
      _ <- connectStoryToArticle(mgtStoryId, mgtArtKey, forceArticleUpdate = forceArticleUpdate)

      // Ensure that there is extant StoryWork for the article that will ensure that the Story is fetched and the Article is updated.
      _ <- StoryWorkService.updateStoryWork(mgtStoryId, addIfInactive = false, deleteIfInactive = false)
    } yield {
      ()
    }
  }

  // This associates mgtStoryId with the ArticleRow mgtArtKey, potentially updating both StoryRow and ArticleRow.
  def connectStoryToArticle(mgtStoryId: Long, mgtArtKey: ArticleKey, forceArticleUpdate: Boolean = false): ValidationNel[FailureResult, Unit] = {
    countPerSecond(counterCategory, "In connectStoryToArticle")

    for {
      // optStoryRow will only be a failure if there was an HBase failure; it'll be Some().success for StoryRow exists, and None.success for none such.
      optStoryRow <- {
        // We want the StoryRow, if it exists.
        val storyRowV = try {
          fetch(mgtStoryId, skipCache = false)(_.withFamilies(_.meta, _.entTags, _.subTags).withColumnsInFamily(_.articles, mgtArtKey))
        } catch {
          case ex: Exception => FailureResult("Exception while reading Relegence Story info for storyId: " + id, ex).failureNel
        }

        storyRowV match {
          case Failure(NonEmptyList(RowNotFound(_, _))) =>
            None.successNel

          case other =>
            other.map(_.some)
        }
      }

//      _ = println(optStoryRow.map(st => st.prettyFormat()).toString)

      // Create the ArtStoryInfo object and compute its hash.
      optArtStoryInfo = optStoryRow.flatMap(_.tryToArtStoryInfo)
      storyInfoHash64 = ArtStoryInfo.murmurHash64(optArtStoryInfo)

      // See if any existing entry in the _.articles family claims that the ArticleRow is up-to-date.
      optExistingAssociationStoryInfoHash = optStoryRow.flatMap(_.articles.get(mgtArtKey))
      associationAlreadyExists            = optExistingAssociationStoryInfoHash.map(_ == storyInfoHash64).getOrElse(false)

//      _ = println(s"optArtStoryInfo=$optArtStoryInfo")
//      _ = println(s"associationAlreadyExists=$associationAlreadyExists")

      _ <- if (associationAlreadyExists && !forceArticleUpdate) {
        ().successNel
      } else {
        for {
          // We're going to need the ArticleRow, which must exist for us to add the association.
          mgtArtRow <- ArticleService.fetch(mgtArtKey)(_.withColumns(_.url, _.relegenceStoryId))

//          _ = mgtArtRow.prettyPrint()

          // If the existing StoryId in ArticleRow is different than the one we're adding, then remove that association from the old StoryRow.articles family.
          _ <- mgtArtRow.relegenceStoryId match {
            case Some(oldStoryId) if oldStoryId != mgtStoryId =>
              StoryService.delete(Schema.Stories.delete(oldStoryId).values(_.articles, Set(mgtArtKey))).map(_ => None)

            case _ =>
              ().successNel
          }
        } yield {
          updateArtRowStoryInfo(mgtStoryId, mgtArtKey, optArtStoryInfo)
        }
      }

      _ <- if (associationAlreadyExists) {
        ().successNel
      } else {
        // Update the StoryRow with the ArticleKey -> StoryInfoHash association.
        StoryService.put(
          Schema.Stories.put(mgtStoryId, writeToWAL = true).valueMap(_.articles, Map(mgtArtKey -> storyInfoHash64))
        )
      }
    } yield {
      ()
    }
  }
}
