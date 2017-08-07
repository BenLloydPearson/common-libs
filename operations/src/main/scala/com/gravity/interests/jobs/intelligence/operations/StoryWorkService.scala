package com.gravity.interests.jobs.intelligence.operations

import com.gravity.hbase.schema.{OpsResult, PutOp}
import com.gravity.interests.jobs.intelligence.operations.ServiceFailures.RowNotFound
import com.gravity.interests.jobs.intelligence._
import com.gravity.interests.jobs.intelligence.operations.StoryService.put
import com.gravity.utilities.components.FailureResult
import com.gravity.utilities.grvz._

import scalaz._
import Scalaz._

object StoryWorkService extends TableOperations[StoryWorkTable, Long, StoryWorkRow] {
 import com.gravity.logging.Logging._
  val table = Schema.StoryWork

  def putOp(id: Long,
            isNewStory: Boolean,
            creationTime: Option[Long],
            lastUpdate: Option[Long],
            grvIngestionTime: Option[Long],
            failStatusCode: Option[Int],
            failErrMessage: Option[String]
  ): ValidationNel[FailureResult, PutOp[StoryWorkTable, Long]] = {
    val q = Schema.StoryWork.delete(id)
      .values(_.meta, Set(Schema.StoryWork.failStatusCode.getQualifier))
      .values(_.meta, Set(Schema.StoryWork.failErrMessage.getQualifier))
      .put(id, writeToWAL = true).value(_.isNewStory, isNewStory)

    creationTime.foreach(v => q.value(_.creationTime, v))
    lastUpdate.foreach(v => q.value(_.lastUpdate, v))
    grvIngestionTime.foreach(v => q.value(_.grvIngestionTime, v))
    failStatusCode.foreach(v => q.value(_.failStatusCode, v))
    failErrMessage.foreach(v => q.value(_.failErrMessage, v))

    q.successNel
  }

//  def saveFail(id: Long,
//               failStatusCode: Option[Int],
//               failErrMessage: Option[String]
//              ): ValidationNel[FailureResult, OpsResult] = {
//    try {
//      for {
//        putOp <- {
//          val po = Schema.StoryWork.put(id, writeToWAL = true)
//          failStatusCode.foreach(v => po.value(_.failStatusCode, v))
//          failErrMessage.foreach(v => po.value(_.failErrMessage, v))
//          po
//        }.successNel
//
//        opsResult <- put(putOp)
//      } yield opsResult
//    } catch {
//      case ex: Exception => FailureResult("Exception while saving Relegence Story info.", ex).failureNel
//    }
//  }
//
  def saveRow(id: Long,
              isNewStory: Boolean,
              creationTime: Option[Long],
              lastUpdate: Option[Long],
              grvIngestionTime: Option[Long],
              failStatusCode: Option[Int],
              failErrMessage: Option[String]
  ): ValidationNel[FailureResult, OpsResult] = {
    try {
      for {
        putOp <- putOp(id, isNewStory, creationTime, lastUpdate, grvIngestionTime, failStatusCode, failErrMessage)

        opsResult <- put(putOp)
      } yield opsResult
    } catch {
      case ex: Exception => FailureResult("Exception while saving Relegence StoryWork.", ex).failureNel
    }
  }

  def readRow(id: Long, skipCache: Boolean): ValidationNel[FailureResult, StoryWorkRow] = {
    try {
      fetch(id, skipCache)(_.withAllColumns)
    } catch {
      case ex: Exception => FailureResult("Exception while reading Relegence StoryWork.", ex).failureNel
    }
  }

  def updateStoryWork(storyId: Long, addIfInactive: Boolean, deleteIfInactive: Boolean): ValidationNel[FailureResult, OpsResult] = {
    StoryService.readRow(storyId, skipCache = true) match {
      case Success(storyRow) =>
        if (!storyRow.isStoryActive && deleteIfInactive)
          delete(Schema.StoryWork.delete(storyId))
        else if (storyRow.isStoryActive || addIfInactive)
          saveRow(storyId, false, storyRow.creationTime.some, storyRow.lastUpdate.some, storyRow.grvIngestionTime, storyRow.failStatusCode, storyRow.failErrMessage)
        else
          OpsResult(0, 0, 0).successNel

      case Failure(NonEmptyList(RowNotFound(_, _))) =>
        saveRow(storyId, true, None, None, None, None, None)
    }
  }
}
