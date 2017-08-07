package com.gravity.interests.jobs.intelligence.operations

import com.gravity.hbase.schema.{PutOp, OpsResult}
import com.gravity.interests.jobs.intelligence.SchemaTypes.TaggerWorkKey
import com.gravity.interests.jobs.intelligence.operations.ServiceFailures.RowNotFound
import com.gravity.interests.jobs.intelligence.{TaggerWorkRow, TaggerWorkTable}
import com.gravity.interests.jobs.intelligence.{CampaignKey, ArticleRow, Schema, ArticleKey}
import com.gravity.utilities.components.FailureResult
import com.gravity.utilities.grvz._

import org.joda.time.DateTime
import scalaz._, Scalaz._
import scala.collection.mutable

object TaggerWorkService extends TableOperations[TaggerWorkTable, TaggerWorkKey, TaggerWorkRow] {
 import com.gravity.logging.Logging._
  val table = Schema.TaggerWork

  def putOpWork(taggerWorkKey: TaggerWorkKey,
                casTitle: Option[String]
               ): ValidationNel[FailureResult, PutOp[TaggerWorkTable, TaggerWorkKey]] = {
    val q = (
      Schema.TaggerWork.put(taggerWorkKey, writeToWAL = true)
        .value(_.reqTimeStamp, new DateTime())
        .value(_.casTitle, casTitle)
      )

    q.successNel
  }

  def putOpFailInfo(taggerWorkKey: TaggerWorkKey,
                    failStatusCode: Option[Int],
                    failErrMessage: Option[String]
                   ): ValidationNel[FailureResult, PutOp[TaggerWorkTable, TaggerWorkKey]] = {
    val q = (
      Schema.TaggerWork.put(taggerWorkKey, writeToWAL = true)
        .value(_.failStatusCode, failStatusCode)
        .value(_.failErrMessage, failErrMessage)
      )

    q.successNel
  }

  def saveWork(taggerWorkKey: TaggerWorkKey,
               casTitle: Option[String]
              ): ValidationNel[FailureResult, OpsResult] = {
    try {
      for {
        putOp <- putOpWork(taggerWorkKey, casTitle)

        opsResult <- put(putOp)
      } yield opsResult
    } catch {
      case ex: Exception => FailureResult("Exception while saving Relegence TaggerWork.", ex).failureNel
    }
  }

  def saveFailInfo(taggerWorkKey: TaggerWorkKey,
                   failStatusCode: Option[Int],
                   failErrMessage: Option[String]
                  ): ValidationNel[FailureResult, OpsResult] = {
    try {
      for {
        putOp <- putOpFailInfo(taggerWorkKey, failStatusCode, failErrMessage)

        opsResult <- put(putOp)
      } yield opsResult
    } catch {
      case ex: Exception => FailureResult("Exception while saving Relegence TaggerWork FailInfo.", ex).failureNel
    }
  }

  def readRow(taggerWorkKey: TaggerWorkKey, skipCache: Boolean): ValidationNel[FailureResult, TaggerWorkRow] = {
    try {
      fetch(taggerWorkKey, skipCache)(_.withAllColumns)
    } catch {
      case ex: Exception => FailureResult("Exception while reading Relegence TaggerWork.", ex).failureNel
    }
  }

  def readFirstN(maxRows: Int): ValidationNel[FailureResult, Seq[TaggerWorkRow]] = {
    try {
      val taggerWork = new mutable.ListBuffer[TaggerWorkRow]()

      Schema.TaggerWork.query2
        .withAllColumns
        .scanUntil { row =>
          taggerWork += row

          taggerWork.size < maxRows   // Should be named "scanWhile" - terminates when this expr is false.
        }

      taggerWork.toSeq.successNel
    } catch {
      case ex: Exception =>
        FailureResult(ex).failureNel
    }
  }
}
